"""分析告警规则业务规则：状态流转、字段校验与告警读取链路。

告警读取不再触发全量重算：同环比结果在增量批次入账时已按当时阈值版本冻结，
本服务只做「规则 × 已冻结结果」的匹配。两个启用方案在同一指标 + 周期上阈值口径
冲突时，按更严格（数值更小）的阈值重算该条告警，并在返回里标注 conflict。
"""
from __future__ import annotations

import re
from typing import Any

from app.services.compare_standard import StandardError, parse_threshold
from app.store import store

MODULE = "alarm"
REQUIRED_FIELDS = ["规则编号", "监控指标", "告警条件"]
STATUS_ORDER = ["草稿", "已启用", "已暂停", "已关闭"]
# 动作 -> 允许执行该动作的源状态；启用同时覆盖草稿首发与暂停后恢复
ACTION_RULES = {
    "启用规则": ("草稿", "已暂停"),
    "暂停规则": ("已启用",),
    "关闭规则": ("已暂停",),
}
NEGATIVE_ACTIONS = []

# 告警条件里阈值的统一抽取口径，例如「同环比增长率 >= 10%」「订单GMV 增长率>=0.05」
_OPERATOR_PATTERN = re.compile(r"(>=|>|<=|<|=|≥|≤)\s*(\d+(?:\.\d+)?%?)\s*$")
# 条件文本里常见的指标无关描述词，提取指标前缀时剥掉
_METRIC_NOISE = ("同环比增长率", "环比增长率", "同比增长率", "增长率", "增长阈值", "告警", "条件")


class AlarmService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("规则编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        # 条件里的阈值同样走统一判定标准，越界不允许保存
        parsed = self.parse_rule_condition({"告警条件": values.get("告警条件")})
        if parsed is None:
            return None, ["告警条件无法解析，需形如「同环比增长率 >= 10%」并指明监控指标"]
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"告警规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于分析告警规则可执行范围"
        allowed_sources = ACTION_RULES[action]
        if entry["status"] not in allowed_sources:
            return None, (
                f"动作「{action}」要求规则处于{'、'.join(allowed_sources)}状态，"
                f"当前为「{entry['status']}」"
            )
        target = "已启用" if action == "启用规则" else ("已暂停" if action == "暂停规则" else "已关闭")
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"告警规则已{action}"

    # ------------------------------------------------------------ 条件解析
    @staticmethod
    def parse_rule_condition(rule: dict[str, Any]) -> dict[str, Any] | None:
        """把告警条件解析成 {metric, operator, threshold_value, threshold_text}。

        以比较运算符为界把条件拆成「指标前缀 + 阈值后缀」，指标前缀剥掉
        「同环比增长率」等通用描述词；解析不出指标或阈值时返回 None，由调用方
        拒绝保存或跳过自动匹配（历史脏数据不会让读取链路报错）。
        """
        condition = str(rule.get("告警条件") or "").strip()
        match = _OPERATOR_PATTERN.search(condition)
        if match is None:
            return None
        operator = match.group(1).replace("≥", ">=").replace("≤", "<=")
        if operator not in (">=", ">", "<=", "<", "="):
            return None
        try:
            threshold_value, threshold_text = parse_threshold(match.group(2))
        except StandardError:
            return None

        prefix = condition[: match.start()].strip()
        for noise in _METRIC_NOISE:
            prefix = prefix.replace(noise, " ").strip()
        metric = prefix or str(rule.get("监控指标") or "").strip()
        if not metric:
            return None
        return {
            "metric": metric,
            "operator": operator,
            "threshold_value": threshold_value,
            "threshold_text": threshold_text,
        }

    # ------------------------------------------------------------ 告警读取链路
    def evaluate(self, *, metric: str | None = None) -> dict[str, Any]:
        """按已冻结的同环比结果评估当前告警清单，不重算任何历史结论。

        返回：
        * items：每条「启用规则 × 命中结果」的评估记录，含是否触发与冲突标注；
        * events：增量链路已落盘的告警事件（只追加，按阈值版本区分）；
        * conflicts：被更严格阈值口径重算的周期冲突明细。
        """
        from app.runtime import compare_pipeline

        rules = [row for row in store.rows(MODULE) if row.get("status") == "已启用"]
        results = compare_pipeline.results(metric=metric)
        items: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []

        for rule in rules:
            parsed = self.parse_rule_condition(rule)
            if parsed is None:
                continue
            if metric and parsed["metric"] != metric:
                continue
            for result in results:
                if result["metric"] != parsed["metric"]:
                    continue
                rate = result["growth_rate"]
                # 默认按结果写入时的阈值版本解释，历史结论不被新阈值改写
                effective_threshold = result["threshold_value"]
                effective_text = result["threshold_text"]
                conflict_plans = self._conflict_plans(results, result)
                conflict = False
                if conflict_plans:
                    # 同周期冲突：取更严格（数值最小）的阈值口径重算本条告警
                    strictest = min(conflict_plans, key=lambda item: item["threshold_value"])
                    if strictest["threshold_value"] < effective_threshold:
                        effective_threshold = strictest["threshold_value"]
                        effective_text = strictest["threshold_text"]
                    conflict = True
                    conflicts.append({
                        "metric": result["metric"],
                        "baseline_period": result["baseline_period"],
                        "compare_period": result["compare_period"],
                        "rule_code": rule["规则编号"],
                        "plan_codes": [item["plan_code"] for item in conflict_plans],
                        "applied_threshold_text": effective_text,
                    })
                triggered = rate is not None and rate >= effective_threshold
                items.append({
                    "rule_code": rule["规则编号"],
                    "level": rule.get("告警级别"),
                    "metric": result["metric"],
                    "plan_code": result["plan_code"],
                    "baseline_period": result["baseline_period"],
                    "compare_period": result["compare_period"],
                    "growth_rate_text": result["growth_rate_text"],
                    "result_threshold_text": result["threshold_text"],
                    "applied_threshold_text": effective_text,
                    "threshold_version": result["threshold_version"],
                    "triggered": triggered,
                    "conflict": conflict,
                })

        return {
            "items": items,
            "events": compare_pipeline.list_alarm_events(metric=metric),
            "conflicts": conflicts,
        }

    @staticmethod
    def _conflict_plans(
        all_results: list[dict[str, Any]], target: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """找出与目标结果同指标、同周期窗口但阈值口径不一致的其他启用方案结果。"""
        peers = [
            item
            for item in all_results
            if item["metric"] == target["metric"]
            and item["baseline_period"] == target["baseline_period"]
            and item["compare_period"] == target["compare_period"]
        ]
        thresholds = {round(item["threshold_value"], 12) for item in peers}
        if len(thresholds) <= 1:
            return []
        return peers
