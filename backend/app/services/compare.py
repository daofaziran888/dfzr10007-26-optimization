"""同环比分析业务规则：状态流转、字段校验、统一判定标准与增量重算入口。

保存链路（创建方案 / 改阈值）必须通过 compare_standard 的统一判定标准，
周期口径或阈值越界直接拒绝；运行态（水位、批次、结果、告警事件）全部委托给
compare_pipeline 里的增量引擎，本服务只负责方案本身的字段与状态。
"""
from __future__ import annotations

from typing import Any

from app.services.compare_pipeline import ComparePipeline
from app.services.compare_standard import StandardError, parse_threshold, validate_window
from app.store import store

MODULE = "compare"
REQUIRED_FIELDS = ["方案编号", "对比指标", "基准周期", "对比周期", "增长阈值"]
STATUS_ORDER = ["草稿", "已启用", "已暂停", "已关闭"]
# 动作 -> 允许执行该动作的源状态；启用同时覆盖草稿首发与暂停后恢复
ACTION_RULES = {
    "启用方案": ("草稿", "已暂停"),
    "暂停方案": ("已启用",),
    "关闭方案": ("已暂停",),
}
NEGATIVE_ACTIONS = []


class CompareService:
    def __init__(self, pipeline: ComparePipeline | None = None) -> None:
        # 延迟到属性里取，避免模块导入期产生循环依赖
        self._pipeline = pipeline

    @property
    def pipeline(self) -> ComparePipeline:
        if self._pipeline is None:
            from app.runtime import compare_pipeline

            self._pipeline = compare_pipeline
        return self._pipeline

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
            rows = [row for row in rows if keyword in str(row.get("方案编号", ""))]
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
        try:
            baseline, compare = validate_window(values.get("基准周期"), values.get("对比周期"))
            threshold_value, threshold_text = parse_threshold(values.get("增长阈值"))
        except StandardError as exc:
            return None, [str(exc)]

        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry["方案编号"] = str(values.get("方案编号")).strip()
        entry["对比指标"] = str(values.get("对比指标")).strip()
        entry["基准周期"] = baseline.canonical
        entry["对比周期"] = compare.canonical
        entry["增长阈值"] = threshold_text
        entry["告警配置"] = str(values.get("告警配置") or "").strip()
        entry["对比维度"] = str(values.get("对比维度") or "").strip()
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        # 阈值随方案首版登记（版本 1），历史结论永远挂在写入时的版本上
        self.pipeline.register_threshold(entry, threshold_value, threshold_text)
        return entry, []

    def update_threshold(self, entry_id: int, raw_threshold: Any) -> tuple[dict[str, Any] | None, str]:
        """增长阈值变更：只生成新版本，不回改历史结果；当前窗口按新版本补算。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"对比方案 {entry_id} 不存在或已归档"
        try:
            threshold_value, threshold_text = parse_threshold(raw_threshold)
        except StandardError as exc:
            return None, str(exc)

        version = self.pipeline.register_threshold(entry, threshold_value, threshold_text)
        entry["增长阈值"] = threshold_text
        recomputed = self.pipeline.recompute_on_threshold_change(entry)
        return entry, (
            f"增长阈值已更新为 {threshold_text}（版本 {version['version']}），"
            f"历史结果按当时阈值保留，当前窗口补算 {len(recomputed)} 条"
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"对比方案 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于同环比分析可执行范围"
        allowed_sources = ACTION_RULES[action]
        if entry["status"] not in allowed_sources:
            return None, (
                f"动作「{action}」要求方案处于{'、'.join(allowed_sources)}状态，"
                f"当前为「{entry['status']}」"
            )
        previous = entry["status"]
        target = "已启用" if action == "启用方案" else ("已暂停" if action == "暂停方案" else "已关闭")
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        # 暂停后增量批次不再匹配该方案（引擎只认已启用），水位保留；
        # 重新启用时无需重放，后续批次从断点继续，已齐端点的窗口补算一次即可。
        if action == "启用方案" and previous != STATUS_ORDER[1]:
            self.pipeline.recompute_on_threshold_change(entry)
        return entry, f"对比方案已{action}"

    # ---------------------------------------------------- 增量链路的只读视图
    def watermarks(self) -> list[dict[str, Any]]:
        return self.pipeline.watermarks()

    def results(self, entry_id: int | None = None) -> list[dict[str, Any]]:
        return self.pipeline.results(plan_id=entry_id)

    def threshold_versions(self, entry_id: int) -> list[dict[str, Any]]:
        return self.pipeline.list_threshold_versions(entry_id)
