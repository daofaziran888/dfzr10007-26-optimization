"""同环比增量重算引擎。

把「批次增量到达 → 水位推进 → 同环比结果 → 告警读取」的链路收口在这一个模块里，
对比方案服务（compare）和告警规则服务（alarm）都只通过本模块读写运行态数据。

核心约定（对应需求逐条落地）：

1. 完成水位按「对比指标 + 基准周期」保存（compare_watermarks），增量只处理
   水位之后、新进入方案窗口的批次，已处理过的批次号记入批次台账，重复批次不重复累计；
2. 每个 (对比指标, 基准周期) 一把互斥锁，两个批次争用同一周期时只允许一个推进，
   抢锁失败直接返回 contended，不排队、不部分累计；
3. 失败不推进水位：周期累计、水位、结果、告警事件在同一把锁内顺序写入，
   抛错后重试从断点（台账 + 水位）继续，已入账的批次不会二次累计；
4. 阈值版本化（threshold_versions）：阈值修改只生成新版本，历史结果冻结在写入时
   的阈值版本上，告警读取也按结果写入时的版本解释，历史结论不被新阈值改写；
5. 已暂停 / 草稿方案不进入新周期：批次匹配只认「已启用」方案，暂停后水位保留，
   重新启用时从断点继续；
6. 同周期冲突（两个启用方案覆盖相同指标 + 周期且阈值不一致）时，按更严格
   （数值更小）的阈值口径重算告警，并在评估结果里标注 conflict。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from app.services.compare_standard import (
    Period,
    StandardError,
    breach,
    growth_rate,
    parse_period,
)

# 运行态表名（与 store 里的静态种子表分开，重启后随进程重建）
TABLE_WATERMARKS = "compare_watermarks"
TABLE_PERIOD_VALUES = "compare_period_values"
TABLE_RESULTS = "compare_results"
TABLE_BATCH_LEDGER = "compare_batch_ledger"
TABLE_THRESHOLD_VERSIONS = "compare_threshold_versions"
TABLE_ALARM_EVENTS = "compare_alarm_events"

STATUS_ENABLED = "已启用"

# 测试 / 演示用故障注入钩子：签名 (metric, baseline_canonical, batch_id, entry) -> None，
# 抛异常即模拟该批次处理失败，用于验证「失败后从断点继续」。
_fail_hook: Callable[..., None] | None = None


def set_fail_hook(hook: Callable[..., None] | None) -> None:
    """安装故障注入钩子，测试结束后置空。"""
    global _fail_hook
    _fail_hook = hook


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


@dataclass
class _Watermark:
    metric: str
    baseline_canonical: str
    # 已完成到的批次序号（按到达顺序递增），新批次只处理序号大于它的
    last_sequence: int = 0
    # 最近一次成功处理的批次号，便于断点排查
    last_batch_id: str | None = None
    updated_at: str | None = None
    # 最近一次失败信息（不影响水位，仅用于可观测）
    last_error: str | None = None


@dataclass
class _Runtime:
    watermarks: dict[tuple[str, str], _Watermark] = field(default_factory=dict)
    # (指标, 周期规范化文本) -> 累计值
    period_values: dict[tuple[str, str], float] = field(default_factory=dict)
    # 结果键 (方案id, 基准周期, 对比周期, 阈值版本) -> 结果记录
    results: dict[tuple[int, str, str, int], dict[str, Any]] = field(default_factory=dict)
    # 批次台账：批次号 -> 批次摘要，重复提交直接命中，不重复累计
    batch_ledger: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 阈值版本：方案id -> [版本记录]，版本号从 1 开始
    threshold_versions: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    # 告警事件：只追加不改写，键为 (方案id, 基准周期, 对比周期, 阈值版本, 规则编号)
    alarm_events: dict[tuple[int, str, str, int, str], dict[str, Any]] = field(default_factory=dict)
    # 自增批次序号
    batch_sequence: int = 0
    # (指标, 基准周期) -> 互斥锁
    locks: dict[tuple[str, str], threading.Lock] = field(default_factory=dict)
    locks_guard: threading.Lock = field(default_factory=threading.Lock)
    # 失败批次的断点：批次号 -> 已入账条目序号集合，重试时跳过，保证不重复累计
    partial_applied: dict[str, set[int]] = field(default_factory=dict)


class BatchConflictError(RuntimeError):
    """同一 (指标, 基准周期) 已有批次在处理，抢锁失败。"""


class ComparePipeline:
    """增量重算引擎；store 由外部注入，测试里可以直接重建清空状态。"""

    def __init__(self, store: Any) -> None:
        self._store = store
        self._rt = _Runtime()

    # ------------------------------------------------------------------ 重置
    def reset(self) -> None:
        """清空全部运行态（测试隔离用，业务代码不调用）。"""
        self._rt = _Runtime()

    def _lock_for(self, key: tuple[str, str]) -> threading.Lock:
        with self._rt.locks_guard:
            lock = self._rt.locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._rt.locks[key] = lock
            return lock

    # ------------------------------------------------------------ 阈值版本化
    def register_threshold(self, plan: dict[str, Any], threshold_value: float, threshold_text: str) -> dict[str, Any]:
        """方案创建 / 阈值修改时登记一个阈值版本，返回新版本记录。

        阈值与上一版完全相同则不新增版本（避免无意义版本号膨胀）。
        """
        plan_id = int(plan["id"])
        versions = self._rt.threshold_versions.setdefault(plan_id, [])
        if versions and abs(versions[-1]["threshold_value"] - threshold_value) < 1e-12:
            return versions[-1]
        version = {
            "plan_id": plan_id,
            "version": len(versions) + 1,
            "threshold_value": threshold_value,
            "threshold_text": threshold_text,
            "effective_from": _now(),
            "plan_status_at_write": plan.get("status"),
        }
        versions.append(version)
        return version

    def current_threshold_version(self, plan_id: int) -> dict[str, Any] | None:
        versions = self._rt.threshold_versions.get(int(plan_id)) or []
        return versions[-1] if versions else None

    def list_threshold_versions(self, plan_id: int) -> list[dict[str, Any]]:
        return list(self._rt.threshold_versions.get(int(plan_id)) or [])

    # -------------------------------------------------------------- 方案匹配
    def _enabled_plans(self) -> list[dict[str, Any]]:
        return [row for row in self._store.rows("compare") if row.get("status") == STATUS_ENABLED]

    def _plan_window(self, plan: dict[str, Any]) -> tuple[Period, Period]:
        return parse_period(plan["基准周期"]), parse_period(plan["对比周期"])

    def _match_plan(self, plan: dict[str, Any], metric: str, period: Period) -> bool:
        """批次条目是否落在该方案窗口内：指标相同且周期是基准/对比两个端点之一。"""
        if plan.get("对比指标") != metric:
            return False
        baseline, compare = self._plan_window(plan)
        if baseline.grain != period.grain:
            return False
        return period.canonical in (baseline.canonical, compare.canonical)

    def _affected_keys(self, batch: dict[str, Any]) -> list[tuple[str, str]]:
        """一个批次涉及的 (指标, 基准周期) 锁键，去重后稳定排序。"""
        keys: set[tuple[str, str]] = set()
        for entry in batch.get("entries", []):
            metric = str(entry.get("metric") or "").strip()
            period = parse_period(entry.get("period"))
            for plan in self._enabled_plans():
                if self._match_plan(plan, metric, period):
                    baseline, _ = self._plan_window(plan)
                    keys.add((metric, baseline.canonical))
        return sorted(keys)

    # -------------------------------------------------------------- 批次入口
    def ingest_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        """接收一个增量批次，返回处理报告。

        报告字段：
        * batch_id / sequence：批次号与全局到达序号；
        * duplicate：命中台账的重复批次，直接返回上次结果，不重复累计；
        * contended：争用同一周期锁失败的条目；
        * accepted / rejected：实际入账与被拒（窗口外、格式错）的条目；
        * computed：本次新算出的同环比结果；
        * error：批次整体失败信息（水位未推进）。
        """
        batch_id = str(batch.get("batch_id") or "").strip()
        if not batch_id:
            raise StandardError("批次号 batch_id 不能为空")
        entries = batch.get("entries") or []
        if not isinstance(entries, list) or not entries:
            raise StandardError("批次 entries 必须是非空列表")

        dup = self._rt.batch_ledger.get(batch_id)
        if dup is not None:
            return {
                "batch_id": batch_id,
                "sequence": dup["sequence"],
                "duplicate": True,
                "accepted": list(dup.get("accepted", ())),
                "rejected": list(dup.get("rejected", ())),
                "computed": dup.get("computed", []),
                "contended": [],
                "error": None,
            }

        # 先做无副作用的格式校验，非法批次不占序号、不建台账
        for entry in entries:
            parse_period(entry.get("period"))
            amount = entry.get("amount")
            if not isinstance(amount, (int, float)) or isinstance(amount, bool):
                raise StandardError(f"批次 {batch_id} 存在非数值 amount，拒绝入账")

        keys = self._affected_keys({"entries": entries})
        locks = [self._lock_for(key) for key in keys]
        acquired = [lock.acquire(blocking=False) for lock in locks]
        if not all(acquired):
            for lock, ok in zip(locks, acquired):
                if ok:
                    lock.release()
            contended = [
                {"metric": metric, "baseline_canonical": baseline}
                for (metric, baseline), ok in zip(keys, acquired)
                if not ok
            ]
            raise BatchConflictError(f"批次 {batch_id} 争用周期锁失败：{contended}")

        self._rt.batch_sequence += 1
        sequence = self._rt.batch_sequence
        report: dict[str, Any] = {
            "batch_id": batch_id,
            "sequence": sequence,
            "duplicate": False,
            "accepted": [],
            "rejected": [],
            "computed": [],
            "contended": [],
            "error": None,
        }
        try:
            # 同锁内处理：累计 -> 水位 -> 结果 -> 告警，任何一步抛错整体回滚思路为
            # 「水位回滚 + 台账不记成功 + 已入账条目打断点标记」，重试时跳过断点前
            # 的条目，从断点继续且不会重复累计。
            wm_snapshot = {
                key: (
                    self._rt.watermarks[key].last_sequence,
                    self._rt.watermarks[key].last_batch_id,
                    self._rt.watermarks[key].updated_at,
                )
                if key in self._rt.watermarks
                else None
                for key in keys
            }
            applied_marks = self._rt.partial_applied.setdefault(batch_id, set())
            for index, entry in enumerate(entries):
                if index in applied_marks:
                    # 上一轮已入账的条目：断点跳过，不重复累计
                    continue
                self._ingest_entry(batch_id, sequence, entry, report)
                applied_marks.add(index)
            # 批次整体成功才入台账并清断点，重复提交时原样返回
            self._rt.partial_applied.pop(batch_id, None)
            self._rt.batch_ledger[batch_id] = {
                "sequence": sequence,
                "accepted": list(report["accepted"]),
                "rejected": list(report["rejected"]),
                "computed": list(report["computed"]),
                "processed_at": _now(),
            }
        except Exception as exc:  # noqa: BLE001 - 失败信息要回传调用方
            report["error"] = str(exc)
            # 失败回滚水位到批次前，已入账条目由 partial_applied 断点接管
            for key, saved in wm_snapshot.items():
                if saved is None:
                    self._rt.watermarks.pop(key, None)
                else:
                    wm = self._rt.watermarks[key]
                    wm.last_sequence, wm.last_batch_id, wm.updated_at = saved
                    wm.last_error = str(exc)
            # 把本次预占的序号让回，台账不记成功，断点保留在 partial_applied
            self._rt.batch_sequence -= 1
            raise
        finally:
            for lock in locks:
                lock.release()
        return report

    def _ingest_entry(self, batch_id: str, sequence: int, entry: dict[str, Any], report: dict[str, Any]) -> None:
        metric = str(entry.get("metric") or "").strip()
        period = parse_period(entry.get("period"))
        amount = float(entry["amount"])

        if _fail_hook is not None:
            _fail_hook(metric, period.canonical, batch_id, entry)

        matched: list[tuple[dict[str, Any], Period, Period]] = []
        for plan in self._enabled_plans():
            if self._match_plan(plan, metric, period):
                baseline, compare = self._plan_window(plan)
                matched.append((plan, baseline, compare))

        if not matched:
            # 窗口外 / 无启用方案：不累计、不推进，暂停方案不会被带入新周期
            report["rejected"].append({
                "metric": metric,
                "period": period.canonical,
                "reason": "out_of_window_or_no_enabled_plan",
            })
            return

        # 同一指标周期可能被多个启用方案覆盖，累计值按 (指标, 周期) 共享一份
        value_key = (metric, period.canonical)
        self._rt.period_values[value_key] = self._rt.period_values.get(value_key, 0.0) + amount
        report["accepted"].append({"metric": metric, "period": period.canonical, "amount": amount})

        # 水位按 (指标, 基准周期) 推进；同锁内保证只有一个批次能推进
        affected_watermark_keys = {
            (metric, baseline.canonical) for _, baseline, _ in matched
        }
        for key in sorted(affected_watermark_keys):
            wm = self._rt.watermarks.get(key)
            if wm is None:
                wm = _Watermark(metric=key[0], baseline_canonical=key[1])
                self._rt.watermarks[key] = wm
            wm.last_sequence = sequence
            wm.last_batch_id = batch_id
            wm.updated_at = _now()
            wm.last_error = None

        # 端点周期齐了就出结果；同一批次可能同时补齐多个方案
        for plan, baseline, compare in matched:
            result = self._compute_if_ready(plan, baseline, compare, batch_id)
            if result is not None:
                report["computed"].append(result)

    def _compute_if_ready(
        self, plan: dict[str, Any], baseline: Period, compare: Period, batch_id: str
    ) -> dict[str, Any] | None:
        metric = plan["对比指标"]
        baseline_value = self._rt.period_values.get((metric, baseline.canonical))
        compare_value = self._rt.period_values.get((metric, compare.canonical))
        if baseline_value is None or compare_value is None:
            return None  # 窗口端点未齐，等后续增量批次，符合「只处理新进入窗口」

        version = self.current_threshold_version(int(plan["id"]))
        if version is None:  # 兼容未登记版本的方案：按当前阈值文本补登
            from app.services.compare_standard import parse_threshold

            value, text = parse_threshold(plan.get("增长阈值"))
            version = self.register_threshold(plan, value, text)

        key = (int(plan["id"]), baseline.canonical, compare.canonical, version["version"])
        rate = growth_rate(baseline_value, compare_value)
        result = {
            "plan_id": int(plan["id"]),
            "plan_code": plan.get("方案编号"),
            "metric": metric,
            "baseline_period": baseline.canonical,
            "compare_period": compare.canonical,
            "baseline_value": baseline_value,
            "compare_value": compare_value,
            "growth_rate": rate,
            "growth_rate_text": "无法计算（基准值为0）" if rate is None else f"{rate * 100:.2f}%",
            "threshold_version": version["version"],
            "threshold_value": version["threshold_value"],
            "threshold_text": version["threshold_text"],
            "breached": breach(rate, version["threshold_value"]),
            "triggered_batch_id": batch_id,
            "computed_at": _now(),
        }
        # 幂等：同 (方案, 窗口, 阈值版本) 结果冻结，后续批次只追加累计不改写历史结论
        existed = self._rt.results.get(key)
        if existed is not None:
            return existed
        self._rt.results[key] = result
        # 结果落盘即推告警事件（同样幂等），告警读取链路不再全量重算
        self._emit_alarm_event(result)
        return result

    # -------------------------------------------------------------- 告警事件
    def _emit_alarm_event(self, result: dict[str, Any]) -> None:
        from app.services.alarm import AlarmService

        for rule in self._store.rows("alarm"):
            if rule.get("status") != STATUS_ENABLED:
                continue
            parsed = AlarmService.parse_rule_condition(rule)
            if parsed is None or parsed["metric"] != result["metric"]:
                continue
            event_key = (
                result["plan_id"],
                result["baseline_period"],
                result["compare_period"],
                result["threshold_version"],
                rule["规则编号"],
            )
            if event_key in self._rt.alarm_events:
                continue
            self._rt.alarm_events[event_key] = {
                "event_key": "|".join(str(part) for part in event_key),
                "rule_code": rule["规则编号"],
                "plan_code": result["plan_code"],
                "metric": result["metric"],
                "baseline_period": result["baseline_period"],
                "compare_period": result["compare_period"],
                "growth_rate_text": result["growth_rate_text"],
                "threshold_text": result["threshold_text"],
                "threshold_version": result["threshold_version"],
                "level": rule.get("告警级别"),
                "triggered": result["breached"],
                "emitted_at": _now(),
            }

    def list_alarm_events(self, *, metric: str | None = None) -> list[dict[str, Any]]:
        events = list(self._rt.alarm_events.values())
        if metric:
            events = [event for event in events if event["metric"] == metric]
        return sorted(events, key=lambda item: item["emitted_at"])

    # ------------------------------------------------------------------ 查询
    def watermark(self, metric: str, baseline_canonical: str) -> dict[str, Any] | None:
        wm = self._rt.watermarks.get((metric, baseline_canonical))
        if wm is None:
            return None
        return {
            "metric": wm.metric,
            "baseline_period": wm.baseline_canonical,
            "last_sequence": wm.last_sequence,
            "last_batch_id": wm.last_batch_id,
            "updated_at": wm.updated_at,
            "last_error": wm.last_error,
        }

    def watermarks(self) -> list[dict[str, Any]]:
        return [self.watermark(metric, baseline) for metric, baseline in sorted(self._rt.watermarks)]

    def period_value(self, metric: str, period_canonical: str) -> float | None:
        return self._rt.period_values.get((metric, period_canonical))

    def results(self, *, plan_id: int | None = None, metric: str | None = None) -> list[dict[str, Any]]:
        items = list(self._rt.results.values())
        if plan_id is not None:
            items = [item for item in items if item["plan_id"] == int(plan_id)]
        if metric is not None:
            items = [item for item in items if item["metric"] == metric]
        return sorted(items, key=lambda item: (item["plan_id"], item["baseline_period"], item["threshold_version"]))

    def batch(self, batch_id: str) -> dict[str, Any] | None:
        return self._rt.batch_ledger.get(batch_id)

    def batches(self) -> list[dict[str, Any]]:
        return [
            {"batch_id": batch_id, **summary}
            for batch_id, summary in sorted(self._rt.batch_ledger.items(), key=lambda pair: pair[1]["sequence"])
        ]

    def breakpoints(self) -> list[dict[str, Any]]:
        """失败批次的断点视图：已入账条目序号，供重试与排障核对。"""
        return [
            {"batch_id": batch_id, "applied_indices": sorted(indices)}
            for batch_id, indices in sorted(self._rt.partial_applied.items())
        ]

    def recompute_on_threshold_change(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        """阈值变更后：历史结果保持冻结，只用新版本重算当前窗口并补发事件。"""
        baseline, compare = self._plan_window(plan)
        result = self._compute_if_ready(plan, baseline, compare, batch_id="threshold-change")
        return [result] if result is not None else []
