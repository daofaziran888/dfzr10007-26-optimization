"""同环比增量重算链路测试。

覆盖需求点：
1. 周期 / 阈值统一判定标准，越界不允许保存；
2. 按对比指标 + 基准周期的完成水位，只处理新进入窗口的批次；
3. 重复批次不重复累计；
4. 两个批次争用同一周期只允许一个推进，失败后从断点继续；
5. 阈值版本化，历史结果按当时阈值保留；
6. 已暂停方案不进入新周期，重新启用从断点继续；
7. 同周期冲突按更严格阈值口径重算告警；
8. 用 COMP-0002（订单GMV / 2026-09 vs 2026-10 / 10%）核对结论与告警清单。
"""
from __future__ import annotations

import threading
import unittest

from app.runtime import compare_pipeline as pipeline
from app.services.alarm import AlarmService
from app.services.compare import CompareService
from app.services.compare_pipeline import BatchConflictError, set_fail_hook
from app.services.compare_standard import (
    MAX_PERIOD_DISTANCE,
    StandardError,
    growth_rate,
    parse_period,
    parse_threshold,
    validate_window,
)
from app.store import store

# 模块加载时拍一份种子快照，每个用例前后恢复，避免用例间互相污染
_SEED_SNAPSHOT = store.snapshot()


class StandardTest(unittest.TestCase):
    """统一判定标准：周期窗口边界与阈值口径。"""

    def test_period_grains_and_canonical(self) -> None:
        self.assertEqual(parse_period("2026-09-15").grain, "day")
        self.assertEqual(parse_period("2026年09月15日").canonical, "2026-09-15")
        month = parse_period("2026/09")
        self.assertEqual((month.grain, month.canonical), ("month", "2026-09"))
        quarter = parse_period("2026Q3")
        self.assertEqual((quarter.grain, quarter.canonical), ("quarter", "2026Q3"))
        self.assertEqual(parse_period("2026年").canonical, "2026")

    def test_window_half_open_boundary(self) -> None:
        baseline, compare = validate_window("2026-09", "2026-10")
        # 月粒度窗口 = [2026-09-01, 2026-11-01) 半开区间
        self.assertEqual(baseline.start.isoformat(), "2026-09-01")
        self.assertEqual(compare.end.isoformat(), "2026-11-01")
        self.assertEqual(baseline.distance_to(compare), 1)

    def test_window_rejects_bad_periods(self) -> None:
        with self.assertRaises(StandardError):
            validate_window("2026-09", "2026Q3")  # 粒度不一致
        with self.assertRaises(StandardError):
            validate_window("2026-10", "2026-09")  # 对比周期早于基准
        with self.assertRaises(StandardError):
            validate_window("2026-01", "2027-02")  # 距离 13 超范围
        with self.assertRaises(StandardError):
            parse_period("2026-13")  # 非法月份
        with self.assertRaises(StandardError):
            parse_period("not-a-period")

    def test_period_distance_limits(self) -> None:
        validate_window("2026-01", "2027-01")  # 距离 12，恰好允许
        with self.assertRaises(StandardError):
            validate_window("2026-01", "2027-02")
        self.assertEqual(MAX_PERIOD_DISTANCE, 12)

    def test_threshold_unified_standard(self) -> None:
        self.assertEqual(parse_threshold("10%"), (0.10, "10.00%"))
        self.assertEqual(parse_threshold(0.1), (0.10, "10.00%"))
        self.assertEqual(parse_threshold("0.05"), (0.05, "5.00%"))
        for bad in ("0%", "101%", "-3%", 0, 1.5, 10, "", None, True):
            with self.assertRaises(StandardError):
                parse_threshold(bad)

    def test_growth_and_breach(self) -> None:
        self.assertAlmostEqual(growth_rate(100.0, 112.0), 0.12)
        self.assertIsNone(growth_rate(0.0, 10.0))  # 基准为零无法计算，不触发
        rate = growth_rate(200.0, 220.0)
        self.assertTrue(rate is not None and rate >= 0.10)


class PipelineTest(unittest.TestCase):
    """增量引擎：水位、幂等、争用、断点、版本、暂停隔离。"""

    def setUp(self) -> None:
        store.restore(_SEED_SNAPSHOT)
        pipeline.reset()
        # 重新登记种子方案的阈值版本（reset 会清掉运行态）
        from app.services.compare_standard import parse_threshold

        for plan in store.rows("compare"):
            value, text = parse_threshold(plan["增长阈值"])
            pipeline.register_threshold(plan, value, text)
        self.service = CompareService(pipeline)
        self.alarm_service = AlarmService()
        self.plan = store.find("compare", 2)  # COMP-0002
        self.assertEqual(self.plan["方案编号"], "COMP-0002")

    def tearDown(self) -> None:
        set_fail_hook(None)
        store.restore(_SEED_SNAPSHOT)
        pipeline.reset()

    # ------------------------------------------------ COMP-0002 主链路核对
    def test_comp0002_incremental_verification(self) -> None:
        metric = "订单GMV"
        # 第一批：只有基准周期 2026-09 进入窗口，结果尚不能产出
        r1 = pipeline.ingest_batch({
            "batch_id": "B-001",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 1000.0}],
        })
        self.assertEqual(r1["accepted"] and r1["computed"], [])
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 1000.0)
        wm = pipeline.watermark(metric, "2026-09")
        self.assertEqual((wm["last_sequence"], wm["last_batch_id"]), (1, "B-001"))

        # 第二批：对比周期 2026-10 增量到达（1120），增长率 12% >= 10%，触发
        r2 = pipeline.ingest_batch({
            "batch_id": "B-002",
            "entries": [{"metric": metric, "period": "2026-10", "amount": 1120.0}],
        })
        self.assertEqual(len(r2["computed"]), 1)
        result = r2["computed"][0]
        self.assertEqual(result["plan_code"], "COMP-0002")
        self.assertAlmostEqual(result["growth_rate"], 0.12)
        self.assertEqual(result["growth_rate_text"], "12.00%")
        self.assertEqual(result["threshold_text"], "10.00%")
        self.assertTrue(result["breached"])
        self.assertEqual(result["threshold_version"], 1)

        # 告警读取链路：命中已启用的 ALAR-0002，触发结果出现在清单里
        evaluation = self.alarm_service.evaluate(metric=metric)
        self.assertEqual(evaluation["conflicts"], [])
        hit = [item for item in evaluation["items"] if item["rule_code"] == "ALAR-0002"]
        self.assertEqual(len(hit), 1)
        self.assertTrue(hit[0]["triggered"])
        self.assertEqual(hit[0]["applied_threshold_text"], "10.00%")
        # 告警事件已随结果落盘，读取不重算
        events = pipeline.list_alarm_events(metric=metric)
        self.assertTrue(any(event["rule_code"] == "ALAR-0002" and event["triggered"] for event in events))

    def test_comp0002_below_threshold_not_triggered(self) -> None:
        metric = "订单GMV"
        pipeline.ingest_batch({
            "batch_id": "B-001",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 1000.0}],
        })
        report = pipeline.ingest_batch({
            "batch_id": "B-002",
            "entries": [{"metric": metric, "period": "2026-10", "amount": 1050.0}],  # +5%
        })
        self.assertFalse(report["computed"][0]["breached"])
        evaluation = self.alarm_service.evaluate(metric=metric)
        self.assertFalse(any(item["triggered"] for item in evaluation["items"]))

    # ------------------------------------------------ 只处理新进入窗口
    def test_out_of_window_entries_are_rejected(self) -> None:
        report = pipeline.ingest_batch({
            "batch_id": "B-WIN",
            "entries": [
                {"metric": "订单GMV", "period": "2026-08", "amount": 999.0},  # 窗口外
                {"metric": "订单GMV", "period": "2026-11", "amount": 999.0},  # 窗口外
                {"metric": "其他指标", "period": "2026-09", "amount": 1.0},    # 无匹配方案
            ],
        })
        self.assertEqual(report["accepted"], [])
        self.assertEqual(len(report["rejected"]), 3)
        self.assertIsNone(pipeline.period_value("订单GMV", "2026-08"))
        # 没有任何条目命中启用方案，不产生水位
        self.assertIsNone(pipeline.watermark("订单GMV", "2026-09"))

    def test_incremental_accumulation_within_period(self) -> None:
        metric = "订单GMV"
        pipeline.ingest_batch({
            "batch_id": "B-A",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 600.0}],
        })
        pipeline.ingest_batch({
            "batch_id": "B-B",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 400.0}],
        })
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 1000.0)
        self.assertEqual(pipeline.watermark(metric, "2026-09")["last_batch_id"], "B-B")

    # ------------------------------------------------ 重复批次幂等
    def test_duplicate_batch_not_accumulated_twice(self) -> None:
        metric = "订单GMV"
        payload = {
            "batch_id": "B-DUP",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 300.0}],
        }
        first = pipeline.ingest_batch(payload)
        second = pipeline.ingest_batch(payload)
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["sequence"], first["sequence"])
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 300.0)
        self.assertEqual(len(pipeline.batches()), 1)

    # ------------------------------------------------ 争用同一周期
    def test_concurrent_contention_only_one_advances(self) -> None:
        metric = "订单GMV"
        barrier = threading.Barrier(2)
        outcomes: list[str] = []

        def worker(batch_id: str, hold: bool) -> None:
            def hook(_metric: str, _period: str, _batch: str, _entry: object) -> None:
                if hold:
                    barrier.wait(timeout=5)
                    barrier.wait(timeout=5)

            set_fail_hook(hook)
            try:
                pipeline.ingest_batch({
                    "batch_id": batch_id,
                    "entries": [{"metric": metric, "period": "2026-09", "amount": 100.0}],
                })
                outcomes.append("ok")
            except BatchConflictError:
                outcomes.append("conflict")
            finally:
                set_fail_hook(None)

        t1 = threading.Thread(target=worker, args=("B-LOCK-1", True))
        t2 = threading.Thread(target=worker, args=("B-LOCK-2", False))
        t1.start()
        barrier.wait(timeout=5)  # 等 t1 拿锁并进入钩子
        t2.start()
        t2.join(timeout=5)
        barrier.wait(timeout=5)  # 放行 t1
        t1.join(timeout=5)

        self.assertEqual(sorted(outcomes), ["conflict", "ok"])
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 100.0)
        # 失败的争用方没有占用批次序号、没有进台账
        self.assertEqual(len(pipeline.batches()), 1)
        # 争用失败后重试同一批次，应当成功且从断点继续（只累计一次）
        retry = pipeline.ingest_batch({
            "batch_id": "B-LOCK-2",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 100.0}],
        })
        self.assertFalse(retry["duplicate"])
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 200.0)

    # ------------------------------------------------ 失败断点续跑
    def test_failure_does_not_advance_watermark_and_resumes(self) -> None:
        metric = "订单GMV"
        attempts = {"n": 0}

        def flaky_hook(_metric: str, _period: str, batch_id: str, _entry: object) -> None:
            if batch_id == "B-FLAKY":
                attempts["n"] += 1
                if attempts["n"] == 1:
                    raise RuntimeError("模拟下游写入失败")

        set_fail_hook(flaky_hook)
        with self.assertRaises(RuntimeError):
            pipeline.ingest_batch({
                "batch_id": "B-FLAKY",
                "entries": [{"metric": metric, "period": "2026-09", "amount": 250.0}],
            })
        set_fail_hook(None)

        # 失败：台账无记录、水位停在初始、批次序号已让回
        self.assertIsNone(pipeline.batch("B-FLAKY"))
        self.assertIsNone(pipeline.watermark(metric, "2026-09"))
        # 重试从断点继续：成功后只累计一次，不会翻倍
        report = pipeline.ingest_batch({
            "batch_id": "B-FLAKY",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 250.0}],
        })
        self.assertEqual(report["sequence"], 1)
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 250.0)
        self.assertEqual(pipeline.watermark(metric, "2026-09")["last_batch_id"], "B-FLAKY")

    def test_multi_entry_failure_resumes_without_double_counting(self) -> None:
        """多元素批次在第二条失败：第一条已入账，重试只补第二条，水位最终才推进。"""
        metric = "订单GMV"

        def fail_second_entry(_m: str, period: str, batch_id: str, _entry: object) -> None:
            if batch_id == "B-MULTI" and period == "2026-10":
                raise RuntimeError("模拟对比周期写入失败")

        set_fail_hook(fail_second_entry)
        with self.assertRaises(RuntimeError):
            pipeline.ingest_batch({
                "batch_id": "B-MULTI",
                "entries": [
                    {"metric": metric, "period": "2026-09", "amount": 1000.0},
                    {"metric": metric, "period": "2026-10", "amount": 1120.0},
                ],
            })
        set_fail_hook(None)

        # 断点：只有第 0 条入账；水位未推进；结果未产出
        self.assertEqual(pipeline.breakpoints(), [{"batch_id": "B-MULTI", "applied_indices": [0]}])
        self.assertIsNone(pipeline.watermark(metric, "2026-09"))
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 1000.0)
        self.assertIsNone(pipeline.period_value(metric, "2026-10"))
        self.assertEqual(pipeline.results(plan_id=2), [])

        # 同批次重试：第 0 条跳过不重复累计，只补第 1 条
        report = pipeline.ingest_batch({
            "batch_id": "B-MULTI",
            "entries": [
                {"metric": metric, "period": "2026-09", "amount": 1000.0},
                {"metric": metric, "period": "2026-10", "amount": 1120.0},
            ],
        })
        self.assertEqual(pipeline.period_value(metric, "2026-09"), 1000.0)
        self.assertEqual(pipeline.period_value(metric, "2026-10"), 1120.0)
        self.assertEqual(pipeline.watermark(metric, "2026-09")["last_batch_id"], "B-MULTI")
        self.assertEqual(pipeline.breakpoints(), [])
        self.assertEqual(len(report["computed"]), 1)
        self.assertTrue(report["computed"][0]["breached"])

    # ------------------------------------------------ 阈值版本化
    def test_threshold_change_keeps_history_and_versions_new_window(self) -> None:
        metric = "订单GMV"
        pipeline.ingest_batch({
            "batch_id": "B-OLD-1",
            "entries": [{"metric": metric, "period": "2026-09", "amount": 1000.0}],
        })
        pipeline.ingest_batch({
            "batch_id": "B-OLD-2",
            "entries": [{"metric": metric, "period": "2026-10", "amount": 1120.0}],  # +12%
        })
        old_results = pipeline.results(plan_id=2)
        self.assertEqual(len(old_results), 1)
        self.assertEqual(old_results[0]["threshold_version"], 1)
        self.assertTrue(old_results[0]["breached"])

        # 阈值收紧到 15%：新版本生效，版本 1 的历史结论原样保留
        entry, message = self.service.update_threshold(2, "15%")
        self.assertIn("版本 2", message)
        self.assertEqual(entry["增长阈值"], "15.00%")
        versions = self.service.threshold_versions(2)
        self.assertEqual([v["version"] for v in versions], [1, 2])

        new_results = pipeline.results(plan_id=2)
        self.assertEqual(len(new_results), 2)
        v1 = next(item for item in new_results if item["threshold_version"] == 1)
        v2 = next(item for item in new_results if item["threshold_version"] == 2)
        self.assertTrue(v1["breached"])   # 12% >= 10% 历史结论保留
        self.assertFalse(v2["breached"])  # 同一份数据按 15% 不再触发
        self.assertEqual(v1["threshold_text"], "10.00%")
        self.assertEqual(v2["threshold_text"], "15.00%")

        # 告警读取：版本 1 的历史告警事件仍在，版本 2 的新事件未触发
        events = pipeline.list_alarm_events(metric=metric)
        triggered_versions = {e["threshold_version"]: e["triggered"] for e in events}
        self.assertEqual(triggered_versions, {1: True, 2: False})

    def test_threshold_save_rejects_out_of_range(self) -> None:
        entry, message = self.service.update_threshold(2, "120%")
        self.assertIsNone(entry)
        self.assertIn("超出允许范围", message)
        entry, message = self.service.update_threshold(2, 0)
        self.assertIsNone(entry)

    # ------------------------------------------------ 暂停方案隔离
    def test_paused_plan_skipped_and_resumes_from_breakpoint(self) -> None:
        # COMP-0003 已暂停：它的指标增量不能被带入新周期
        report = pipeline.ingest_batch({
            "batch_id": "B-PAUSE",
            "entries": [{"metric": "活跃用户数", "period": "2026Q3", "amount": 800.0}],
        })
        self.assertEqual(report["accepted"], [])
        self.assertTrue(report["rejected"])
        self.assertIsNone(pipeline.period_value("活跃用户数", "2026Q3"))

        # 启用 COMP-0003 后，同样的周期数据可以正常进入窗口并从空断点累计
        entry, _ = self.service.run_action(3, "启用方案")
        self.assertEqual(entry["status"], "已启用")
        report = pipeline.ingest_batch({
            "batch_id": "B-RESUME",
            "entries": [{"metric": "活跃用户数", "period": "2026Q3", "amount": 800.0}],
        })
        self.assertEqual(pipeline.period_value("活跃用户数", "2026Q3"), 800.0)
        self.assertEqual(pipeline.watermark("活跃用户数", "2026Q2")["last_batch_id"], "B-RESUME")

    # ------------------------------------------------ 同周期冲突从严
    def test_conflicting_plans_use_stricter_threshold(self) -> None:
        # 新建一个与 COMP-0002 同指标同窗口、阈值更松（20%）的启用方案
        entry, missing = self.service.create_entry({
            "方案编号": "COMP-0004",
            "对比指标": "订单GMV",
            "基准周期": "2026-09",
            "对比周期": "2026-10",
            "增长阈值": "20%",
        })
        self.assertEqual(missing, [])
        self.service.run_action(int(entry["id"]), "启用方案")

        pipeline.ingest_batch({
            "batch_id": "B-C1",
            "entries": [{"metric": "订单GMV", "period": "2026-09", "amount": 1000.0}],
        })
        pipeline.ingest_batch({
            "batch_id": "B-C2",
            "entries": [{"metric": "订单GMV", "period": "2026-10", "amount": 1120.0}],  # +12%
        })

        evaluation = self.alarm_service.evaluate(metric="订单GMV")
        self.assertTrue(evaluation["conflicts"])
        # 12% 对 20% 本不触发，但冲突后统一按更严格的 10% 口径重算 -> 触发
        for item in evaluation["items"]:
            if item["rule_code"] == "ALAR-0002":
                self.assertTrue(item["conflict"])
                self.assertEqual(item["applied_threshold_text"], "10.00%")
                self.assertTrue(item["triggered"])

    # ------------------------------------------------ 保存链路校验
    def test_create_entry_enforces_unified_standard(self) -> None:
        ok, errors = self.service.create_entry({
            "方案编号": "COMP-X1",
            "对比指标": "指标X",
            "基准周期": "2026-10",
            "对比周期": "2026-09",  # 顺序反了
            "增长阈值": "10%",
        })
        self.assertIsNone(ok)
        self.assertTrue(any("对比周期必须晚于基准周期" in e for e in errors))

        ok, errors = self.service.create_entry({
            "方案编号": "COMP-X2",
            "对比指标": "指标X",
            "基准周期": "2026-09",
            "对比周期": "2026-10",
            "增长阈值": "200%",  # 阈值越界
        })
        self.assertIsNone(ok)
        self.assertTrue(any("超出允许范围" in e for e in errors))

        ok, errors = self.service.create_entry({
            "方案编号": "COMP-X3",
            "对比指标": "指标X",
            "基准周期": "2026-09",
            "对比周期": "2026Q4",  # 粒度不一致
            "增长阈值": "10%",
        })
        self.assertIsNone(ok)
        self.assertTrue(any("粒度不一致" in e for e in errors))

        ok, errors = self.service.create_entry({
            "方案编号": "COMP-X4",
            "对比指标": "指标X",
            "基准周期": "2026-09",
            "对比周期": "2026-10",
            "增长阈值": "10%",
        })
        self.assertIsNotNone(ok)
        self.assertEqual(errors, [])
        # 保存时周期与阈值已规范化
        self.assertEqual(ok["基准周期"], "2026-09")
        self.assertEqual(ok["增长阈值"], "10.00%")


class AlarmConditionParseTest(unittest.TestCase):
    """告警条件解析走统一阈值标准。"""

    def setUp(self) -> None:
        self.service = AlarmService()

    def test_parse_seed_condition(self) -> None:
        parsed = self.service.parse_rule_condition({"告警条件": "订单GMV 同环比增长率 >= 10%"})
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["metric"], "订单GMV")
        self.assertEqual(parsed["operator"], ">=")
        self.assertEqual(parsed["threshold_value"], 0.10)

    def test_parse_falls_back_to_monitor_metric(self) -> None:
        parsed = self.service.parse_rule_condition({
            "监控指标": "订单GMV",
            "告警条件": "同环比增长率 >= 10%",
        })
        assert parsed is not None
        self.assertEqual(parsed["metric"], "订单GMV")

    def test_parse_rejects_bad_condition(self) -> None:
        self.assertIsNone(self.service.parse_rule_condition({"告警条件": "样本数据异常"}))
        self.assertIsNone(self.service.parse_rule_condition({"告警条件": "增长率 >= 0%"}))


if __name__ == "__main__":
    unittest.main()
