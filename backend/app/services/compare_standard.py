"""同环比统一判定标准：周期口径、窗口边界与增长阈值的唯一解析入口。

对比方案保存、增量批次进入窗口、告警读取三处都只能走这里的解析函数，
保证「同环比阈值与基准周期按统一判定标准计算，超出允许范围不允许保存」。

设计口径（窗口边界与阈值优先级的落地说明）：

* 周期粒度支持 日 / 月 / 季 / 年，基准周期与对比周期必须同粒度；
* 对比周期序数必须严格大于基准周期序数，周期距离允许 1..MAX_PERIOD_DISTANCE，
  环比、同比共用这一判定标准，不按业务叫法另开口子；
* 方案窗口 = [基准周期起点, 对比周期终点) 的半开区间，批次只有落在基准或对比
  两个端点周期上才算「新进入窗口」，中间周期与窗口外周期一律不累计；
* 阈值统一折算成 [0, 1] 的小数参与计算，对外保留百分位文本；
  裸小数只允许 (0, 1]，百分数只允许 (0%, 100%]，越界即拒绝保存。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

# 周期距离的统一允许范围：1 个周期（典型环比）到 12 个周期（典型年度同比）
MIN_PERIOD_DISTANCE = 1
MAX_PERIOD_DISTANCE = 12
# 阈值（小数口径）的统一允许范围
THRESHOLD_MIN_EXCLUSIVE = 0.0
THRESHOLD_MAX_INCLUSIVE = 1.0


class StandardError(ValueError):
    """统一判定标准不通过时抛出，消息可直接回给调用方展示。"""


@dataclass(frozen=True)
class Period:
    """规范化后的周期。ordinal 用于距离比较，start/end 用于窗口边界判定。"""

    grain: str  # day / month / quarter / year
    canonical: str  # 统一后的周期文本，作为水位与结果的持久化键
    ordinal: int
    start: date
    end: date  # 半开区间右端点（不含）

    def distance_to(self, other: "Period") -> int:
        if self.grain != other.grain:
            raise StandardError("基准周期与对比周期粒度不一致，无法同环比比较")
        return other.ordinal - self.ordinal


_GRAIN_LABELS = {"day": "日", "month": "月", "quarter": "季", "year": "年"}


def _period_error(text: Any) -> StandardError:
    return StandardError(f"周期「{text}」无法识别，允许格式：2026-09-15 / 2026-09 / 2026Q3 / 2026")


def parse_period(text: Any) -> Period:
    """把外部周期文本规范化成 Period；无法识别或非法日期直接拒绝。"""
    if not isinstance(text, str):
        raise _period_error(text)
    raw = text.strip()
    if not raw:
        raise _period_error(text)

    # 日粒度：2026-09-15、2026/09/15、2026年09月15日
    day_match = re.fullmatch(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", raw) or re.fullmatch(
        r"(\d{4})年(\d{1,2})月(\d{1,2})日?", raw
    )
    if day_match:
        year, month, day = (int(part) for part in day_match.groups())
        try:
            start = date(year, month, day)
        except ValueError as exc:
            raise _period_error(text) from exc
        return Period("day", start.isoformat(), start.toordinal(), start, date.fromordinal(start.toordinal() + 1))

    # 季粒度：2026Q3、2026-Q3、2026年第3季度
    quarter_match = re.fullmatch(r"(\d{4})\s*[Qq]([1-4])", raw) or re.fullmatch(
        r"(\d{4})-Q([1-4])", raw
    ) or re.fullmatch(r"(\d{4})年第?([1-4])季度", raw)
    if quarter_match:
        year, quarter = (int(part) for part in quarter_match.groups())
        start_month = (quarter - 1) * 3 + 1
        start = date(year, start_month, 1)
        end_year, end_month = (year, start_month + 3) if quarter < 4 else (year + 1, 1)
        end = date(end_year, end_month, 1)
        return Period("quarter", f"{year}Q{quarter}", year * 4 + (quarter - 1), start, end)

    # 月粒度：2026-09、2026/09、2026年09月
    month_match = re.fullmatch(r"(\d{4})[-/](\d{1,2})", raw) or re.fullmatch(
        r"(\d{4})年(\d{1,2})月?", raw
    )
    if month_match:
        year, month = (int(part) for part in month_match.groups())
        if not 1 <= month <= 12:
            raise _period_error(text)
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month + 1, 1)
        return Period("month", f"{year:04d}-{month:02d}", year * 12 + (month - 1), start, end)

    # 年粒度：2026、2026年
    year_match = re.fullmatch(r"(\d{4})", raw) or re.fullmatch(r"(\d{4})年", raw)
    if year_match:
        year = int(year_match.group(1))
        return Period("year", str(year), year, date(year, 1, 1), date(year + 1, 1, 1))

    raise _period_error(text)


def validate_window(baseline_text: Any, compare_text: Any) -> tuple[Period, Period]:
    """方案保存前的窗口校验：同粒度、对比周期在基准之后、距离落在统一允许范围。"""
    baseline = parse_period(baseline_text)
    compare = parse_period(compare_text)
    if baseline.grain != compare.grain:
        raise StandardError(
            f"基准周期与对比周期粒度不一致（{_GRAIN_LABELS[baseline.grain]} vs "
            f"{_GRAIN_LABELS[compare.grain]}），不允许保存"
        )
    distance = baseline.distance_to(compare)
    if distance <= 0:
        raise StandardError("对比周期必须晚于基准周期，不允许保存")
    if not MIN_PERIOD_DISTANCE <= distance <= MAX_PERIOD_DISTANCE:
        raise StandardError(
            f"周期距离 {distance} 超出允许范围 "
            f"[{MIN_PERIOD_DISTANCE}, {MAX_PERIOD_DISTANCE}]，不允许保存"
        )
    return baseline, compare


def parse_threshold(raw: Any) -> tuple[float, str]:
    """把增长阈值统一折算为 (小数, 百分位文本)。

    * 数字按裸小数处理，只接受 (0, 1]，避免把 10 误当 10%；
    * 字符串以 % 结尾按百分数处理，只接受 (0%, 100%]；
    * 不带 % 的字符串按裸小数处理，规则同上。
    """
    if isinstance(raw, bool):  # bool 是 int 的子类，显式挡掉
        raise StandardError("增长阈值必须是数值或百分位文本，不允许保存")
    if isinstance(raw, (int, float)):
        value = float(raw)
        source = str(raw)
    elif isinstance(raw, str) and raw.strip():
        source = raw.strip()
        try:
            if source.endswith("%"):
                percent = float(source[:-1].strip())
                if not 0.0 < percent <= 100.0:
                    raise StandardError(
                        f"增长阈值 {source} 超出允许范围 (0%, 100%]，不允许保存"
                    )
                return percent / 100.0, f"{percent:.2f}%"
            value = float(source)
        except StandardError:
            # StandardError 继承 ValueError，必须先于下面的 ValueError 放行
            raise
        except ValueError as exc:
            raise StandardError(f"增长阈值「{raw}」无法解析为数值，不允许保存") from exc
    else:
        raise StandardError("增长阈值为空或类型无法识别，不允许保存")

    if not THRESHOLD_MIN_EXCLUSIVE < value <= THRESHOLD_MAX_INCLUSIVE:
        raise StandardError(
            f"增长阈值 {source} 超出允许范围 (0, 1]（百分位口径 (0%, 100%]），不允许保存"
        )
    return value, f"{value * 100:.2f}%"


def format_growth(rate: float | None) -> str:
    """增长率统一展示口径；基准为零无法计算时返回 None 由调用方决定文案。"""
    if rate is None:
        return "—"
    return f"{rate * 100:.2f}%"


def growth_rate(baseline_value: float, compare_value: float) -> float | None:
    """同环比增长率 = (对比值 - 基准值) / |基准值|；基准为零时无法计算。"""
    if baseline_value == 0:
        return None
    return (compare_value - baseline_value) / abs(baseline_value)


def breach(rate: float | None, threshold: float) -> bool:
    """触发口径：增长率达到或超过阈值即触发；无法计算（基准为零）不触发。"""
    if rate is None:
        return False
    return rate >= threshold
