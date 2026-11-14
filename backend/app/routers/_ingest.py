"""增量批次入参适配：把接口载荷交给增量引擎，并隔离路由层与引擎的异常类型。"""
from __future__ import annotations

from typing import Any

from app.runtime import compare_pipeline


def ingest_batch_payload(values: dict[str, Any]) -> dict[str, Any]:
    """校验批次外壳并调用引擎；entries 形如 [{metric, period, amount}, ...]。"""
    batch_id = str(values.get("batch_id") or "").strip()
    entries = values.get("entries")
    if not batch_id:
        from app.services.compare_standard import StandardError

        raise StandardError("批次号 batch_id 不能为空")
    if not isinstance(entries, list) or not entries:
        from app.services.compare_standard import StandardError

        raise StandardError("entries 必须是非空列表，每项包含 metric / period / amount")
    return compare_pipeline.ingest_batch({"batch_id": batch_id, "entries": entries})
