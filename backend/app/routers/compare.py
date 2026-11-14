"""同环比分析接口：维护对比方案，并暴露增量批次、完成水位与同环比结果链路。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.routers._ingest import ingest_batch_payload
from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.compare import CompareService
from app.services.compare_pipeline import BatchConflictError
from app.services.compare_standard import StandardError

router = APIRouter(prefix="/api/compare", tags=["同环比分析"])

service = CompareService()

LIST_FIELDS = ["方案编号", "对比指标", "基准周期", "对比周期", "增长阈值", "告警配置", "对比维度", "方案状态"]
STATUSES = ["草稿", "已启用", "已暂停", "已关闭"]


class ThresholdPayload(BaseModel):
    """增长阈值变更入参。"""

    增长阈值: Any = Field(description="裸小数 (0,1] 或百分位文本 (0%,100%]")


# 注意：所有静态路径（/runtime/*、/ingest、/export）必须声明在 /{entry_id} 之前，
# 否则 FastAPI 按声明顺序匹配时会把静态段当成 entry_id 解析失败。
@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按方案编号检索"),
    status: str | None = Query(default=None, description="草稿、已启用、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按方案编号与状态过滤同环比分析列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条对比方案；周期口径或增长阈值越界时按统一判定标准拒绝保存。"""
    entry, errors = service.create_entry(payload.values)
    if errors:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="对比方案已登记", entry=entry)


@router.post("/ingest")
def ingest(payload: EntryPayload) -> dict[str, Any]:
    """增量批次入口：只处理新进入窗口的条目，争用同一周期时只允许一个推进。"""
    try:
        return ingest_batch_payload(payload.values)
    except BatchConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except StandardError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runtime/watermarks")
def watermarks() -> dict[str, Any]:
    """完成水位：按对比指标 + 基准周期查看增量处理到哪个批次。"""
    return {"items": service.watermarks()}


@router.get("/runtime/results")
def runtime_results(metric: str | None = None) -> dict[str, Any]:
    """全量同环比结果只读视图，供告警读取链路与排障核对。"""
    return {"items": service.pipeline.results(metric=metric)}


@router.get("/runtime/batches")
def runtime_batches() -> dict[str, Any]:
    """批次台账：重复批次命中台账，不重复累计。"""
    return {"items": service.pipeline.batches()}


@router.get("/runtime/breakpoints")
def runtime_breakpoints() -> dict[str, Any]:
    """失败批次断点：已入账条目序号，重试时从断点继续。"""
    return {"items": service.pipeline.breakpoints()}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出同环比分析清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "compare", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条对比方案明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"对比方案 {entry_id} 不存在或已归档")
    return entry


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条对比方案执行启用方案、暂停方案、关闭方案；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.put("/{entry_id}/threshold", response_model=ActionResult)
def update_threshold(entry_id: int, payload: ThresholdPayload) -> ActionResult:
    """增长阈值变更：只生成新版本，历史同环比结果按当时阈值保留。"""
    entry, message = service.update_threshold(entry_id, payload.增长阈值)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}/results")
def entry_results(entry_id: int) -> dict[str, Any]:
    """读取单条方案的同环比结果（含每个阈值版本），结果按写入时阈值冻结。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"对比方案 {entry_id} 不存在或已归档")
    return {"plan_id": entry_id, "items": service.results(entry_id)}


@router.get("/{entry_id}/threshold-versions")
def entry_threshold_versions(entry_id: int) -> dict[str, Any]:
    """读取阈值版本链，用于核对「阈值变化只影响新版本」。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"对比方案 {entry_id} 不存在或已归档")
    return {"plan_id": entry_id, "items": service.threshold_versions(entry_id)}
