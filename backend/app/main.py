"""数据洞察与自助分析平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """启动时给种子对比方案补齐阈值版本 1，保证增量链路一开始就有版本可挂结果。

    阈值解析复用统一判定标准；种子数据本身合法，解析失败只跳过不阻断启动。
    """
    from app.services.compare_standard import StandardError, parse_threshold

    from app.runtime import compare_pipeline

    for plan in store.rows("compare"):
        if compare_pipeline.current_threshold_version(int(plan["id"])) is not None:
            continue
        try:
            value, text = parse_threshold(plan.get("增长阈值"))
        except StandardError:
            continue
        compare_pipeline.register_threshold(plan, value, text)
    yield


app = FastAPI(title="数据洞察与自助分析平台", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for module in ROUTERS:
    app.include_router(module.router)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()
