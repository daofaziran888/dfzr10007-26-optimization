"""运行态单例：把增量引擎绑定到内存仓库上。

单独成模块是为了打破 services 之间的循环导入：compare / alarm 服务在函数内部
延迟 import 本模块，而不是在模块顶层互相引用。
"""
from __future__ import annotations

from app.services.compare_pipeline import ComparePipeline
from app.store import store

compare_pipeline = ComparePipeline(store)
