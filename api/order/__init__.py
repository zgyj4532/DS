"""
订单系统模块 - 整合所有订单相关功能
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI

from .cart import router as cart_router
from .order import router as order_router, start_order_expire_task, start_wechat_status_sync_task  # 新增导入
from .refund import router as refund_router
from .merchant import router as merchant_router
from .logistics import register_logistics_routes

def register_routes(app: "FastAPI"):
    """注册订单系统路由到主应用"""
    # 统一使用 "订单系统" 作为主 tag，所有订单相关接口都归类到订单系统
    # 注意：地址功能已统一使用用户系统的地址功能
    app.include_router(cart_router, prefix="/cart", tags=["订单系统"])
    app.include_router(order_router, prefix="/order", tags=["订单系统"])
    app.include_router(refund_router, prefix="/refund", tags=["订单系统"])
    app.include_router(merchant_router, prefix="/merchant", tags=["订单系统"])
    register_logistics_routes(app)

    # ==================== 新增：启动后台任务（确保只执行一次） ====================
    # 注意：这些函数内部使用了 daemon thread，多次调用不会重复启动
    try:
        start_order_expire_task()
        start_wechat_status_sync_task()
    except Exception as e:
        # 如果已经启动会抛出异常，忽略
        import logging
        logging.getLogger(__name__).debug(f"后台任务启动检查: {e}")