from fastapi import FastAPI
from src.cart import router as cart_router
from src.order import router as order_router
from src.refund import router as refund_router
from src.address import router as address_router
from src.merchant import router as merchant_router

def register_all(app: FastAPI):
    app.include_router(cart_router, prefix="/cart", tags=["购物车"])
    app.include_router(order_router, prefix="/order", tags=["订单"])
    app.include_router(refund_router, prefix="/refund", tags=["退款"])
    app.include_router(address_router, prefix="/address", tags=["地址"])
    app.include_router(merchant_router, prefix="/merchant", tags=["商家后台"])