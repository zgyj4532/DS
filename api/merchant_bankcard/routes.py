# api/merchant_bankcard/routes.py
"""
商家银行卡管理模块
实现独立的银行卡绑定、解绑、管理功能
"""
from fastapi import APIRouter, Depends, HTTPException, Form
from pydantic import BaseModel, Field
from typing import Optional, List
from services.merchant_bankcard_service import MerchantBankcardService
from core.auth import get_current_user
from core.response import success_response, error_response

router = APIRouter(prefix="/merchant-bankcard", tags=["商家银行卡管理"])

class BankcardBindRequest(BaseModel):
    """银行卡绑定请求"""
    account_name: str = Field(..., description="开户名称")
    account_number: str = Field(..., description="银行卡号")
    account_bank: str = Field(..., description="开户银行")
    bank_name: str = Field(..., description="开户行全称（含支行）")
    bank_branch_id: str = Field(..., description="开户行联行号")
    bank_address_code: str = Field(..., description="开户银行地区码（6位数字）")
    account_type: str = Field(..., description="账户类型: BANK_ACCOUNT_TYPE_PERSONAL/CORPORATE")
    sms_code: Optional[str] = Field(None, description="短信验证码")

class BankcardUpdateRequest(BaseModel):
    """银行卡更新请求"""
    account_id: int = Field(..., description="账户ID")
    account_bank: Optional[str] = Field(None, description="开户银行")
    bank_name: Optional[str] = Field(None, description="开户行全称")
    sms_code: Optional[str] = Field(None, description="短信验证码")

@router.post("/bind", summary="绑定银行卡")
async def bind_bankcard(
    data: BankcardBindRequest,
    current_user: dict = Depends(get_current_user)
):
    """绑定银行卡"""
    try:
        service = MerchantBankcardService()
        result = await service.bind_bankcard(current_user["id"], data.dict())
        return success_response(data=result, message="银行卡绑定成功")
    except Exception as e:
        return error_response(message=str(e))

@router.post("/unbind", summary="解绑银行卡")
async def unbind_bankcard(
    account_id: int,
    pay_password: str = Form(..., description="支付密码"),
    current_user: dict = Depends(get_current_user)
):
    """解绑银行卡"""
    try:
        service = MerchantBankcardService()
        result = await service.unbind_bankcard(current_user["id"], account_id, pay_password)
        return success_response(data=result, message="银行卡解绑成功")
    except Exception as e:
        return error_response(message=str(e))

@router.post("/verify-sms", summary="发送短信验证码")
async def send_sms_code(
    account_number: str = Form(..., description="银行卡号"),
    current_user: dict = Depends(get_current_user)
):
    """发送短信验证码"""
    try:
        service = MerchantBankcardService()
        result = await service.send_sms_code(current_user["id"], account_number)
        return success_response(data=result, message="验证码已发送")
    except Exception as e:
        return error_response(message=str(e))

@router.get("/list", summary="获取银行卡列表")
async def list_bankcards(
    current_user: dict = Depends(get_current_user)
):
    """查询已绑定的银行卡列表"""
    try:
        service = MerchantBankcardService()
        result = service.list_bankcards(current_user["id"])
        return success_response(data=result)
    except Exception as e:
        return error_response(message=str(e))

@router.post("/set-default", summary="设置默认银行卡")
async def set_default_bankcard(
    account_id: int,
    current_user: dict = Depends(get_current_user)
):
    """设置默认结算账户"""
    try:
        service = MerchantBankcardService()
        result = service.set_default_bankcard(current_user["id"], account_id)
        return success_response(data=result, message="默认账户设置成功")
    except Exception as e:
        return error_response(message=str(e))

def register_merchant_bankcard_routes(app):
    """注册路由到主应用"""
    app.include_router(router)