# models/schemas/finance.py - 财务系统 Pydantic 模型
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any


class ResponseModel(BaseModel):
    """通用响应模型"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class UserCreateRequest(BaseModel):
    """创建用户请求模型"""
    mobile: str = Field(..., min_length=11, max_length=11, pattern=r"^1[3-9]\d{9}$")
    name: str = Field(..., min_length=2, max_length=50)
    referrer_id: Optional[int] = None

    @field_validator('referrer_id')
    @classmethod
    def validate_referrer_id(cls, v):
        if v is not None and v < 0:
            raise ValueError("推荐人ID必须为非负整数")
        return v


class ProductCreateRequest(BaseModel):
    """创建商品请求模型（财务系统）"""
    name: str = Field(..., min_length=2, max_length=255)
    price: float = Field(..., gt=0)
    stock: int = Field(..., ge=0)
    is_member_product: int = Field(..., ge=0, le=1)
    merchant_id: int = Field(..., ge=0)


class OrderRequest(BaseModel):
    """订单请求模型"""
    order_no: str
    user_id: int = Field(..., gt=0)
    product_id: int = Field(..., gt=0)
    quantity: int = Field(1, ge=1, le=100)
    points_to_use: float = Field(0, ge=0, description="使用积分数，支持小数点后4位精度")


class WithdrawalRequest(BaseModel):
    """提现请求模型"""
    user_id: int = Field(..., gt=0)
    amount: float = Field(..., gt=0, le=100000)
    withdrawal_type: str = Field('user', pattern=r'^(user|merchant)$')


class WithdrawalAuditRequest(BaseModel):
    """提现审核请求模型"""
    withdrawal_id: int = Field(..., gt=0)
    approve: bool
    auditor: str = Field('admin', min_length=1)


class RewardAuditRequest(BaseModel):
    """奖励审核请求模型"""
    reward_ids: List[int] = Field(..., min_length=1)
    approve: bool
    auditor: str = Field('admin', min_length=1)


class CouponUseRequest(BaseModel):
    """优惠券使用请求模型"""
    user_id: int = Field(..., gt=0)
    coupon_id: int = Field(..., gt=0)
    order_amount: float = Field(..., gt=0)


class RefundRequest(BaseModel):
    """退款请求模型"""
    order_no: str


# ==================== 微信支付商户账户提现到银行卡（自提）请求模型 ====================
class MerchantWithdrawToBankcardRequest(BaseModel):
    """
    商户账户提现到银行卡（自提）请求模型

    用途：商户号余额→对公/对私银行卡
    接口地址：POST https://api.mch.weixin.qq.com/v3/merchant/fund/withdraw
    """
    out_request_no: str = Field(..., min_length=1, max_length=32,
                                description="商户提现单号，由商户自定义生成，必须是字母数字")
    amount: int = Field(..., gt=0, le=800000000,
                        description="提现金额，单位：分，不能超过8亿元")
    account_type: str = Field('BASIC', pattern=r'^(BASIC|OPERATION|FEES)$',
                              description="出款账户类型：BASIC=基本账户，OPERATION=运营账户，FEES=手续费账户")
    bank_memo: Optional[str] = Field(None, max_length=32,
                                     description="银行附言，展示在收款银行系统中的附言")
    remark: Optional[str] = Field(None, max_length=56,
                                  description="提现备注，商户自定义字段")
    notify_url: Optional[str] = Field(None, max_length=256,
                                      description="提现结果通知地址，异步接收提现结果通知的回调地址")


class MerchantWithdrawQueryRequest(BaseModel):
    """
    查询商户提现状态请求模型

    接口地址：GET https://api.mch.weixin.qq.com/v3/merchant/fund/withdraw/out-request-no/{out_request_no}
    """
    out_request_no: str = Field(..., min_length=1, max_length=32,
                                description="商户提现单号")