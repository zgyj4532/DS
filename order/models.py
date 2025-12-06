from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List
from decimal import Decimal


class CartAdd(BaseModel):
    user_id: int
    product_id: int
    quantity: int = 1

class OrderCreate(BaseModel):
    user_id: int
    address_id: Optional[int] = None
    custom_address: Optional[dict] = None

class OrderPay(BaseModel):
    order_number: str
    pay_way: str

class RefundApply(BaseModel):
    order_number: str
    refund_type: str
    reason_code: str

class MBindBank(BaseModel):
    bank_name: str
    bank_account: str

class MWithdraw(BaseModel):
    amount: float