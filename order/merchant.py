from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from database_setup import get_conn
from .finance import get_balance, bind_bank, withdraw
from typing import List, Dict, Any, Optional

router = APIRouter()

class MerchantManager:
    @staticmethod
    def list_orders(status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """SELECT o.*, u.name AS user_name, u.phone AS user_phone
                         FROM Orders o JOIN Users u ON o.user_id=u.id"""
                params = []
                if status:
                    sql += " WHERE o.status=%s"
                    params.append(status)
                sql += " ORDER BY o.created_at DESC LIMIT %s"
                params.append(limit)
                cur.execute(sql, params)
                orders = cur.fetchall()
                for o in orders:
                    cur.execute("""SELECT oi.*, p.name AS product_name
                                   FROM Order_Items oi JOIN Products p ON oi.product_id=p.id
                                   WHERE oi.order_id=%s""", o["id"])
                    o["items"] = cur.fetchall()
                return orders

    @staticmethod
    def ship(order_number: str) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE Orders SET status='pending_recv' WHERE order_number=%s AND status='pending_ship'", order_number)
                conn.commit()
                return cur.rowcount > 0

    @staticmethod
    def approve_refund(order_number: str, approve: bool = True, reject_reason: Optional[str] = None):
        from .refund import RefundManager
        RefundManager.audit(order_number, approve, reject_reason)

# ---------------- 路由 ----------------
class MShip(BaseModel):
    order_number: str

class MRefundAudit(BaseModel):
    order_number: str
    approve: bool
    reject_reason: Optional[str] = None

class MBindBank(BaseModel):
    bank_name: str
    bank_account: str

class MWithdraw(BaseModel):
    amount: float

@router.get("/orders")
def m_orders(status: Optional[str] = None):
    return MerchantManager.list_orders(status)

@router.post("/ship")
def m_ship(body: MShip):
    return {"ok": MerchantManager.ship(body.order_number)}

@router.post("/approve_refund")
def m_refund_audit(body: MRefundAudit):
    MerchantManager.approve_refund(body.order_number, body.approve, body.reject_reason)
    return {"ok": True}

@router.get("/balance")
def m_balance():
    return get_balance()

@router.post("/bind_bank")
def m_bind(body: MBindBank):
    bind_bank(body.bank_name, body.bank_account)
    return {"ok": True}

@router.post("/withdraw")
def m_withdraw(body: MWithdraw):
    ok = withdraw(Decimal(str(body.amount)))
    if not ok:
        raise HTTPException(400, "余额不足")
    return {"ok": True}