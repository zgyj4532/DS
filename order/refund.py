from fastapi import APIRouter
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
from database_setup import get_conn
from .finance import reverse_split_on_refund
from typing import Optional, Dict, Any

router = APIRouter()

class RefundManager:
    @staticmethod
    def apply(order_number: str, refund_type: str, reason_code: str) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM Refunds WHERE order_number=%s", order_number)
                if cur.fetchone():
                    return False
                cur.execute("""INSERT INTO Refunds(order_number,refund_type,reason,status)
                               VALUES(%s,%s,%s,'applied')""", (order_number, refund_type, reason_code))
                conn.commit()
                return True

    @staticmethod
    def audit(order_number: str, approve: bool = True, reject_reason: Optional[str] = None) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                new_status = "success" if approve else "rejected"
                cur.execute("UPDATE Refunds SET status=%s,reject_reason=%s WHERE order_number=%s",
                            (new_status, reject_reason, order_number))
                if approve:
                    cur.execute("UPDATE Orders SET status='refund' WHERE order_number=%s", order_number)
                    reverse_split_on_refund(order_number)
                conn.commit()
                return True

    @staticmethod
    def progress(order_number: str) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM Refunds WHERE order_number=%s", order_number)
                return cur.fetchone()

from fastapi import HTTPException   # 顶部记得一起导

# ---------------- 路由 ----------------
class RefundApply(BaseModel):
    order_number: str
    refund_type: str
    reason_code: str

class RefundAudit(BaseModel):
    order_number: str
    approve: bool
    reject_reason: Optional[str] = None

@router.post("/apply")
def refund_apply(body: RefundApply):
    ok = RefundManager.apply(body.order_number, body.refund_type, body.reason_code)
    if not ok:
        raise HTTPException(400, "该订单已申请过退款")
    return {"ok": True}

@router.post("/audit")
def refund_audit(body: RefundAudit):
    RefundManager.audit(body.order_number, body.approve, body.reject_reason)
    return {"ok": True}

@router.get("/progress/{order_number}")
def refund_progress(order_number: str):
    return RefundManager.progress(order_number) or {}