from decimal import Decimal
from datetime import date, timedelta
from database_setup import get_conn

# ---------- 订单分账 ----------
def split_order_funds(order_number: str, total: Decimal, is_vip: bool):
    with get_conn() as conn:
        with conn.cursor() as cur:
            merchant = total * Decimal("0.8")
            cur.execute("INSERT INTO order_split(order_number,item_type,amount) VALUES(%s,'merchant',%s)", (order_number, merchant))
            pool_total = total * Decimal("0.2")
            pools = {"public": 0.01, "maintain": 0.01, "subsidy": 0.12, "director": 0.02,
                     "shop": 0.01, "city": 0.01, "branch": 0.005, "fund": 0.015}
            for k, v in pools.items():
                amt = pool_total * Decimal(str(v))
                cur.execute("INSERT INTO order_split(order_number,item_type,amount,pool_type) VALUES(%s,'pool',%s,%s)",
                            (order_number, amt, k))
            conn.commit()

# ---------- 退款回冲 ----------
def reverse_split_on_refund(order_number: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT SUM(amount) AS m FROM order_split WHERE order_number=%s AND item_type='merchant'", order_number)
            m = cur.fetchone()["m"] or Decimal("0")
            cur.execute("UPDATE Merchant_Balance SET balance=balance-%s WHERE merchant_id=1", m)
            conn.commit()

# ---------- 商家余额 ----------
def get_balance(merchant_id: int = 1):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT balance,bank_name,bank_account FROM Merchant_Balance WHERE merchant_id=%s", merchant_id)
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO Merchant_Balance(merchant_id,balance) VALUES(%s,0)", merchant_id)
                conn.commit()
                return {"balance": Decimal("0"), "bank_name": "", "bank_account": ""}
            return row

def bind_bank(bank_name: str, bank_account: str, merchant_id: int = 1):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE Merchant_Balance SET bank_name=%s,bank_account=%s WHERE merchant_id=%s",
                        (bank_name, bank_account, merchant_id))
            conn.commit()

def withdraw(amount: Decimal, merchant_id: int = 1) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM Merchant_Balance WHERE merchant_id=%s", merchant_id)
            bal = cur.fetchone()["balance"]
            if bal < amount:
                return False
            cur.execute("UPDATE Merchant_Balance SET balance=balance-%s WHERE merchant_id=%s", (amount, merchant_id))
            conn.commit()
            return True

def generate_statement():
    with get_conn() as conn:
        with conn.cursor() as cur:
            yesterday = date.today() - timedelta(days=1)
            cur.execute("SELECT closing_balance FROM merchant_statement WHERE merchant_id=1 AND date<%s ORDER BY date DESC LIMIT 1", yesterday)
            row = cur.fetchone()
            opening = row["closing_balance"] if row else Decimal("0")
            cur.execute("SELECT SUM(amount) AS income FROM order_split WHERE item_type='merchant' AND DATE(created_at)=%s", yesterday)
            income = cur.fetchone()["income"] or Decimal("0")
            withdraw = Decimal("0")  # 简化
            closing = opening + income - withdraw
            cur.execute("""INSERT INTO merchant_statement(merchant_id,date,opening_balance,income,withdraw,closing_balance)
                           VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
                           opening_balance=VALUES(opening_balance),income=VALUES(income),withdraw=VALUES(withdraw),closing_balance=VALUES(closing_balance)""",
                        (1, yesterday, opening, income, withdraw, closing))
            conn.commit()

# 追加到 finance.py 尾部即可
def settle_to_merchant(amount: Decimal, merchant_id: int = 1):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE Merchant_Balance SET balance=balance+%s WHERE merchant_id=%s",
                        (amount, merchant_id))
            conn.commit()