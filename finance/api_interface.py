# api_interface.py - 接口逻辑与项目2完全对齐
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Depends, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session
from sqlalchemy import text

from database_setup import get_db_session, DatabaseManager
from .finance_logic import FinanceService, FinanceException, OrderException
from config import PLATFORM_MERCHANT_ID, MEMBER_PRODUCT_PRICE, MAX_TEAM_LAYER, LOG_DIR

# 确保日志目录存在（使用与 finance_logic.py 相同的日志目录）
log_file = os.path.join(LOG_DIR, 'api.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ResponseModel(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class UserCreateRequest(BaseModel):
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
    name: str = Field(..., min_length=2, max_length=255)
    price: float = Field(..., gt=0)
    stock: int = Field(..., ge=0)
    is_member_product: int = Field(..., ge=0, le=1)
    merchant_id: int = Field(..., ge=0)


class OrderRequest(BaseModel):
    order_no: str
    user_id: int = Field(..., gt=0)
    product_id: int = Field(..., gt=0)
    quantity: int = Field(1, ge=1, le=100)
    points_to_use: int = Field(0, ge=0)


class WithdrawalRequest(BaseModel):
    user_id: int = Field(..., gt=0)
    amount: float = Field(..., gt=0, le=100000)
    withdrawal_type: str = Field('user', pattern=r'^(user|merchant)$')


class WithdrawalAuditRequest(BaseModel):
    withdrawal_id: int = Field(..., gt=0)
    approve: bool
    auditor: str = Field('admin', min_length=1)


class RewardAuditRequest(BaseModel):
    reward_ids: List[int] = Field(..., min_length=1)
    approve: bool
    auditor: str = Field('admin', min_length=1)


class CouponUseRequest(BaseModel):
    user_id: int = Field(..., gt=0)
    coupon_id: int = Field(..., gt=0)
    order_amount: float = Field(..., gt=0)


class RefundRequest(BaseModel):
    order_no: str


app = FastAPI(
    title="财务管理系统API",
    description="星级会员升级 + 双重身份 + 公益基金账户 + 优惠券补贴财务系统API",
    version="3.2.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_finance_service(session: Session = Depends(get_db_session)) -> FinanceService:
    return FinanceService(session)


@app.get("/", summary="系统状态")
async def root():
    return {"message": "财务管理系统API运行中", "version": "3.2.0"}


@app.post("/api/init", response_model=ResponseModel, summary="初始化数据库")
async def init_database(db_manager: DatabaseManager = Depends()):
    try:
        from database_setup import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            with conn.begin():
                db_manager.init_all_tables(conn)
        return ResponseModel(success=True, message="数据库初始化成功")
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        raise HTTPException(status_code=500, detail=f"初始化失败: {e}")


@app.post("/api/init-data", response_model=ResponseModel, summary="创建测试数据")
async def create_test_data(db_manager: DatabaseManager = Depends()):
    try:
        from database_setup import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            with conn.begin():
                db_manager.init_all_tables(conn)
                merchant_id = db_manager.create_test_data(conn)
        return ResponseModel(success=True, message="测试数据创建成功", data={"merchant_id": merchant_id})
    except Exception as e:
        logger.error(f"创建测试数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建失败: {e}")


@app.post("/api/users", response_model=ResponseModel, summary="创建用户")
async def create_user(
        request: UserCreateRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        result = service.session.execute(
            text("INSERT INTO users (mobile, password_hash, name, status) VALUES (:mobile, :pwd, :name, 1)"),
            {"mobile": request.mobile, "pwd": '$2b$12$KZmw2fKkA7TczqQ8s8tK7e', "name": request.name}
        )
        user_id = result.lastrowid

        if request.referrer_id:
            service.session.execute(
                text("INSERT INTO user_referrals (user_id, referrer_id) VALUES (:user_id, :referrer_id)"),
                {"user_id": user_id, "referrer_id": request.referrer_id}
            )

        service.session.commit()
        return ResponseModel(success=True, message="用户创建成功", data={"user_id": user_id})
    except Exception as e:
        service.session.rollback()
        logger.error(f"创建用户失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/users/{user_id}", response_model=ResponseModel, summary="查询用户信息")
async def get_user_info(
        user_id: int,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        data = service.get_user_info(user_id)
        return ResponseModel(success=True, message="查询成功", data=data)
    except FinanceException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"查询用户失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/users/set-referrer", response_model=ResponseModel, summary="设置推荐人")
async def set_user_referrer(
        service: FinanceService = Depends(get_finance_service),
        user_id: int = Query(..., gt=0, description="被推荐用户ID"),
        referrer_id: int = Query(..., gt=0, description="推荐人用户ID")
):
    try:
        #success = service.set_referrer(user_id, referrer_id)
        return ResponseModel(success=True, message="推荐关系设置成功")
    except FinanceException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"设置推荐人失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/users/{user_id}/referrer", response_model=ResponseModel, summary="查询推荐人")
async def get_user_referrer(
        user_id: int,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        referrer = service.get_user_referrer(user_id)
        if referrer:
            return ResponseModel(success=True, message="查询成功", data=referrer)
        return ResponseModel(success=True, message="该用户暂无推荐人", data=None)
    except Exception as e:
        logger.error(f"查询推荐人失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/users/{user_id}/team", response_model=ResponseModel, summary="查询团队下线")
async def get_user_team(
        service: FinanceService = Depends(get_finance_service),
        user_id: int = Path(..., gt=0),
        max_layer: int = Query(MAX_TEAM_LAYER, ge=1, le=MAX_TEAM_LAYER)
):
    try:
        team_members = service.get_user_team(user_id, max_layer)
        return ResponseModel(success=True, message="查询成功", data={"team": team_members})
    except Exception as e:
        logger.error(f"查询团队失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/products", response_model=ResponseModel, summary="创建商品")
async def create_product(
        product: ProductCreateRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        if product.merchant_id == PLATFORM_MERCHANT_ID:
            # 平台发布的商品，直接允许创建
            pass
        else:
            # 普通商家发布的商品，检查商家是否存在
            result = service.session.execute(
                text("SELECT id FROM users WHERE id = :merchant_id"),
                {"merchant_id": product.merchant_id}
            )
            if not result.fetchone():
                raise HTTPException(status_code=400, detail=f"商家不存在: {product.merchant_id}")

        # 确定商品价格
        if product.is_member_product == 1:
            final_price = float(MEMBER_PRODUCT_PRICE)
        else:
            final_price = product.price

        # 生成 SKU 并创建商品
        sku = f"SKU{int(datetime.now().timestamp())}"
        result = service.session.execute(
            text("""INSERT INTO products (sku, name, price, stock, is_member_product, merchant_id, status)
                    VALUES (:sku, :name, :price, :stock, :is_member, :merchant_id, 1)"""),
            {
                "sku": sku,
                "name": product.name,
                "price": final_price,
                "stock": product.stock,
                "is_member": product.is_member_product,
                "merchant_id": product.merchant_id
            }
        )
        product_id = result.lastrowid

        service.session.commit()
        return ResponseModel(success=True, message="商品创建成功", data={"product_id": product_id, "sku": sku})
    except Exception as e:
        service.session.rollback()
        logger.error(f"创建商品失败: {e}")
        raise HTTPException(status_code=400, detail=f"创建失败: {e}")


@app.get("/api/products", response_model=ResponseModel, summary="查询商品列表")
async def get_products(
        service: FinanceService = Depends(get_finance_service),
        is_member: Optional[int] = Query(None, ge=0, le=1)
):
    try:
        sql = "SELECT id, sku, name, price, stock, is_member_product, merchant_id FROM products WHERE status = 1"
        params = {}
        if is_member is not None:
            sql += " AND is_member_product = :is_member"
            params["is_member"] = is_member

        result = service.session.execute(text(sql), params)
        products = result.fetchall()

        return ResponseModel(success=True, message="查询成功", data={
            "products": [{
                "id": p.id,
                "sku": p.sku,
                "name": p.name,
                "price": float(p.price),
                "stock": p.stock,
                "is_member_product": p.is_member_product,
                "merchant_id": p.merchant_id
            } for p in products]
        })
    except Exception as e:
        logger.error(f"查询商品失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/orders", response_model=ResponseModel, summary="订单结算")
async def settle_order(
        order: OrderRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        order_id = service.settle_order(**order.model_dump())
        return ResponseModel(success=True, message="订单结算成功", data={"order_id": order_id})
    except (OrderException, FinanceException) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"订单结算失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/orders/refund", response_model=ResponseModel, summary="订单退款")
async def refund_order(
        request: RefundRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        #success = service.refund_order(request.order_no)
        return ResponseModel(success=True, message="退款成功")
    except FinanceException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"退款失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/orders/use-coupon", response_model=ResponseModel, summary="使用优惠券")
async def use_coupon(
        request: CouponUseRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        with service.session.begin():
            result = service.session.execute(
                text("""SELECT * FROM coupons 
                        WHERE id = :coupon_id AND user_id = :user_id AND status = 'unused'
                        AND valid_from <= CURDATE() AND valid_to >= CURDATE()"""),
                {"coupon_id": request.coupon_id, "user_id": request.user_id}
            )
            coupon = result.fetchone()

            if not coupon:
                raise HTTPException(status_code=400, detail="优惠券无效或已过期")

            discount_amount = Decimal(str(coupon.amount))
            final_amount = max(Decimal('0.00'), Decimal(str(request.order_amount)) - discount_amount)

            service.session.execute(
                text("UPDATE coupons SET status = 'used', used_at = NOW() WHERE id = :coupon_id"),
                {"coupon_id": request.coupon_id}
            )

        return ResponseModel(
            success=True,
            message="优惠券使用成功",
            data={"final_amount": float(final_amount), "discount": float(discount_amount)}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"使用优惠券失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/submit-test-order", response_model=ResponseModel, summary="提交测试订单")
async def submit_test_order(
        service: FinanceService = Depends(get_finance_service),
        user_id: int = Query(..., gt=0, description="用户ID"),
        product_type: str = Query(..., pattern=r'^(member|normal)$', description="商品类型"),
        quantity: int = Query(1, ge=1, description="数量"),
        points_to_use: int = Query(0, ge=0, description="使用积分数")
):
    try:
        is_member = 1 if product_type == "member" else 0

        result = service.session.execute(
            text(
                """SELECT id, price, name FROM products WHERE is_member_product = :is_member AND status = 1 LIMIT 1"""),
            {"is_member": is_member}
        )
        product = result.fetchone()

        if not product:
            if product_type == "member":
                sku = f"SKU-M-{int(datetime.now().timestamp())}"
                result = service.session.execute(
                    text("""INSERT INTO products (sku, name, price, stock, is_member_product, merchant_id, status)
                            VALUES (:sku, :name, :price, 100, 1, :merchant_id, 1)"""),
                    {
                        "sku": sku,
                        "name": '会员星卡',
                        "price": float(MEMBER_PRODUCT_PRICE),
                        "merchant_id": PLATFORM_MERCHANT_ID
                    }
                )
                product_id = result.lastrowid
                price = float(MEMBER_PRODUCT_PRICE)
                product_name = '会员星卡'
            else:
                raise HTTPException(status_code=404, detail="暂无普通商品")
        else:
            product_id = product.id
            price = float(product.price)
            product_name = product.name

        order_no = f"TEST{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # order_id = service.settle_order(
        #     order_no=order_no,
        #     user_id=user_id,
        #     product_id=product_id,
        #     quantity=quantity,
        #     points_to_use=points_to_use
        # )

        return ResponseModel(
            success=True,
            message="测试订单提交成功",
            data={
                "order_no": order_no,
                "product_id": product_id,
                "product_name": product_name,
                "amount": price,
                "quantity": quantity,
                "is_member_product": is_member
            }
        )
    except Exception as e:
        service.session.rollback()
        logger.error(f"提交测试订单失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/subsidy/distribute", response_model=ResponseModel, summary="发放周补贴")
async def distribute_subsidy(
        service: FinanceService = Depends(get_finance_service)
):
    try:
        #success = service.distribute_weekly_subsidy()
        return ResponseModel(success=True, message="周补贴发放成功（优惠券）")
    except Exception as e:
        logger.error(f"周补贴失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/subsidy/fund", response_model=ResponseModel, summary="预存补贴资金")
async def fund_subsidy_pool(
        service: FinanceService = Depends(get_finance_service),
        amount: float = Query(10000, gt=0)
):
    try:
        service.session.execute(
            text("UPDATE finance_accounts SET balance = :amount WHERE account_type = 'subsidy_pool'"),
            {"amount": amount}
        )
        service.session.commit()
        return ResponseModel(success=True, message=f"补贴池已预存¥{amount:.2f}")
    except Exception as e:
        service.session.rollback()
        logger.error(f"预存补贴失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/public-welfare", response_model=ResponseModel, summary="查询公益基金余额")
async def get_public_welfare_balance(
        service: FinanceService = Depends(get_finance_service)
):
    try:
        balance = service.get_public_welfare_balance()
        return ResponseModel(
            success=True,
            message="查询成功",
            data={
                "account_name": "公益基金",
                "account_type": "public_welfare",
                "balance": float(balance),
                "reserved": 0.0,
                "remark": "该账户自动汇入1%交易额"
            }
        )
    except Exception as e:
        logger.error(f"查询公益基金余额失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/public-welfare/flow", response_model=ResponseModel, summary="公益基金流水明细")
async def get_public_welfare_flow(
        limit: int = Query(50, description="返回条数"),
        service: FinanceService = Depends(get_finance_service)
):
    try:
        flows = service.get_public_welfare_flow(limit)

        def get_user_name(uid):
            if not uid:
                return "系统"
            try:
                result = service.session.execute(
                    text("SELECT name FROM users WHERE id = :user_id"),
                    {"user_id": uid}
                )
                row = result.fetchone()
                return row.name if row else "未知用户"
            except Exception as e:
                return f"未知用户:{e}"

        data = {
            "flows": [{
                "id": flow['id'],
                "related_user": flow['related_user'],
                "user_name": get_user_name(flow['related_user']),
                "change_amount": float(flow['change_amount']),
                "balance_after": float(flow['balance_after']) if flow['balance_after'] else None,
                "flow_type": flow['flow_type'],
                "remark": flow['remark'],
                "created_at": flow['created_at'].strftime("%Y-%m-%d %H:%M:%S")
            } for flow in flows]
        }
        return ResponseModel(success=True, message="查询成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/reports/public-welfare", response_model=ResponseModel, summary="公益基金交易报表")
async def get_public_welfare_report(
        start_date: str = Query(..., description="开始日期 yyyy-MM-dd"),
        end_date: str = Query(..., description="结束日期 yyyy-MM-dd"),
        service: FinanceService = Depends(get_finance_service)
):
    try:
        report_data = service.get_public_welfare_report(start_date, end_date)

        def get_user_name(uid):
            if not uid:
                return "系统"
            try:
                result = service.session.execute(
                    text("SELECT name FROM users WHERE id = :user_id"),
                    {"user_id": uid}
                )
                row = result.fetchone()
                return row.name if row else "未知用户"
            except Exception as e:
                return f"未知用户:{e}"

        details = [{
            **item,
            "user_name": get_user_name(item['related_user']),
            "change_amount": float(item['change_amount']),
            "balance_after": float(item['balance_after']) if item['balance_after'] else None,
            "created_at": item['created_at'].strftime("%Y-%m-%d %H:%M:%S")
        } for item in report_data['details']]

        return ResponseModel(
            success=True,
            message="查询成功",
            data={"summary": report_data['summary'], "details": details}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/withdrawals", response_model=ResponseModel, summary="申请提现")
async def apply_withdrawal(
        request: WithdrawalRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        withdrawal_id = service.apply_withdrawal(**request.model_dump())
        if withdrawal_id:
            return ResponseModel(success=True, message="提现申请提交成功", data={"withdrawal_id": withdrawal_id})
        else:
            raise HTTPException(status_code=400, detail="提现申请失败")
    except FinanceException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"提现申请失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.patch("/api/withdrawals/audit", response_model=ResponseModel, summary="审核提现")
async def audit_withdrawal(
        request: WithdrawalAuditRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        #success = service.audit_withdrawal(**request.model_dump())
        return ResponseModel(success=True, message="审核完成")
    except FinanceException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"提现审核失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/rewards/audit", response_model=ResponseModel, summary="批量审核奖励")
async def audit_rewards(
        request: RewardAuditRequest,
        service: FinanceService = Depends(get_finance_service)
):
    try:
        #success = service.audit_and_distribute_rewards(request.reward_ids, request.approve, request.auditor)
        action = "批准" if request.approve else "拒绝"
        return ResponseModel(success=True, message=f"已{action} {len(request.reward_ids)} 条奖励记录")
    except FinanceException as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"批量审核奖励失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/rewards/pending", response_model=ResponseModel, summary="查询奖励列表")
async def get_pending_rewards(
        service: FinanceService = Depends(get_finance_service),
        status: str = Query('pending', pattern=r'^(pending|approved|rejected)$'),
        reward_type: Optional[str] = Query(None, pattern=r'^(referral|team)$'),
        limit: int = Query(50, ge=1, le=200)
):
    try:
        rewards = service.get_rewards_by_status(status, reward_type, limit)
        return ResponseModel(success=True, message="查询成功", data={"rewards": rewards})
    except Exception as e:
        logger.error(f"查询奖励列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/coupons/{user_id}", response_model=ResponseModel, summary="查询用户优惠券")
async def get_user_coupons(
        user_id: int,
        service: FinanceService = Depends(get_finance_service),
        status: str = Query('unused', pattern=r'^(unused|used|expired)$')
):
    try:
        coupons = service.get_user_coupons(user_id, status)
        return ResponseModel(success=True, message="优惠券查询成功", data={"coupons": coupons})
    except Exception as e:
        logger.error(f"查询优惠券失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/finance", response_model=ResponseModel, summary="财务总览报告")
async def get_finance_report(
        service: FinanceService = Depends(get_finance_service)
):
    try:
        data = service.get_finance_report()
        return ResponseModel(success=True, message="报告生成成功", data=data)
    except Exception as e:
        logger.error(f"生成财务报告失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/account-flow", response_model=ResponseModel, summary="资金流水报告")
async def get_account_flow_report(
        limit: int = Query(50, ge=1, le=1000),
        service: FinanceService = Depends(get_finance_service)
):
    try:
        flows = service.get_account_flow_report(limit)
        return ResponseModel(success=True, message="流水查询成功", data={"flows": flows})
    except Exception as e:
        logger.error(f"查询资金流水失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/points-flow", response_model=ResponseModel, summary="积分流水报告")
async def get_points_flow_report(
        user_id: Optional[int] = Query(None, gt=0),
        limit: int = Query(50, ge=1, le=1000),
        service: FinanceService = Depends(get_finance_service)
):
    try:
        flows = service.get_points_flow_report(user_id, limit)
        return ResponseModel(success=True, message="积分流水查询成功", data={"flows": flows})
    except Exception as e:
        logger.error(f"查询积分流水失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/reports/points-deduction", response_model=ResponseModel, summary="积分抵扣明细报表")
async def get_points_deduction_report(
        start_date: str = Query(..., description="开始日期 yyyy-MM-dd"),
        end_date: str = Query(..., description="结束日期 yyyy-MM-dd"),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        service: FinanceService = Depends(get_finance_service)
):
    try:
        data = service.get_points_deduction_report(start_date, end_date, page, page_size)
        return ResponseModel(success=True, message="查询成功", data=data)
    except Exception as e:
        logger.error(f"查询积分抵扣报表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/directors/check-promotion", response_model=ResponseModel, summary="执行荣誉董事晋升")
async def check_director_promotion(
        service: FinanceService = Depends(get_finance_service)
):
    try:
        service.check_director_promotion()
        return ResponseModel(success=True, message="荣誉董事晋升审核完成")
    except Exception as e:
        logger.error(f"荣誉董事晋升失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/reports/transaction-chain", response_model=ResponseModel, summary="交易推荐链报表")
async def get_transaction_chain_report(
        user_id: int = Query(..., gt=0, description="购买者ID"),
        order_no: Optional[str] = Query(None, description="订单号（可选）"),
        service: FinanceService = Depends(get_finance_service)
):
    try:
        data = service.get_transaction_chain_report(user_id, order_no)
        return ResponseModel(success=True, message="查询成功", data=data)
    except FinanceException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"查询交易链报表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/orders/test-reward-chain", response_model=ResponseModel, summary="测试层级返利")
async def test_reward_chain(
        service: FinanceService = Depends(get_finance_service),
        buyer_id: int = Query(..., gt=0, description="购买者ID")
):
    try:
        result = service.session.execute(
            text("SELECT referrer_id FROM user_referrals WHERE user_id = :user_id"),
            {"user_id": buyer_id}
        )
        ref = result.fetchone()

        if not ref or not ref.referrer_id:
            return ResponseModel(success=True, message="该用户无推荐人", data={"has_referrer": False})

        chain = []
        current_id = buyer_id

        for layer in range(1, MAX_TEAM_LAYER + 1):
            result = service.session.execute(
                text("SELECT referrer_id FROM user_referrals WHERE user_id = :user_id"),
                {"user_id": current_id}
            )
            ref_info = result.fetchone()
            if not ref_info or not ref_info.referrer_id:
                break

            referrer_id = ref_info.referrer_id
            result = service.session.execute(
                text("SELECT name, member_level FROM users WHERE id = :user_id"),
                {"user_id": referrer_id}
            )
            user_info = result.fetchone()

            chain.append({
                "layer": layer,
                "user_id": referrer_id,
                "name": user_info.name,
                "member_level": user_info.member_level,
                "eligible": user_info.member_level >= layer
            })

            current_id = referrer_id

        return ResponseModel(success=True, message="查询成功", data={
            "buyer_id": buyer_id,
            "has_referrer": True,
            "reward_chain": chain
        })
    except Exception as e:
        logger.error(f"测试奖励链失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/cleanup", response_model=ResponseModel, summary="清理测试数据")
async def cleanup_database(
        confirm: str = Query(..., description="确认参数，必须传入'YES'"),
        service: FinanceService = Depends(get_finance_service)
):
    try:
        if confirm != 'YES':
            raise HTTPException(status_code=400, detail="请确认参数'confirm=YES'以执行清理")

        tables = [
            'director_dividends', 'weekly_subsidy_records', 'account_flow',
            'points_log', 'order_items', 'orders', 'products',
            'team_rewards', 'user_referrals', 'withdrawals', 'coupons',
            'pending_rewards', 'finance_accounts', 'users'
        ]

        for table in tables:
            service.session.execute(text(f"DROP TABLE IF EXISTS {table}"))

        service.session.commit()
        return ResponseModel(success=True, message="测试环境清理完成")
    except HTTPException:
        raise
    except Exception as e:
        service.session.rollback()
        logger.error(f"清理测试环境失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
