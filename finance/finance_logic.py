# finance_logic.py - 业务逻辑与项目2完全一致
import logging
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from config import (
    AllocationKey, ALLOCATIONS, MAX_POINTS_VALUE, TAX_RATE,
    POINTS_DISCOUNT_RATE, MEMBER_PRODUCT_PRICE, COUPON_VALID_DAYS,
    PLATFORM_MERCHANT_ID, MAX_PURCHASE_PER_DAY, MAX_TEAM_LAYER,
    LOG_FILE
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FinanceException(Exception):
    pass

class OrderException(FinanceException):
    pass

class InsufficientBalanceException(FinanceException):
    def __init__(self, account: str, required: Decimal, available: Decimal):
        super().__init__(
            f"余额不足: {account} | 需要: ¥{required:.2f} | 当前: ¥{available:.2f}"
        )
        self.account = account
        self.required = required
        self.available = available

class FinanceService:
    def __init__(self, session: Session):
        self.session = session

    def _check_pool_balance(self, account_type: str, required_amount: Decimal) -> bool:
        balance = self.get_account_balance(account_type)
        if balance < required_amount:
            raise InsufficientBalanceException(account_type, required_amount, balance)
        return True

    def _check_user_balance(self, user_id: int, required_amount: Decimal, balance_type: str = 'promotion_balance') -> bool:
        balance = self.get_user_balance(user_id, balance_type)
        if balance < required_amount:
            raise InsufficientBalanceException(f"user:{user_id}:{balance_type}", required_amount, balance)
        return True

    def check_purchase_limit(self, user_id: int) -> bool:
        result = self.session.execute(
            text("SELECT COUNT(*) as count FROM orders WHERE user_id = :user_id AND is_member_order = 1 AND created_at >= NOW() - INTERVAL 24 HOUR AND status != 'refunded'"),
            {"user_id": user_id}
        )
        return result.fetchone().count < MAX_PURCHASE_PER_DAY

    def get_account_balance(self, account_type: str) -> Decimal:
        result = self.session.execute(
            text("SELECT balance FROM finance_accounts WHERE account_type = :type"),
            {"type": account_type}
        )
        row = result.fetchone()
        return Decimal(str(row.balance)) if row else Decimal('0')

    def get_user_balance(self, user_id: int, balance_type: str = 'promotion_balance') -> Decimal:
        result = self.session.execute(
            text(f"SELECT {balance_type} FROM users WHERE id = :user_id"),
            {"user_id": user_id}
        )
        row = result.fetchone()
        return Decimal(str(getattr(row, balance_type, 0))) if row else Decimal('0')

    def settle_order(self, order_no: str, user_id: int, product_id: int, quantity: int = 1, points_to_use: int = 0) -> int:
        logger.info(f"\n🛒 订单结算开始: {order_no}")
        try:
            with self.session.begin():
                result = self.session.execute(
                    text("SELECT price, is_member_product, merchant_id FROM products WHERE id = :product_id AND status = 1 FOR UPDATE"),
                    {"product_id": product_id}
                )
                product = result.fetchone()
                if not product:
                    raise OrderException(f"商品不存在或已下架: {product_id}")

                merchant_id = product.merchant_id
                if merchant_id != PLATFORM_MERCHANT_ID:
                    result = self.session.execute(
                        text("SELECT id FROM users WHERE id = :merchant_id"),
                        {"merchant_id": merchant_id}
                    )
                    if not result.fetchone():
                        raise OrderException(f"商家不存在: {merchant_id}")

                if product.is_member_product and not self.check_purchase_limit(user_id):
                    raise OrderException("24小时内购买会员商品超过限制（最多2份）")

                unit_price = Decimal(str(product.price))
                original_amount = unit_price * quantity

                result = self.session.execute(
                    text("SELECT member_level, points FROM users WHERE id = :user_id FOR UPDATE"),
                    {"user_id": user_id}
                )
                user = result.fetchone()
                if not user:
                    raise OrderException(f"用户不存在: {user_id}")

                points_discount = Decimal('0')
                final_amount = original_amount

                if not product.is_member_product and points_to_use > 0:
                    self._apply_points_discount(user_id, user, points_to_use, original_amount)
                    points_discount = Decimal(points_to_use) * POINTS_DISCOUNT_RATE
                    final_amount = original_amount - points_discount
                    logger.info(f"💳 积分抵扣: {points_to_use}分 = ¥{points_discount}")

                order_id = self._create_order(
                    order_no, user_id, merchant_id, product_id,
                    final_amount, original_amount, points_discount, product.is_member_product
                )

                if product.is_member_product:
                    self._process_member_order(order_id, user_id, user, unit_price, quantity)
                else:
                    self._process_normal_order(order_id, user_id, merchant_id, final_amount, user.member_level)

            logger.info(f"✅ 订单结算成功: ID={order_id}")
            return order_id
        except Exception as e:
            logger.error(f"订单结算失败: {e}")
            raise

    def _apply_points_discount(self, user_id: int, user, points_to_use: int, amount: Decimal) -> None:
        if user.points < points_to_use:
            raise OrderException(f"积分不足，当前{user.points}分")

        max_discount = amount * Decimal('0.5')
        if points_to_use > max_discount:
            raise OrderException(f"积分抵扣不能超过订单金额的50%（最多{int(max_discount)}分）")

        self.session.execute(
            text("UPDATE users SET points = points - :points WHERE id = :user_id"),
            {"points": points_to_use, "user_id": user_id}
        )
        self.session.execute(
            text("UPDATE finance_accounts SET balance = balance + :points WHERE account_type = 'company_points'"),
            {"points": points_to_use}
        )

    def _create_order(self, order_no: str, user_id: int, merchant_id: int,
                      product_id: int, total_amount: Decimal, original_amount: Decimal,
                      points_discount: Decimal, is_member: bool) -> int:
        result = self.session.execute(
            text("""INSERT INTO orders (order_no, user_id, merchant_id, total_amount, original_amount, points_discount, is_member_order, status)
                    VALUES (:order_no, :user_id, :merchant_id, :total_amount, :original_amount, :points_discount, :is_member, 'completed')"""),
            {
                "order_no": order_no, "user_id": user_id, "merchant_id": merchant_id,
                "total_amount": total_amount, "original_amount": original_amount,
                "points_discount": points_discount, "is_member": is_member
            }
        )
        order_id = result.lastrowid

        self.session.execute(
            text("""INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price)
                    VALUES (:order_id, :product_id, 1, :unit_price, :total_price)"""),
            {
                "order_id": order_id,
                "product_id": product_id,
                "unit_price": original_amount,
                "total_price": original_amount
            }
        )
        return order_id

    def _process_member_order(self, order_id: int, user_id: int, user,
                              unit_price: Decimal, quantity: int) -> None:
        total_amount = unit_price * quantity
        self._allocate_funds_to_pools(order_id, total_amount)

        old_level = user.member_level
        new_level = min(old_level + quantity, 6)

        self.session.execute(
            text("UPDATE users SET member_level = :level, level_changed_at = NOW() WHERE id = :user_id"),
            {"level": new_level, "user_id": user_id}
        )

        points_earned = int(unit_price * quantity)
        self.session.execute(
            text("UPDATE users SET points = points + :points WHERE id = :user_id"),
            {"points": points_earned, "user_id": user_id}
        )
        result = self.session.execute(
            text("SELECT points FROM users WHERE id = :user_id"),
            {"user_id": user_id}
        )
        new_points = result.fetchone().points
        # 使用 helper 插入 points_log
        self._insert_points_log(user_id=user_id,
                                change_amount=points_earned,
                                balance_after=new_points,
                                type='member',
                                reason='购买会员商品获得积分',
                                related_order=order_id)
        logger.info(f"🎉 用户升级: {old_level}星 → {new_level}星, 获得积分: {points_earned}")

        self._create_pending_rewards(order_id, user_id, old_level, new_level)

        company_points = int(total_amount * Decimal('0.20'))
        self._add_pool_balance('company_points', Decimal(company_points), f"订单#{order_id} 公司积分分配")

    def _allocate_funds_to_pools(self, order_id: int, total_amount: Decimal) -> None:
        platform_revenue = total_amount * Decimal('0.80')
        # 使用 helper 统一处理平台池子余额变更与流水
        self._add_pool_balance('platform_revenue_pool', platform_revenue, f"订单#{order_id} 平台收入")

        for purpose, percent in ALLOCATIONS.items():
            if purpose == AllocationKey.PLATFORM_REVENUE_POOL:
                continue
            alloc_amount = total_amount * percent
            # 统一通过 helper 更新各类池子与记录流水
            self._add_pool_balance(purpose.value, alloc_amount, f"订单#{order_id} 分配到{purpose.value}")
            if purpose == AllocationKey.PUBLIC_WELFARE:
                logger.info(f"🎗️ 公益基金获得: ¥{alloc_amount}")

    def _create_pending_rewards(self, order_id: int, buyer_id: int, old_level: int, new_level: int) -> None:
        if old_level == 0:
            result = self.session.execute(
                text("SELECT referrer_id FROM user_referrals WHERE user_id = :user_id"),
                {"user_id": buyer_id}
            )
            referrer = result.fetchone()
            if referrer and referrer.referrer_id:
                reward_amount = MEMBER_PRODUCT_PRICE * Decimal('0.50')
                self.session.execute(
                    text("""INSERT INTO pending_rewards (user_id, reward_type, amount, order_id, status)
                            VALUES (:user_id, 'referral', :amount, :order_id, 'pending')"""),
                    {
                        "user_id": referrer.referrer_id,
                        "amount": reward_amount,
                        "order_id": order_id
                    }
                )
                logger.info(f"🎁 推荐奖励待审核: 用户{referrer.referrer_id} ¥{reward_amount}")

        if old_level == 0 and new_level == 1:
            logger.info("0星升级1星，不产生团队奖励")
            return

        target_layer = new_level
        current_id = buyer_id
        target_referrer = None

        for _ in range(target_layer):
            result = self.session.execute(
                text("SELECT referrer_id FROM user_referrals WHERE user_id = :user_id"),
                {"user_id": current_id}
            )
            ref = result.fetchone()
            if not ref or not ref.referrer_id:
                break
            target_referrer = ref.referrer_id
            current_id = ref.referrer_id

        if target_referrer:
            result = self.session.execute(
                text("SELECT member_level FROM users WHERE id = :user_id"),
                {"user_id": target_referrer}
            )
            referrer_level = result.fetchone().member_level

            if referrer_level >= target_layer:
                reward_amount = MEMBER_PRODUCT_PRICE * Decimal('0.50')
                self.session.execute(
                    text("""INSERT INTO pending_rewards (user_id, reward_type, amount, order_id, layer, status)
                            VALUES (:user_id, 'team', :amount, :order_id, :layer, 'pending')"""),
                    {
                        "user_id": target_referrer,
                        "amount": reward_amount,
                        "order_id": order_id,
                        "layer": target_layer
                    }
                )
                logger.info(f"🎁 团队奖励待审核: 用户{target_referrer} L{target_layer} ¥{reward_amount}")

    def _process_normal_order(self, order_id: int, user_id: int, merchant_id: int,
                              final_amount: Decimal, member_level: int) -> None:
        if merchant_id != PLATFORM_MERCHANT_ID:
            merchant_amount = final_amount * Decimal('0.80')
            # 更新商家余额并记录流水
            # new_merchant_balance = self._update_user_balance(merchant_id, 'merchant_balance', merchant_amount)
            # self._insert_account_flow(account_type='merchant_balance',
            #                           related_user=merchant_id,
            #                           change_amount=merchant_amount,
            #                           flow_type='income',
            #                           remark=f"普通商品收益 - 订单#{order_id}")
            logger.info(f"💰 商家{merchant_id}到账: ¥{merchant_amount}")
        else:
            platform_amount = final_amount * Decimal('0.80')
            # 平台自营商品收入进入平台池子
            self._add_pool_balance('platform_revenue_pool', platform_amount, f"平台自营商品收入 - 订单#{order_id}")
            logger.info(f"💰 平台自营商品收入: ¥{platform_amount}")

            for purpose, percent in ALLOCATIONS.items():
                alloc_amount = final_amount * percent
                # 统一通过 helper 更新池子并记录流水
                self._add_pool_balance(purpose.value, alloc_amount, f"订单#{order_id} 分配到{purpose.value}", related_user=user_id)
                if purpose == AllocationKey.PUBLIC_WELFARE:
                    logger.info(f"🎗️ 公益基金获得: ¥{alloc_amount}")

        if member_level >= 1:
            points_earned = int(final_amount)
            # 使用 helper 更新用户积分并返回新积分
            new_points_dec = self._update_user_balance(user_id, 'points', Decimal(points_earned))
            new_points = int(new_points_dec)
            self._insert_points_log(user_id=user_id,
                                    change_amount=points_earned,
                                    balance_after=new_points,
                                    type='member',
                                    reason='购买获得积分',
                                    related_order=order_id)
            logger.info(f"💎 用户获得积分: {points_earned}")

        if merchant_id != PLATFORM_MERCHANT_ID:
            merchant_points = int(final_amount * Decimal('0.20'))
            if merchant_points > 0:
                new_mp_dec = self._update_user_balance(merchant_id, 'merchant_points', Decimal(merchant_points))
                new_merchant_points = int(new_mp_dec)
                self._insert_points_log(user_id=merchant_id,
                                        change_amount=merchant_points,
                                        balance_after=new_merchant_points,
                                        type='merchant',
                                        reason='销售获得积分',
                                        related_order=order_id)
                logger.info(f"💎 商家获得积分: {merchant_points}")

    def audit_and_distribute_rewards(self, reward_ids: List[int], approve: bool, auditor: str = 'admin') -> bool:
        try:
            if not reward_ids:
                raise FinanceException("奖励ID列表不能为空")

            placeholders = ','.join([f":id{i}" for i in range(len(reward_ids))])
            params = {f"id{i}": rid for i, rid in enumerate(reward_ids)}

            result = self.session.execute(
                text(f"""SELECT id, user_id, reward_type, amount, order_id, layer
                         FROM pending_rewards WHERE id IN ({placeholders}) AND status = 'pending'"""),
                params
            )
            rewards = result.fetchall()

            if not rewards:
                raise FinanceException("未找到待审核的奖励记录")

            if approve:
                today = datetime.now().date()
                valid_to = today + timedelta(days=COUPON_VALID_DAYS)

                for reward in rewards:
                    result = self.session.execute(
                        text("""INSERT INTO coupons (user_id, coupon_type, amount, valid_from, valid_to, status)
                                VALUES (:user_id, 'user', :amount, :valid_from, :valid_to, 'unused')"""),
                        {
                            "user_id": reward.user_id,
                            "amount": reward.amount,
                            "valid_from": today,
                            "valid_to": valid_to
                        }
                    )
                    coupon_id = result.lastrowid

                    self.session.execute(
                        text("UPDATE pending_rewards SET status = 'approved' WHERE id = :id"),
                        {"id": reward.id}
                    )

                    reward_desc = '推荐' if reward.reward_type == 'referral' else f"团队L{reward.layer}"
                    self._record_flow(
                        account_type='coupon',
                        related_user=reward.user_id,
                        change_amount=0,
                        flow_type='coupon',
                        remark=f"{reward_desc}奖励发放优惠券#{coupon_id} ¥{reward.amount:.2f}"
                    )
                    logger.info(f"✅ 奖励{reward.id}已批准，发放优惠券{coupon_id}")
            else:
                self.session.execute(
                    text(f"UPDATE pending_rewards SET status = 'rejected' WHERE id IN ({placeholders})"),
                    params
                )
                logger.info(f"❌ 已拒绝 {len(reward_ids)} 条奖励")

            self.session.commit()
            return True

        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ 审核奖励失败: {e}")
            return False

    def get_rewards_by_status(self, status: str = 'pending', reward_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        sql = """SELECT pr.id, pr.user_id, u.name as user_name, pr.reward_type, pr.amount, pr.order_id, pr.layer, pr.status, pr.created_at
                 FROM pending_rewards pr JOIN users u ON pr.user_id = u.id WHERE pr.status = :status"""
        params = {"status": status}

        if reward_type:
            sql += " AND pr.reward_type = :reward_type"
            params["reward_type"] = reward_type

        sql += " ORDER BY pr.created_at DESC LIMIT :limit"
        params["limit"] = limit

        result = self.session.execute(text(sql), params)
        rewards = result.fetchall()

        return [{
            "id": r.id,
            "user_id": r.user_id,
            "user_name": r.user_name,
            "reward_type": r.reward_type,
            "amount": float(r.amount),
            "order_id": r.order_id,
            "layer": r.layer,
            "status": r.status,
            "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S")
        } for r in rewards]

    def refund_order(self, order_no: str) -> bool:
        try:
            with self.session.begin():
                result = self.session.execute(
                    text("SELECT * FROM orders WHERE order_no = :order_no FOR UPDATE"),
                    {"order_no": order_no}
                )
                order = result.fetchone()

                if not order or order.status == 'refunded':
                    raise FinanceException("订单不存在或已退款")

                is_member = order.is_member_order
                user_id = order.user_id
                amount = Decimal(str(order.total_amount))
                merchant_id = order.merchant_id

                logger.info(f"\n💸 订单退款: {order_no} (会员商品: {is_member})")

                if is_member:
                    result = self.session.execute(
                        text("SELECT referrer_id FROM user_referrals WHERE user_id = :user_id"),
                        {"user_id": user_id}
                    )
                    referrer = result.fetchone()
                    if referrer and referrer.referrer_id:
                        reward_amount = Decimal(str(order.original_amount)) * Decimal('0.50')
                        self.session.execute(
                            text("""UPDATE users SET promotion_balance = promotion_balance - :amount
                                    WHERE id = :user_id AND promotion_balance >= :amount"""),
                            {"amount": reward_amount, "user_id": referrer.referrer_id}
                        )

                    result = self.session.execute(
                        text("SELECT user_id, reward_amount FROM team_rewards WHERE order_id = :order_id"),
                        {"order_id": order.id}
                    )
                    rewards = result.fetchall()
                    for reward in rewards:
                        self.session.execute(
                            text("""UPDATE users SET promotion_balance = promotion_balance - :amount
                                    WHERE id = :user_id AND promotion_balance >= :amount"""),
                            {"amount": reward.reward_amount, "user_id": reward.user_id}
                        )

                    user_points = int(order.original_amount)
                    self.session.execute(
                        text("UPDATE users SET points = GREATEST(points - :points, 0) WHERE id = :user_id"),
                        {"points": user_points, "user_id": user_id}
                    )
                    self.session.execute(
                        text("UPDATE users SET member_level = GREATEST(member_level - 1, 0) WHERE id = :user_id"),
                        {"user_id": user_id}
                    )
                    logger.info(f"⚠️ 用户{user_id}退款后降级")

                merchant_amount = amount * Decimal('0.80')

                if is_member:
                    self._check_pool_balance('platform_revenue_pool', merchant_amount)
                    # 从平台收入池扣减并记录流水
                    self._add_pool_balance('platform_revenue_pool', -merchant_amount, f"退款 - 订单#{order_no}")
                else:
                    if merchant_id == PLATFORM_MERCHANT_ID:
                        self._add_pool_balance('platform_revenue_pool', -merchant_amount, f"退款 - 订单#{order_no}")
                    else:
                        self._check_user_balance(merchant_id, merchant_amount, 'merchant_balance')
                        self.session.execute(
                            text("UPDATE users SET merchant_balance = merchant_balance - :amount WHERE id = :merchant_id"),
                            {"amount": merchant_amount, "merchant_id": merchant_id}
                        )

                self.session.execute(
                    text("UPDATE orders SET refund_status = 'refunded', updated_at = NOW() WHERE id = :order_id"),
                    {"order_id": order.id}
                )

            logger.info(f"✅ 订单退款成功: {order_no}")
            return True

        except Exception as e:
            logger.error(f"❌ 退款失败: {e}")
            return False

    def distribute_weekly_subsidy(self) -> bool:
        logger.info("\n📊 周补贴发放开始（优惠券形式）")

        pool_balance = self.get_account_balance('subsidy_pool')
        if pool_balance <= 0:
            logger.warning("❌ 补贴池余额不足")
            return False

        result = self.session.execute(text("SELECT SUM(points) as total FROM users WHERE points > 0"))
        user_points = Decimal(str(result.fetchone().total or 0))

        result = self.session.execute(text("SELECT SUM(merchant_points) as total FROM users WHERE merchant_points > 0"))
        merchant_points = Decimal(str(result.fetchone().total or 0))

        result = self.session.execute(text("SELECT balance as total FROM finance_accounts WHERE account_type = 'company_points'"))
        company_points = Decimal(str(result.fetchone().total or 0))

        total_points = user_points + merchant_points + company_points

        if total_points <= 0:
            logger.warning("❌ 总积分为0，无法发放补贴")
            return False

        points_value = pool_balance / total_points
        if points_value > MAX_POINTS_VALUE:
            points_value = MAX_POINTS_VALUE

        logger.info(f"补贴池: ¥{pool_balance} | 用户积分: {user_points} | 商家积分: {merchant_points} | 公司积分: {company_points}（仅参与计算） | 积分值: ¥{points_value:.4f}/分")

        total_distributed = Decimal('0')
        today = datetime.now().date()
        valid_to = today + timedelta(days=COUPON_VALID_DAYS)

        result = self.session.execute(text("SELECT id, points FROM users WHERE points > 0"))
        users = result.fetchall()

        try:
            with self.session.begin():
                for user in users:
                    user_points = Decimal(str(user.points))
                    subsidy_amount = user_points * points_value
                    deduct_points = int(subsidy_amount)

                    if subsidy_amount <= 0:
                        continue

                    result = self.session.execute(
                        text("""INSERT INTO coupons (user_id, coupon_type, amount, valid_from, valid_to, status)
                                VALUES (:user_id, 'user', :amount, :valid_from, :valid_to, 'unused')"""),
                        {
                            "user_id": user.id,
                            "amount": subsidy_amount,
                            "valid_from": today,
                            "valid_to": valid_to
                        }
                    )
                    coupon_id = result.lastrowid

                    self.session.execute(
                        text("UPDATE users SET points = points - :points WHERE id = :user_id"),
                        {"points": deduct_points, "user_id": user.id}
                    )

                    self.session.execute(
                        text("""INSERT INTO weekly_subsidy_records (user_id, week_start, subsidy_amount, points_before, points_deducted, coupon_id)
                                VALUES (:user_id, :week_start, :subsidy_amount, :points_before, :points_deducted, :coupon_id)"""),
                        {
                            "user_id": user.id,
                            "week_start": today,
                            "subsidy_amount": subsidy_amount,
                            "points_before": user.points,
                            "points_deducted": deduct_points,
                            "coupon_id": coupon_id
                        }
                    )

                    total_distributed += subsidy_amount
                    logger.info(f"用户{user.id}: 优惠券¥{subsidy_amount:.2f}, 扣积分{deduct_points}")

                result = self.session.execute(text("SELECT id, merchant_points FROM users WHERE merchant_points > 0"))
                merchants = result.fetchall()

                for merchant in merchants:
                    merchant_points = Decimal(str(merchant.merchant_points))
                    subsidy_amount = merchant_points * points_value
                    deduct_points = int(subsidy_amount)

                    if subsidy_amount <= 0:
                        continue

                    self.session.execute(
                        text("""INSERT INTO weekly_subsidy_records (user_id, week_start, subsidy_amount, points_before, points_deducted, coupon_id)
                                VALUES (:user_id, :week_start, :subsidy_amount, :points_before, :points_deducted, :coupon_id)"""),
                        {
                            "user_id": merchant.id,
                            "week_start": today,
                            "subsidy_amount": subsidy_amount,
                            "points_before": merchant.merchant_points,
                            "points_deducted": deduct_points,
                            "coupon_id": coupon_id
                        }
                    )

                    total_distributed += subsidy_amount
                    logger.info(f"商家{merchant.id}: 优惠券¥{subsidy_amount:.2f}, 扣积分{deduct_points}")

                logger.info(f"ℹ️ 公司积分{company_points}未扣除，未发放优惠券")

            logger.info(f"✅ 周补贴完成: 发放¥{total_distributed:.2f}优惠券（补贴池余额不变: ¥{pool_balance}，公司积分不扣除）")
            return True
        except Exception as e:
            logger.error(f"❌ 周补贴发放失败: {e}")
            return False

    def apply_withdrawal(self, user_id: int, amount: float, withdrawal_type: str = 'user') -> Optional[int]:
        try:
            balance_field = 'promotion_balance' if withdrawal_type == 'user' else 'merchant_balance'
            amount_decimal = Decimal(str(amount))

            self._check_user_balance(user_id, amount_decimal, balance_field)

            tax_amount = amount_decimal * TAX_RATE
            actual_amount = amount_decimal - tax_amount

            status = 'pending_manual' if amount_decimal > 5000 else 'pending_auto'

            result = self.session.execute(
                text("""INSERT INTO withdrawals (user_id, amount, tax_amount, actual_amount, status)
                        VALUES (:user_id, :amount, :tax_amount, :actual_amount, :status)"""),
                {
                    "user_id": user_id,
                    "amount": amount_decimal,
                    "tax_amount": tax_amount,
                    "actual_amount": actual_amount,
                    "status": status
                }
            )
            withdrawal_id = result.lastrowid

            self.session.execute(
                text(f"UPDATE users SET {balance_field} = {balance_field} - :amount WHERE id = :user_id"),
                {"amount": amount_decimal, "user_id": user_id}
            )

            self._record_flow(
                account_type=balance_field,
                related_user=user_id,
                change_amount=-amount_decimal,
                flow_type='expense',
                remark=f"{withdrawal_type}_提现申请冻结 #{withdrawal_id}"
            )

            self.session.execute(
                text("UPDATE finance_accounts SET balance = balance + :amount WHERE account_type = 'company_balance'"),
                {"amount": tax_amount}
            )

            self._record_flow(
                account_type='company_balance',
                related_user=user_id,
                change_amount=tax_amount,
                flow_type='income',
                remark=f"{withdrawal_type}_提现个税 #{withdrawal_id}"
            )

            self.session.commit()
            logger.info(f"💸 提现申请 #{withdrawal_id}: ¥{amount_decimal}（税¥{tax_amount:.2f}，实到¥{actual_amount:.2f}）")
            return withdrawal_id

        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ 提现申请失败: {e}")
            return None

    def audit_withdrawal(self, withdrawal_id: int, approve: bool, auditor: str = 'admin') -> bool:
        try:
            result = self.session.execute(
                text("SELECT * FROM withdrawals WHERE id = :withdrawal_id FOR UPDATE"),
                {"withdrawal_id": withdrawal_id}
            )
            withdraw = result.fetchone()

            if not withdraw or withdraw.status not in ['pending_auto', 'pending_manual']:
                raise FinanceException("提现记录不存在或已处理")

            new_status = 'approved' if approve else 'rejected'
            self.session.execute(
                text("""UPDATE withdrawals SET status = :status, audit_remark = :remark, processed_at = NOW()
                        WHERE id = :withdrawal_id"""),
                {
                    "status": new_status,
                    "remark": f"{auditor}审核",
                    "withdrawal_id": withdrawal_id
                }
            )

            if approve:
                self._record_flow(
                    account_type='withdrawal',
                    related_user=withdraw.user_id,
                    change_amount=withdraw.actual_amount,
                    flow_type='income',
                    remark=f"提现到账 #{withdrawal_id}"
                )
                logger.info(f"✅ 提现审核通过 #{withdrawal_id}，到账¥{withdraw.actual_amount:.2f}")
            else:
                balance_field = 'promotion_balance' if withdraw.withdrawal_type == 'user' else 'merchant_balance'
                self.session.execute(
                    text(f"UPDATE users SET {balance_field} = {balance_field} + :amount WHERE id = :user_id"),
                    {"amount": withdraw.amount, "user_id": withdraw.user_id}
                )

                self._record_flow(
                    account_type=balance_field,
                    related_user=withdraw.user_id,
                    change_amount=withdraw.amount,
                    flow_type='income',
                    remark=f"提现拒绝退回 #{withdrawal_id}"
                )
                logger.info(f"❌ 提现审核拒绝 #{withdrawal_id}")

            self.session.commit()
            return True

        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ 提现审核失败: {e}")
            return False

    def _record_flow(self, account_type: str, related_user: Optional[int],
                     change_amount: Decimal, flow_type: str,
                     remark: str, account_id: Optional[int] = None) -> None:
        # 兼容封装：使用内部统一的 account_flow 插入函数
        self._insert_account_flow(account_type=account_type,
                                   related_user=related_user,
                                   change_amount=change_amount,
                                   flow_type=flow_type,
                                   remark=remark,
                                   account_id=account_id)

    def _insert_account_flow(self, account_type: str, related_user: Optional[int],
                             change_amount: Decimal, flow_type: str,
                             remark: str, account_id: Optional[int] = None) -> None:
        """在 `account_flow` 中插入流水，并通过 `_get_balance_after` 计算插入时的余额。
        该函数应在事务上下文中调用（不负责提交/回滚）。"""
        balance_after = self._get_balance_after(account_type, related_user)
        self.session.execute(
            text("""INSERT INTO account_flow (account_id, account_type, related_user, change_amount, balance_after, flow_type, remark, created_at)
                    VALUES (:account_id, :account_type, :related_user, :change_amount, :balance_after, :flow_type, :remark, NOW())"""),
            {
                "account_id": account_id,
                "account_type": account_type,
                "related_user": related_user,
                "change_amount": change_amount,
                "balance_after": balance_after,
                "flow_type": flow_type,
                "remark": remark
            }
        )

    def _add_pool_balance(self, account_type: str, amount: Decimal, remark: str, related_user: Optional[int] = None) -> Decimal:
        """对平台/池子类账户 (`finance_accounts`) 增减余额并记录流水。
        返回更新后的余额（Decimal）。"""
        self.session.execute(
            text("UPDATE finance_accounts SET balance = balance + :amount WHERE account_type = :type"),
            {"amount": amount, "type": account_type}
        )
        result = self.session.execute(
            text("SELECT balance FROM finance_accounts WHERE account_type = :type"),
            {"type": account_type}
        )
        row = result.fetchone()
        balance_after = Decimal(str(row.balance)) if row else Decimal('0')
        # 记录流水（income/expense 由 amount 正负决定）
        flow_type = 'income' if amount >= 0 else 'expense'
        self._insert_account_flow(account_type=account_type,
                                   related_user=related_user,
                                   change_amount=amount,
                                   flow_type=flow_type,
                                   remark=remark)
        return balance_after

    def _insert_points_log(self, user_id: int, change_amount: int, balance_after: int, type: str, reason: str, related_order: Optional[int] = None) -> None:
        """插入 `points_log` 记录。change_amount 使用整数（分/积分）。"""
        self.session.execute(
            text("""INSERT INTO points_log (user_id, change_amount, balance_after, type, reason, related_order, created_at)
                    VALUES (:user_id, :change, :balance, :type, :reason, :related_order, NOW())"""),
            {
                "user_id": user_id,
                "change": change_amount,
                "balance": balance_after,
                "type": type,
                "reason": reason,
                "related_order": related_order
            }
        )

    def _update_user_balance(self, user_id: int, field: str, delta: Decimal) -> Decimal:
        """对 `users` 表的指定余额字段做增减，并返回更新后的值。
        注意：`field` 必须是受信任的字段名（由调用处保证）。"""
        # 使用字符串插值构造字段位置（确保调用方只传入受控字段名）
        self.session.execute(
            text(f"UPDATE users SET {field} = {field} + :delta WHERE id = :user_id"),
            {"delta": delta, "user_id": user_id}
        )
        result = self.session.execute(
            text(f"SELECT {field} FROM users WHERE id = :user_id"),
            {"user_id": user_id}
        )
        row = result.fetchone()
        return Decimal(str(getattr(row, field, 0))) if row else Decimal('0')

    def _get_balance_after(self, account_type: str, related_user: Optional[int] = None) -> Decimal:
        if related_user and account_type in ['promotion_balance', 'merchant_balance']:
            field = account_type
            result = self.session.execute(
                text(f"SELECT {field} FROM users WHERE id = :user_id"),
                {"user_id": related_user}
            )
            row = result.fetchone()
            return Decimal(str(getattr(row, field, 0))) if row else Decimal('0')
        else:
            return self.get_account_balance(account_type)

    def get_public_welfare_balance(self) -> Decimal:
        return self.get_account_balance('public_welfare')

    def get_public_welfare_flow(self, limit: int = 50) -> List[Dict[str, Any]]:
        result = self.session.execute(
            text("""SELECT id, related_user, change_amount, balance_after, flow_type, remark, created_at
                    FROM account_flow WHERE account_type = 'public_welfare'
                    ORDER BY created_at DESC LIMIT :limit"""),
            {"limit": limit}
        )
        flows = result.fetchall()

        return [{
            "id": f.id,
            "related_user": f.related_user,
            "change_amount": float(f.change_amount),
            "balance_after": float(f.balance_after) if f.balance_after else None,
            "flow_type": f.flow_type,
            "remark": f.remark,
            "created_at": f.created_at.strftime("%Y-%m-%d %H:%M:%S")
        } for f in flows]

    def get_public_welfare_report(self, start_date: str, end_date: str) -> Dict[str, Any]:
        result = self.session.execute(
            text("""SELECT COUNT(*) as total_transactions,
                           SUM(CASE WHEN flow_type = 'income' THEN change_amount ELSE 0 END) as total_income,
                           SUM(CASE WHEN flow_type = 'expense' THEN change_amount ELSE 0 END) as total_expense
                    FROM account_flow WHERE account_type = 'public_welfare'
                    AND DATE(created_at) BETWEEN :start_date AND :end_date"""),
            {"start_date": start_date, "end_date": end_date}
        )
        summary = result.fetchone()

        result = self.session.execute(
            text("""SELECT id, related_user, change_amount, balance_after, flow_type, remark, created_at
                    FROM account_flow WHERE account_type = 'public_welfare'
                    AND DATE(created_at) BETWEEN :start_date AND :end_date
                    ORDER BY created_at DESC"""),
            {"start_date": start_date, "end_date": end_date}
        )
        details = result.fetchall()

        return {
            "summary": {
                "total_transactions": summary.total_transactions or 0,
                "total_income": float(summary.total_income or 0),
                "total_expense": float(summary.total_expense or 0),
                "net_balance": float((summary.total_income or 0) - (summary.total_expense or 0))
            },
            "details": [{
                "id": d.id,
                "related_user": d.related_user,
                "change_amount": float(d.change_amount),
                "balance_after": float(d.balance_after) if d.balance_after else None,
                "flow_type": d.flow_type,
                "remark": d.remark,
                "created_at": d.created_at.strftime("%Y-%m-%d %H:%M:%S")
            } for d in details]
        }

    def set_referrer(self, user_id: int, referrer_id: int) -> bool:
        try:
            result = self.session.execute(
                text("SELECT member_level FROM users WHERE id = :referrer_id"),
                {"referrer_id": referrer_id}
            )
            referrer = result.fetchone()
            if not referrer:
                raise FinanceException(f"推荐人不存在: {referrer_id}")

            if user_id == referrer_id:
                raise FinanceException("不能设置自己为推荐人")

            result = self.session.execute(
                text("SELECT referrer_id FROM user_referrals WHERE user_id = :user_id"),
                {"user_id": user_id}
            )
            if result.fetchone():
                raise FinanceException("用户已存在推荐人，无法重复设置")

            self.session.execute(
                text("INSERT INTO user_referrals (user_id, referrer_id) VALUES (:user_id, :referrer_id)"),
                {"user_id": user_id, "referrer_id": referrer_id}
            )

            self.session.commit()
            logger.info(f"✅ 用户{user_id}的推荐人设置为{referrer_id}（{referrer.member_level}星）")
            return True

        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ 设置推荐人失败: {e}")
            return False

    def get_user_referrer(self, user_id: int) -> Optional[Dict[str, Any]]:
        result = self.session.execute(
            text("""SELECT ur.referrer_id, u.name, u.member_level
                    FROM user_referrals ur JOIN users u ON ur.referrer_id = u.id
                    WHERE ur.user_id = :user_id"""),
            {"user_id": user_id}
        )
        row = result.fetchone()
        return {
            "referrer_id": row.referrer_id,
            "name": row.name,
            "member_level": row.member_level
        } if row else None

    def get_user_team(self, user_id: int, max_layer: int = MAX_TEAM_LAYER) -> List[Dict[str, Any]]:
        result = self.session.execute(
            text("""WITH RECURSIVE team_tree AS (
                        SELECT user_id, referrer_id, 1 as layer FROM user_referrals WHERE referrer_id = :user_id
                        UNION ALL
                        SELECT ur.user_id, ur.referrer_id, tt.layer + 1
                        FROM user_referrals ur JOIN team_tree tt ON ur.referrer_id = tt.user_id
                        WHERE tt.layer < :max_layer
                    )
                    SELECT tt.user_id, u.name, u.member_level, tt.layer
                    FROM team_tree tt JOIN users u ON tt.user_id = u.id
                    ORDER BY tt.layer, tt.user_id"""),
            {"user_id": user_id, "max_layer": max_layer}
        )
        return [{
            "user_id": r.user_id,
            "name": r.name,
            "member_level": r.member_level,
            "layer": r.layer
        } for r in result.fetchall()]

    def check_director_promotion(self) -> bool:
        try:
            logger.info("\n👑 荣誉董事晋升审核")

            result = self.session.execute(text("SELECT id FROM users WHERE member_level = 6"))
            six_star_users = result.fetchall()

            promoted_count = 0
            for user in six_star_users:
                user_id = user.id

                result = self.session.execute(
                    text("""SELECT COUNT(DISTINCT u.id) as count
                            FROM user_referrals ur JOIN users u ON ur.user_id = u.id
                            WHERE ur.referrer_id = :user_id AND u.member_level = 6"""),
                    {"user_id": user_id}
                )
                direct_count = result.fetchone().count

                result = self.session.execute(
                    text("""WITH RECURSIVE team AS (
                                SELECT user_id, referrer_id, 1 as level FROM user_referrals WHERE referrer_id = :user_id
                                UNION ALL
                                SELECT ur.user_id, ur.referrer_id, t.level + 1
                                FROM user_referrals ur JOIN team t ON ur.referrer_id = t.user_id
                                WHERE t.level < 6
                            )
                            SELECT COUNT(DISTINCT t.user_id) as count
                            FROM team t JOIN users u ON t.user_id = u.id
                            WHERE u.member_level = 6"""),
                    {"user_id": user_id}
                )
                total_count = result.fetchone().count

                if direct_count >= 3 and total_count >= 10:
                    result = self.session.execute(
                        text("UPDATE users SET status = 9 WHERE id = :user_id AND status != 9"),
                        {"user_id": user_id}
                    )
                    if result.rowcount > 0:
                        promoted_count += 1
                        logger.info(f"🎉 用户{user_id}晋升为荣誉董事！（直接:{direct_count}, 团队:{total_count}）")

            self.session.commit()
            logger.info(f"👑 荣誉董事审核完成: 晋升{promoted_count}人")
            return True

        except Exception as e:
            self.session.rollback()
            logger.error(f"❌ 荣誉董事审核失败: {e}")
            return False

    def get_user_info(self, user_id: int) -> Dict[str, Any]:
        result = self.session.execute(
            text("""SELECT id, mobile, name, member_level, points, promotion_balance,
                           merchant_points, merchant_balance, status
                    FROM users WHERE id = :user_id"""),
            {"user_id": user_id}
        )
        user = result.fetchone()

        if not user:
            raise FinanceException("用户不存在")

        roles = []
        if user.points > 0 or user.promotion_balance > 0:
            roles.append("普通用户")
        if user.merchant_points > 0 or user.merchant_balance > 0:
            roles.append("商家")

        star_level = "荣誉董事" if user.status == 9 else (f"{user.member_level}星级会员" if user.member_level > 0 else "非会员")

        result = self.session.execute(
            text("""SELECT COUNT(*) as count, SUM(amount) as total_amount
                    FROM coupons WHERE user_id = :user_id AND status = 'unused'"""),
            {"user_id": user_id}
        )
        coupons = result.fetchone()

        return {
            "id": user.id,
            "mobile": user.mobile,
            "name": user.name,
            "member_level": user.member_level,
            "points": user.points,
            "promotion_balance": float(user.promotion_balance),
            "merchant_points": user.merchant_points,
            "merchant_balance": float(user.merchant_balance),
            "roles": roles,
            "star_level": star_level,
            "status": user.status,
            "coupons": {
                "unused_count": coupons.count or 0,
                "total_amount": float(coupons.total_amount or 0)
            }
        }

    def get_user_coupons(self, user_id: int, status: str = 'unused') -> List[Dict[str, Any]]:
        result = self.session.execute(
            text("""SELECT id, coupon_type, amount, status, valid_from, valid_to, used_at, created_at
                    FROM coupons WHERE user_id = :user_id AND status = :status
                    ORDER BY created_at DESC"""),
            {"user_id": user_id, "status": status}
        )
        coupons = result.fetchall()

        return [{
            "id": c.id,
            "coupon_type": c.coupon_type,
            "amount": float(c.amount),
            "status": c.status,
            "valid_from": c.valid_from.strftime("%Y-%m-%d"),
            "valid_to": c.valid_to.strftime("%Y-%m-%d"),
            "used_at": c.used_at.strftime("%Y-%m-%d %H:%M:%S") if c.used_at else None,
            "created_at": c.created_at.strftime("%Y-%m-%d %H:%M:%S")
        } for c in coupons]

    def get_finance_report(self) -> Dict[str, Any]:
        result = self.session.execute(text("""SELECT SUM(points) as points, SUM(promotion_balance) as balance FROM users"""))
        user = result.fetchone()

        result = self.session.execute(text("""SELECT SUM(merchant_points) as points, SUM(merchant_balance) as balance
                                              FROM users WHERE merchant_points > 0 OR merchant_balance > 0"""))
        merchant = result.fetchone()

        result = self.session.execute(text("SELECT account_name, account_type, balance FROM finance_accounts"))
        pools = result.fetchall()

        public_welfare_balance = self.get_public_welfare_balance()

        result = self.session.execute(text("""SELECT COUNT(*) as count, SUM(amount) as total_amount
                                              FROM coupons WHERE status = 'unused'"""))
        coupons = result.fetchone()

        platform_pools = []
        for pool in pools:
            if pool.balance > 0:
                balance = int(pool.balance) if 'points' in pool.account_type else float(pool.balance)
                platform_pools.append({
                    "name": pool.account_name,
                    "type": pool.account_type,
                    "balance": balance
                })

        return {
            "user_assets": {
                "total_points": int(user.points or 0),
                "total_balance": float(user.balance or 0)
            },
            "merchant_assets": {
                "total_points": int(merchant.points or 0),
                "total_balance": float(merchant.balance or 0)
            },
            "platform_pools": platform_pools,
            "public_welfare_fund": {
                "account_name": "公益基金",
                "account_type": "public_welfare",
                "balance": float(public_welfare_balance),
                "reserved": 0.0,
                "remark": "该账户自动汇入1%交易额"
            },
            "coupons_summary": {
                "unused_count": coupons.count or 0,
                "total_amount": float(coupons.total_amount or 0),
                "remark": "周补贴改为发放优惠券"
            }
        }

    def get_account_flow_report(self, limit: int = 50) -> List[Dict[str, Any]]:
        result = self.session.execute(
            text("""SELECT id, account_id, account_type, related_user, change_amount, balance_after, flow_type, remark, created_at
                    FROM account_flow ORDER BY created_at DESC LIMIT :limit"""),
            {"limit": limit}
        )
        flows = result.fetchall()

        return [{
            "id": f.id,
            "account_id": f.account_id,
            "account_type": f.account_type,
            "related_user": f.related_user,
            "change_amount": float(f.change_amount),
            "balance_after": float(f.balance_after) if f.balance_after else None,
            "flow_type": f.flow_type,
            "remark": f.remark,
            "created_at": f.created_at.strftime("%Y-%m-%d %H:%M:%S")
        } for f in flows]

    def get_points_flow_report(self, user_id: Optional[int] = None, limit: int = 50) -> List[Dict[str, Any]]:
        params = {"limit": limit}
        sql = """SELECT id, user_id, change_amount, balance_after, type, reason, related_order, created_at
                 FROM points_log"""

        if user_id:
            sql += " WHERE user_id = :user_id"
            params["user_id"] = user_id

        sql += " ORDER BY created_at DESC LIMIT :limit"

        result = self.session.execute(text(sql), params)
        flows = result.fetchall()

        return [{
            "id": f.id,
            "user_id": f.user_id,
            "change_amount": f.change_amount,
            "balance_after": f.balance_after,
            "type": f.type,
            "reason": f.reason,
            "related_order": f.related_order,
            "created_at": f.created_at.strftime("%Y-%m-%d %H:%M:%S")
        } for f in flows]

    def get_points_deduction_report(self, start_date: str, end_date: str, page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        offset = (page - 1) * page_size
        result = self.session.execute(
            text("""SELECT COUNT(*) as total
                    FROM orders o JOIN points_log pl ON o.id = pl.related_order
                    WHERE o.points_discount > 0 AND pl.type = 'member' AND pl.reason = '积分抵扣支付'
                    AND DATE(o.created_at) BETWEEN :start_date AND :end_date"""),
            {"start_date": start_date, "end_date": end_date}
        )
        total_count = result.fetchone().total

        result = self.session.execute(
            text("""SELECT o.id as order_id, o.order_no, o.user_id, u.name as user_name, u.member_level,
                           o.original_amount, o.points_discount, o.total_amount, ABS(pl.change_amount) as points_used, o.created_at
                    FROM orders o JOIN points_log pl ON o.id = pl.related_order JOIN users u ON o.user_id = u.id
                    WHERE o.points_discount > 0 AND pl.type = 'member' AND pl.reason = '积分抵扣支付'
                    AND DATE(o.created_at) BETWEEN :start_date AND :end_date
                    ORDER BY o.created_at DESC LIMIT :page_size OFFSET :offset"""),
            {
                "start_date": start_date,
                "end_date": end_date,
                "page_size": page_size,
                "offset": offset
            }
        )
        records = result.fetchall()

        result = self.session.execute(
            text("""SELECT COUNT(*) as total_orders, SUM(ABS(pl.change_amount)) as total_points,
                           SUM(o.points_discount) as total_discount_amount
                    FROM orders o JOIN points_log pl ON o.id = pl.related_order
                    WHERE o.points_discount > 0 AND pl.type = 'member' AND pl.reason = '积分抵扣支付'
                    AND DATE(o.created_at) BETWEEN :start_date AND :end_date"""),
            {"start_date": start_date, "end_date": end_date}
        )
        summary = result.fetchone()

        return {
            "summary": {
                "total_orders": summary.total_orders or 0,
                "total_points_used": int(summary.total_points or 0),
                "total_discount_amount": float(summary.total_discount_amount or 0)
            },
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total_count,
                "total_pages": (total_count + page_size - 1) // page_size
            },
            "records": [{
                "order_id": r.order_id,
                "order_no": r.order_no,
                "user_id": r.user_id,
                "user_name": r.user_name,
                "member_level": r.member_level,
                "original_amount": float(r.original_amount),
                "points_discount": float(r.points_discount),
                "total_amount": float(r.total_amount),
                "points_used": int(r.points_used),
                "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S")
            } for r in records]
        }

    def get_transaction_chain_report(self, user_id: int, order_no: Optional[str] = None) -> Dict[str, Any]:
        if order_no:
            result = self.session.execute(
                text("""SELECT id, order_no, total_amount, original_amount, is_member_order
                        FROM orders WHERE order_no = :order_no AND user_id = :user_id"""),
                {"order_no": order_no, "user_id": user_id}
            )
        else:
            result = self.session.execute(
                text("""SELECT id, order_no, total_amount, original_amount, is_member_order
                        FROM orders WHERE user_id = :user_id
                        ORDER BY created_at DESC LIMIT 1"""),
                {"user_id": user_id}
            )

        order = result.fetchone()
        if not order:
            raise FinanceException("未找到订单")

        chain = []
        current_id = user_id
        level = 0

        while current_id and level < MAX_TEAM_LAYER:
            result = self.session.execute(
                text("""SELECT u.id, u.name, u.member_level, ur.referrer_id
                        FROM users u LEFT JOIN user_referrals ur ON u.id = ur.user_id
                        WHERE u.id = :user_id"""),
                {"user_id": current_id}
            )
            user_info = result.fetchone()

            if not user_info:
                break

            level += 1

            result = self.session.execute(
                text("""SELECT reward_amount, created_at FROM team_rewards
                        WHERE order_id = :order_id AND layer = :layer"""),
                {"order_id": order.id, "layer": level}
            )
            team_reward = result.fetchone()

            referral_reward = None
            if level == 1:
                result = self.session.execute(
                    text("""SELECT amount FROM pending_rewards
                            WHERE order_id = :order_id AND reward_type = 'referral' AND status = 'approved'"""),
                    {"order_id": order.id}
                )
                ref_reward = result.fetchone()
                if ref_reward:
                    referral_reward = float(ref_reward.amount)

            chain.append({
                "layer": level,
                "user_id": user_info.id,
                "name": user_info.name,
                "member_level": user_info.member_level,
                "is_referrer": (level == 1),
                "referral_reward": referral_reward,
                "team_reward": {
                    "amount": float(team_reward.reward_amount) if team_reward else 0.00,
                    "has_reward": team_reward is not None
                },
                "referrer_id": user_info.referrer_id
            })

            if not user_info.referrer_id:
                break
            current_id = user_info.referrer_id

        total_referral = chain[0]['referral_reward'] if chain and chain[0]['referral_reward'] else 0.00
        total_team = sum(item['team_reward']['amount'] for item in chain)

        return {
            "order_id": order.id,
            "order_no": order.order_no,
            "is_member_order": bool(order.is_member_order),
            "total_amount": float(order.total_amount),
            "original_amount": float(order.original_amount),
            "reward_summary": {
                "total_referral_reward": total_referral,
                "total_team_reward": total_team,
                "grand_total": total_referral + total_team
            },
            "chain": chain
        }