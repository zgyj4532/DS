# services/merchant_bankcard_service.py
"""
商家银行卡管理服务
实现银行卡绑定、解绑、验证等核心业务逻辑
"""
import uuid
import datetime
import hashlib
from typing import Dict, Any, Optional, List
from fastapi import HTTPException
from core.database import get_conn
from core.wechat_pay_client import WechatPayClient
from core.config import WECHAT_PAY_MCH_ID
from core.table_access import build_dynamic_select, build_dynamic_insert, build_dynamic_update
import logging

logger = logging.getLogger(__name__)


class MerchantBankcardService:
    """商家银行卡管理服务类"""

    def __init__(self):
        self.pay_client = WechatPayClient()

    async def bind_bankcard(self, user_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """绑定银行卡"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 验证实名认证状态
                cur.execute("""
                    SELECT status FROM merchant_realname_verification 
                    WHERE user_id = %s
                """, (user_id,))
                realname = cur.fetchone()
                if not realname or realname['status'] != 'approved':
                    raise HTTPException(status_code=400, detail="请先完成实名认证")

                # 2. 验证银行卡信息格式
                account_number = data['account_number']
                if not self._validate_bankcard_format(account_number):
                    raise HTTPException(status_code=400, detail="银行卡号格式不正确")

                # 3. 调用银行接口验证（模拟）
                # 实际应调用银行四要素验证接口
                logger.info(f"验证银行卡信息: {account_number}")
                # 模拟验证通过

                # 4. 验证短信验证码（模拟）
                # 实际应调用短信服务商验证
                if not data.get('sms_code'):
                    raise HTTPException(status_code=400, detail="短信验证码不能为空")

                # 模拟验证码验证
                if data['sms_code'] != '123456':  # 生产环境应调用真实短信验证服务
                    raise HTTPException(status_code=400, detail="短信验证码错误")

                # 5. 加密敏感信息
                encrypted_data = self._encrypt_bank_data(data)

                # 6. 检查是否已绑定
                cur.execute("""
                    SELECT id FROM merchant_settlement_accounts 
                    WHERE user_id = %s AND account_number_encrypted = %s AND status = 1
                """, (user_id, encrypted_data['account_number_encrypted']))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail="该银行卡已绑定")

                # 7. 保存到数据库
                insert_data = {
                    "user_id": user_id,
                    "sub_mchid": data.get('sub_mchid'),
                    "account_type": data['account_type'],
                    "account_bank": data['account_bank'],
                    "bank_name": data['bank_name'],
                    "bank_branch_id": data['bank_branch_id'],
                    "bank_address_code": data['bank_address_code'],
                    "account_name_encrypted": encrypted_data['account_name_encrypted'],
                    "account_number_encrypted": encrypted_data['account_number_encrypted'],
                    "verify_result": 'VERIFYING',
                    "is_default": 0,  # 新绑定的设为非默认
                    "status": 1,
                    "created_at": datetime.datetime.now(),
                    "bind_at": datetime.datetime.now()
                }

                insert_sql = build_dynamic_insert(cur, "merchant_settlement_accounts", insert_data)
                cur.execute(insert_sql)
                account_id = cur.lastrowid

                # 8. 如果有审核中的进件，自动更新
                cur.execute("""
                    SELECT id FROM wx_applyment 
                    WHERE user_id = %s AND applyment_state = 'APPLYMENT_STATE_EDITTING'
                    ORDER BY created_at DESC LIMIT 1
                """, (user_id,))
                draft = cur.fetchone()
                if draft:
                    # 更新关联的进件草稿
                    cur.execute("""
                        UPDATE wx_applyment 
                        SET bank_account_info = JSON_SET(
                            COALESCE(bank_account_info, '{}'),
                            '$.account_id', %s
                        )
                        WHERE id = %s
                    """, (account_id, draft['id']))

                conn.commit()
                logger.info(f"用户 {user_id} 绑定银行卡: {account_id}")

                return {
                    "account_id": account_id,
                    "verify_status": "VERIFYING"
                }

    async def unbind_bankcard(self, user_id: int, account_id: int, pay_password: str) -> Dict[str, Any]:
        """解绑银行卡"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 验证银行卡是否存在且属于该用户
                cur.execute("""
                    SELECT * FROM merchant_settlement_accounts 
                    WHERE id = %s AND user_id = %s AND status = 1
                """, (account_id, user_id))
                account = cur.fetchone()
                if not account:
                    raise HTTPException(status_code=404, detail="银行卡不存在")

                # 2. 验证支付密码（模拟）
                # 实际应从users表获取支付密码哈希并验证
                if not self._verify_pay_password(user_id, pay_password):
                    raise HTTPException(status_code=400, detail="支付密码错误")

                # 3. 检查是否有未完成的订单或提现
                cur.execute("""
                    SELECT COUNT(*) as count FROM orders 
                    WHERE merchant_id = %s AND status IN ('pending_pay', 'pending_ship', 'pending_recv')
                """, (user_id,))
                if cur.fetchone()['count'] > 0:
                    raise HTTPException(status_code=400, detail="存在未完成订单，无法解绑")

                # 4. 软删除银行卡
                update_sql = build_dynamic_update(
                    cur,
                    "merchant_settlement_accounts",
                    {
                        "status": 0,
                        "updated_at": datetime.datetime.now()
                    },
                    "id = %s"
                )
                cur.execute(update_sql, (account_id,))

                # 5. 记录操作日志
                log_data = {
                    "user_id": user_id,
                    "operation_type": "unbind",
                    "target_id": account_id,
                    "remark": "解绑银行卡",
                    "admin_key": "SYSTEM",
                    "created_at": datetime.datetime.now()
                }
                insert_sql = build_dynamic_insert(cur, "user_bankcard_operations", log_data)
                cur.execute(insert_sql)

                conn.commit()
                logger.info(f"用户 {user_id} 解绑银行卡: {account_id}")

                return {"account_id": account_id, "status": "unbinded"}

    async def send_sms_code(self, user_id: int, account_number: str) -> Dict[str, Any]:
        """发送短信验证码"""
        # 模拟发送短信验证码
        # 实际应集成阿里云短信、腾讯云短信等
        logger.info(f"发送短信验证码: 用户 {user_id}, 银行卡 {account_number}")

        # 这里只是模拟，实际应：
        # 1. 调用银行接口验证手机号
        # 2. 生成6位验证码
        # 3. 调用短信服务商发送
        # 4. 将验证码存入Redis，设置5分钟过期

        return {
            "session_id": str(uuid.uuid4()),
            "expired_in": 300
        }

    def list_bankcards(self, user_id: int) -> List[Dict[str, Any]]:
        """获取银行卡列表"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                select_sql = build_dynamic_select(
                    cur,
                    "merchant_settlement_accounts",
                    where_clause="user_id = %s AND status = 1",
                    select_fields=[
                        "id",
                        "account_bank",
                        "bank_name",
                        "account_type",
                        "verify_result",
                        "verify_fail_reason",
                        "is_default",
                        "bind_at"
                    ]
                )
                cur.execute(select_sql, (user_id,))
                accounts = cur.fetchall()

                # 脱敏处理：只显示银行卡尾号4位
                for account in accounts:
                    # 解密尾号（实际应从加密字段解密后提取）
                    account['account_number_tail'] = self._get_tail_from_encrypted(
                        account.get('account_number_encrypted', '')
                    )
                    # 移除敏感字段
                    account.pop('account_number_encrypted', None)
                    account.pop('account_name_encrypted', None)

                return accounts

    def set_default_bankcard(self, user_id: int, account_id: int) -> Dict[str, Any]:
        """设置默认银行卡"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 验证银行卡是否存在且属于该用户
                cur.execute("""
                    SELECT id FROM merchant_settlement_accounts 
                    WHERE id = %s AND user_id = %s AND status = 1
                """, (account_id, user_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="银行卡不存在")

                # 2. 将所有设为非默认
                cur.execute("""
                    UPDATE merchant_settlement_accounts 
                    SET is_default = 0 
                    WHERE user_id = %s
                """, (user_id,))

                # 3. 设置指定卡为默认
                update_sql = build_dynamic_update(
                    cur,
                    "merchant_settlement_accounts",
                    {
                        "is_default": 1,
                        "updated_at": datetime.datetime.now()
                    },
                    "id = %s"
                )
                cur.execute(update_sql, (account_id,))

                conn.commit()
                logger.info(f"用户 {user_id} 设置默认银行卡: {account_id}")

                return {"account_id": account_id, "is_default": 1}

    def _validate_bankcard_format(self, account_number: str) -> bool:
        """验证银行卡号格式"""
        # 简单的格式验证：6-19位数字
        import re
        return bool(re.match(r'^\d+$', account_number)) and 6 <= len(account_number) <= 19

    def _encrypt_bank_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """加密银行卡敏感数据"""
        encrypted_data = {}

        # 开户名称
        encrypted_data['account_name_encrypted'] = self.pay_client.encrypt_sensitive_data(
            data['account_name']
        )

        # 银行账号
        encrypted_data['account_number_encrypted'] = self.pay_client.encrypt_sensitive_data(
            data['account_number']
        )

        return encrypted_data

    def _verify_pay_password(self, user_id: int, pay_password: str) -> bool:
        """验证支付密码（模拟）"""
        # 实际应从数据库查询并验证哈希
        # 这里仅做演示
        return pay_password == "123456"  # 生产环境必须实现真实验证

    def _get_tail_from_encrypted(self, encrypted: str) -> str:
        """从加密数据中获取尾号（模拟）"""
        # 实际应解密后获取尾号
        # 这里仅做演示
        return "8888"