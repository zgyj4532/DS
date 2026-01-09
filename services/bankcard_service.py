# services/bankcard_service.py
import base64
import json
import os
import re
import uuid  # ✅ 添加这行
import pymysql
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from core.database import get_conn
from core.logging import get_logger
from core.wx_pay_client import wxpay_client
from core.table_access import build_dynamic_select, build_dynamic_insert, build_dynamic_update
from fastapi import HTTPException

logger = get_logger(__name__)


class BankcardService:
    """统一银行卡管理服务 - 支持绑定、改绑、解绑、查询完整生命周期"""

    @staticmethod
    def _get_wechat_settlement_info_from_api(user_id: int) -> Tuple[str, Dict[str, Any]]:
        """【Mock/真实】获取微信结算账户信息"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sub_mchid, applyment_state, is_draft
                    FROM wx_applyment 
                    WHERE user_id = %s 
                    ORDER BY id DESC 
                    LIMIT 1
                    """,
                    (user_id,)
                )
                record = cur.fetchone()
                if not record:
                    raise Exception("F001: 未找到微信进件记录")
                if record['is_draft'] == 1:
                    raise Exception("F002: 微信进件未完成，无法绑定")
                if record['applyment_state'] != 'APPLYMENT_STATE_FINISHED':
                    raise Exception(f"F003: 微信进件状态异常: {record['applyment_state']}")

                sub_mchid = record['sub_mchid']
                wechat_data = wxpay_client.query_settlement_account(sub_mchid)
                if not wechat_data or not wechat_data.get('account_number'):
                    raise Exception("F004: 微信接口返回数据异常")
                return sub_mchid, wechat_data

    @staticmethod
    def _extract_last_4(card_number: str) -> str:
        return card_number.strip()[-4:]

    @staticmethod
    def _verify_with_wechat_data(
            local_name: str, local_number: str, local_bank: str, wechat_data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """验证数据一致性"""
        try:
            wechat_masked = wechat_data.get('account_number', '')
            wechat_last_4 = BankcardService._extract_last_4(
                wechat_masked.replace('*', '9')
            )
            local_last_4 = BankcardService._extract_last_4(local_number)

            if wechat_last_4 != local_last_4:
                return False, f"卡号后4位不匹配: 输入尾号={local_last_4}, 微信尾号={wechat_last_4}"

            wechat_bank = wechat_data.get('account_bank', '').strip()
            if wechat_bank != local_bank.strip():
                return False, f"开户银行不匹配: 输入='{local_bank}', 微信='{wechat_bank}'"

            verify_result = wechat_data.get('verify_result', '')
            if verify_result != 'VERIFY_SUCCESS':
                fail_reason = wechat_data.get('verify_fail_reason', '')
                return False, f"微信验证未通过: {verify_result}, 原因: {fail_reason}"

            return True, "验证通过"
        except Exception as e:
            return False, f"验证异常: {str(e)}"

    @staticmethod
    def bind_bankcard(
            user_id: int,
            bank_name: str,
            bank_account: str,
            account_name: str,
            bank_branch_id: Optional[str],
            bank_address_code: str,
            is_default: bool = True,
            admin_key: Optional[str] = None,
            ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """绑定银行卡（验证微信数据一致性）"""
        logger.info(f"【绑定开始】user_id={user_id}")
        try:
            sub_mchid, wechat_data = BankcardService._get_wechat_settlement_info_from_api(user_id)
            is_valid, msg = BankcardService._verify_with_wechat_data(
                account_name, bank_account, bank_name, wechat_data
            )
            if not is_valid:
                raise Exception(f"F005: {msg}")

            account_type = BankcardService._map_account_type(wechat_data['account_type'])

            encrypted_number = BankcardService._encrypt_sensitive(bank_account)
            encrypted_name = BankcardService._encrypt_sensitive(account_name)

            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 检查重复绑定
                    cur.execute(
                        """
                        SELECT id FROM merchant_settlement_accounts 
                        WHERE user_id = %s AND account_number_encrypted = %s AND status = 1
                        LIMIT 1
                        """,
                        (user_id, encrypted_number)
                    )
                    if cur.fetchone():
                        raise Exception("F016: 已绑定相同的银行卡")

                    # 获取现有记录
                    cur.execute(
                        "SELECT id FROM merchant_settlement_accounts WHERE user_id = %s AND status = 1 LIMIT 1",
                        (user_id,)
                    )
                    existing_record = cur.fetchone()

                    if existing_record:
                        # 更新旧记录
                        old_data = BankcardService._get_account_record(cur, existing_record['id'])
                        cur.execute(
                            """
                            UPDATE merchant_settlement_accounts 
                            SET account_type = %s, account_bank = %s, bank_name = %s,
                                bank_branch_id = %s, bank_address_code = %s,
                                account_name_encrypted = %s, account_number_encrypted = %s,
                                verify_result = 'VERIFY_SUCCESS', is_default = %s,
                                status = 1, bind_at = NOW(), updated_at = NOW()
                            WHERE id = %s
                            """,
                            (
                                account_type, bank_name[:128], bank_name[:128],
                                bank_branch_id, bank_address_code,
                                encrypted_name, encrypted_number, is_default, existing_record['id']
                            )
                        )
                        new_data = BankcardService._get_account_record(cur, existing_record['id'])
                        conn.commit()
                        BankcardService._log_operation(
                            user_id, 'bind', existing_record['id'], old_data, new_data, admin_key, ip_address
                        )
                        account_id = existing_record['id']
                        action = 'updated'
                    else:
                        # 插入新记录
                        cur.execute(
                            """
                            INSERT INTO merchant_settlement_accounts 
                            (user_id, sub_mchid, account_type, account_bank, bank_name,
                             bank_branch_id, bank_address_code, account_name_encrypted,
                             account_number_encrypted, verify_result, is_default, status, bind_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'VERIFY_SUCCESS', %s, 1, NOW())
                            """,
                            (
                                user_id, sub_mchid, account_type,
                                bank_name[:128], bank_name[:128], bank_branch_id, bank_address_code,
                                encrypted_name, encrypted_number, is_default
                            )
                        )
                        account_id = cur.lastrowid
                        conn.commit()
                        BankcardService._log_operation(
                            user_id, 'bind', account_id, None,
                            {'bank_name': bank_name, 'account_type': account_type},
                            admin_key, ip_address
                        )
                        action = 'created'

                    # 同步到user_bankcards表
                    cur.execute(
                        """
                        INSERT INTO user_bankcards (user_id, bank_name, bank_account)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE bank_account = VALUES(bank_account)
                        """,
                        (user_id, bank_name, bank_account)
                    )
                    conn.commit()

                    return {
                        'msg': 'ok',
                        'account_id': account_id,
                        'action': action,
                        'verify_method': 'wechat_api',
                        'verify_status': 'success'
                    }
        except pymysql.MySQLError as e:
            raise Exception(f"F006: 数据库操作失败 - {e}")
        except Exception as e:
            raise

    @staticmethod
    async def unbind_bankcard(user_id: int, account_id: int, pay_password: str) -> Dict[str, Any]:
        """解绑银行卡"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 验证银行卡是否存在且属于该用户
                cur.execute(
                    """
                    SELECT * FROM merchant_settlement_accounts 
                    WHERE id = %s AND user_id = %s AND status = 1
                    """,
                    (account_id, user_id)
                )
                account = cur.fetchone()
                if not account:
                    raise HTTPException(status_code=404, detail="银行卡不存在")

                # 2. 验证支付密码
                if not BankcardService._verify_pay_password(user_id, pay_password):
                    raise HTTPException(status_code=400, detail="支付密码错误")

                # 3. 检查未完成订单
                cur.execute(
                    """
                    SELECT COUNT(*) as count FROM orders 
                    WHERE merchant_id = %s AND status IN ('pending_pay', 'pending_ship', 'pending_recv')
                    """,
                    (user_id,)
                )
                if cur.fetchone()['count'] > 0:
                    raise HTTPException(status_code=400, detail="存在未完成订单，无法解绑")

                # 4. 软删除
                update_sql = build_dynamic_update(
                    cur,
                    "merchant_settlement_accounts",
                    {
                        "status": 0,
                        "updated_at": datetime.now()
                    },
                    "id = %s"
                )
                cur.execute(update_sql, (account_id,))

                # 5. 记录操作日志
                BankcardService._log_operation(
                    user_id, 'unbind', account_id, None,
                    {'status': 'unbinded'}, 'SYSTEM', '127.0.0.1'
                )
                conn.commit()

                return {"account_id": account_id, "status": "unbinded"}

    @staticmethod
    async def send_sms_code(user_id: int, account_number: str) -> Dict[str, Any]:
        """发送短信验证码（模拟实现）"""
        logger.info(f"【短信验证码】user_id={user_id}, 卡号={account_number[-4:]}")

        # 生产环境应集成真实短信服务商
        return {
            "session_id": str(uuid.uuid4()),
            "expired_in": 300,
            "mock_code": "123456"  # Mock模式下的测试验证码
        }

    @staticmethod
    def list_bankcards(user_id: int) -> List[Dict[str, Any]]:
        """获取银行卡列表（脱敏）"""
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

                for account in accounts:
                    account['account_number_tail'] = BankcardService._extract_last_4(
                        BankcardService._decrypt_local_encrypted(
                            account['account_number_encrypted']
                        )
                    )
                    account.pop('account_number_encrypted', None)
                    account.pop('account_name_encrypted', None)

                return accounts

    @staticmethod
    def set_default_bankcard(user_id: int, account_id: int) -> Dict[str, Any]:
        """设置默认银行卡"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 验证银行卡存在且属于用户
                cur.execute(
                    """
                    SELECT id FROM merchant_settlement_accounts 
                    WHERE id = %s AND user_id = %s AND status = 1
                    """,
                    (account_id, user_id)
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="银行卡不存在")

                # 2. 将所有设为非默认
                cur.execute(
                    "UPDATE merchant_settlement_accounts SET is_default = 0 WHERE user_id = %s",
                    (user_id,)
                )

                # 3. 设置指定卡为默认
                update_sql = build_dynamic_update(
                    cur,
                    "merchant_settlement_accounts",
                    {
                        "is_default": 1,
                        "updated_at": datetime.now()
                    },
                    "id = %s"
                )
                cur.execute(update_sql, (account_id,))

                conn.commit()
                logger.info(f"用户 {user_id} 设置默认银行卡: {account_id}")

                return {"account_id": account_id, "is_default": 1}

    @staticmethod
    def modify_bankcard(
            user_id: int, new_bank_name: str, new_bank_account: str, new_account_name: str,
            bank_branch_id: Optional[str], bank_address_code: Optional[str],
            admin_key: Optional[str], ip_address: Optional[str]
    ) -> Dict[str, Any]:
        """申请改绑银行卡"""
        logger.info(f"【改绑申请开始】user_id={user_id}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 获取当前有效记录
                cur.execute(
                    """
                    SELECT id, sub_mchid, account_type,
                           account_number_encrypted, account_name_encrypted,
                           account_bank, bank_name, modify_application_no
                    FROM merchant_settlement_accounts
                    WHERE user_id=%s AND status=1
                    ORDER BY id DESC LIMIT 1
                    """,
                    (user_id,)
                )
                old = cur.fetchone()
                if not old:
                    raise Exception("F008: 未找到有效银行卡记录")

                if old.get('modify_application_no'):
                    raise Exception("F017: 已存在进行中的改绑申请")

                # 2. 校验新卡是否与旧卡相同
                try:
                    old_plain_number = BankcardService._decrypt_local_encrypted(old['account_number_encrypted'])
                    if old_plain_number == new_bank_account and old['account_bank'] == new_bank_name:
                        raise Exception("F018: 新卡信息与当前绑定卡信息相同")
                except Exception as e:
                    if "F018" in str(e):
                        raise

                # 3. 加密新卡信息
                new_number_enc = BankcardService._encrypt_sensitive(new_bank_account)
                new_name_enc = BankcardService._encrypt_sensitive(new_account_name)

                # 4. 备份旧卡信息
                old_backup = {
                    "account_number_encrypted": old["account_number_encrypted"],
                    "account_name_encrypted": old["account_name_encrypted"],
                    "account_bank": old["account_bank"],
                    "bank_name": old["bank_name"],
                }

                # 5. 调用微信接口
                sub_mchid = old["sub_mchid"]
                wx_resp = wxpay_client.modify_settlement_account(
                    sub_mchid,
                    {
                        "account_type": old["account_type"],
                        "account_bank": new_bank_name[:128],
                        "bank_name": new_bank_name[:128],
                        "bank_branch_id": bank_branch_id or "",
                        "bank_address_code": bank_address_code or "",
                        "account_number": new_bank_account,
                        "account_name": new_account_name,
                    }
                )
                application_no = wx_resp.get("application_no")
                if not application_no:
                    raise Exception("F009: 微信接口未返回申请单号")

                # 6. 更新数据库
                cur.execute(
                    """
                    UPDATE merchant_settlement_accounts
                    SET new_account_number_encrypted=%s,
                        new_account_name_encrypted=%s,
                        new_bank_name=%s,
                        old_account_backup=%s,
                        modify_application_no=%s,
                        verify_result='VERIFYING',
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (
                        new_number_enc,
                        new_name_enc,
                        new_bank_name,
                        json.dumps(old_backup, ensure_ascii=False),
                        application_no,
                        old["id"]
                    )
                )
                conn.commit()

                # 7. 记录日志
                BankcardService._log_operation(
                    user_id, 'modify_apply', old["id"], None,
                    {
                        "application_no": application_no,
                        "new_account": {
                            "bank_name": new_bank_name,
                            "bank_account": new_bank_account,
                            "account_name": new_account_name
                        }
                    },
                    admin_key, ip_address
                )

                return {"msg": "ok", "application_no": application_no, "status": "pending_review"}

    @staticmethod
    def poll_modify_status(user_id: int, application_no: str) -> Dict[str, Any]:
        """轮询改绑审核状态"""
        logger.info(f"【轮询改绑状态】user_id={user_id}, application_no={application_no}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, sub_mchid,
                           new_bank_name,
                           new_account_name_encrypted, new_account_number_encrypted,
                           old_account_backup
                    FROM merchant_settlement_accounts
                    WHERE user_id=%s AND modify_application_no=%s
                    LIMIT 1
                    """,
                    (user_id, application_no)
                )
                record = cur.fetchone()
                if not record:
                    raise Exception("F013: 未找到改绑申请记录")

            # 查询微信状态
            sub_mchid = record["sub_mchid"]
            wx_resp = wxpay_client.query_application_status(sub_mchid, application_no)

            status_map = {
                'APPLYMENT_STATE_EDITTING': 'VERIFYING',
                'APPLYMENT_STATE_AUDITING': 'VERIFYING',
                'APPLYMENT_STATE_REJECTED': 'VERIFY_FAIL',
                'APPLYMENT_STATE_CANCELED': 'VERIFY_FAIL',
                'APPLYMENT_STATE_FINISHED': 'VERIFY_SUCCESS'
            }
            new_status = status_map.get(wx_resp.get('applyment_state'), 'VERIFY_FAIL')
            fail_reason = wx_resp.get('applyment_state_msg', '')

            # 更新数据库
            with get_conn() as conn:
                with conn.cursor() as cur:
                    if new_status == 'VERIFY_SUCCESS':
                        cur.execute(
                            """
                            UPDATE merchant_settlement_accounts
                            SET account_bank=%s,
                                bank_name=%s,
                                account_name_encrypted=%s,
                                account_number_encrypted=%s,
                                verify_result='VERIFY_SUCCESS',
                                modify_application_no=NULL,
                                modify_fail_reason=NULL,
                                new_account_number_encrypted=NULL,
                                new_account_name_encrypted=NULL,
                                new_bank_name=NULL,
                                old_account_backup=NULL,
                                updated_at=NOW()
                            WHERE id=%s
                            """,
                            (
                                record["new_bank_name"],
                                record["new_bank_name"],
                                record["new_account_name_encrypted"],
                                record["new_account_number_encrypted"],
                                record["id"]
                            )
                        )

                        # 同步更新user_bankcards
                        new_plain_number = BankcardService._decrypt_local_encrypted(
                            record["new_account_number_encrypted"]
                        )
                        cur.execute(
                            """
                            UPDATE user_bankcards
                            SET bank_name=%s, bank_account=%s
                            WHERE user_id=%s
                            """,
                            (record["new_bank_name"], new_plain_number, user_id)
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE merchant_settlement_accounts
                            SET verify_result=%s,
                                modify_fail_reason=%s,
                                modify_application_no=NULL,
                                new_account_number_encrypted=NULL,
                                new_account_name_encrypted=NULL,
                                new_bank_name=NULL,
                                old_account_backup=NULL,
                                updated_at=NOW()
                            WHERE id=%s
                            """,
                            (new_status, fail_reason, record["id"])
                        )

                    BankcardService._log_operation(
                        user_id,
                        "modify_success" if new_status == "VERIFY_SUCCESS" else "modify_fail",
                        record["id"],
                        None,
                        {"verify_result": new_status, "fail_reason": fail_reason},
                        "SYSTEM",
                        "127.0.0.1"
                    )
                    conn.commit()

            return {
                "msg": "ok",
                "application_no": application_no,
                "status": new_status,
                "detail": fail_reason,
                "is_completed": new_status in ["VERIFY_SUCCESS", "VERIFY_FAIL"]
            }

    @staticmethod
    def query_bind_status(user_id: int) -> Dict[str, Any]:
        """查询绑定状态"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT sub_mchid FROM wx_applyment WHERE user_id = %s ORDER BY id DESC LIMIT 1",
                    (user_id,)
                )
                sub_mchid_record = cur.fetchone()
                if not sub_mchid_record:
                    return {'is_bound': False, 'reason': '未找到微信进件记录'}

                sub_mchid = sub_mchid_record['sub_mchid']

                cur.execute(
                    """
                    SELECT id, status, verify_result, bind_at 
                    FROM merchant_settlement_accounts 
                    WHERE user_id = %s AND status = 1 LIMIT 1
                    """,
                    (user_id,)
                )
                local_record = cur.fetchone()
                is_bound = local_record is not None

                wechat_display = {}
                if is_bound and sub_mchid:
                    try:
                        wechat_display = wxpay_client.query_settlement_account(sub_mchid)
                    except:
                        pass

                return {
                    'is_bound': is_bound,
                    'account_id': local_record['id'] if local_record else None,
                    'verify_result': local_record['verify_result'] if local_record else None,
                    'wechat_display_info': wechat_display,
                    'bind_at': local_record['bind_at'].strftime('%Y-%m-%d %H:%M:%S') if local_record and local_record[
                        'bind_at'] else None
                }

    @staticmethod
    def get_operation_logs(user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """获取操作日志列表"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, operation_type, target_id, old_val, new_val, admin_key, ip_address, created_at
                    FROM user_bankcard_operations 
                    WHERE user_id = %s ORDER BY created_at DESC LIMIT %s
                    """,
                    (user_id, limit)
                )
                logs = cur.fetchall()
                return [
                    {
                        'id': log['id'],
                        'operation_type': log['operation_type'],
                        'target_id': log['target_id'],
                        'old_val': json.loads(log['old_val']) if log['old_val'] else None,
                        'new_val': json.loads(log['new_val']) if log['new_val'] else None,
                        'admin_key': log['admin_key'],
                        'ip_address': log['ip_address'],
                        'created_at': log['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                    }
                    for log in logs
                ]

    @staticmethod
    def query_my_bankcard(user_id: int) -> Dict[str, Any]:
        """查询我的银行卡（仅返回验证成功的卡）"""
        logger.info(f"【查询我的银行卡】user_id={user_id}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, account_bank, account_number_encrypted, account_name_encrypted,
                           verify_result, bind_at, modify_application_no, modify_fail_reason
                    FROM merchant_settlement_accounts 
                    WHERE user_id = %s AND status = 1 AND verify_result = 'VERIFY_SUCCESS'
                    ORDER BY is_default DESC, bind_at DESC LIMIT 1
                    """,
                    (user_id,)
                )
                record = cur.fetchone()
                if not record:
                    return {"has_bankcard": False, "message": "您尚未绑定银行卡"}

                try:
                    full_account = BankcardService._decrypt_local_encrypted(record['account_number_encrypted'])
                    full_name = BankcardService._decrypt_local_encrypted(record['account_name_encrypted'])

                    result = {
                        "has_bankcard": True,
                        "account_id": record['id'],
                        "account_bank": record['account_bank'],
                        "account_number": full_account,
                        "account_name": full_name,
                        "account_number_tail": full_account[-4:],
                        "verify_result": record['verify_result'],
                        "bind_at": record['bind_at'].strftime('%Y-%m-%d %H:%M:%S') if record['bind_at'] else None
                    }

                    if record['modify_application_no']:
                        result['modify_status'] = 'in_progress'
                        result['modify_application_no'] = record['modify_application_no']
                        if record['verify_result'] == 'VERIFY_FAIL' and record['modify_fail_reason']:
                            result['modify_fail_reason'] = record['modify_fail_reason']
                    else:
                        result['modify_status'] = 'none'

                    return result
                except Exception as e:
                    logger.error(f"解密失败: {e}")
                    return {"has_bankcard": True, "account_id": record['id'], "error": "F015: 解密失败"}

    # ==================== 内部工具方法 ====================
    @staticmethod
    def _get_account_record(cursor, account_id: int) -> Optional[Dict]:
        cursor.execute(
            """
            SELECT id, user_id, account_type, account_bank, bank_name, bank_branch_id,
                   bank_address_code, verify_result, is_default, status
            FROM merchant_settlement_accounts WHERE id = %s
            """,
            (account_id,)
        )
        return cursor.fetchone()

    @staticmethod
    def _log_operation(
            user_id: int, operation_type: str, target_id: Optional[int],
            old_val: Optional[Dict], new_val: Optional[Dict],
            admin_key: Optional[str], ip_address: Optional[str]
    ):
        """审计日志"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO user_bankcard_operations 
                        (user_id, operation_type, target_id, old_val, new_val, admin_key, ip_address)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            user_id, operation_type, target_id,
                            json.dumps(old_val, ensure_ascii=False) if old_val else None,
                            json.dumps(new_val, ensure_ascii=False) if new_val else None,
                            admin_key, ip_address
                        )
                    )
                conn.commit()
        except Exception as e:
            logger.error(f"日志记录失败: {e}")

    @staticmethod
    def _encrypt_sensitive(plaintext: str) -> str:
        """本地AES-GCM加密"""
        key = wxpay_client.apiv3_key[:32]
        return wxpay_client._encrypt_local(plaintext, key)

    @staticmethod
    def _decrypt_local_encrypted(encrypted_data: str) -> str:
        """本地AES-GCM解密"""
        key = wxpay_client.apiv3_key[:32]
        return wxpay_client._decrypt_local(encrypted_data, key)

    @staticmethod
    def _verify_pay_password(user_id: int, pay_password: str) -> bool:
        """验证支付密码（模拟实现）"""
        # 生产环境应查询users表的pay_password_hash字段并验证
        return pay_password == "123456"  # Mock模式

    @staticmethod
    def _map_account_type(wechat_account_type: str) -> str:
        """映射微信账户类型到数据库值"""
        mapping = {
            'ACCOUNT_TYPE_PRIVATE': 'BANK_ACCOUNT_TYPE_PERSONAL',
            'ACCOUNT_TYPE_BUSINESS': 'BANK_ACCOUNT_TYPE_CORPORATE'
        }
        return mapping.get(wechat_account_type, 'BANK_ACCOUNT_TYPE_PERSONAL')