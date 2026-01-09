# core/wx_pay_client.py
import json
import time
import uuid
import base64
import os
import hashlib
import datetime
from typing import Dict, Any, Optional
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import requests
from core.config import (
    WECHAT_PAY_MCH_ID, WECHAT_PAY_API_V3_KEY,
    WECHAT_PAY_API_CERT_PATH, WECHAT_PAY_API_KEY_PATH,
    WECHAT_PAY_PLATFORM_CERT_PATH, WECHAT_APP_ID, WECHAT_APP_SECRET
)
from core.database import get_conn
from core.logging import get_logger

logger = get_logger(__name__)


class WeChatPayClient:
    """微信支付V3 API客户端（完整版，支持进件+结算账户+Mock模式）"""

    BASE_URL = "https://api.mch.weixin.qq.com"

    def __init__(self):
        # ✅ Mock模式开关
        self.mock_mode = os.getenv('WX_MOCK_MODE', 'false').lower() == 'true'
        if self.mock_mode:
            logger.warning("⚠️ 【MOCK模式】已启用，所有微信接口调用均为模拟！")

        # 商户配置
        self.mchid = WECHAT_PAY_MCH_ID
        self.apiv3_key = WECHAT_PAY_API_V3_KEY.encode('utf-8')
        self.cert_path = WECHAT_PAY_API_CERT_PATH
        self.key_path = WECHAT_PAY_API_KEY_PATH
        self.platform_cert_path = WECHAT_PAY_PLATFORM_CERT_PATH

        # Mock模式下不强制加载证书
        self.private_key = self._load_private_key()
        self.wechat_public_key = self._load_wechat_public_key()

        # 初始化Mock测试数据（如需要）
        if self.mock_mode:
            self._ensure_mock_applyment_exists()

    # ==================== MOCK数据生成器 ====================
    def _ensure_mock_applyment_exists(self):
        """确保Mock模式下有测试用的进件记录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT user_id FROM wx_applyment 
                        WHERE user_id = 1 AND applyment_state = 'APPLYMENT_STATE_FINISHED'
                    """)
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO wx_applyment 
                            (user_id, business_code, sub_mchid, applyment_state, is_draft,
                             subject_type, subject_info, contact_info, bank_account_info)
                            VALUES (1, 'MOCK_BUSINESS_001', 'MOCK_SUB_MCHID_001', 
                                    'APPLYMENT_STATE_FINISHED', 0,
                                    'SUBJECT_TYPE_INDIVIDUAL', '{}', '{}', '{}')
                        """)
                        conn.commit()
                        logger.info("✅ Mock模式：已自动创建测试进件记录 (user_id=1)")
        except Exception as e:
            logger.debug(f"Mock初始化失败（可忽略）: {e}")

    def _generate_mock_application_no(self, sub_mchid: str) -> str:
        """生成模拟的申请单号"""
        return f"MOCK_APP_{int(time.time())}_{sub_mchid}"

    def _get_mock_settlement_data(self, sub_mchid: str) -> Dict[str, Any]:
        """模拟微信结算账户查询返回 - 从数据库读取实际数据"""
        logger.info(f"【MOCK】模拟查询结算账户: sub_mchid={sub_mchid} (从数据库读取)")

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT account_bank, bank_name, 
                               account_number_encrypted, account_name_encrypted,
                               bank_address_code
                        FROM merchant_settlement_accounts
                        WHERE sub_mchid = %s AND status = 1
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (sub_mchid,)
                    )
                    record = cur.fetchone()

                    if record:
                        try:
                            # 使用与BankcardService相同的解密逻辑
                            full_number = self._decrypt_local_encrypted(record['account_number_encrypted'])
                            masked_number = full_number[:6] + '*' * (len(full_number) - 10) + full_number[-4:]

                            full_name = self._decrypt_local_encrypted(record['account_name_encrypted'])

                            return {
                                'account_type': 'ACCOUNT_TYPE_PRIVATE',
                                'account_bank': record['account_bank'],
                                'bank_name': record['bank_name'] or record['account_bank'],
                                'account_number': masked_number,
                                'account_name': full_name,
                                'verify_result': 'VERIFY_SUCCESS',
                                'verify_fail_reason': '',
                                'bank_address_code': record['bank_address_code'] or '100000'
                            }
                        except Exception as e:
                            logger.warning(f"Mock解密失败: {e}")

                    # 返回默认Mock数据
                    return {
                        'account_type': 'ACCOUNT_TYPE_PRIVATE',
                        'account_bank': '工商银行',
                        'bank_name': '中国工商银行股份有限公司北京朝阳支行',
                        'account_number': '6222021234567890000',
                        'account_name': '测试用户',
                        'verify_result': 'VERIFY_SUCCESS',
                        'verify_fail_reason': '',
                        'bank_address_code': '100000'
                    }
        except Exception as e:
            logger.warning(f"Mock读取数据库失败: {e}")
            return {
                'account_type': 'ACCOUNT_TYPE_PRIVATE',
                'account_bank': '工商银行',
                'bank_name': '中国工商银行股份有限公司北京朝阳支行',
                'account_number': '6222021234567890000',
                'account_name': '测试用户',
                'verify_result': 'VERIFY_SUCCESS',
                'verify_fail_reason': '',
                'bank_address_code': '100000'
            }

    def _get_mock_application_status(self, application_no: str) -> Dict[str, Any]:
        """模拟微信申请状态查询"""
        try:
            app_time = int(application_no.split('_')[2])
            elapsed = time.time() - app_time
        except:
            elapsed = 999

        if elapsed < 5:
            return {
                'applyment_state': 'APPLYMENT_STATE_AUDITING',
                'applyment_state_msg': '审核中，请稍后...'
            }
        else:
            if os.getenv('WX_MOCK_APPLY_RESULT') == 'FAIL':
                return {
                    'applyment_state': 'APPLYMENT_STATE_REJECTED',
                    'applyment_state_msg': '银行账户信息有误'
                }
            return {
                'applyment_state': 'APPLYMENT_STATE_FINISHED',
                'applyment_state_msg': '审核通过'
            }

    # ==================== 证书加载 ====================
    def _load_private_key(self):
        """加载商户私钥（PEM格式）"""
        try:
            with open(self.key_path, 'rb') as f:
                return serialization.load_pem_private_key(
                    f.read(),
                    password=None,
                    backend=default_backend()
                )
        except Exception as e:
            logger.error(f"加载微信支付私钥失败: {e}")
            if not self.mock_mode:
                raise
            return None

    def _load_wechat_public_key(self):
        """加载微信支付平台公钥"""
        try:
            with open(self.platform_cert_path, 'rb') as f:
                return serialization.load_pem_public_key(
                    f.read(),
                    backend=default_backend()
                )
        except Exception as e:
            logger.warning(f"加载微信支付平台公钥失败: {e}")
            if not self.mock_mode:
                raise
            return None

    # ==================== 加密与签名 ====================
    def _rsa_encrypt_with_wechat_public_key(self, plaintext: str) -> str:
        """使用微信支付平台公钥加密（用于敏感数据）"""
        if self.mock_mode:
            logger.info(f"【MOCK】模拟RSA加密: {plaintext[:5]}...")
            return base64.b64encode(f"MOCK_ENC_{plaintext}".encode()).decode()

        if not self.wechat_public_key:
            raise Exception("微信支付平台公钥未加载")

        ciphertext = self.wechat_public_key.encrypt(
            plaintext.encode('utf-8'),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return base64.b64encode(ciphertext).decode('utf-8')

    def _sign(self, method: str, url: str, timestamp: str, nonce_str: str, body: str = '') -> str:
        """RSA-SHA256签名"""
        if self.mock_mode:
            return "MOCK_SIGNATURE"

        sign_str = f'{method}\n{url}\n{timestamp}\n{nonce_str}\n{body}\n'
        signature = self.private_key.sign(
            sign_str.encode('utf-8'),
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    def _build_auth_header(self, method: str, url: str, body: str = '') -> str:
        """构建Authorization请求头"""
        timestamp = str(int(time.time()))
        nonce_str = str(uuid.uuid4()).replace('-', '')
        signature = self._sign(method, url, timestamp, nonce_str, body)
        auth_str = f'mchid="{self.mchid}",serial_no="{self.mchid}",nonce_str="{nonce_str}",timestamp="{timestamp}",signature="{signature}"'
        return f'WECHATPAY2-SHA256-RSA2048 {auth_str}'

    # ==================== 进件相关API（来自wechat_pay_client.py） ====================
    def submit_applyment(self, applyment_data: Dict[str, Any]) -> Dict[str, Any]:
        """提交进件申请"""
        if self.mock_mode:
            logger.info("【MOCK】模拟提交进件申请")
            return {
                "applyment_id": int(time.time()),
                "state_msg": "提交成功"
            }

        url = f"{self.BASE_URL}/v3/applyment4sub/applyment/"
        payload = {
            "business_code": applyment_data["business_code"],
            "contact_info": json.loads(applyment_data["contact_info"]),
            "subject_info": json.loads(applyment_data["subject_info"]),
            "bank_account_info": json.loads(applyment_data["bank_account_info"]),
        }

        body_str = json.dumps(payload, ensure_ascii=False)
        headers = {
            'Authorization': self._build_auth_header('POST', url, body_str),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        response = requests.post(url, data=body_str.encode('utf-8'), headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def query_applyment_status(self, applyment_id: int) -> Dict[str, Any]:
        """查询进件状态"""
        if self.mock_mode:
            logger.info(f"【MOCK】模拟查询进件状态: {applyment_id}")
            return self._get_mock_application_status(f"MOCK_{applyment_id}")

        url = f"{self.BASE_URL}/v3/applyment4sub/applyment/applyment_id/{applyment_id}"
        headers = {'Authorization': self._build_auth_header('GET', url), 'Accept': 'application/json'}

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def upload_image(self, image_content: bytes, content_type: str) -> str:
        """上传图片获取media_id"""
        if self.mock_mode:
            logger.info("【MOCK】模拟上传图片")
            return f"MOCK_MEDIA_{int(time.time())}"

        url = f"{self.BASE_URL}/v3/merchant/media/upload"
        files = {
            'file': (
                'image.jpg',
                image_content,
                content_type,
                {'Content-Disposition': 'form-data; name="file"; filename="image.jpg"'}
            )
        }

        headers = {'Authorization': self._build_auth_header('POST', url)}
        response = requests.post(url, files=files, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json().get('media_id')

    def verify_signature(self, signature: str, timestamp: str, nonce: str, body: str) -> bool:
        """验证回调签名"""
        if self.mock_mode:
            logger.info("【MOCK】跳过签名验证")
            return True

        try:
            if not os.path.exists(self.platform_cert_path):
                logger.warning("微信支付平台证书不存在，跳过验签")
                return True

            with open(self.platform_cert_path, 'rb') as f:
                platform_key = serialization.load_pem_public_key(
                    f.read(),
                    backend=default_backend()
                )

            message = f"{timestamp}\n{nonce}\n{body}\n"
            signature_bytes = base64.b64decode(signature)

            platform_key.verify(
                signature_bytes,
                message.encode('utf-8'),
                padding.PKCS1v15(),
                hashes.SHA256()
            )
            return True
        except Exception as e:
            logger.error(f"签名验证失败: {str(e)}")
            return False

    def decrypt_callback_data(self, resource: dict) -> dict:
        """解密回调数据（AES-256-GCM）"""
        if self.mock_mode:
            logger.info("【MOCK】模拟解密回调数据")
            return json.loads(resource.get("ciphertext", "{}"))

        try:
            cipher_text = resource.get("ciphertext", "")
            nonce = resource.get("nonce", "")
            associated_data = resource.get("associated_data", "")

            key = self.apiv3_key
            aesgcm = AESGCM(key)
            # 修复：删除错误的 cipher.update_associated_data 调用
            # AESGCM 的 decrypt 方法直接处理关联数据

            decrypted = aesgcm.decrypt(
                nonce.encode('utf-8'),
                base64.b64decode(cipher_text),
                associated_data.encode('utf-8')
            )
            return json.loads(decrypted.decode('utf-8'))
        except Exception as e:
            logger.error(f"解密失败: {str(e)}")
            return json.loads(resource.get("ciphertext", "{}"))

    def encrypt_sensitive_data(self, data: str) -> str:
        """使用微信支付公钥加密敏感数据（兼容旧客户端）"""
        return self._rsa_encrypt_with_wechat_public_key(data)

    # ==================== 结算账户相关API（来自wx_pay_client.py） ====================
    def query_settlement_account(self, sub_mchid: str) -> Dict[str, Any]:
        """查询结算账户"""
        if self.mock_mode:
            return self._get_mock_settlement_data(sub_mchid)

        url = f'/v3/apply4sub/sub_merchants/{sub_mchid}/settlement'
        headers = {'Authorization': self._build_auth_header('GET', url), 'Accept': 'application/json'}

        response = requests.get(self.BASE_URL + url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        return {
            'account_type': data.get('account_type'),
            'account_bank': data.get('account_bank'),
            'bank_name': data.get('bank_name'),
            'account_number': data.get('account_number'),
            'account_name': data.get('account_name'),
            'verify_result': data.get('verify_result', 'VERIFYING'),
            'verify_fail_reason': data.get('verify_fail_reason', '')
        }

    def modify_settlement_account(self, sub_mchid: str, account_info: Dict[str, Any]) -> Dict[str, Any]:
        """修改结算账户"""
        if self.mock_mode:
            logger.info(f"【MOCK】模拟提交改绑申请: sub_mchid={sub_mchid}")
            return {
                'application_no': self._generate_mock_application_no(sub_mchid),
                'sub_mchid': sub_mchid,
                'status': 'APPLYMENT_STATE_AUDITING'
            }

        url = f'/v3/apply4sub/sub_merchants/{sub_mchid}/modify-settlement'
        body = {
            "account_type": account_info['account_type'],
            "account_bank": account_info['account_bank'],
            "bank_name": account_info.get('bank_name', ''),
            "bank_branch_id": account_info.get('bank_branch_id', ''),
            "bank_address_code": account_info['bank_address_code'],
            "account_number": self._rsa_encrypt_with_wechat_public_key(account_info['account_number']),
            "account_name": self._rsa_encrypt_with_wechat_public_key(account_info['account_name'])
        }
        body_str = json.dumps(body, ensure_ascii=False)
        headers = {
            'Authorization': self._build_auth_header('POST', url, body_str),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        response = requests.post(self.BASE_URL + url, data=body_str.encode('utf-8'), headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def query_application_status(self, sub_mchid: str, application_no: str) -> Dict[str, Any]:
        """查询改绑申请状态"""
        if self.mock_mode:
            return self._get_mock_application_status(application_no)

        url = f'/v3/apply4sub/sub_merchants/{sub_mchid}/application/{application_no}'
        headers = {'Authorization': self._build_auth_header('GET', url), 'Accept': 'application/json'}

        response = requests.get(self.BASE_URL + url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    # ==================== 本地加密解密工具 ====================
    @staticmethod
    def _encrypt_local(plaintext: str, key: bytes) -> str:
        """本地AES-GCM加密"""
        iv = os.urandom(12)
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(iv, plaintext.encode('utf-8'), b'')
        return base64.b64encode(iv + ciphertext).decode('utf-8')

    @staticmethod
    def _decrypt_local(encrypted_data: str, key: bytes) -> str:
        """本地AES-GCM解密"""
        combined = base64.b64decode(encrypted_data)
        iv, ciphertext = combined[:12], combined[12:]
        aesgcm = AESGCM(key)
        return aesgcm.decrypt(iv, ciphertext, b'').decode('utf-8')


# 全局客户端实例
wxpay_client = WeChatPayClient()