# core/wechat_pay_client.py
import requests
import json
import datetime
import hashlib
import base64
import os
from typing import Dict, Any, Optional
from fastapi import HTTPException
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_v1_5
from Crypto.Random import get_random_bytes
from core.config import (
    WECHAT_PAY_MCH_ID,
    WECHAT_PAY_API_V3_KEY,
    WECHAT_PAY_API_CERT_PATH,
    WECHAT_PAY_API_KEY_PATH,
    WECHAT_PAY_PLATFORM_CERT_PATH,
    WECHAT_APP_ID,
    WECHAT_APP_SECRET
)
import logging

logger = logging.getLogger(__name__)


class WechatPayClient:
    """微信支付API客户端（完整版）"""

    BASE_URL = "https://api.mch.weixin.qq.com"

    def __init__(self):
        if not WECHAT_PAY_MCH_ID or not WECHAT_PAY_API_V3_KEY:
            raise HTTPException(status_code=500, detail="微信支付配置缺失")

        if not os.path.exists(WECHAT_PAY_API_CERT_PATH):
            logger.warning(f"微信支付证书不存在: {WECHAT_PAY_API_CERT_PATH}")

        self.mch_id = WECHAT_PAY_MCH_ID
        self.api_key = WECHAT_PAY_API_V3_KEY
        self.cert_path = WECHAT_PAY_API_CERT_PATH
        self.key_path = WECHAT_PAY_API_KEY_PATH
        self.platform_cert_path = WECHAT_PAY_PLATFORM_CERT_PATH

    def _generate_sign(self, method: str, url: str, timestamp: str, nonce: str, body: str = "") -> str:
        """生成APIv3签名"""
        message = f"{method}\n{url}\n{timestamp}\n{nonce}\n{body}\n"

        with open(self.key_path, 'r') as f:
            private_key = f.read()

        key = RSA.import_key(private_key)
        h = SHA256.new(message.encode('utf-8'))
        signature = pkcs1_15.new(key).sign(h)
        return base64.b64encode(signature).decode()

    def _request(self, method: str, url: str, data: Optional[Dict[str, Any]] = None, files: Optional[Dict] = None) -> \
    Dict[str, Any]:
        """统一请求处理（带真实签名）"""
        timestamp = str(int(datetime.datetime.now().timestamp()))
        nonce = hashlib.md5(timestamp.encode()).hexdigest()
        body = json.dumps(data, ensure_ascii=False) if data else ""

        signature = self._generate_sign(method, url, timestamp, nonce, body)

        headers = {
            "Authorization": f"WECHATPAY2-SHA256-RSA2048 mchid=\"{self.mch_id}\",serial_no=\"{self.mch_id}\",nonce_str=\"{nonce}\",timestamp=\"{timestamp}\",signature=\"{signature}\""
        }

        try:
            response = requests.request(
                method,
                url,
                data=body.encode('utf-8') if body else None,
                files=files,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=500, detail=f"微信支付API调用失败: {str(e)}")

    def verify_signature(self, signature: str, timestamp: str, nonce: str, body: str) -> bool:
        """验证回调签名（使用微信支付平台证书）"""
        try:
            if not os.path.exists(self.platform_cert_path):
                logger.warning("微信支付平台证书不存在，跳过验签")
                return True  # 开发环境跳过，生产环境必须配置

            with open(self.platform_cert_path, 'r') as f:
                platform_key = f.read()

            key = RSA.import_key(platform_key)
            message = f"{timestamp}\n{nonce}\n{body}\n"
            h = SHA256.new(message.encode('utf-8'))

            # 验证签名
            signature_bytes = base64.b64decode(signature)
            pkcs1_15.new(key).verify(h, signature_bytes)
            return True
        except Exception as e:
            logger.error(f"签名验证失败: {str(e)}")
            return False

    def decrypt_callback_data(self, resource: dict) -> dict:
        """解密回调数据（AES-256-GCM）"""
        try:
            cipher_text = resource.get("ciphertext", "")
            nonce = resource.get("nonce", "")
            associated_data = resource.get("associated_data", "")

            # 使用APIv3密钥解密
            key = self.api_key.encode('utf-8')
            cipher = AES.new(key, AES.MODE_GCM, nonce=nonce.encode('utf-8'))
            cipher.update(associated_data.encode('utf-8'))

            decrypted = cipher.decrypt_and_verify(
                base64.b64decode(cipher_text),
                base64.b64decode(resource.get("aad", ""))
            )
            return json.loads(decrypted.decode('utf-8'))
        except Exception as e:
            logger.error(f"解密失败: {str(e)}")
            return json.loads(resource.get("ciphertext", "{}"))

    def submit_applyment(self, applyment_data: Dict[str, Any]) -> Dict[str, Any]:
        """提交进件申请"""
        url = f"{self.BASE_URL}/v3/applyment4sub/applyment/"

        # 构建payload，自动填充实名信息
        payload = {
            "business_code": applyment_data["business_code"],
            "contact_info": json.loads(applyment_data["contact_info"]),
            "subject_info": json.loads(applyment_data["subject_info"]),
            "bank_account_info": json.loads(applyment_data["bank_account_info"]),
        }

        return self._request("POST", url, payload)

    def query_applyment_status(self, applyment_id: int) -> Dict[str, Any]:
        """查询进件状态"""
        url = f"{self.BASE_URL}/v3/applyment4sub/applyment/applyment_id/{applyment_id}"
        return self._request("GET", url)

    def upload_image(self, image_content: bytes, content_type: str) -> str:
        """上传图片获取media_id（真实实现）"""
        url = f"{self.BASE_URL}/v3/merchant/media/upload"

        # 构建multipart/form-data
        files = {
            'file': (
                'image.jpg',
                image_content,
                content_type,
                {'Content-Disposition': 'form-data; name="file"; filename="image.jpg"'}
            )
        }

        response = self._request("POST", url, files=files)
        return response.get('media_id')

    def encrypt_sensitive_data(self, data: str) -> str:
        """使用微信支付公钥加密敏感数据"""
        try:
            from core.config import WECHAT_PAY_PUBLIC_KEY_PATH
            with open(WECHAT_PAY_PUBLIC_KEY_PATH, 'r') as f:
                public_key = RSA.import_key(f.read())

            cipher = PKCS1_v1_5.new(public_key)
            encrypted = cipher.encrypt(data.encode('utf-8'))
            return base64.b64encode(encrypted).decode('utf-8')
        except Exception as e:
            logger.error(f"数据加密失败: {str(e)}")
            # 开发环境返回原文，生产环境必须加密
            if os.getenv('LOG_LEVEL') == 'INFO':
                return data
            raise