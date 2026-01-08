# core/response.py
from typing import Any, Dict
from core.json_response import DecimalJSONResponse

def success_response(data: Any = None, message: str = "操作成功", code: int = 0) -> Dict[str, Any]:
    """
    成功响应
    """
    return {
        "code": code,
        "message": message,
        "data": data,
        "success": True
    }

def error_response(message: str = "操作失败", code: int = -1, data: Any = None) -> Dict[str, Any]:
    """
    错误响应
    """
    return {
        "code": code,
        "message": message,
        "data": data,
        "success": False
    }

class ApiResponse(DecimalJSONResponse):
    """
    自定义响应类（如果需要更复杂的响应处理）
    """
    pass