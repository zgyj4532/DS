# core/rate_limiter.py
import time
import threading
import asyncio
from functools import wraps
from collections import defaultdict, deque
from typing import Callable, Any, Optional, Union
import os

from core.logging import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """微信支付V3 API专用限流器（线程安全，支持异步）

    限制策略：每个sub_mchid独立计数，默认1秒5次请求
    参考微信文档：QPS ≥ 10，但结算账户类接口建议更保守
    """

    def __init__(self, max_calls: int = 5, period: int = 1):
        """
        :param max_calls: 时间窗口内最大调用次数
        :param period: 时间窗口（秒）
        """
        self.max_calls = max_calls
        self.period = period
        # 存储结构：{key: deque([timestamp, ...])}
        self.calls = defaultdict(deque)
        self.lock = threading.Lock()

        logger.info(f"RateLimiter初始化: max_calls={max_calls}, period={period}s")

    def __call__(self, func: Callable) -> Callable:
        """装饰器模式：自动识别同步/异步函数"""
        if asyncio.iscoroutinefunction(func):
            return self._async_decorator(func)
        else:
            return self._sync_decorator(func)

    def _sync_decorator(self, func: Callable) -> Callable:
        """同步函数装饰器"""
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # 从参数中提取sub_mchid
            sub_mchid = args[1] if len(args) > 1 else None
            if not sub_mchid:
                logger.warning(f"无法提取sub_mchid，限流器跳过: {func.__name__}")
                return func(*args, **kwargs)

            key = f"{func.__name__}_{sub_mchid}"

            with self.lock:
                now = time.time()
                call_queue = self.calls[key]
                # 清理过期记录
                self._cleanup_expired(call_queue, now)

                # 检查限流
                sleep_time = self._check_limit(call_queue, now)

                # 记录本次调用
                call_queue.append(now)

            # 在锁外等待
            if sleep_time:
                time.sleep(sleep_time)
                # 等待后重新检查
                with self.lock:
                    now = time.time()
                    call_queue = self.calls[key]
                    self._cleanup_expired(call_queue, now)
                    # 重新检查是否仍超限
                    if len(call_queue) >= self.max_calls:
                        raise Exception("限流异常：等待后仍超限，请求过于频繁")

            return func(*args, **kwargs)

        return wrapper

    def _async_decorator(self, func: Callable) -> Callable:
        """异步函数装饰器"""
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            # 从参数中提取sub_mchid
            sub_mchid = args[1] if len(args) > 1 else None
            if not sub_mchid:
                logger.warning(f"无法提取sub_mchid，限流器跳过: {func.__name__}")
                return await func(*args, **kwargs)

            key = f"{func.__name__}_{sub_mchid}"

            with self.lock:
                now = time.time()
                call_queue = self.calls[key]
                # 清理过期记录
                self._cleanup_expired(call_queue, now)

                # 检查限流
                sleep_time = self._check_limit(call_queue, now)

                # 记录本次调用
                call_queue.append(now)

            # 在锁外等待
            if sleep_time:
                await asyncio.sleep(sleep_time)
                # 等待后重新检查
                with self.lock:
                    now = time.time()
                    call_queue = self.calls[key]
                    self._cleanup_expired(call_queue, now)
                    # 重新检查是否仍超限
                    if len(call_queue) >= self.max_calls:
                        raise Exception("限流异常：等待后仍超限，请求过于频繁")

            return await func(*args, **kwargs)

        return async_wrapper

    def _cleanup_expired(self, queue: deque, now: float):
        """清理过期记录"""
        while queue and now - queue[0] > self.period:
            queue.popleft()

    def _check_limit(self, queue: deque, now: float) -> Optional[float]:
        """检查是否超限，返回等待时间"""
        if len(queue) >= self.max_calls:
            wait_time = self.period - (now - queue[0])
            logger.warning(
                f"微信接口限流触发: 当前{len(queue)}次/{self.period}秒, "
                f"需等待{wait_time:.2f}秒"
            )
            return max(0, wait_time)
        return None

    def get_stats(self, key: Optional[str] = None) -> dict:
        """获取限流统计（清理后）"""
        with self.lock:
            now = time.time()
            if key:
                queue = self.calls.get(key, deque())
                self._cleanup_expired(queue, now)
                return {
                    "key": key,
                    "current_calls": len(queue),
                    "window_seconds": self.period,
                    "max_calls": self.max_calls
                }
            else:
                stats = {}
                for k, q in self.calls.items():
                    self._cleanup_expired(q, now)
                    stats[k] = {
                        "current_calls": len(q),
                        "oldest_call_age": now - q[0] if q else None
                    }
                return stats

    def reset(self, key: Optional[str] = None):
        """重置限流计数（用于测试）"""
        with self.lock:
            if key:
                if key in self.calls:
                    del self.calls[key]
                logger.info(f"限流计数已重置: {key}")
            else:
                self.calls.clear()
                logger.info("限流计数已全局重置")


# 全局限流器实例
# 建议：结算账户类接口更严格（5次/秒），查询类可放宽（10次/秒）
settlement_rate_limiter = RateLimiter(max_calls=5, period=1)
query_rate_limiter = RateLimiter(max_calls=10, period=1)