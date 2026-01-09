# core/scheduler.py
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from core.database import get_conn
from core.wx_pay_client import WeChatPayClient  # ✅ 修复：WechatPayClient → WeChatPayClient
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TaskScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.pay_client = WeChatPayClient()  # ✅ 修复：WechatPayClient → WeChatPayClient

    def start(self):
        """启动所有定时任务"""
        # 每天凌晨2点清理过期草稿
        self.scheduler.add_job(
            self.clean_expired_drafts,
            CronTrigger(hour=2, minute=0),
            id="clean_expired_drafts",
            replace_existing=True
        )

        # 每5分钟轮询审核中的进件状态
        self.scheduler.add_job(
            self.poll_applyment_status,
            CronTrigger(minute="*/5"),
            id="poll_applyment_status",
            replace_existing=True
        )

        # 每天9点检查审核超时（超过2个工作日）
        self.scheduler.add_job(
            self.check_audit_timeout,
            CronTrigger(hour=9, minute=0),
            id="check_audit_timeout",
            replace_existing=True
        )

        self.scheduler.start()
        logger.info("定时任务管理器已启动")

    def shutdown(self):
        """关闭定时任务"""
        self.scheduler.shutdown()
        logger.info("定时任务管理器已关闭")

    def clean_expired_drafts(self):
        """清理过期草稿"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM wx_applyment 
                        WHERE draft_expired_at < NOW() 
                        AND is_draft = 1
                    """)
                    deleted = cur.rowcount
                    conn.commit()
                    logger.info(f"清理了 {deleted} 条过期草稿")
        except Exception as e:
            logger.error(f"清理过期草稿失败: {str(e)}", exc_info=True)

    def poll_applyment_status(self):
        """轮询审核中的进件状态"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 获取所有审核中的进件
                    cur.execute("""
                        SELECT applyment_id, user_id 
                        FROM wx_applyment 
                        WHERE applyment_state = 'APPLYMENT_STATE_AUDITING'
                    """)
                    applyments = cur.fetchall()

                    for row in applyments:
                        try:
                            # 调用微信支付API查询状态
                            status_info = self.pay_client.query_applyment_status(row['applyment_id'])
                            new_state = status_info.get("applyment_state")

                            # 如果状态有变化，直接更新数据库并推送
                            if new_state and new_state != 'APPLYMENT_STATE_AUDITING':
                                # 更新状态
                                cur.execute("""
                                    UPDATE wx_applyment 
                                    SET applyment_state = %s, 
                                        applyment_state_msg = %s,
                                        sub_mchid = %s,
                                        finished_at = CASE WHEN %s = 'APPLYMENT_STATE_FINISHED' THEN NOW() ELSE finished_at END
                                    WHERE applyment_id = %s
                                """, (
                                    new_state,
                                    status_info.get("state_msg"),
                                    status_info.get("sub_mchid"),
                                    new_state,
                                    row['applyment_id']
                                ))

                                # 如果审核通过，绑定商户号
                                if new_state == "APPLYMENT_STATE_FINISHED":
                                    cur.execute("""
                                        UPDATE users u
                                        JOIN wx_applyment wa ON u.id = wa.user_id
                                        SET u.wechat_sub_mchid = %s
                                        WHERE wa.applyment_id = %s
                                    """, (status_info.get("sub_mchid"), row['applyment_id']))

                                conn.commit()

                                # 推送通知（使用同步方法）
                                from core.push_service import push_service
                                push_service.send_applyment_status_notification_sync(
                                    row['user_id'],
                                    new_state,
                                    status_info.get("state_msg", "")
                                )
                            else:
                                # 更新轮询时间
                                cur.execute("""
                                    UPDATE wx_applyment 
                                    SET applyment_state_msg = %s,
                                        updated_at = NOW()
                                    WHERE applyment_id = %s
                                """, (status_info.get("state_msg"), row['applyment_id']))
                                conn.commit()

                        except Exception as e:
                            logger.error(f"轮询进件 {row['applyment_id']} 状态失败: {str(e)}")

        except Exception as e:
            logger.error(f"轮询进件状态失败: {str(e)}", exc_info=True)

    def check_audit_timeout(self):
        """检查审核超时（超过2个工作日）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 计算2个工作日前的日期（简化处理）
                    timeout_date = datetime.now() - timedelta(days=3)  # 考虑周末

                    cur.execute("""
                        SELECT wa.id, wa.applyment_id, wa.user_id, u.name, u.mobile
                        FROM wx_applyment wa
                        JOIN users u ON wa.user_id = u.id
                        WHERE wa.applyment_state = 'APPLYMENT_STATE_AUDITING'
                        AND wa.submitted_at < %s
                        AND wa.is_timeout_alerted = 0
                    """, (timeout_date,))

                    timeout_applyments = cur.fetchall()
                    for applyment in timeout_applyments:
                        # 发送超时预警通知
                        logger.warning(
                            f"进件审核超时预警: 用户 {applyment['name']} ({applyment['mobile']}) "
                            f"进件 {applyment['applyment_id']} 已超时2个工作日未处理"
                        )

                        # 标记为已预警
                        cur.execute("""
                            UPDATE wx_applyment 
                            SET is_timeout_alerted = 1
                            WHERE id = %s
                        """, (applyment['id'],))

                    conn.commit()
                    logger.info(f"检查审核超时完成，发现 {len(timeout_applyments)} 条超时记录")

        except Exception as e:
            logger.error(f"检查审核超时失败: {str(e)}", exc_info=True)


scheduler = TaskScheduler()