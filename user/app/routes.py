from fastapi import HTTPException
import uuid
import datetime

from .models import (
    SetStatusReq, AuthReq, AuthResp, UpdateProfileReq, SelfDeleteReq,
    FreezeReq, ResetPwdReq, AdminResetPwdReq, SetLevelReq, AddressReq,
    PointsReq, UserInfoResp
)

from config import get_conn
from ..user_service import UserService, UserStatus, verify_pwd, hash_pwd
from ..address_service import AddressService
from ..points_service import add_points
from ..reward_service import TeamRewardService
from ..director_service import DirectorService


def _err(msg: str):
    raise HTTPException(status_code=400, detail=msg)


def register_routes(app):
    @app.post("/user/set-status", summary="冻结/注销/恢复正常")
    def set_user_status(body: SetStatusReq):
        try:
            ok = UserService.set_status(body.mobile, body.new_status, body.reason)
            return {"success": ok}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/user/auth", summary="一键登录（不存在则自动注册）")
    def user_auth(body: AuthReq):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, password_hash, member_level, status FROM users WHERE mobile=%s", (body.mobile,))
                row = cur.fetchone()

                if row:
                    if not verify_pwd(body.password, row["password_hash"]):
                        raise HTTPException(status_code=400, detail="手机号或密码错误")
                    status = row["status"]
                    if status == UserStatus.FROZEN:
                        raise HTTPException(status_code=403, detail="账号已冻结")
                    if status == UserStatus.DELETED:
                        raise HTTPException(status_code=403, detail="账号已注销")
                    token = str(uuid.uuid4())
                    return AuthResp(uid=row["id"], token=token, level=row["member_level"], is_new=False)

                try:
                    uid = UserService.register(
                        mobile=body.mobile,
                        pwd=body.password,
                        name=body.name,
                        referrer_mobile=None
                    )
                except ValueError as e:
                    raise HTTPException(status_code=400, detail=str(e))

                token = str(uuid.uuid4())
                return AuthResp(uid=uid, token=token, level=0, is_new=True)

    @app.post("/user/update-profile", summary="修改资料（昵称/头像/密码）")
    def update_profile(body: UpdateProfileReq):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, password_hash FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="用户不存在")

                if body.new_password:
                    if not body.old_password:
                        raise HTTPException(status_code=400, detail="请提供旧密码")
                    if not verify_pwd(body.old_password, u["password_hash"]):
                        raise HTTPException(status_code=400, detail="旧密码错误")
                    new_hash = hash_pwd(body.new_password)
                    cur.execute("UPDATE users SET password_hash=%s WHERE id=%s", (new_hash, u["id"]))

                if body.name is not None:
                    cur.execute("UPDATE users SET name=%s WHERE id=%s", (body.name, u["id"]))
                if body.avatar_path is not None:
                    cur.execute("UPDATE users SET avatar_path=%s WHERE id=%s", (body.avatar_path, u["id"]))

                conn.commit()
        return {"msg": "ok"}

    @app.post("/user/self-delete", summary="用户自助注销账号")
    def self_delete(body: SelfDeleteReq):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, password_hash, status FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="用户不存在")

                if not verify_pwd(body.password, u["password_hash"]):
                    raise HTTPException(status_code=403, detail="密码错误")

                cur.execute(
                    "INSERT INTO audit_log(user_id, op_type, old_val, new_val, reason) VALUES (%s,'SELF_DELETE',%s,%s,%s)",
                    (u["id"], int(u["status"]), int(UserStatus.DELETED), body.reason)
                )
                cur.execute("UPDATE users SET status=%s WHERE id=%s", (int(UserStatus.DELETED), u["id"]))
                conn.commit()
        return {"msg": "账号已注销"}

    @app.put("/user/freeze", summary="后台冻结用户")
    def freeze_user(body: FreezeReq):
        if body.admin_key != "admin2025":
            raise HTTPException(status_code=403, detail="后台口令错误")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, status FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="用户不存在")
                if u["status"] == UserStatus.DELETED:
                    raise HTTPException(status_code=400, detail="账号已注销，无法冻结")

                new_status = UserStatus.FROZEN.value
                if u["status"] == new_status:
                    return {"msg": "已是冻结状态"}

                cur.execute(
                    "INSERT INTO audit_log(user_id, op_type, old_val, new_val, reason) VALUES (%s,'FREEZE',%s,%s,%s)",
                    (u["id"], u["status"], new_status, body.reason)
                )
                cur.execute("UPDATE users SET status=%s WHERE id=%s", (new_status, u["id"]))
                conn.commit()
        return {"msg": "已冻结"}

    @app.put("/user/unfreeze", summary="后台解冻用户")
    def unfreeze_user(body: FreezeReq):
        if body.admin_key != "admin2025":
            raise HTTPException(status_code=403, detail="后台口令错误")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, status FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="用户不存在")

                new_status = UserStatus.NORMAL.value
                if u["status"] == new_status:
                    return {"msg": "已是正常状态"}

                cur.execute(
                    "INSERT INTO audit_log(user_id, op_type, old_val, new_val, reason) VALUES (%s,'UNFREEZE',%s,%s,%s)",
                    (u["id"], u["status"], new_status, body.reason)
                )
                cur.execute("UPDATE users SET status=%s WHERE id=%s", (new_status, u["id"]))
                conn.commit()
        return {"msg": "已解冻"}

    @app.post("/user/reset-password", summary="找回密码（短信验证）")
    def reset_password(body: ResetPwdReq):
        if body.sms_code != "111111":
            raise HTTPException(status_code=400, detail="验证码错误")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="手机号未注册")

                new_hash = hash_pwd(body.new_password)
                cur.execute("UPDATE users SET password_hash=%s WHERE id=%s", (new_hash, u["id"]))
                conn.commit()
        return {"msg": "密码已重置"}

    @app.put("/admin/user/reset-pwd", summary="后台重置用户密码")
    def admin_reset_password(body: AdminResetPwdReq):
        if body.admin_key != "admin2025":
            raise HTTPException(status_code=403, detail="后台口令错误")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="用户不存在")

                new_hash = hash_pwd(body.new_password)
                cur.execute("UPDATE users SET password_hash=%s WHERE id=%s", (new_hash, u["id"]))
                cur.execute(
                    "INSERT INTO audit_log(user_id, op_type, old_val, new_val, reason) VALUES (%s,'RESET_PWD',0,1,'后台重置')",
                    (u["id"],)
                )
                conn.commit()
        return {"msg": "密码已重置"}

    @app.post("/user/upgrade", summary="升 1 星")
    def upgrade(mobile: str):
        try:
            new_lv = UserService.upgrade_one_star(mobile)
            return {"new_level": new_lv}
        except ValueError as e:
            _err(str(e))

    @app.post("/user/set-level", summary="后台调星")
    def set_level(body: SetLevelReq):
        try:
            old = UserService.set_level(body.mobile, body.new_level, body.reason)
            return {"old_level": old, "new_level": body.new_level}
        except ValueError as e:
            _err(str(e))

    @app.get("/user/info", summary="用户详情（个人中心）", response_model=UserInfoResp)
    def user_info(mobile: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, mobile, name, avatar_path, member_level, referral_code "
                    "FROM users WHERE mobile=%s AND status != %s",
                    (mobile, UserStatus.DELETED.value)
                )
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="用户不存在或已注销")

                cur.execute(
                    "SELECT ru.mobile, ru.name, ru.member_level "
                    "FROM user_referrals r JOIN users ru ON ru.id=r.referrer_id "
                    "WHERE r.user_id=%s",
                    (u["id"],)
                )
                referrer = cur.fetchone()

                cur.execute(
                    "SELECT COUNT(*) AS c FROM user_referrals WHERE referrer_id=%s",
                    (u["id"],)
                )
                direct_count = cur.fetchone()["c"]

                cur.execute(
                    """
                    WITH RECURSIVE team AS (
                        SELECT id, 0 AS layer FROM users WHERE id=%s
                        UNION ALL
                        SELECT r.user_id, t.layer + 1
                        FROM user_referrals r
                        JOIN team t ON t.id = r.referrer_id
                        WHERE t.layer < 6
                    )
                    SELECT COUNT(*) - 1 AS c FROM team
                    """,
                    (u["id"],)
                )
                team_total = cur.fetchone()["c"]

                cur.execute(
                    "SELECT member_points, merchant_points, withdrawable_balance "
                    "FROM users WHERE id=%s",
                    (u["id"],)
                )
                assets = cur.fetchone()

        return UserInfoResp(
            uid=u["id"],
            mobile=u["mobile"],
            name=u["name"],
            avatar_path=u["avatar_path"],
            member_level=u["member_level"],
            referral_code=u["referral_code"],
            direct_count=direct_count,
            team_total=team_total,
            assets={
                "member_points": assets["member_points"],
                "merchant_points": assets["merchant_points"],
                "withdrawable_balance": assets["withdrawable_balance"]
            },
            referrer=referrer
        )

    @app.get("/user/list", summary="分页列表+筛选")
    def user_list(
        id_start: int = None,
        id_end: int = None,
        level_start: int = 0,
        level_end: int = 6,
        page: int = 1,
        size: int = 20,
    ):
        if level_start > level_end or (id_start is not None and id_end is not None and id_start > id_end):
            _err("区间左值不能大于右值")
        where, args = [], []
        if id_start is not None:
            where.append("id >= %s")
            args.append(id_start)
        if id_end is not None:
            where.append("id <= %s")
            args.append(id_end)
        where.append("member_level BETWEEN %s AND %s")
        args.extend([level_start, level_end])
        sql_where = "WHERE " + " AND ".join(where) if where else ""
        limit_sql = "LIMIT %s OFFSET %s"
        args.extend([size, (page - 1) * size])
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT id, mobile, name, member_level, created_at FROM users {sql_where} ORDER BY id {limit_sql}", args)
                rows = cur.fetchall()
                cur.execute(f"SELECT COUNT(*) AS c FROM users {sql_where}", args[:-2])
                total = cur.fetchone()["c"]
                return {"rows": rows, "total": total, "page": page, "size": size}

    @app.post("/user/bind-referrer", summary="绑定推荐人")
    def bind_referrer(mobile: str, referrer_mobile: str):
        try:
            UserService.bind_referrer(mobile, referrer_mobile)
            return {"msg": "ok"}
        except ValueError as e:
            _err(str(e))

    @app.get("/user/refer-direct", summary="直推列表")
    def refer_direct(mobile: str, page: int = 1, size: int = 10):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (mobile,))
                u = cur.fetchone()
                if not u:
                    _err("用户不存在")
                cur.execute("SELECT COUNT(*) AS c FROM user_referrals WHERE referrer_id=%s", (u["id"],))
                total = cur.fetchone()["c"]
                cur.execute("""
                    SELECT u.id, u.mobile, u.name, u.member_level, u.created_at
                    FROM user_referrals r
                    JOIN users u ON u.id = r.user_id
                    WHERE r.referrer_id=%s
                    ORDER BY u.created_at DESC
                    LIMIT %s OFFSET %s
                """, (u["id"], size, (page - 1) * size))
                rows = cur.fetchall()
                return {"rows": rows, "total": total, "page": page, "size": size}

    @app.get("/user/refer-team", summary="团队列表（递归）")
    def refer_team(mobile: str, max_layer: int = 6):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    WITH RECURSIVE team AS (
                        SELECT id, mobile, name, member_level, 0 AS layer FROM users WHERE mobile=%s
                        UNION ALL
                        SELECT u.id, u.mobile, u.name, u.member_level, t.layer + 1
                        FROM user_referrals r
                        JOIN users u ON u.id = r.user_id
                        JOIN team t ON t.id = r.referrer_id
                        WHERE t.layer < %s
                    )
                    SELECT id, mobile, name, member_level, layer
                    FROM team
                    WHERE layer > 0
                    ORDER BY layer, id
                """, (mobile, max_layer))
                rows = cur.fetchall()
                return {"rows": rows}

    # 地址模块
    @app.post("/address", summary="新增地址")
    def address_add(body: AddressReq):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    _err("用户不存在")
                addr_id = AddressService.add_address(
                    u["id"], body.name, body.phone, body.province, body.city,
                    body.district, body.detail, body.is_default, body.addr_type
                )
                return {"addr_id": addr_id}

    @app.put("/address/default", summary="把已有地址设为默认")
    def set_default_addr(addr_id: int, mobile: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM addresses WHERE id=%s", (addr_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="地址不存在")
                cur.execute("SELECT id FROM users WHERE mobile=%s", (mobile,))
                u = cur.fetchone()
                if not u or u["id"] != row["user_id"]:
                    raise HTTPException(status_code=403, detail="地址不属于当前用户")

                cur.execute("UPDATE addresses SET is_default=0 WHERE user_id=%s", (u["id"],))
                cur.execute("UPDATE addresses SET is_default=1 WHERE id=%s", (addr_id,))
                conn.commit()
        return {"msg": "ok"}

    @app.delete("/address/{addr_id}", summary="删除地址")
    def delete_addr(addr_id: int, mobile: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM addresses WHERE id=%s", (addr_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="地址不存在")
                cur.execute("SELECT id FROM users WHERE mobile=%s", (mobile,))
                u = cur.fetchone()
                if not u or u["id"] != row["user_id"]:
                    raise HTTPException(status_code=403, detail="地址不属于当前用户")

                cur.execute("DELETE FROM addresses WHERE id=%s", (addr_id,))
                conn.commit()
        return {"msg": "ok"}

    @app.get("/address/list", summary="地址列表")
    def address_list(mobile: str, page: int = 1, size: int = 5):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (mobile,))
                u = cur.fetchone()
                if not u:
                    _err("用户不存在")
                rows = AddressService.get_address_list(u["id"], page, size)
                return {"rows": rows}

    @app.post("/address/return", summary="商家设置退货地址")
    def return_addr_set(body: AddressReq):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (body.mobile,))
                u = cur.fetchone()
                if not u:
                    _err("商家不存在")
                addr_id = AddressService.add_address(
                    u["id"], body.name, body.phone, body.province, body.city,
                    body.district, body.detail, is_default=True, addr_type="return"
                )
                return {"addr_id": addr_id}

    @app.get("/address/return", summary="查看退货地址")
    def return_addr_get(mobile: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (mobile,))
                u = cur.fetchone()
                if not u:
                    _err("商家不存在")
                addr = AddressService.get_default_address(u["id"])
                if not addr:
                    _err("未设置退货地址")
                return addr

    # 积分模块
    @app.post("/points", summary="增减积分")
    def points(body: PointsReq):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM users WHERE mobile=%s", (body.mobile,))
                    row = cur.fetchone()
                    if not row:
                        raise HTTPException(status_code=404, detail="用户不存在")
                    user_id = row["id"]
            add_points(user_id, body.points_type, body.amount, body.reason)
            return {"msg": "ok"}
        except ValueError as e:
            _err(str(e))

    @app.get("/points/balance", summary="积分余额")
    def points_balance(mobile: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT member_points, merchant_points, withdrawable_balance FROM users WHERE mobile=%s", (mobile,))
                row = cur.fetchone()
                if not row:
                    _err("用户不存在")
                return row

    @app.get("/points/log", summary="积分流水")
    def points_log(mobile: str, points_type: str = "member", page: int = 1, size: int = 10):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (mobile,))
                u = cur.fetchone()
                if not u:
                    _err("用户不存在")
                where, args = ["user_id=%s", "points_type=%s"], [u["id"], points_type]
                sql_where = " AND ".join(where)
                sql = f"""
                    SELECT change_amount, reason, related_order, created_at
                    FROM points_log
                    WHERE {sql_where}
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """
                args.extend([size, (page - 1) * size])
                cur.execute(sql, args)
                rows = cur.fetchall()
                cur.execute(f"SELECT COUNT(*) AS c FROM points_log WHERE {sql_where}", args[:-2])
                total = cur.fetchone()["c"]
                return {"rows": rows, "total": total, "page": page, "size": size}

    # 团队奖励模块
    @app.get("/reward/list", summary="我的团队奖励")
    def reward_list(mobile: str, page: int = 1, size: int = 10):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE mobile=%s", (mobile,))
                u = cur.fetchone()
                if not u:
                    _err("用户不存在")
                rows = TeamRewardService.get_reward_list_by_user(u["id"], page, size)
                return {"rows": rows}

    @app.get("/reward/by-order/{order_id}", summary="按订单查看奖励")
    def reward_by_order(order_id: int):
        rows = TeamRewardService.get_reward_by_order(order_id)
        return {"rows": rows}

    # 董事模块
    @app.post("/director/try-promote", summary="晋升荣誉董事")
    def director_try_promote(user_id: int):
        ok = DirectorService.try_promote(user_id)
        return {"success": ok}

    @app.get("/director/is", summary="是否荣誉董事")
    def director_is(user_id: int):
        return {"is_director": DirectorService.is_director(user_id)}

    @app.get("/director/dividend", summary="分红明细")
    def director_dividend(user_id: int, page: int = 1, size: int = 10):
        rows = DirectorService.get_dividend_detail(user_id, page, size)
        return {"rows": rows}

    @app.get("/director/list", summary="所有活跃董事")
    def director_list(page: int = 1, size: int = 10):
        rows = DirectorService.list_all_directors(page, size)
        return {"rows": rows}

    @app.post("/director/calc-week", summary="手动触发周分红（仅内部）")
    def director_calc_week(period: datetime.date):
        total_paid = DirectorService.calc_week_dividend(period)
        return {"total_paid": total_paid}

    # 审计日志
    @app.get("/audit", summary="等级变动审计")
    def audit_list(mobile: str = None, page: int = 1, size: int = 10):
        where, args = "", []
        if mobile:
            where = "WHERE u.mobile=%s"
            args.append(mobile)
        with get_conn() as conn:
            with conn.cursor() as cur:
                count_sql = f"SELECT COUNT(*) AS c FROM audit_log a JOIN users u ON u.id=a.user_id {where}"
                cur.execute(count_sql, args)
                total = cur.fetchone()["c"]
                sql = f"""
                    SELECT u.mobile, a.old_val, a.new_val, a.reason, a.created_at
                    FROM audit_log a
                    JOIN users u ON u.id=a.user_id
                    {where}
                    ORDER BY a.created_at DESC
                    LIMIT %s OFFSET %s
                """
                args.extend([size, (page - 1) * size])
                cur.execute(sql, args)
                rows = cur.fetchall()
                return {"rows": rows, "total": total, "page": page, "size": size}

    @app.post("/user/grant-merchant", summary="后台赋予商户身份")
    def grant_merchant(mobile: str, admin_key: str):
        if admin_key != "gm2025":
            raise HTTPException(status_code=403, detail="口令错误")
        if UserService.grant_merchant(mobile):
                return {"msg": "已赋予商户身份"}
        raise HTTPException(status_code=404, detail="用户不存在")

    @app.get("/user/is-merchant", summary="查询是否商户")
    def is_merchant(mobile: str):
        return {"is_merchant": UserService.is_merchant(mobile)}
