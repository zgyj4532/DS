import pymysql
from fastapi import FastAPI

from config import CFG
from database_setup import initialize_database
from .routes import register_routes


def ensure_database():
    try:
        pymysql.connect(**CFG, cursorclass=pymysql.cursors.DictCursor).close()
    except pymysql.err.OperationalError as e:
        if e.args[0] == 1049:
            print("📦 数据库不存在，正在自动创建并初始化 …")
            initialize_database()
            print("✅ 自动初始化完成！")
        else:
            raise


ensure_database()


app = FastAPI(title="用户中心", version="1.0.0")


register_routes(app)
