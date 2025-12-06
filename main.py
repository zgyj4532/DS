import sys
import pathlib
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 添加项目根目录到路径
sys.path.insert(0, str(pathlib.Path(__file__).parent))

# 导入数据库初始化
from database_setup import initialize_database

# 导入财务管理系统 API
from finance.api_interface import app as finance_app

# 导入用户中心应用
from user.app.app import app as user_app, ensure_database

# 创建主应用
app = FastAPI(
    title="综合管理系统API",
    description="财务管理系统 + 用户中心",
    version="1.0.0"
)

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载财务管理系统路由（保留原有路径）
# 将 finance_app 的所有路由挂载到 /finance 前缀下
for route in finance_app.routes:
    app.routes.append(route)

# 挂载用户中心路由（保留原有路径）
# 将 user_app 的所有路由挂载到主应用
for route in user_app.routes:
    app.routes.append(route)


if __name__ == "__main__":
    # 初始化数据库表结构
    print("正在初始化数据库...")
    initialize_database()
    
    # 确保用户中心数据库
    ensure_database()
    
    print("启动综合管理系统 API...")
    print("财务管理系统 API 文档: http://127.0.0.1:8000/docs")
    print("用户中心 API 文档: http://127.0.0.1:8000/docs")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,  # 如需热重载，请使用: uvicorn main:app --reload
        log_level="info",
        access_log=True
    )
