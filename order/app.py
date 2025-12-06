from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from .routes import register_routes

router = APIRouter()

app = FastAPI(
    title="电商全功能",
    version="1.0",
    description="含商家端 + 全中文接口文档",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载静态页（首页/商家页）
app.mount("/static", StaticFiles(directory="static"), name="static")

# 注册路由
register_routes(app)