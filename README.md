# 电商小程序后端api

## 启动前准备

请先创建并配置 `.env` 文件在根目录中，示例如下：

MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_DATABASE=sql_db
---

## 运行 / 启动说明

- **Linux（systemd）**

	如果使用 systemd 管理服务，可按以下步骤操作：

	- 将服务单元文件 `ds.service` 放到 `/etc/systemd/system/`（如尚未部署服务单元）。
	- 重新加载 systemd：

		```bash
		sudo systemctl daemon-reload
		```

	- 启动服务：

		```bash
		sudo systemctl start ds.service
		```

	- 重启服务（在更新后常用）：

		```bash
		sudo systemctl restart ds.service
		```

	- 查看服务状态：

		```bash
		sudo systemctl status ds.service
		```

	- 查看实时日志：

		```bash
		sudo journalctl -u ds.service -f
		```

- **Windows**

	Windows 系统可使用 [uv](https://docs.astral.sh/uv/getting-started/installation/)（按项目约定的工具）运行与调试：

	- 安装 [uv](https://docs.astral.sh/uv/getting-started/installation/)（根据你使用的包管理器或安装方式）。
	- 初始化虚拟环境：

		```powershell
		uv venv
		```

	- 根据 `pyproject.toml`（或项目的 .toml 配置）同步/安装依赖：

		```powershell
		uv sync
		```

	- 以调试/运行模式启动项目：

		```powershell
		uv run main.py
		```

保留说明
启动后，访问 http://127.0.0.1:8000/docs 查看 API 文档。

或者访问 http://127.0.0.1:8000/redoc 查看 ReDoc 文档。

---

## AI 辅助免责声明

本项目在 AI/大型语言模型（包括 GitHub Copilot、ChatGPT 及相关工具）的协助下开发，受到了偶尔知道自己在做什么的人类的监督。

---