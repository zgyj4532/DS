pip install -r requirements.txt

python main.py

启动后，访问 http://127.0.0.1:8000/docs 查看 API 文档。

或者访问 http://127.0.0.1:8000/redoc 查看 ReDoc 文档。

需要自己配置.env文件

文件示例：
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_DATABASE=finan_manage_db
