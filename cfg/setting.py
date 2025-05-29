from pathlib import Path


FLASK_SERVER_URL = "http://127.0.0.1:8383/easy/submit"
FLASK_FIND_URL="http://127.0.0.1:8383/easy/query"
UPLOAD_DIR = Path("D:\\heygem_data\\face2face\\temp")  # 本地存储文件的目录
DB_PATH = "E:\\pywejin\\aiperson\\tasks.db"  # SQLite 数据库文件
log_dir="E:\\pywejin\\aiperson\\log_path"
CALLBACK_URL = "http://192.168.0.51:10923/interface/report_ai_digital_human_lipsync_video"##回调地址
SERVER="http://192.168.0.45:45532"##webdav服务器地址
WEBDAV_HOST="192.168.0.45"
WEBDAV_PORT=45532
WEBDAV_USER="webdav_writer"
WEBDAV_PASSWORD="Webdav_WriterOnlyNeedWrite__"
# 配置
# FLASK_SERVER_URL = "http://127.0.0.1:8383/easy/submit"
# FLASK_FIND_URL="http://127.0.0.1:8383/easy/query"
# UPLOAD_DIR = Path("D:\\heygem_data\\face2face\\temp")  # 本地存储文件的目录
# DB_PATH = "tasks.db"  # SQLite 数据库文件
# UPLOAD_DIR.mkdir(exist_ok=True)