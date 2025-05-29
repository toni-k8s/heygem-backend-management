from contextlib import asynccontextmanager
import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pathlib import Path
import asyncio
import aiosqlite
import aiohttp
import logging
import uuid
import os

import requests

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# 配置
FLASK_SERVER_URL = "http://127.0.0.1:8383/easy/submit" 
url1 = "http://127.0.0.1:8383/easy/query"#查询合成状态
UPLOAD_DIR = Path("D:\\heygem_data\\face2face\\temp")  # 本地存储文件的目录
DB_PATH = "tasks.db"  # SQLite 数据库文件
UPLOAD_DIR.mkdir(exist_ok=True)


def extract_filename(url: str) -> str:
    return url.split("/")[-1] 

# 异步下载文件
async def download_file(url: str, dest_path: Path) -> None:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=400, detail=f"无法下载文件: {url}")
                with dest_path.open("wb") as f:
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
        except Exception as e:
            logger.error(f"Error downloading file {url}: {e}")
            raise HTTPException(status_code=400, detail=f"下载文件失败: {url}")

# 异步初始化数据库
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                audio_url TEXT NOT NULL,
                video_url TEXT NOT NULL,
                audio_filename TEXT NOT NULL,
                video_filename TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    logger.info("Database initialized")

# 异步插入任务
async def insert_task(audio_url: str, video_url: str, audio_filename: str, video_filename: str, code: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO tasks (code, audio_url, video_url, audio_filename, video_filename, status) VALUES (?, ?, ?, ?, ?, ?)",
            (code, audio_url, video_url, audio_filename, video_filename, "pending")
        )
        await db.commit()
        return cursor.lastrowid

# 异步获取待处理任务
async def get_pending_tasks() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT task_id, code, audio_url, video_url, audio_filename, video_filename, status FROM tasks WHERE status = 'pending' ORDER BY created_at")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# 异步更新任务状态
async def update_task_status(code: str, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET status = ? WHERE code = ?", (status, code))
        await db.commit()

# 异步提交任务到 Flask
# async def submit_task_to_flask(task: dict) -> bool:
#     payload = {
#         "audio_url": task["audio_filename"],  # 只传递文件名
#         "video_url": task["video_filename"],  # 只传递文件名
#         "code": task["code"],
#         "watermark_switch": 0,
#         "digital_auth": 0,
#         "chaofen": 1,
#         "pn": 1
#     }
#     async with aiohttp.ClientSession() as session:
#         try:
#             async with session.post(f"{FLASK_SERVER_URL}/easy/submit", json=payload) as response:
#                 response_data = await response.json()
#                 if response_data["code"] == 10000:
#                     logger.info(f"Task {task['code']} submitted successfully")
#                     return True
#                 elif response_data["code"] == 10001:
#                     logger.info(f"Flask server is busy, task {task['code']} will retry")
#                     return False
#                 else:
#                     logger.error(f"Failed to submit task {task['code']}: {response_data['msg']}")
#                     return False
#         except Exception as e:
#             logger.error(f"Error submitting task {task['code']}: {e}")
#             return False
def submit_task_to_flask(task: dict) -> bool:
    payload = {
        "audio_url": task["audio_filename"],  # 只传递文件名
        "video_url": task["video_filename"],  # 只传递文件名
        "code": task["code"],
        "watermark_switch": 0,
        "digital_auth": 0,
        "chaofen": 1,
        "pn": 1
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(FLASK_SERVER_URL, data=json.dumps(payload), headers=headers)
        
        # 检查返回的状态码并处理
        if response.status_code == 200:
            response_data = response.json()
            if response_data["code"] == 10000:
                logger.info(f"Task {task['code']} submitted successfully")
                return True
            elif response_data["code"] == 10001:
                logger.info(f"Flask server is busy, task {task['code']} will retry")
                return False
            else:
                logger.error(f"Failed to submit task {task['code']}: {response_data.get('msg', 'No message')}")
                return False
        else:
            logger.error(f"Failed to submit task {task['code']}, server returned status code: {response.status_code}")
            return False

    except requests.RequestException as e:
        logger.error(f"Error submitting task {task['code']}: {e}")
        return False
# 异步查询任务状态
async def query_task_status(code: str) -> dict:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url1, params={"code": code}) as response:
                return await response.json()
        except Exception as e:
            logger.error(f"Error querying task {code}: {e}")
            return {"code": 9999, "success": False, "msg": "查询失败"}

# 异步任务处理循环
async def process_task_queue():
    
    current_task = None
    while True:
        if current_task is None:
            pending_tasks = await get_pending_tasks()
            if pending_tasks:
                current_task = pending_tasks[0]
                await update_task_status(current_task["code"], "running")
                logger.info(f"Processing task {current_task['code']}")

                # 下载文件
                audio_filename = current_task["audio_filename"]
                video_filename = current_task["video_filename"]
                audio_path = UPLOAD_DIR / f"{current_task['code']}_{audio_filename}"
                video_path = UPLOAD_DIR / f"{current_task['code']}_{video_filename}"

                try:
                    await download_file(current_task["audio_url"], audio_path)
                    await download_file(current_task["video_url"], video_path)
                except HTTPException as e:
                    logger.error(f"Download failed for task {current_task['code']}: {e.detail}")
                    await update_task_status(current_task["code"], "error")
                    current_task = None
                    continue

                # 提交任务
                while not  submit_task_to_flask(current_task):
                    logger.info(f"Task {current_task['code']} waiting for Flask server")
                    await asyncio.sleep(5)

                # 查询状态
                while True:
                    status_response = await query_task_status(current_task["code"])
                    if status_response["code"] == 10000:
                        task_data = status_response["data"]
                        if task_data["status"] in ["success", "error"]:
                            await update_task_status(current_task["code"], task_data["status"])
                            logger.info(f"Task {current_task['code']} completed with status {task_data['status']}")
                            current_task = None
                            break
                    elif status_response["code"] == 10004:
                        await update_task_status(current_task["code"], "error")
                        logger.error(f"Task {current_task['code']} not found")
                        current_task = None
                        break
                    await asyncio.sleep(5)
        await asyncio.sleep(1)

# Lifespan 事件
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    asyncio.create_task(process_task_queue())
    yield
    logger.info("Application shutting down")

app = FastAPI(lifespan=lifespan)

# 异步 JSON 上传端点
@app.post("/upload_json")
async def upload_json(request: Request):
    try:
        data = await request.json()
        audio_url = data.get("audio_url")
        video_url = data.get("video_url")

        if not audio_url or not video_url:
            raise HTTPException(status_code=400, detail="缺少 audio_url 或 video_url")

        # 提取文件名
        audio_filename = extract_filename(audio_url)
        video_filename = extract_filename(video_url)

        # 插入任务
        task_code = str(uuid.uuid4())
        task_id = await insert_task(audio_url, video_url, audio_filename, video_filename, task_code)
        logger.info(f"Task {task_code} added to database, task_id: {task_id}")

        return JSONResponse(content={
            "success": True,
            "task_id": task_id,
            "code": task_code,
            "message": "Task queued"
        })
    except Exception as e:
        logger.error(f"Error processing JSON upload: {e}")
        raise HTTPException(status_code=500, detail="JSON 上传失败")

# 异步查询任务状态
@app.get("/task_status/{task_id}")
async def get_task_status(task_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT task_id, code, status FROM tasks WHERE task_id = ?", (task_id,))
        row = await cursor.fetchone()
        if row:
            task_id, code, status = row["task_id"], row["code"], row["status"]
            if status == "running":
                status_response = await query_task_status(code)
                if status_response["code"] == 10000:
                    return JSONResponse(content={
                        "task_id": task_id,
                        "code": code,
                        "status": status_response["data"]["status"],
                        "progress": status_response["data"]["progress"],
                        "result": status_response["data"]["result"],
                        "message": status_response["data"]["msg"]
                    })
            return JSONResponse(content={
                "task_id": task_id,
                "code": code,
                "status": status,
                "message": f"Task is {status}"
            })
        return JSONResponse(content={
            "task_id": task_id,
            "status": "not_found",
            "message": "Task not found"
        })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")