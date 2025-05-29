from contextlib import asynccontextmanager
import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pathlib import Path
from datetime import datetime, timedelta, timezone
import asyncio
import aiosqlite
import aiohttp
import pytz
import uuid
import os
import shutil
import requests
from cfg.setting import FLASK_SERVER_URL, FLASK_FIND_URL, UPLOAD_DIR, DB_PATH, log_dir, CALLBACK_URL
from back_video.sync_video import upload_to_webdav,notify_frontend
# import logging
from model_verify.model_ask import UploadRequestModel
from DB_data.model_eva import manager
from log_path.LogMasterFile import logging
from DB_data.DatabaseCenter import init_db

# 配置日志
# logging.basicConfig(level=logging.INFO)
# logging = logging.getlogging(__name__)
# log_filename = f"{datetime.now().strftime('%Y-%m-%d')}.log"
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
#                     handlers=[logging.FileHandler(os.path.join(log_dir, log_filename), encoding='utf-8'), logging.StreamHandler()])
# app = FastAPI()



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
            logging.error(f"错误下载文件 {url}: {e}")
            raise HTTPException(status_code=400, detail=f"下载文件失败: {url}")

# # 异步初始化数据库
# async def init_db():
#     async with aiosqlite.connect(DB_PATH) as db:
#         await db.execute("""
#             CREATE TABLE IF NOT EXISTS tasks (
#                 task_id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 code TEXT NOT NULL UNIQUE,
#                 username TEXT NOT NULL,  
#                 user_group TEXT NOT NULL,     
#                 audio_url TEXT NOT NULL,
#                 video_url TEXT NOT NULL,
#                 audio_filename TEXT NOT NULL,
#                 video_filename TEXT NOT NULL,
#                 status TEXT NOT NULL,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 completed_at TIMESTAMP  
#             )
#         """)
#         await db.commit()
#     logging.info("数据库已初始化")

# 异步插入任务
# async def insert_task(username: str,user_group:str, audio_url: str, video_url: str, audio_filename: str, video_filename: str, code: str,) -> int:
#     tz = pytz.timezone('Asia/Shanghai')
#     created_at = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
#     async with aiosqlite.connect(DB_PATH) as db:
#         cursor = await db.execute(
#             "INSERT INTO tasks (code, username, user_group,audio_url, video_url, audio_filename, video_filename, status,created_at) VALUES (?, ?, ?, ?, ?, ?,?,?,?)",
#             (code,username, user_group,audio_url, video_url, audio_filename, video_filename, "pending",created_at)
#         )
#         await db.commit()
#         return cursor.lastrowid

# # 异步获取待处理任务
# async def get_pending_tasks() -> list:
#     async with aiosqlite.connect(DB_PATH) as db:
#         db.row_factory = aiosqlite.Row
#         cursor = await db.execute("SELECT task_id, code, audio_url, video_url, audio_filename, video_filename, status FROM tasks WHERE status = 'pending' ORDER BY created_at")
#         rows = await cursor.fetchall()
#         return [dict(row) for row in rows]

# # 更新任务状态
# async def update_task_status(code: str, status: str):
#     async with aiosqlite.connect(DB_PATH) as db:
#         await db.execute("UPDATE tasks SET status = ? WHERE code = ?", (status, code))
#         logging.info(f"任务 {code} 状态更新为 {status}")
#         await db.commit()
# 更新任务完成时间
# async def update_completed_time(code: str):
#     CHINA_TZ = timezone(timedelta(hours=8))
#     """
#     根据任务 code 更新完成时间completed_at使用当前北京时间。
#     不修改任务状态。
#     """
#     completed_time = datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")

#     async with aiosqlite.connect(DB_PATH) as db:
#         await db.execute(
#             "UPDATE tasks SET completed_at = ? WHERE code = ?",
#             (completed_time, code)
#         )
#         await db.commit()

#     logging.info(f"任务 {code} 的完成时间已更新为 {completed_time}")



# 同步提交任务到 Flask
def submit_task_to_flask(task: dict) -> bool:
    payload = {
        "audio_url": task["audio_filename"],  # uuid传递文件名
        "video_url": task["video_filename"],  # uuid传递文件名
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
        
        logging.info(f"正在提交任务 {task['code']}到服务器")
        if response.status_code == 200:
            response_data = response.json()
            if response_data["code"] == 10000:
                logging.info(f"任务 {task['code']} 成功提交")
                return True
            elif response_data["code"] == 10001:
                logging.info(f"服务器繁忙 {task['code']} 重试")
                return False
            else:
                logging.error(f"未能提交任务 {task['code']}: {response_data.get('msg', 'No message')}")
                return False
        else:
            logging.error(f"未能提交任务{task['code']}, 服务器返回状态代码: {response.status_code}")
            return False
    except requests.RequestException as e:
        logging.error(f"错误提交任务 {task['code']}: {e}")
        return False

# 异步查询任务状态
async def query_task_status(code: str) -> dict:
    async with aiohttp.ClientSession() as session:
        try:
            headers = {"Accept": "application/json"}
            await asyncio.sleep(3)  
            async with session.get(FLASK_FIND_URL, params={"code": code}, headers=headers) as response:
                content_type = response.headers.get("Content-Type", "")

                try:
                    
                    data = await response.json()
                    return data
                except aiohttp.ContentTypeError:
                    # fallback: Content-Type 错误但内容是 JSON
                    text = await response.text()
                    logging.warning(f"接口返回 /easy/query, Content-Type 非标准({content_type})，尝试手动解析 JSON，内容: {text[:500]}")
                    try:
                        data = json.loads(text)
                        return data
                    except json.JSONDecodeError:
                        logging.error(f"手动解析失败: 无法解析为 JSON。内容: {text[:500]}")
                        return {"code": 9999, "success": False, "msg": f"Invalid JSON response: {content_type}"}
        except Exception as e:
            logging.error(f"错误查询任务 {code}: {e}")
            return {"code": 9999, "success": False, "msg": "查询失败"}

# 异步任务处理循环
async def process_task_queue():
    current_task = None
    while True:
        if current_task is None:
            pending_tasks = await manager.get_pending_tasks()
            if pending_tasks:
                current_task = pending_tasks[0]
                await manager.update_task_status(current_task["code"], "running")
                logging.info(f"处理任务{current_task['code']}")

                # 下载文件
                audio_filename = current_task["audio_filename"]
                video_filename = current_task["video_filename"]
                audio_path = UPLOAD_DIR / f"{current_task['code']}_{audio_filename}"
                video_path = UPLOAD_DIR / f"{current_task['code']}_{video_filename}"
                flask_audio_path = UPLOAD_DIR / audio_filename
                flask_video_path = UPLOAD_DIR / video_filename

                try:
                    await download_file(current_task["audio_url"], audio_path)
                    await download_file(current_task["video_url"], video_path)
                    shutil.copy(audio_path, flask_audio_path)
                    shutil.copy(video_path, flask_video_path)
                    logging.info(f"复制文件到服务器路径: {flask_audio_path}, {flask_video_path}")
                except (HTTPException, OSError) as e:
                    logging.error(f"下载或复制失败的任务 {current_task['code']}: {e}")
                    await manager.update_task_status(current_task["code"], "error")
                    try:
                        flask_audio_path.unlink(missing_ok=True)
                        flask_video_path.unlink(missing_ok=True)
                    except OSError as e:
                        logging.error(f"任务清理失败 {current_task['code']}: {e}")
                    current_task = None
                    continue

                # 提交任务
                while not submit_task_to_flask(current_task):
                    logging.info(f"任务 {current_task['code']} 等待服务器")
                    await asyncio.sleep(5)

                # 查询状态
                while True:
                    status_response = await query_task_status(current_task["code"])
                    if status_response.get("code") == 10000:
                        task_data = status_response.get("data", {})
                        data_msg = task_data.get("msg", "")
                        if data_msg == "任务完成":
                            await manager.update_task_status(current_task["code"], "success")
                            await manager.update_completed_time(current_task["code"])

                            logging.info(f"任务 {current_task['code']} 已完成，稍后上传服务器 和回调")
                            #上传到服务器 和回调
                            try:
                                upload_url = await upload_to_webdav(current_task['code'])
                                # if upload_url:
                                logging.info(f"服务器视频上传成功=> {upload_url}")
                                logging.info(f"任务 {current_task['video_id']}回调到{CALLBACK_URL},任务编号{current_task['code']}")
                                ###回调
                                try:
                                    await notify_frontend(CALLBACK_URL, current_task["video_id"],upload_url)
                                    logging.info(f"回调成功 {current_task['code']}")
                                except Exception as e:
                                    logging.error(f"任务{current_task['code']}回调失败,{e}")   
                            except Exception as e:
                                await manager.update_task_status(current_task["code"], "error")
                                logging.error(f"上传失败 {current_task['code']}: {e}, 任务状态已更新为 error")
                                # logging.info(f"任务 {current_task['code']} 上传失败: {e}")  
                                            
                            try:
                                flask_audio_path.unlink(missing_ok=True)
                                flask_video_path.unlink(missing_ok=True)
                                logging.info(f"清理文件: {flask_audio_path}, {flask_video_path}")
                            except OSError as e:
                                logging.error(f"Cleanup failed for task {current_task['code']}: {e}")
                            current_task = None
                            break
                        elif data_msg in ["任务不存在"]:
                            await manager.update_task_status(current_task["code"], "error")
                            logging.error(f"任务 {current_task['code']} 异常: {data_msg}")
                            try:
                                flask_audio_path.unlink(missing_ok=True)
                                flask_video_path.unlink(missing_ok=True)
                                logging.info(f"清理文件: {flask_audio_path}, {flask_video_path}")
                            except OSError as e:
                                logging.error(f"任务清理失败 {current_task['code']}: {e}")
                            current_task = None
                            break
                        else:
                            logging.info(f"Task {current_task['code']} 未完成, msg: {data_msg}, retrying...")
                    elif status_response.get("code") == 10004:  # 新增处理
                        logging.warning(f"Task {current_task['code']} not found on server, marking as error")
                        await manager.update_task_status(current_task["code"], "error")
                        try:
                            flask_audio_path.unlink(missing_ok=True)
                            flask_video_path.unlink(missing_ok=True)
                            logging.info(f"清理文件: {flask_audio_path}, {flask_video_path}")
                        except OSError as e:
                            logging.error(f"任务文件清理失败 {current_task['code']}: {e}")
                        current_task = None
                        break
                    else:
                        logging.info(
                            f"任务的无效查询响应 {current_task['code']}: {status_response.get('msg', 'No message')}")
                    await asyncio.sleep(5)
        await asyncio.sleep(1)

        

# Lifespan 事件
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    asyncio.create_task(process_task_queue())
    yield
    logging.info("应用程序关闭")

app = FastAPI(lifespan=lifespan)

# 异步 JSON 上传端点
@app.post("/upload_json")
async def upload_json(request: Request):
    try:

        data = await request.json()
        upload_data=UploadRequestModel(**data)

        username = upload_data.username
        user_group = upload_data.user_group
        audio_url = upload_data.audio_url
        video_url = upload_data.video_url

        video_url=str(video_url)
        audio_url=str(audio_url)
        if not audio_url or not video_url:
            raise HTTPException(status_code=400, detail="缺少 audio_url 或 video_url")
        # elif not audio_url.endswith(".wav") or audio_url.endswith(".mp3"):
        elif not (audio_url.endswith(".wav") or audio_url.endswith(".mp3")):
            raise HTTPException(status_code=400, detail="不支持的文件格式，请上传 wav 或者MP3 文件")
        elif not video_url.endswith(".mp4"):
            raise HTTPException(status_code=400, detail="不支持此文件")

        audio_filename = extract_filename(audio_url)
        video_filename = extract_filename(video_url)

        task_code = str(uuid.uuid4())
        task_id = await manager.insert_task(username, user_group,audio_url, video_url, audio_filename, video_filename, task_code )
        logging.info(f"任务 {task_code} 添加到数据库，task_id: {task_id}")

        return JSONResponse(content={
            "success": True,
            "task_id": task_id,
            "code": task_code,
            "video_name": f"{task_code}-r.mp4",
            "message": "Task queued"
        })
    except Exception as e:
        logging.error(f"错误处理JSON上传d: {e}")
        return JSONResponse(content={
            "code": 422,
            "success": False,
            "error": str(e),
            "message": "上传失败"
        })
        
        # raise HTTPException(status_code=500, detail="JSON 上传失败")

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
                if status_response.get("code") == 10000:
                    task_data = status_response.get("data", {})
                    return JSONResponse(content={
                        "task_id": task_id,
                        "code": code,
                        "status": task_data.get("status", status),
                        "progress": task_data.get("progress", 0),
                        "result": task_data.get("result", ""),
                        "message": task_data.get("msg", "")
                    })
            return JSONResponse(content={
                "task_id": task_id,
                "code": code,
                "status": status,
                "message": f"任务 {status}"
            })
        return JSONResponse(content={
            "task_id": task_id,
            "status": "not_found",
            "message": "Task not found"
        })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")