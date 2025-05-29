# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel, HttpUrl
# from fastapi.responses import JSONResponse
# import os
# import httpx
# import asyncio
# import uuid
# app = FastAPI()
#
# SAVE_DIR = "./media"
# os.makedirs(SAVE_DIR, exist_ok=True)
#
# card=str(uuid.uuid4())
# class MediaRequest(BaseModel):
#     urls: list[HttpUrl]
#
# async def download_media(url: str, save_dir: str) -> str:
#     url_str = str(url)
#     filename = url_str.split("/")[-1]
#
#     # 生成文件保存路径
#     save_path = os.path.join(save_dir, filename)
#
#     async with httpx.AsyncClient() as client:  # 使用异步 HTTP 客户端
#         response = await client.get(url_str, timeout=10)
#         response.raise_for_status()
#         with open(save_path, "wb") as f:
#             f.write(response.content)  # 写入文件
#
#     return save_path
#
#
# @app.post("/download_media")
# async def download_media_post(payload: MediaRequest):
#
#     try:
#
#         tasks = [download_media(url, SAVE_DIR) for url in payload.urls]
#         results = await asyncio.gather(*tasks)
#         a=results[0]
#         b=results[1]
#         a = a.split("\\")[-1]
#         b = b.split("\\")[-1]
#         return JSONResponse(content={"status": "success", "code": 2000,"msg":"任务已经提交","video":a,"audio":b})
#
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))
#
#
# if __name__ == '__main__':
#     import uvicorn
#
#     uvicorn.run(app, host="0.0.0.0", port=8000)
#








# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel, HttpUrl
# from fastapi.responses import JSONResponse
# import os
# import requests
#
# app = FastAPI()
#
# # 保存文件的路径
# SAVE_DIR = "./downloads"
# os.makedirs(SAVE_DIR, exist_ok=True)
#
# # 定义请求体模型
# class MediaURLs(BaseModel):
#     mp4_url: HttpUrl
#     wav_url: HttpUrl
#
# # 下载函数
# def download_file(url: str, save_dir: str) -> str:
#     filename = url.split("/")[-1]
#     save_path = os.path.join(save_dir, filename)
#     response = requests.get(url, timeout=15)
#     response.raise_for_status()
#     with open(save_path, "wb") as f:
#         f.write(response.content)
#     return save_path
#
# @app.post("/download_media")
# def download_media(urls: MediaURLs):
#     try:
#         mp4_path = download_file(urls.mp4_url, SAVE_DIR)
#         wav_path = download_file(urls.wav_url, SAVE_DIR)
#         return JSONResponse(content={
#             "status": "success",
#             "mp4_path": mp4_path,
#             "wav_path": wav_path
#         })
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, HttpUrl
from typing import Literal
import asyncio
from uuid import uuid4
from enum import Enum
from dataclasses import dataclass
import os
import time
import traceback
from y_utils.logger import logger
from y_utils.config import GlobalConfig
from service.trans_dh_service import TransDhTask, Status

app = FastAPI()

# 状态枚举
class TaskStatus(str, Enum):
    pending = "pending"
    running = "running"
    success = "success"
    error = "error"

# 请求体结构
class SubmitRequest(BaseModel):
    audio_url: HttpUrl
    video_url: HttpUrl
    code: str
    watermark_switch: Literal[0, 1] = 0
    digital_auth: Literal[0, 1] = 0
    chaofen: Literal[0, 1] = 0
    pn: Literal[0, 1] = 1

@dataclass
class Task:
    code: str
    data: SubmitRequest
    status: TaskStatus = TaskStatus.pending
    progress: int = 0
    result: str = ""
    msg: str = ""

task_queue: asyncio.Queue = asyncio.Queue()
task_map: dict[str, Task] = {}

async def process_task(task: Task):
    task.status = TaskStatus.running
    task.progress = 10
    try:
        # 检查 TransDhTask 是否忙碌
        if TransDhTask.instance().run_flag:
            task.status = TaskStatus.error
            task.msg = "忙碌中"
            return

        # 获取锁并设置任务状态
        try:
            TransDhTask.instance().run_lock.acquire()
            TransDhTask.instance().run_flag = True
            TransDhTask.instance().task_dic[task.code] = (Status.run, 0, "", "")
        except Exception as e:
            task.status = TaskStatus.error
            task.msg = "获取锁异常"
            return
        finally:
            TransDhTask.instance().run_lock.release()

        # 运行 TransDhTask.work（阻塞操作，使用 asyncio.to_thread）
        await asyncio.to_thread(
            TransDhTask.instance().work,
            str(task.data.audio_url),
            str(task.data.video_url),
            task.code,
            task.data.watermark_switch,
            task.data.digital_auth,
            task.data.chaofen,
            task.data.pn
        )

        # 轮询 TransDhTask.task_dic 获取任务状态
        while task.code in TransDhTask.instance().task_dic:
            status, progress, result, msg = TransDhTask.instance().task_dic[task.code]
            task.progress = progress
            task.result = result
            task.msg = msg
            if status == Status.success:
                task.status = TaskStatus.success
                break
            elif status == Status.error:
                task.status = TaskStatus.error
                break
            await asyncio.sleep(1)  # 每秒检查一次状态

    except Exception as e:
        task.status = TaskStatus.error
        task.msg = f"系统异常: {str(e)}"
        traceback.print_exc()
    finally:
        # 清理 TransDhTask 状态
        try:
            TransDhTask.instance().run_lock.acquire()
            if task.code in TransDhTask.instance().task_dic:
                del TransDhTask.instance().task_dic[task.code]
            TransDhTask.instance().run_flag = False
        except Exception:
            pass
        finally:
            TransDhTask.instance().run_lock.release()

        # 清理 task_map 中的任务
        if task.code in task_map:
            del task_map[task.code]  # 删除已完成任务

# 后台工作者：从队列中拉任务处理
async def worker():
    while True:
        task: Task = await task_queue.get()
        await process_task(task)
        task_queue.task_done()

# 启动后台任务
@app.on_event("startup")
async def startup_event():
    # 初始化 TransDhTask
    TransDhTask.instance()
    logger.info("******************* TransDhServer服务启动 *******************")
    if not os.path.exists(GlobalConfig.instance().temp_dir):
        logger.info("创建临时目录")
        os.makedirs(GlobalConfig.instance().temp_dir)
    if not os.path.exists(GlobalConfig.instance().result_dir):
        logger.info("创建结果目录")
        os.makedirs(GlobalConfig.instance().result_dir)
    # 启动 worker
    asyncio.create_task(worker())

# 提交任务接口
@app.post("/easy/submit")
async def submit_task(data: SubmitRequest):
    # 使用客户端提供的 code
    task = Task(code=data.code, data=data)
    task_map[data.code] = task
    await task_queue.put(task)
    return {
        "code": 10000,  # 与 app_local.py 一致
        "success": True,
        "msg": "成功",
        "data": {}
    }

# 查询任务接口
@app.get("/easy/query")
async def query_task(code: str = Query(...)):
    task = task_map.get(code)
    if not task:
        return {
            "code": 10004,
            "success": False,
            "msg": "任务不存在",
            "data": {}
        }
    return {
        "code": 10000,
        "success": True,
        "msg": "",
        "data": {
            "code": task.code,
            "status": task.status.value,
            "progress": task.progress,
            "result": task.result,
            "msg": task.msg
        }
    }

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8383)