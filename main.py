import asyncio
from contextlib import asynccontextmanager
import uuid
import aiosqlite
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from cfg.setting import DB_PATH
from model_verify.model_ask import UploadRequestModel
from woker.fpp import extract_filename, init_db, process_task_queue, query_task_status
from DB_data.model_eva import manager
from fastapi import FastAPI, Request
from log_path.LogMasterFile import logging
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    asyncio.create_task(process_task_queue())
    yield
    logging.info("off")

app = FastAPI(lifespan=lifespan)
@app.post("/Lipsync_ai")
async def Lipsync_ai(request: Request):
    try:

        data = await request.json()
        upload_data=UploadRequestModel(**data)

        username = upload_data.username
        user_group = upload_data.user_group
        video_id = upload_data.video_id
        # face_map = upload_data.face_map

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
        task_id = await manager.insert_task(username, user_group,video_id,audio_url, video_url, audio_filename, video_filename, task_code )
        logging.info(f"任务 {task_code} 添加到数据库，task_id: {task_id}")

        return JSONResponse(content={
            "success": True,
            "task_id": task_id,
            "code": task_code,
            # "video_name": f"{task_code}-r.mp4",
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
    uvicorn.run(app, host="0.0.0.0", port=15458, log_level="debug")