from datetime import datetime, timedelta, timezone
import aiosqlite
import pytz
from cfg.setting import DB_PATH

from log_path.LogMasterFile import logging
class data_hub_center:
    # 异步插入任务
    async def insert_task(self,username: str,user_group:str, video_id:str,audio_url: str, video_url: str, audio_filename: str, video_filename: str, code: str,) -> int:
        tz = pytz.timezone('Asia/Shanghai')
        created_at = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "INSERT INTO tasks (code, username, user_group,video_id,audio_url, video_url, audio_filename, video_filename, status,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (code,username, user_group,video_id,audio_url, video_url, audio_filename, video_filename, "pending",created_at)
            )
            await db.commit()
            return cursor.lastrowid
    
    # 异步获取待处理任务
    async def get_pending_tasks(self,) -> list:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT task_id, code,video_id,audio_url, video_url, audio_filename, video_filename, status FROM tasks WHERE status = 'pending' ORDER BY created_at")
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    # 更新任务状态
    async def update_task_status(self,code: str, status: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE tasks SET status = ? WHERE code = ?", (status, code))
            logging.info(f"任务 {code} 状态更新为 {status}")
            await db.commit()
    async def update_completed_time(self,code: str):
        CHINA_TZ = timezone(timedelta(hours=8))
        """
        根据任务 code 更新完成时间completed_at使用当前北京时间。
        不修改任务状态。
        """
        completed_time = datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tasks SET completed_at = ? WHERE code = ?",
                (completed_time, code)
            )
            await db.commit()

        logging.info(f"任务 {code} 的完成时间已更新")
manager=data_hub_center()




# from pytz import timezone
# from datetime import datetime

# class data_hub_center:
#     async def insert_task(
#         self,
#         username: str,
#         user_group: str,
#         video_id: str,
#         audio_url: str,
#         video_url: str,
#         audio_filename: str,
#         video_filename: str,
#         code: str,
#     ) -> int:
#         tz = timezone('Asia/Shanghai')
#         created_at = datetime.now(tz)

#         task = await objects.create(
#             ModelData,
#             code=code,
#             username=username,
#             user_group=user_group,
#             video_id=video_id,
#             audio_url=audio_url,
#             video_url=video_url,
#             audio_filename=audio_filename,
#             video_filename=video_filename,
#             status="pending",
#             created_at=created_at
#         )

#         return task.id
#     async def get_pending_tasks(self) -> list:
#         query = ModelData.select().where(ModelData.status == "pending").order_by(ModelData.created_at)
#         tasks = await objects.execute(query)
#         return [model_to_dict(task) for task in tasks]
#     async def update_task_status(self, code: str, status: str):
#         query = (ModelData
#                  .update({ModelData.status: status})
#                  .where(ModelData.code == code))
#         await objects.execute(query)
#         logging.info(f"任务 {code} 状态更新为 {status}")
#     async def update_completed_time(self, code: str):
#         """
#         根据任务 code 更新 completed_at 字段为当前北京时间。
#         不修改任务状态。
#         """
#         CHINA_TZ = timezone(timedelta(hours=8))
#         completed_time = datetime.now(CHINA_TZ)

#         query = (
#             ModelData
#             .update({ModelData.completed_at: completed_time})
#             .where(ModelData.code == code)
#         )

#         await objects.execute(query)
#         logging.info(f"任务 {code} 的完成时间已更新")
    
# manager=data_hub_center()