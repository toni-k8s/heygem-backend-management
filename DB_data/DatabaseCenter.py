# 异步初始化数据库
from cfg.setting import DB_PATH
from log_path.LogMasterFile import logging
import aiosqlite


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT,  
                code TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL,  
                user_group TEXT NOT NULL,     
                audio_url TEXT NOT NULL,
                video_url TEXT NOT NULL,
                audio_filename TEXT NOT NULL,
                video_filename TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP  
            )
        """)
        await db.commit()
    logging.info("数据库已初始化")

# import peewee
# import datetime

# import peewee_async

# # 初始化数据库连接
# db = peewee.MySQLDatabase(
#     'ovo',
#     user='root',
#     password='crisandtoni',
#     host='localhost',
#     port=3306
# )

# # 基础模型
# class BaseModel(peewee.Model):
#     class Meta:
#         database = db

# # 数据模型
# class ModelData(BaseModel):
#     id = peewee.AutoField(primary_key=True)  # 推荐使用默认的 id 字段
#     video_id = peewee.CharField(max_length=255, null=False)
#     code = peewee.CharField(max_length=255, unique=True, null=False)
#     username = peewee.CharField(max_length=255, null=False)
#     user_group = peewee.CharField(max_length=255, null=False)
#     audio_url = peewee.CharField(max_length=255, null=False)
#     video_url = peewee.CharField(max_length=255, null=False)
#     audio_filename = peewee.CharField(max_length=255, null=False)
#     video_filename = peewee.CharField(max_length=255, null=False)
#     status = peewee.CharField(max_length=50, null=False)
#     created_at = peewee.DateTimeField(default=datetime.datetime.now)  # 当前时间

#     class Meta:
#         table_name = "tasks"  # 修正表名拼写错误


# # ✅ 创建表
# if __name__ == "__main__":
#     db.connect()
#     db.create_tables([ModelData])
#     print("表已成功创建")
#     db.close()