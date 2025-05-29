# import requests

# # WebDAV 服务器地址
# base_url = "http://192.168.0.45:45532"

# # 新建目录名
# folder_name = "PEOPLE_AI"

# # 完整 URL
# mkdir_url = f"{base_url}/{folder_name}"

# # 认证信息
# auth = ("webdav_writer", "Webdav_WriterOnlyNeedWrite__")

# # 发起 MKCOL 请求
# response = requests.request("MKCOL", mkdir_url, auth=auth)

# if response.status_code in [201, 200]:
#     print("✅ 成功创建目录 PEOPLE_AI")
# elif response.status_code == 405:
#     print("⚠️ 目录已存在")
# else:
#     print(f"❌ 创建失败: {response.status_code} - {response.text}")

import requests

import aiohttp
import aiofiles
import urllib.parse
from pathlib import Path
from cfg.setting import UPLOAD_DIR,SERVER,WEBDAV_HOST,WEBDAV_PORT,WEBDAV_USER,WEBDAV_PASSWORD
from log_path.LogMasterFile import logging
async def create_webdav_directory(session: aiohttp.ClientSession, dir_url: str):
    async with session.request("MKCOL", dir_url) as resp:
        if resp.status not in [200, 201, 204, 405]:
            # 405已存在，可忽略
            raise Exception(f"WebDAV 创建目录失败: HTTP {resp.status}")

async def upload_to_webdav(task_code: str) -> str:
  
    video_name = f"{task_code}-r.mp4"
    video_path = Path(UPLOAD_DIR) / video_name
    filename = video_path.name


    remote_dir = f"/PEOPLE_AI/{task_code}/"
    # remote_url = f"http://webdav_writer:Webdav_WriterOnlyNeedWrite__@192.168.0.45:45532{remote_dir}{filename}"
    # remote_dir_url = f"http://webdav_writer:Webdav_WriterOnlyNeedWrite__@192.168.0.45:45532{remote_dir}".
    auth = f"{WEBDAV_USER}:{WEBDAV_PASSWORD}"
    remote_url = f"http://{auth}@{WEBDAV_HOST}:{WEBDAV_PORT}{remote_dir}{filename}"
    remote_dir_url = f"http://{auth}@{WEBDAV_HOST}:{WEBDAV_PORT}{remote_dir}"


    remote_url = urllib.parse.quote(remote_url, safe=':/@')
    remote_dir_url = urllib.parse.quote(remote_dir_url, safe=':/@')

    async with aiohttp.ClientSession() as session:
   
        await create_webdav_directory(session, remote_dir_url)

        # 上传文件
        async with aiofiles.open(video_path, 'rb') as f:
            video_data = await f.read()
        async with session.put(remote_url, data=video_data) as resp:
            if resp.status not in [200, 201, 204]:
                raise Exception(f"WebDAV 上传失败: HTTP {resp.status}")

    result_url = f"{remote_dir}{filename}"
    return result_url
async def notify_frontend(callback_url: str, video_id:str, video_url: str):
    payload = {
            # "video_url": video_url,  
            # "status": "success",
            # "message": f"任务 {face_map} 的视频 {video_id} 已完成"
            "video_id": video_id,
            # "AIFace": face_map,
            "lipsyncVideo": video_url,
        }
    headers = {
        "YFS30B416E71F32B85B82694A17587D4A7B": "y",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(callback_url, json=payload, timeout=10,headers=headers) as response:
                response.raise_for_status()
                logging.info(f"消息成功通知: {callback_url}, 响应: {response.status}")
        except aiohttp.ClientError as e:

            logging.error(f"消息通知失败: URL={callback_url}, 错误={e}")

            raise Exception(f"消息通知失败: {e}")