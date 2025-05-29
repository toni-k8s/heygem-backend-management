from pydantic import BaseModel,HttpUrl, field_validator
from typing import Union, Dict
class UploadRequestModel(BaseModel):
    username: str       
    user_group: str         
    # video_id: str
    video_id: Union[str, Dict[str, str]]
    # face_map: str
    audio_url: HttpUrl  
    video_url: HttpUrl  
    @field_validator('video_id')
    def parse_video_id(cls, v):
        if isinstance(v, dict):
            
            if '$oid' in v:
                return v['$oid']
            else:
                raise ValueError("video_id的不支持的dict格式,预期 {'$oid': '...'}")
        return v  
