from beanie import Document
from datetime import datetime
from typing import Dict, List
from pydantic import Field
import pytz

def get_karachi_time():
    karachi_tz = pytz.timezone('Asia/Karachi')
    return datetime.now(karachi_tz).replace(tzinfo=None)

class UserPreference(Document):
    userid: str = Field(..., description="User identifier")
    clusters: Dict[str, List[str]] = Field(default_factory=dict, description="Selected clusters by domain")
    years: Dict[str, List[str]] = Field(default_factory=dict, description="Selected years by category")
    created_at: datetime = Field(default_factory=get_karachi_time)
    
    class Settings:
        name = "user_preferences"
        indexes = [
            "userid",
        ]
    
    def update_timestamp(self):
        self.created_at = get_karachi_time()