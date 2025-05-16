from typing import List
from pydantic import BaseModel
from app.models.log_entry import LogEntry

class LogResponse(BaseModel):
    logs: List[LogEntry]
    count: int