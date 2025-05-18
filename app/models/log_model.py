from typing import List
from pydantic import BaseModel

class LogEntry(BaseModel):
    task_id: str
    timestamp: str
    level: str
    message: str

class LogResponse(BaseModel):
    logs: List[LogEntry]
    count: int