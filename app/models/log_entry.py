from pydantic import BaseModel

class LogEntry(BaseModel):
    task_id: str
    timestamp: str
    level: str
    message: str