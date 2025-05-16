from typing import List
from pydantic import BaseModel


class Cluster(BaseModel):
    """Model for a URL cluster"""
    id: str
    path: str
    url_count: int
    urls: List[str]