from typing import List
from pydantic import BaseModel


class YearCluster(BaseModel):
    """Model for files clustered by year"""
    year: str
    files: List[str]