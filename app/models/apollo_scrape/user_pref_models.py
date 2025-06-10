from pydantic import BaseModel, Field
from typing import Dict, List
from datetime import datetime
from app.models.base import BaseResponse

class UserPreferenceRequest(BaseModel):
    clusters: Dict[str, List[str]] = Field(
        default_factory=dict, 
        description="Selected clusters by domain", 
        example={"example.com": ["1.1", "1.2"], "test.com": ["2.1"]}
    )
    years: Dict[str, List[str]] = Field(
        default_factory=dict, 
        description="Selected years by category", 
        example={"documents": ["2023", "2024"], "reports": ["2024"]}
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "clusters": {
                    "example.com": ["1.1", "1.2"],
                    "test.com": ["2.1"]
                },
                "years": {
                    "documents": ["2023", "2024"],
                    "reports": ["2024"]
                }
            }
        }

class UserPreferenceResponse(BaseModel):
    clusters: Dict[str, List[str]] = Field(description="Selected clusters by domain")
    years: Dict[str, List[str]] = Field(description="Selected years by category")
    created_at: datetime = Field(description="Creation timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        json_schema_extra = {
            "example": {
                "clusters": {
                    "example.com": ["1.1", "1.2"],
                    "test.com": ["2.1"]
                },
                "years": {
                    "documents": ["2023", "2024"],
                    "reports": ["2024"]
                },
                "created_at": "2025-01-27T10:00:00.000Z"
            }
        }

class UserPreferenceDataResponse(BaseResponse):
    data: UserPreferenceResponse = Field(description="User preference data")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "User preference retrieved successfully",
                "timestamp": "2025-01-27T10:00:00.000Z",
                "data": {
                    "clusters": {
                        "example.com": ["1.1", "1.2"],
                        "test.com": ["2.1"]
                    },
                    "years": {
                        "documents": ["2023", "2024"],
                        "reports": ["2024"]
                    },
                    "created_at": "2025-01-27T10:00:00.000Z"
                }
            }
        }