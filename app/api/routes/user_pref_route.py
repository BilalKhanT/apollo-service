from fastapi import APIRouter, HTTPException, status
import logging

from app.models.apollo_scrape.user_pref_models import (
    UserPreferenceRequest, 
    UserPreferenceDataResponse,
)
from app.models.base import ErrorResponse
from app.controllers.apollo_scrape.user_pref_controller import UserPreferenceController

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/user-preference", tags=["User Preferences"])

@router.post(
    "",
    response_model=UserPreferenceDataResponse,
    responses={
        200: {
            "description": "User preference saved successfully",
            "model": UserPreferenceDataResponse
        },
        400: {
            "description": "Invalid request parameters",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Save user preference",
    description="Save or update user's selected clusters and years for auto-fill functionality."
)
async def save_user_preference(request: UserPreferenceRequest) -> UserPreferenceDataResponse:
    try:
        preference = await UserPreferenceController.save_user_preference(request)
        
        return UserPreferenceDataResponse(
            success=True,
            message="User preference saved successfully",
            data=preference
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error saving user preference: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error saving user preference: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save user preference: {str(e)}"
        )

@router.get(
    "/",
    response_model=UserPreferenceDataResponse,
    responses={
        200: {
            "description": "User preference retrieved successfully",
            "model": UserPreferenceDataResponse
        },
        404: {
            "description": "User preference not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get user preference",
    description="Retrieve user's saved clusters and years selection."
)
async def get_user_preference() -> UserPreferenceDataResponse:
    try:
        preference = await UserPreferenceController.get_user_preference()
        
        if not preference:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No preference found for user"
            )
        
        return UserPreferenceDataResponse(
            success=True,
            message="User preference retrieved successfully",
            data=preference
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving user preference: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve user preference: {str(e)}"
        )