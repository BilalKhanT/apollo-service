from typing import Optional
from fastapi import HTTPException
import logging

from app.models.database.apollo_scraper.user_pref_db_model import UserPreference
from app.models.apollo_scrape.user_pref_models import (
    UserPreferenceRequest, 
    UserPreferenceResponse
)

logger = logging.getLogger(__name__)

class UserPreferenceController:
    
    @staticmethod
    async def save_user_preference(request: UserPreferenceRequest) -> UserPreferenceResponse:
        try:
            existing_preference = await UserPreference.find_one(
                UserPreference.userid == request.userid
            )
            
            if existing_preference:
                logger.info(f"Updating preference for user: {request.userid}")
                existing_preference.clusters = request.clusters
                existing_preference.years = request.years
                existing_preference.update_timestamp()
                await existing_preference.save()
                preference = existing_preference
            else:
                logger.info(f"Creating new preference for user: {request.userid}")
                preference = UserPreference(
                    userid=request.userid,
                    clusters=request.clusters,
                    years=request.years
                )
                await preference.insert()
            
            return UserPreferenceResponse(
                userid=preference.userid,
                clusters=preference.clusters,
                years=preference.years,
                created_at=preference.created_at
            )
            
        except Exception as e:
            logger.error(f"Error saving user preference: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to save user preference: {str(e)}"
            )
    
    @staticmethod
    async def get_user_preference(userid: str) -> Optional[UserPreferenceResponse]:
        try:
            preference = await UserPreference.find_one(
                UserPreference.userid == userid
            )
            
            if not preference:
                return None
            
            return UserPreferenceResponse(
                userid=preference.userid,
                clusters=preference.clusters,
                years=preference.years,
                created_at=preference.created_at
            )
            
        except Exception as e:
            logger.error(f"Error getting user preference for {userid}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get user preference: {str(e)}"
            )