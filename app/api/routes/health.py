from fastapi import APIRouter
import logging
from app.core.celery_app import celery_app

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Health"])

@router.get("/health")
async def health_check():

    celery_status = "ok"
    try:
        inspected = celery_app.control.inspect()
        active_workers = inspected.active()
        if not active_workers:
            celery_status = "no active workers"
    except Exception as e:
        logger.error(f"Error checking Celery health: {e}")
        celery_status = f"error: {str(e)}"
    
    return {
        "status": "ok",
        "celery": celery_status
    }