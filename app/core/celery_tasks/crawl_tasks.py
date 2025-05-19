# app/tasks/crawl_tasks.py

import logging
from celery import shared_task
from app.core.orchestrator import orchestrator
import traceback

logger = logging.getLogger(__name__)

@shared_task(bind=True, name="crawl.run_crawler")
def run_crawler(self, task_id, **kwargs):

    from app.core.task_manager import task_manager
    
    logger.info(f"Starting Celery crawler task {task_id}")
    
    try:
        task_manager.update_task_status(task_id, status="running", progress=0.0)
        task_manager.publish_log(task_id, f"Starting crawler for {kwargs.get('base_url')}", "info")
        result = orchestrator.run_crawl(task_id=task_id, **kwargs)
        task_manager.update_task_status(
            task_id,
            status="completed" if result.get("status") != "failed" else "failed",
            progress=100.0 if result.get("status") != "failed" else result.get("progress", 0.0),
            result=result,
            error=result.get("error")
        )
        
        logger.info(f"Completed Celery crawler task {task_id}")
        return result
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in crawler task: {error_msg}")
        logger.error(traceback.format_exc())
        
        task_manager.update_task_status(
            task_id,
            status="failed",
            error=error_msg
        )
        
        task_manager.publish_log(task_id, f"Crawler task failed: {error_msg}", "error")
        raise