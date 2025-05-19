import logging
from typing import Dict, Any, List
from celery import shared_task
from app.core.task_manager import task_manager
from app.core.orchestrator import orchestrator

logger = logging.getLogger(__name__)

@shared_task(bind=True, name="scrape.run_scraper")
def run_scraper(
    self,
    task_id: str,
    cluster_ids: List[str],
    years: List[str] = None,
    url_clusters_file: str = None,
    year_clusters_file: str = None
) -> Dict[str, Any]:
    
    logger.info(f"Starting Celery task for scraper with task_id {task_id}")
    task_manager.update_task_status(task_id, status="running", progress=0.0)
    task_manager.publish_log(task_id, "Starting scrape and download workflow", "info")
    
    try:
        result = orchestrator.run_scrape_download(
            task_id=task_id,
            cluster_ids=cluster_ids,
            years=years,
            url_clusters_file=url_clusters_file,
            year_clusters_file=year_clusters_file
        )
        
        task_manager.publish_log(task_id, "Scraper task completed successfully", "info")
        return result
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error in scraper task: {error_msg}")

        task_manager.update_task_status(
            task_id,
            status="failed",
            error=error_msg
        )
        
        task_manager.publish_log(task_id, f"Scraper task failed: {error_msg}", "error")
        raise