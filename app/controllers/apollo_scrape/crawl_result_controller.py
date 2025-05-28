from typing import List, Optional
from fastapi import HTTPException
import logging
from app.models.database.apollo_scraper.crawl_result_model import CrawlResult

logger = logging.getLogger(__name__)


class CrawlResultController:
    @staticmethod
    async def create_crawl_result(
        task_id: str,
        link_found: int,
        pages_scraped: int,
        clusters: dict,
        yearclusters: dict
    ) -> CrawlResult:
        try:
            existing = await CrawlResult.find_one(CrawlResult.task_id == task_id)
            if existing:
                logger.warning(f"Crawl result for task {task_id} already exists")
                return existing
            
            crawl_result = CrawlResult(
                task_id=task_id,
                link_found=link_found,
                pages_scraped=pages_scraped,
                clusters=clusters,
                yearclusters=yearclusters
            )
            
            await crawl_result.insert()
            logger.info(f"Created crawl result for task {task_id}")
            return crawl_result
            
        except Exception as e:
            logger.error(f"Error creating crawl result for task {task_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to create crawl result: {str(e)}")
    
    @staticmethod
    async def get_crawl_result(task_id: str) -> Optional[CrawlResult]:
        try:
            return await CrawlResult.find_one(CrawlResult.task_id == task_id)
        except Exception as e:
            logger.error(f"Error fetching crawl result for task {task_id}: {str(e)}")
            return None
    
    @staticmethod
    async def list_crawl_results() -> List[CrawlResult]:
        try:
            # Sort by created_at in descending order (latest first)
            # If created_at is not available, fall back to updated_at
            return await CrawlResult.find_all().sort([
                ("created_at", -1),  # -1 for descending order
                ("updated_at", -1)   # Secondary sort field
            ]).to_list()
        except Exception as e:
            logger.error(f"Error listing crawl results: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to list crawl results: {str(e)}")
    
    @staticmethod
    async def mark_as_scraped(task_id: str) -> bool:
        try:
            crawl_result = await CrawlResult.find_one(CrawlResult.task_id == task_id)
            if not crawl_result:
                logger.warning(f"Crawl result not found for task {task_id}")
                return False
            
            crawl_result.is_scraped = True
            crawl_result.update_timestamp()
            await crawl_result.save()
            
            logger.info(f"Marked crawl result {task_id} as scraped")
            return True
            
        except Exception as e:
            logger.error(f"Error marking crawl result {task_id} as scraped: {str(e)}")
            return False