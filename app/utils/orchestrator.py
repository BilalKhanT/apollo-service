import os
import logging
import time
import traceback
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from app.utils.task_manager import task_manager
from app.utils.config import (
    CRAWLER_USER_AGENT, CRAWLER_TIMEOUT, CRAWLER_NUM_WORKERS,
    CRAWLER_DELAY_BETWEEN_REQUESTS, CRAWLER_INACTIVITY_TIMEOUT,
    CRAWLER_SAVE_INTERVAL, CRAWLER_RESPECT_ROBOTS_TXT,
    DEFAULT_URL_PATTERNS_TO_IGNORE, FILE_EXTENSIONS,
    SOCIAL_MEDIA_KEYWORDS, BANK_KEYWORDS, CLUSTER_MIN_SIZE,
    CLUSTER_PATH_DEPTH, CLUSTER_SIMILARITY_THRESHOLD,
    EXPIRY_DAYS,MAX_DOWNLOAD_WORKERS, DATA_DIR
)

from app.services.apollo import Apollo
from app.services.link_processor import LinkProcessor
from app.services.url_clusterer import URLClusterer
from app.services.year_extractor import YearExtractor
from app.services.scraper import ClusterScraper
from app.services.downloader import FileDownloader

logger = logging.getLogger(__name__)

class ProgressTracker:
    
    def __init__(self, task_id: str, start_percent: float = 0.0, end_percent: float = 100.0):
        self.task_id = task_id
        self.start_percent = start_percent
        self.end_percent = end_percent
        self.last_update_time = time.time()
        self.last_progress = start_percent
    
    def update(self, progress_within_section: float) -> None:
    
        total_progress = self.start_percent + (
            (self.end_percent - self.start_percent) * min(1.0, progress_within_section)
        )
        
        task_manager.update_task_status(
            self.task_id,
            progress=total_progress
        )
        
        self.last_progress = total_progress
        self.last_update_time = time.time()
    
    def create_sub_tracker(self, start_percent: float, end_percent: float) -> 'ProgressTracker':
      
        return ProgressTracker(
            self.task_id,
            start_percent=start_percent,
            end_percent=end_percent
        )
    
    def log_progress(self, message: str, force: bool = False) -> None:
    
        current_time = time.time()
        if force or (current_time - self.last_update_time >= 2.0):
            task_manager.publish_log(
                self.task_id,
                f"{message} (Progress: {self.last_progress:.1f}%)",
                "info"
            )
            self.last_update_time = current_time


class WorkflowStageManager:
    
    def __init__(self, task_id: str, base_directory: str):
        self.task_id = task_id
        self.base_directory = base_directory
        
        self.dirs = self._create_directories(base_directory)
    
    def _create_directories(self, base_directory: str) -> Dict[str, str]:
        dir_names = [
            "crawl", "process", "clusters", "scraped", 
            "downloads", "metadata"
        ]
        
        dirs = {name: os.path.join(base_directory, name) for name in dir_names}
        
        for dir_path in dirs.values():
            Path(dir_path).mkdir(exist_ok=True, parents=True)
        
        return dirs
    
    def get_dir(self, key: str) -> str:
        return self.dirs.get(key, "")
    
    def generate_file_paths(self, base_url: str) -> Dict[str, str]:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        url_part = base_url.replace('://', '_').replace('/', '_')[:30]
        crawl_id = f"{timestamp}_{url_part}"
        
        return {
            "all_links_file": os.path.join(self.dirs["crawl"], f"{crawl_id}_all_links.json"),
            "categorized_file": os.path.join(self.dirs["process"], f"{crawl_id}_categorized.json"),
            "url_clusters_file": os.path.join(self.dirs["clusters"], f"{crawl_id}_url_clusters.json"),
            "year_clusters_file": os.path.join(self.dirs["clusters"], f"{crawl_id}_year_clusters.json")
        }
    
    def create_output_dirs(self, stage: str) -> Tuple[str, str]:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        
        if stage == "scrape":
            scrape_output_dir = os.path.join(self.dirs["scraped"], f"scrape_{timestamp}")
            metadata_output_dir = os.path.join(self.dirs["metadata"], f"metadata_{timestamp}")
            Path(scrape_output_dir).mkdir(exist_ok=True, parents=True)
            Path(metadata_output_dir).mkdir(exist_ok=True, parents=True)
            return scrape_output_dir, metadata_output_dir
        
        elif stage == "download":
            download_output_dir = os.path.join(self.dirs["downloads"], f"download_{timestamp}")
            Path(download_output_dir).mkdir(exist_ok=True, parents=True)
            return download_output_dir, ""
        
        return "", ""
    
    def find_latest_file(self, file_type: str) -> Optional[str]:
        if file_type == "url_clusters":
            file_suffix = "_url_clusters.json"
            dir_path = self.dirs["clusters"]
        elif file_type == "year_clusters":
            file_suffix = "_year_clusters.json"
            dir_path = self.dirs["clusters"]
        else:
            return None
        
        files = [f for f in os.listdir(dir_path) if f.endswith(file_suffix)]
        if not files:
            return None
        
        files.sort(reverse=True)
        
        return os.path.join(dir_path, files[0])


class ServiceFactory:
    
    @staticmethod
    def create_crawler(
        base_url: str,
        output_file: str,
        max_links: float,
        max_pages: float,
        depth_limit: float,
        domain_restriction: bool,
        scrape_pdfs_and_xls: bool
    ) -> Apollo:
        return Apollo(
            base_url=base_url,
            output_file=output_file,
            max_links_to_scrape=max_links,
            max_pages_to_scrape=max_pages,
            depth_limit=depth_limit,
            domain_restriction=domain_restriction,
            url_patterns_to_ignore=DEFAULT_URL_PATTERNS_TO_IGNORE,
            scrape_pdfs_and_xls=scrape_pdfs_and_xls,
            delay_between_requests=CRAWLER_DELAY_BETWEEN_REQUESTS,
            respect_robots_txt=CRAWLER_RESPECT_ROBOTS_TXT,
            user_agent=CRAWLER_USER_AGENT,
            timeout=CRAWLER_TIMEOUT,
            num_workers=CRAWLER_NUM_WORKERS,
            save_interval=CRAWLER_SAVE_INTERVAL,
            inactivity_timeout=CRAWLER_INACTIVITY_TIMEOUT
        )
    
    @staticmethod
    def create_link_processor(
        input_file: str,
        output_file: str
    ) -> LinkProcessor:
        return LinkProcessor(
            input_file=input_file,
            output_file=output_file,
            num_workers=CRAWLER_NUM_WORKERS,
            file_extensions=FILE_EXTENSIONS,
            social_media_keywords=SOCIAL_MEDIA_KEYWORDS,
            bank_keywords=BANK_KEYWORDS
        )
    
    @staticmethod
    def create_url_clusterer(
        input_file: str,
        output_file: str
    ) -> URLClusterer:
        return URLClusterer(
            input_file=input_file,
            output_file=output_file,
            min_cluster_size=CLUSTER_MIN_SIZE,
            path_depth=CLUSTER_PATH_DEPTH,
            similarity_threshold=CLUSTER_SIMILARITY_THRESHOLD
        )
    
    @staticmethod
    def create_year_extractor(
        input_file: str,
        output_file: str
    ) -> YearExtractor:
        return YearExtractor(
            input_file=input_file,
            output_file=output_file
        )
    
    @staticmethod
    def create_scraper(
        json_file_path: str,
        output_dir: str,
        metadata_dir: str
    ) -> ClusterScraper:
        return ClusterScraper(
            json_file_path=json_file_path,
            output_dir=output_dir,
            metadata_dir=metadata_dir,
            expiry_days=EXPIRY_DAYS
        )
    
    @staticmethod
    def create_downloader() -> FileDownloader:
        return FileDownloader(
            max_workers=MAX_DOWNLOAD_WORKERS,
            timeout=CRAWLER_TIMEOUT
        )


class ApolloOrchestrator:
    
    def __init__(self, base_directory: str = None):

        self.base_directory = base_directory or DATA_DIR
        self.logger = logger
        Path(self.base_directory).mkdir(exist_ok=True, parents=True)
        
        self.active_crawlers = {}
    
    def publish_log(self, task_id: str, message: str, level: str = "info") -> None:
        
        task_manager.publish_log(task_id, message, level)
            
        if level == "debug":
            self.logger.debug(message)
        elif level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
    
    def run_crawl(
        self,
        task_id: str,
        base_url: str,
        max_links_to_scrape: Optional[int] = None,
        max_pages_to_scrape: Optional[int] = None,
        depth_limit: Optional[int] = None,
        domain_restriction: bool = True,
        scrape_pdfs_and_xls: bool = True,
        stop_scraper: bool = False
    ) -> Dict[str, Any]:
      
        max_links = float("inf") if max_links_to_scrape is None else max_links_to_scrape
        max_pages = float("inf") if max_pages_to_scrape is None else max_pages_to_scrape
        depth_limit = float("inf") if depth_limit is None else depth_limit
        workflow = WorkflowStageManager(task_id, self.base_directory)
        task_manager.update_task_status(
            task_id,
            status="running",
            progress=0.0
        )

        self.publish_log(task_id, f"Starting crawl workflow for {base_url}", "info")

        if stop_scraper:
            return self._handle_stop_all_crawlers(task_id)
        
        try:
            file_paths = workflow.generate_file_paths(base_url)
            
            self._run_crawl_stage(
                task_id=task_id,
                base_url=base_url,
                max_links=max_links,
                max_pages=max_pages,
                depth_limit=depth_limit,
                domain_restriction=domain_restriction,
                scrape_pdfs_and_xls=scrape_pdfs_and_xls,
                file_paths=file_paths,
                workflow=workflow
            )

            return task_manager.get_task_status(task_id)
            
        except Exception as e:
            error_msg = f"Error in crawl workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)
    
    def _handle_stop_all_crawlers(self, task_id: str) -> Dict[str, Any]:
        self.publish_log(task_id, "Stop signal received. Checking for running crawlers...", "info")
        running_tasks = task_manager.list_tasks(task_type="crawl", status="running")
        
        for task in running_tasks:
            if task['id'] == task_id:
                continue
            
            self.publish_log(task_id, f"Stopping crawler task {task['id']}...", "info")
            self.stop_crawl(task['id'])
        
        return {"status": "stopped", "message": "Stop signal sent to all running crawlers"}
    
    def _run_crawl_stage(
        self,
        task_id: str,
        base_url: str,
        max_links: float,
        max_pages: float,
        depth_limit: float,
        domain_restriction: bool,
        scrape_pdfs_and_xls: bool,
        file_paths: Dict[str, str],
        workflow: WorkflowStageManager
    ) -> None:

        progress = ProgressTracker(task_id, 0.0, 100.0)

        crawler_progress = progress.create_sub_tracker(0.0, 40.0)
        self._execute_crawl_phase(
            task_id=task_id,
            base_url=base_url,
            max_links=max_links,
            max_pages=max_pages,
            depth_limit=depth_limit,
            domain_restriction=domain_restriction,
            scrape_pdfs_and_xls=scrape_pdfs_and_xls,
            output_file=file_paths["all_links_file"],
            progress=crawler_progress
        )
        
        processor_progress = progress.create_sub_tracker(40.0, 60.0)
        self._execute_processing_phase(
            task_id=task_id,
            input_file=file_paths["all_links_file"],
            output_file=file_paths["categorized_file"],
            progress=processor_progress
        )
        
        clustering_progress = progress.create_sub_tracker(60.0, 80.0)
        self._execute_clustering_phase(
            task_id=task_id,
            input_file=file_paths["categorized_file"],
            output_file=file_paths["url_clusters_file"],
            progress=clustering_progress
        )

        year_extraction_progress = progress.create_sub_tracker(80.0, 100.0)
        self._execute_year_extraction_phase(
            task_id=task_id,
            input_file=file_paths["categorized_file"],
            output_file=file_paths["year_clusters_file"],
            file_paths=file_paths,
            progress=year_extraction_progress
        )
        
        self.publish_log(task_id, f"Crawl workflow completed successfully for {base_url}", "info")
    
    def _execute_crawl_phase(
        self,
        task_id: str,
        base_url: str,
        max_links: float,
        max_pages: float,
        depth_limit: float,
        domain_restriction: bool,
        scrape_pdfs_and_xls: bool,
        output_file: str,
        progress: ProgressTracker
    ) -> Dict[str, Any]:

        task_manager.update_task_status(
            task_id,
            status="crawling",
            progress=5.0
        )
        
        self.publish_log(
            task_id, 
            f"Starting crawler for {base_url} with limits: "
            f"max_links={max_links}, max_pages={max_pages}, depth_limit={depth_limit}", 
            "info"
        )

        def status_callback(status_data):
            execution_time = status_data.get('execution_time_seconds', 0)
            crawler_progress = status_data.get('progress', 0) / 100.0  # Convert to 0-1 range
            
            progress.update(crawler_progress)

            crawl_result = {
                "crawl_results": {
                    "total_links_found": status_data.get('links_found', 0),
                    "total_pages_scraped": status_data.get('pages_scraped', 0),
                    "execution_time_seconds": execution_time
                }
            }

            task_manager.update_task_status(
                task_id,
                result=crawl_result
            )

            if status_data.get('pages_scraped', 0) % 2 == 0:  
                progress.log_progress(
                    f"Crawl progress: {status_data.get('pages_scraped', 0)} pages scraped, "
                    f"{status_data.get('links_found', 0)} links found"
                )

        crawler = ServiceFactory.create_crawler(
            base_url=base_url,
            output_file=output_file,
            max_links=max_links,
            max_pages=max_pages,
            depth_limit=depth_limit,
            domain_restriction=domain_restriction,
            scrape_pdfs_and_xls=scrape_pdfs_and_xls
        )

        self.active_crawlers[task_id] = crawler

        crawler.register_status_callback(status_callback)

        self.publish_log(task_id, f"Starting crawler for {base_url}", "info")
        crawl_result = crawler.start()

        self.publish_log(
            task_id,
            f"Crawling completed. Found {crawl_result['summary']['total_links_found']} links, "
            f"scraped {crawl_result['summary']['total_pages_scraped']} pages.",
            "info"
        )

        if task_id in self.active_crawlers:
            del self.active_crawlers[task_id]

        progress.update(1.0)  

        task_manager.update_task_status(
            task_id,
            result={"crawl_complete": True, "crawl_results": crawl_result["summary"]}
        )
        
        return crawl_result
    
    def _execute_processing_phase(
        self,
        task_id: str,
        input_file: str,
        output_file: str,
        progress: ProgressTracker
    ) -> Dict[str, Any]:

        task_manager.update_task_status(
            task_id,
            status="processing",
            progress=progress.start_percent
        )
        
        self.publish_log(task_id, "Processing links...", "info")
        processor = ServiceFactory.create_link_processor(
            input_file=input_file,
            output_file=output_file
        )

        process_result = processor.process()
        
        self.publish_log(
            task_id,
            f"Link processing completed. Categorized {process_result['summary']['total_links']} links into "
            f"{process_result['summary']['file_links_count']} file links, "
            f"{process_result['summary']['bank_links_count']} bank links, "
            f"{process_result['summary']['social_media_links_count']} social media links, and "
            f"{process_result['summary']['misc_links_count']} miscellaneous links.",
            "info"
        )
        
        progress.update(1.0)  

        task_manager.update_task_status(
            task_id,
            result={
                "process_complete": True,
                "process_results": process_result["summary"]
            }
        )
        
        return process_result
    
    def _execute_clustering_phase(
        self,
        task_id: str,
        input_file: str,
        output_file: str,
        progress: ProgressTracker
    ) -> Dict[str, Any]:

        task_manager.update_task_status(
            task_id,
            status="clustering",
            progress=progress.start_percent
        )
        
        self.publish_log(task_id, "Clustering URLs...", "info")
        clusterer = ServiceFactory.create_url_clusterer(
            input_file=input_file,
            output_file=output_file
        )
        
        cluster_result = clusterer.cluster()

        self.publish_log(
            task_id,
            f"URL clustering completed. Identified {cluster_result['summary']['total_domains']} domains and "
            f"{cluster_result['summary']['total_clusters']} clusters across "
            f"{cluster_result['summary']['total_urls']} URLs.",
            "info"
        )

        progress.update(1.0) 

        task_manager.update_task_status(
            task_id,
            result={
                "cluster_complete": True,
                "cluster_results": cluster_result["summary"]
            }
        )
        
        return cluster_result
    
    def _execute_year_extraction_phase(
        self,
        task_id: str,
        input_file: str,
        output_file: str,
        file_paths: Dict[str, str],
        progress: ProgressTracker
    ) -> Dict[str, Any]:

        task_manager.update_task_status(
            task_id,
            status="year_extraction",
            progress=progress.start_percent
        )
        
        self.publish_log(task_id, "Extracting years from file URLs...", "info")
        year_extractor = ServiceFactory.create_year_extractor(
            input_file=input_file,
            output_file=output_file
        )
        
        year_result = year_extractor.process()

        year_count = len(year_result)
        file_count = sum(len(files) for files in year_result.values())
        
        self.publish_log(
            task_id,
            f"Year extraction completed. Identified {year_count} distinct years across {file_count} files.",
            "info"
        )
        
        progress.update(1.0)  

        task_manager.update_task_status(
            task_id,
            status="completed",
            progress=100.0,
            result={
                "year_extraction_complete": True,
                "year_extraction_results": {
                    "total_years": year_count,
                    "total_files": file_count
                },
                "output_files": file_paths
            }
        )
        
        return year_result
    
    def stop_crawl(self, task_id: str) -> Dict[str, Any]:
        
        self.publish_log(task_id, f"Attempting to stop crawl task {task_id} gracefully...", "info")

        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            error_msg = f"Task {task_id} not found"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        if task_status.get("type") != "crawl":
            error_msg = f"Task {task_id} is not a crawl task"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        current_status = task_status.get("status")
        if current_status in ["completed", "failed", "stopped"]:
            msg = f"Task {task_id} is already in '{current_status}' state"
            self.publish_log(task_id, msg, "info")
            return {"success": True, "message": msg}
        
        return self._stop_active_crawler(task_id, task_status)
    
    def _stop_active_crawler(self, task_id: str, task_status: Dict[str, Any]) -> Dict[str, Any]:
        try:
            crawler_instance = self.active_crawlers.get(task_id)
            
            if crawler_instance:
                self.publish_log(task_id, "Stopping crawler...", "info")
                stop_result = crawler_instance.stop()
                
                if stop_result:
                    cleanup_result = crawler_instance.cleanup()

                    if task_id in self.active_crawlers:
                        del self.active_crawlers[task_id]

                    task_manager.update_task_status(
                        task_id,
                        status="stopped",
                        progress=100.0,
                        result={
                            **task_status.get("result", {}),
                            "stopped_at": datetime.now().isoformat(),
                            "stopped_gracefully": True
                        }
                    )
                    
                    self.publish_log(task_id, "Crawler stopped gracefully", "info")
                    return {
                        "success": True, 
                        "message": "Crawler stopped gracefully",
                        "cleanup_completed": cleanup_result
                    }
                else:
                    self.publish_log(task_id, "Failed to stop crawler", "error")
                    return {"success": False, "message": "Failed to stop crawler"}
            else:
                self.publish_log(task_id, "No active crawler instance found, updating task status to stopped", "info")

                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=100.0,
                    result={
                        **task_status.get("result", {}),
                        "stopped_at": datetime.now().isoformat(),
                        "stopped_gracefully": False
                    }
                )
                
                return {
                    "success": True, 
                    "message": "Task marked as stopped but no active crawler found",
                    "cleanup_completed": False
                }
        
        except Exception as e:
            error_msg = f"Error stopping crawler: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            
            return {"success": False, "message": error_msg}
    
    def run_scrape_download(
        self,
        task_id: str,
        cluster_ids: List[str],
        years: List[str] = None,
        url_clusters_file: str = None,
        year_clusters_file: str = None
    ) -> Dict[str, Any]:
        
        workflow = WorkflowStageManager(task_id, self.base_directory)

        task_manager.update_task_status(
            task_id,
            status="initializing",
            progress=0.0
        )

        self.publish_log(task_id, "Starting scrape and download workflow", "info")
        
        try:
            progress = ProgressTracker(task_id, 0.0, 100.0)

            init_progress = progress.create_sub_tracker(0.0, 5.0)
            url_clusters_file, year_clusters_file = self._execute_initialization_phase(
                task_id=task_id,
                cluster_ids=cluster_ids,
                years=years,
                url_clusters_file=url_clusters_file, 
                year_clusters_file=year_clusters_file,
                workflow=workflow,
                progress=init_progress
            )

            if cluster_ids:
                scraping_progress = progress.create_sub_tracker(5.0, 60.0)
                self._execute_scraping_phase(
                    task_id=task_id,
                    cluster_ids=cluster_ids,
                    url_clusters_file=url_clusters_file,
                    workflow=workflow,
                    progress=scraping_progress
                )
            else:
                self.publish_log(task_id, "No clusters specified for scraping. Skipping scrape step.", "info")
                task_manager.update_task_status(
                    task_id,
                    progress=60.0,
                    result={
                        "scrape_complete": False,
                        "scrape_skipped": True
                    }
                )

            if years:
                transition_progress = progress.create_sub_tracker(60.0, 65.0)
                self._execute_transition_phase(task_id, transition_progress)

                download_progress = progress.create_sub_tracker(65.0, 95.0)
                self._execute_downloading_phase(
                    task_id=task_id,
                    years=years,
                    year_clusters_file=year_clusters_file,
                    workflow=workflow,
                    progress=download_progress
                )
            else:
                self.publish_log(task_id, "No years specified for downloading. Skipping download step.", "info")
                task_manager.update_task_status(
                    task_id,
                    progress=95.0,
                    result={
                        **task_manager.get_task_status(task_id).get("result", {}),
                        "download_complete": False,
                        "download_skipped": True
                    }
                )
            
            final_progress = progress.create_sub_tracker(95.0, 100.0)
            self._execute_finalization_phase(task_id, final_progress)

            return task_manager.get_task_status(task_id)
            
        except Exception as e:
            error_msg = f"Error in scrape/download workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)
    
    def _execute_initialization_phase(
        self,
        task_id: str,
        cluster_ids: List[str],
        years: List[str],
        url_clusters_file: str,
        year_clusters_file: str,
        workflow: WorkflowStageManager,
        progress: ProgressTracker
    ) -> Tuple[str, str]:

        task_manager.update_task_status(
            task_id,
            status="checking_files",
            progress=progress.start_percent
        )

        if not url_clusters_file and cluster_ids:
            url_clusters_file = workflow.find_latest_file("url_clusters")
            if not url_clusters_file:
                error_msg = "No URL clusters file found"
                self.publish_log(task_id, error_msg, "error")
                raise FileNotFoundError(error_msg)
                
            self.publish_log(
                task_id, 
                f"Using most recent URL clusters file: {os.path.basename(url_clusters_file)}", 
                "info"
            )
        
        if not year_clusters_file and years:
            year_clusters_file = workflow.find_latest_file("year_clusters")
            if not year_clusters_file:
                error_msg = "No year clusters file found"
                self.publish_log(task_id, error_msg, "error")
                raise FileNotFoundError(error_msg)
                
            self.publish_log(
                task_id, 
                f"Using most recent year clusters file: {os.path.basename(year_clusters_file)}", 
                "info"
            )

        progress.update(1.0)  
        task_manager.update_task_status(
            task_id,
            status="preparing",
            progress=progress.end_percent
        )
        
        return url_clusters_file, year_clusters_file
    
    def _execute_scraping_phase(
        self,
        task_id: str,
        cluster_ids: List[str],
        url_clusters_file: str,
        workflow: WorkflowStageManager,
        progress: ProgressTracker
    ) -> Dict[str, Any]:

        task_manager.update_task_status(
            task_id,
            status="scraping",
            progress=progress.start_percent
        )
        
        scrape_output_dir, metadata_output_dir = workflow.create_output_dirs("scrape")

        self.publish_log(task_id, f"Preparing to scrape {len(cluster_ids)} clusters", "info")
        scraper = ServiceFactory.create_scraper(
            json_file_path=url_clusters_file,
            output_dir=scrape_output_dir,
            metadata_dir=metadata_output_dir
        )

        self.publish_log(task_id, f"Starting scraping of clusters: {cluster_ids}", "info")
        scrape_result = scraper.scrape_clusters(cluster_ids, task_id=task_id)

        self.publish_log(
            task_id,
            f"Scraping completed. Scraped {scrape_result['pages_scraped']} pages from "
            f"{len(scrape_result['clusters_scraped'])} clusters.",
            "info"
        )

        progress.update(1.0)  

        task_manager.update_task_status(
            task_id,
            progress=progress.end_percent,
            result={
                "scrape_complete": True,
                "scrape_results": {
                    "pages_scraped": scrape_result["pages_scraped"],
                    "clusters_scraped": len(scrape_result["clusters_scraped"]),
                    "scrape_output_dir": scrape_output_dir,
                    "metadata_output_dir": metadata_output_dir
                }
            }
        )
        
        return scrape_result
    
    def _execute_transition_phase(
        self,
        task_id: str,
        progress: ProgressTracker
    ) -> None:

        task_manager.update_task_status(
            task_id,
            status="preparing_download",
            progress=progress.start_percent
        )

        self.publish_log(task_id, "Scraping phase complete. Preparing for download phase...", "info")

        time.sleep(0.5)

        progress.update(1.0) 
    
    def _execute_downloading_phase(
        self,
        task_id: str,
        years: List[str],
        year_clusters_file: str,
        workflow: WorkflowStageManager,
        progress: ProgressTracker
    ) -> Dict[str, Any]:

        task_manager.update_task_status(
            task_id,
            status="downloading",
            progress=progress.start_percent
        )

        download_output_dir, _ = workflow.create_output_dirs("download")

        self.publish_log(task_id, f"Preparing to download files for years: {years}", "info")
        downloader = ServiceFactory.create_downloader()

        self.publish_log(task_id, f"Starting download of files for years: {years}", "info")
        download_result = downloader.download_files_by_year(
            json_file=year_clusters_file,
            years_to_download=years,
            base_folder=download_output_dir,
            task_id=task_id
        )

        self.publish_log(
            task_id,
            f"File downloading completed. Successfully downloaded {download_result['successful']} files, "
            f"failed to download {download_result['failed']} files.",
            "info"
        )

        progress.update(1.0)  

        task_status = task_manager.get_task_status(task_id)
        task_manager.update_task_status(
            task_id,
            progress=progress.end_percent,
            result={
                **(task_status.get("result", {})),
                "download_complete": True,
                "download_results": {
                    "files_downloaded": download_result["successful"],
                    "files_failed": download_result["failed"],
                    "download_output_dir": download_output_dir
                }
            }
        )
        
        return download_result
    
    def _execute_finalization_phase(
        self,
        task_id: str,
        progress: ProgressTracker
    ) -> None:

        task_manager.update_task_status(
            task_id,
            status="finalizing",
            progress=progress.start_percent
        )
        
        self.publish_log(task_id, "Finalizing workflow and generating summary...", "info")

        time.sleep(0.5)

        progress.update(1.0)  

        task_manager.update_task_status(
            task_id,
            status="completed",
            progress=progress.end_percent
        )
        
        self.publish_log(task_id, "Scrape and download workflow completed successfully", "info")
    
    def get_available_clusters(self, url_clusters_file: str = None) -> List[Dict[str, Any]]:
        
        workflow = WorkflowStageManager("temp", self.base_directory)

        if not url_clusters_file:
            url_clusters_file = workflow.find_latest_file("url_clusters")
            if not url_clusters_file:
                return []

        try:
            with open(url_clusters_file, 'r') as f:
                clusters_data = json.load(f)

            clusters_info = []
            for domain, domain_data in clusters_data.get("clusters", {}).items():
                clusters_info.append({
                    "id": domain_data.get("id"),
                    "name": domain,
                    "type": "domain",
                    "url_count": domain_data.get("count", 0)
                })

                for sub_cluster in domain_data.get("clusters", []):
                    clusters_info.append({
                        "id": sub_cluster.get("id"),
                        "name": f"{domain} - {sub_cluster.get('path', 'unknown-path')}",
                        "type": "path",
                        "url_count": sub_cluster.get("url_count", 0)
                    })
            
            return clusters_info
        
        except Exception as e:
            logger.error(f"Error getting available clusters: {str(e)}")
            return []
    
    def get_available_years(self, year_clusters_file: str = None) -> List[Dict[str, Any]]:
        
        workflow = WorkflowStageManager("temp", self.base_directory)

        if not year_clusters_file:
            year_clusters_file = workflow.find_latest_file("year_clusters")
            if not year_clusters_file:
                return []

        try:
            with open(year_clusters_file, 'r') as f:
                year_data = json.load(f)

            years_info = []
            for year, files in year_data.items():
                years_info.append({
                    "year": year,
                    "files_count": len(files)
                })

            return sorted(
                years_info,
                key=lambda y: (y["year"] == "No Year", y["year"]),
                reverse=True
            )
        
        except Exception as e:
            logger.error(f"Error getting available years: {str(e)}")
            return []

orchestrator = ApolloOrchestrator()