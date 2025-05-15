import uuid
import threading
import os
import logging
import time
import json
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime

from app.utils.redis_client import RedisClient
from app.utils.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

logger = logging.getLogger(__name__)

class TaskManager:
    """
    A class to manage long-running tasks.
    Provides task creation, status tracking, and cleanup.
    """
    
    def __init__(self, data_dir: str = "apollo_data"):
        """
        Initialize the task manager.
        
        Args:
            data_dir: Directory to store task data
        """
        self.data_dir = data_dir
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize Redis client for real-time updates
        self.redis_client = RedisClient(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD
        )
    
    def create_task(self, task_type: str, params: Dict[str, Any] = None) -> str:
        """
        Create a new task and return its ID.
        
        Args:
            task_type: Type of task (e.g., 'crawl', 'scrape', 'download')
            params: Parameters for the task
            
        Returns:
            Task ID
        """
        task_id = str(uuid.uuid4())
        
        with self.lock:
            self.tasks[task_id] = {
                'id': task_id,
                'type': task_type,
                'status': 'created',
                'progress': 0.0,
                'params': params or {},
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'completed_at': None,
                'result': None,
                'error': None
            }
            
            # Publish initial task status to Redis
            self.publish_task_update(task_id)
        
        return task_id
    
    def update_task_status(
        self, 
        task_id: str, 
        status: str = None, 
        progress: float = None, 
        result: Any = None, 
        error: str = None
    ) -> bool:
        """
        Update the status of a task.
        
        Args:
            task_id: ID of the task to update
            status: New status
            progress: Progress percentage (0-100)
            result: Task result
            error: Error message
            
        Returns:
            True if task was updated, False if task doesn't exist
        """
        with self.lock:
            if task_id not in self.tasks:
                return False
            
            task = self.tasks[task_id]
            
            if status is not None:
                task['status'] = status
                
                # Set completed_at if the task has completed
                if status in ['completed', 'failed', 'error']:
                    task['completed_at'] = datetime.now().isoformat()
            
            if progress is not None:
                task['progress'] = progress
            
            if result is not None:
                # If result already exists, merge new result with existing one
                if task['result'] is not None:
                    # If both are dictionaries, merge them
                    if isinstance(task['result'], dict) and isinstance(result, dict):
                        task['result'].update(result)
                    else:
                        # Otherwise, replace the result
                        task['result'] = result
                else:
                    task['result'] = result
            
            if error is not None:
                task['error'] = error
            
            task['updated_at'] = datetime.now().isoformat()
            
            # Publish task update to Redis
            self.publish_task_update(task_id)
            
            return True
    
    def publish_task_update(self, task_id: str) -> bool:
        """
        Publish task status update to Redis for real-time notifications.
        
        Args:
            task_id: ID of the task to publish update for
            
        Returns:
            True if update was published, False otherwise
        """
        task = self.get_task_status(task_id)
        if task is None:
            return False
        
        task_type = task.get('type', '')
        
        # Format update data based on task type
        if task_type == 'crawl':
            # Extract relevant data for crawl tasks
            crawl_results = task.get('result', {}).get('crawl_results', {})
            update = {
                'id': task_id,
                'type': task_type,
                'status': task.get('status', ''),
                'progress': task.get('progress', 0.0),
                'current_stage': task.get('status', ''),
                'links_found': crawl_results.get('total_links_found', 0),
                'pages_scraped': crawl_results.get('total_pages_scraped', 0),
                'error': task.get('error'),
                'clusters_ready': task.get('result', {}).get('cluster_complete', False) and 
                                task.get('result', {}).get('year_extraction_complete', False)
            }
        elif task_type == 'scrape':
            # Extract relevant data for scrape tasks
            scrape_results = task.get('result', {}).get('scrape_results', {})
            download_results = task.get('result', {}).get('download_results', {})
            update = {
                'id': task_id,
                'type': task_type,
                'status': task.get('status', ''),
                'progress': task.get('progress', 0.0),
                'pages_scraped': scrape_results.get('pages_scraped', 0),
                'files_downloaded': download_results.get('files_downloaded', 0),
                'error': task.get('error')
            }
        else:
            # Default format for other task types
            update = {
                'id': task_id,
                'type': task_type,
                'status': task.get('status', ''),
                'progress': task.get('progress', 0.0),
                'error': task.get('error')
            }
        
        # Publish to task-specific channel
        task_channel = f"task:{task_id}"
        recipients = self.redis_client.publish(task_channel, update)
        
        # Also publish to global tasks channel
        global_channel = "tasks:all"
        self.redis_client.publish(global_channel, update)
        
        return recipients > 0
    
    def publish_log(self, task_id: str, message: str, level: str = "info") -> bool:
        """
        Publish a log message for a task to Redis.
        
        Args:
            task_id: ID of the task
            message: Log message
            level: Log level (debug, info, warning, error)
            
        Returns:
            True if log was published, False otherwise
        """
        if task_id not in self.tasks:
            return False
        
        log_entry = {
            'task_id': task_id,
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        }
        
        # Publish to task-specific logs channel
        log_channel = f"logs:{task_id}"
        recipients = self.redis_client.publish(log_channel, log_entry)
        
        # Also publish to global logs channel
        global_log_channel = "logs:all"
        self.redis_client.publish(global_log_channel, log_entry)
        
        return recipients > 0
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task status dictionary, or None if task doesn't exist
        """
        with self.lock:
            return self.tasks.get(task_id)
    
    def list_tasks(self, task_type: str = None, status: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        List tasks, optionally filtered by type and status.
        
        Args:
            task_type: Filter by task type
            status: Filter by task status
            limit: Maximum number of tasks to return
            
        Returns:
            List of task status dictionaries
        """
        with self.lock:
            filtered_tasks = self.tasks.values()
            
            if task_type:
                filtered_tasks = [task for task in filtered_tasks if task['type'] == task_type]
            
            if status:
                filtered_tasks = [task for task in filtered_tasks if task['status'] == status]
            
            # Sort by created_at (newest first)
            sorted_tasks = sorted(
                filtered_tasks, 
                key=lambda task: task.get('created_at', ''), 
                reverse=True
            )
            
            return sorted_tasks[:limit]
    
    def cleanup_old_tasks(self, max_age_hours: int = 24) -> int:
        """
        Clean up tasks older than the specified age.
        
        Args:
            max_age_hours: Maximum age of tasks in hours
            
        Returns:
            Number of tasks cleaned up
        """
        current_time = datetime.now()
        tasks_to_remove = []
        
        with self.lock:
            for task_id, task in self.tasks.items():
                created_at = datetime.fromisoformat(task['created_at'])
                age_hours = (current_time - created_at).total_seconds() / 3600
                
                if age_hours > max_age_hours:
                    tasks_to_remove.append(task_id)
            
            for task_id in tasks_to_remove:
                del self.tasks[task_id]
            
            return len(tasks_to_remove)
    
    def run_task_in_background(
        self, 
        task_id: str, 
        task_func: Callable[..., Any], 
        *args, 
        **kwargs
    ) -> threading.Thread:
        """
        Run a task in a background thread and track its status.
        
        Args:
            task_id: ID of the task
            task_func: Function to run
            *args: Arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Thread object
        """
        def _run_task():
            try:
                self.update_task_status(task_id, status='running', progress=0.0)
                # Publish log that task is starting
                self.publish_log(task_id, f"Task {task_id} started", "info")
                
                result = task_func(*args, **kwargs)
                
                self.update_task_status(task_id, status='completed', progress=100.0, result=result)
                # Publish log that task is complete
                self.publish_log(task_id, f"Task {task_id} completed successfully", "info")
            except Exception as e:
                logger.exception(f"Error running task {task_id}: {str(e)}")
                self.update_task_status(task_id, status='failed', error=str(e))
                # Publish log that task failed
                self.publish_log(task_id, f"Task {task_id} failed: {str(e)}", "error")
        
        thread = threading.Thread(target=_run_task)
        thread.daemon = True
        thread.start()
        
        return thread

# Create a global task manager instance
task_manager = TaskManager()