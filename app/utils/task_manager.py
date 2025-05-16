import uuid
import threading
import os
import logging
import time
import json
import traceback
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
        try:
            logger.info(f"Initializing TaskManager with data_dir={data_dir}")
            self.data_dir = data_dir
            self.tasks: Dict[str, Dict[str, Any]] = {}
            self.lock = threading.Lock()
            
            # Add a dictionary to store logs for each task
            self.task_logs: Dict[str, List[Dict[str, Any]]] = {}
            
            # Set a maximum number of logs to store per task
            self.max_logs_per_task = 1000
            
            # Add a lock for logs
            self.logs_lock = threading.Lock()
            
            # Create data directory if it doesn't exist
            logger.info(f"Creating data directory: {data_dir}")
            os.makedirs(data_dir, exist_ok=True)
            
            # Initialize Redis client for real-time updates
            logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT} (DB: {REDIS_DB})")
            try:
                self.redis_client = RedisClient(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    password=REDIS_PASSWORD
                )
                logger.info("Successfully connected to Redis")
                
                # Test Redis connection with a simple set/get operation
                try:
                    test_key = f"test_connection_{uuid.uuid4()}"
                    test_value = "connection_test"
                    self.redis_client.publish("test_channel", {"message": "Testing connection"})
                    logger.info("Redis connection test successful")
                except Exception as test_error:
                    logger.warning(f"Redis connection test failed: {str(test_error)}")
                
            except Exception as redis_error:
                logger.error(f"Failed to initialize Redis client: {str(redis_error)}")
                logger.error(traceback.format_exc())
                # Continue without Redis - we'll handle this in other methods
                self.redis_client = None
                
        except Exception as e:
            logger.error(f"Error during TaskManager initialization: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def create_task(self, task_type: str, params: Dict[str, Any] = None) -> str:
        """
        Create a new task and return its ID.
        
        Args:
            task_type: Type of task (e.g., 'crawl', 'scrape', 'download')
            params: Parameters for the task
            
        Returns:
            Task ID
        """
        logger.info(f"Creating new task of type '{task_type}'")
        task_id = str(uuid.uuid4())
        logger.info(f"Generated task ID: {task_id}")
        
        try:
            # Log params summary (without sensitive data)
            if params:
                param_keys = list(params.keys())
                logger.info(f"Task params keys: {param_keys}")
                
                # Test JSON serialization of params
                try:
                    json.dumps(params)
                    logger.info("Params are JSON serializable")
                except (TypeError, ValueError) as json_error:
                    logger.warning(f"Task params are not JSON serializable: {str(json_error)}")
                    logger.warning("Will use simplified params")
                    params = {"error": "Original params not serializable", "task_type": task_type}
            
            logger.info(f"Acquiring lock for task {task_id}")
            with self.lock:
                logger.info(f"Lock acquired for task {task_id}")
                
                # Create task object
                task_created_at = datetime.now().isoformat()
                logger.info(f"Creating task object with created_at={task_created_at}")
                
                self.tasks[task_id] = {
                    'id': task_id,
                    'type': task_type,
                    'status': 'created',
                    'progress': 0.0,
                    'params': params or {},
                    'created_at': task_created_at,
                    'updated_at': task_created_at,
                    'completed_at': None,
                    'result': None,
                    'error': None
                }
                
                logger.info(f"Task {task_id} created in memory")
            
            # Initialize empty log list for this task
            with self.logs_lock:
                self.task_logs[task_id] = []
                logger.info(f"Initialized empty log list for task {task_id}")
            
            # Publish task update to Redis (outside of lock)
            if hasattr(self, 'redis_client') and self.redis_client is not None:
                logger.info(f"Publishing task {task_id} update to Redis")
                try:
                    publish_result = self.publish_task_update(task_id)
                    logger.info(f"Redis publish result: {publish_result}")
                except Exception as redis_error:
                    logger.error(f"Error publishing task update to Redis: {str(redis_error)}")
                    logger.error(traceback.format_exc())
                    # Continue despite Redis error
            else:
                logger.warning("Redis client not available, skipping task update publication")
            
            logger.info(f"Task {task_id} creation completed successfully")
            return task_id
            
        except Exception as e:
            logger.error(f"Error creating task: {str(e)}")
            logger.error(traceback.format_exc())
            # Return a fallback ID in case of error
            fallback_id = f"error-{str(uuid.uuid4())}"
            logger.info(f"Returning fallback task ID: {fallback_id}")
            return fallback_id
    
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
        logger.info(f"Updating task {task_id} status: {status}, progress: {progress}")
        
        try:
            with self.lock:
                if task_id not in self.tasks:
                    logger.warning(f"Task {task_id} not found for update")
                    return False
                
                task = self.tasks[task_id]
                
                if status is not None:
                    task['status'] = status
                    logger.info(f"Updated task {task_id} status to {status}")
                    
                    # Set completed_at if the task has completed
                    if status in ['completed', 'failed', 'error']:
                        task['completed_at'] = datetime.now().isoformat()
                        logger.info(f"Set completed_at timestamp for task {task_id}")
                
                if progress is not None:
                    task['progress'] = progress
                    logger.info(f"Updated task {task_id} progress to {progress}")
                
                if result is not None:
                    # If result already exists, merge new result with existing one
                    if task['result'] is not None:
                        # If both are dictionaries, merge them
                        if isinstance(task['result'], dict) and isinstance(result, dict):
                            task['result'].update(result)
                            logger.info(f"Merged result dictionary for task {task_id}")
                        else:
                            # Otherwise, replace the result
                            task['result'] = result
                            logger.info(f"Replaced result for task {task_id}")
                    else:
                        task['result'] = result
                        logger.info(f"Set initial result for task {task_id}")
                    
                    # Log result keys but not values (could be large)
                    if isinstance(result, dict):
                        result_keys = list(result.keys())
                        logger.info(f"Result keys: {result_keys}")
                
                if error is not None:
                    task['error'] = error
                    logger.error(f"Set error for task {task_id}: {error}")
                
                task['updated_at'] = datetime.now().isoformat()
                
                # Publish task update to Redis (deferred to outside the lock)
            
            # Publish task update to Redis outside of the lock
            if hasattr(self, 'redis_client') and self.redis_client is not None:
                logger.info(f"Publishing task {task_id} update to Redis after status change")
                try:
                    publish_result = self.publish_task_update(task_id)
                    logger.info(f"Redis publish result: {publish_result}")
                except Exception as redis_error:
                    logger.error(f"Error publishing task update to Redis: {str(redis_error)}")
                    logger.error(traceback.format_exc())
                    # Continue despite Redis error
            else:
                logger.warning("Redis client not available, skipping task update publication")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating task status: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def store_log(self, task_id: str, log_entry: Dict[str, Any]) -> None:
        """
        Store a log entry for a task.
        
        Args:
            task_id: ID of the task
            log_entry: Log entry to store
        """
        try:
            with self.logs_lock:
                # Initialize log list for this task if it doesn't exist
                if task_id not in self.task_logs:
                    self.task_logs[task_id] = []
                
                # Add the log entry
                self.task_logs[task_id].append(log_entry)
                
                # Trim logs if they exceed the maximum
                if len(self.task_logs[task_id]) > self.max_logs_per_task:
                    # Remove oldest logs
                    self.task_logs[task_id] = self.task_logs[task_id][-self.max_logs_per_task:]
                    logger.info(f"Trimmed logs for task {task_id} to {self.max_logs_per_task} entries")
        except Exception as e:
            logger.error(f"Error storing log for task {task_id}: {str(e)}")
    
    def get_and_clear_logs(self, task_id: str) -> List[Dict[str, Any]]:
        """
        Get and clear all logs for a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            List of log entries
        """
        try:
            with self.logs_lock:
                # Get logs
                logs = self.task_logs.get(task_id, [])
                
                # Clear logs
                if task_id in self.task_logs:
                    self.task_logs[task_id] = []
                    logger.info(f"Cleared logs for task {task_id}")
                
                return logs
        except Exception as e:
            logger.error(f"Error getting and clearing logs for task {task_id}: {str(e)}")
            return []
    
    def publish_task_update(self, task_id: str) -> bool:
        """
        Publish task status update to Redis for real-time notifications.
        
        Args:
            task_id: ID of the task to publish update for
            
        Returns:
            True if update was published, False otherwise
        """
        logger.info(f"Publishing task {task_id} update to Redis")
        
        if not hasattr(self, 'redis_client') or self.redis_client is None:
            logger.warning("Redis client not available, cannot publish task update")
            return False
        
        try:
            task = self.get_task_status(task_id)
            if task is None:
                logger.warning(f"Cannot publish update for non-existent task {task_id}")
                return False
            
            task_type = task.get('type', '')
            logger.info(f"Preparing update for task type: {task_type}")
            
            # Format update data based on task type
            try:
                if task_type == 'crawl':
                    # Extract relevant data for crawl tasks
                    result = task.get('result', {})
                    if result is None:
                        result = {}
                        logger.warning(f"Task {task_id} result is None, using empty dict")
                    
                    crawl_results = {}
                    if isinstance(result, dict):
                        crawl_results = result.get('crawl_results', {})
                        if crawl_results is None:
                            crawl_results = {}
                            logger.warning(f"Task {task_id} crawl_results is None, using empty dict")
                    else:
                        logger.warning(f"Task {task_id} result is not a dict, it's a {type(result)}")
                        
                    logger.info(f"Preparing crawl update for task {task_id}")
                    update = {
                        'id': task_id,
                        'type': task_type,
                        'status': task.get('status', ''),
                        'progress': task.get('progress', 0.0),
                        'current_stage': task.get('status', ''),
                        'links_found': crawl_results.get('total_links_found', 0),
                        'pages_scraped': crawl_results.get('total_pages_scraped', 0),
                        'error': task.get('error'),
                        'clusters_ready': False
                    }
                    
                    # Only check for cluster completion if we have result
                    if isinstance(result, dict):
                        update['clusters_ready'] = (
                            result.get('cluster_complete', False) and 
                            result.get('year_extraction_complete', False)
                        )
                    
                elif task_type == 'scrape':
                    # Extract relevant data for scrape tasks
                    result = task.get('result', {})
                    if result is None:
                        result = {}
                        logger.warning(f"Task {task_id} result is None, using empty dict")
                        
                    scrape_results = {}
                    download_results = {}
                    
                    if isinstance(result, dict):
                        scrape_results = result.get('scrape_results', {})
                        if scrape_results is None:
                            scrape_results = {}
                            
                        download_results = result.get('download_results', {})
                        if download_results is None:
                            download_results = {}
                    else:
                        logger.warning(f"Task {task_id} result is not a dict, it's a {type(result)}")
                    
                    logger.info(f"Preparing scrape update for task {task_id}")
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
                    logger.info(f"Preparing generic update for task {task_id}")
                    update = {
                        'id': task_id,
                        'type': task_type,
                        'status': task.get('status', ''),
                        'progress': task.get('progress', 0.0),
                        'error': task.get('error')
                    }
                
                # Check if update is JSON serializable
                logger.info(f"Testing JSON serialization of update for task {task_id}")
                try:
                    update_json = json.dumps(update)
                    logger.info(f"Update is JSON serializable, length: {len(update_json)}")
                except (TypeError, ValueError) as json_error:
                    logger.error(f"Update not JSON serializable: {str(json_error)}")
                    # Simplify update to make it serializable
                    update = {
                        'id': task_id,
                        'type': task_type,
                        'status': task.get('status', ''),
                        'progress': task.get('progress', 0.0),
                        'error': task.get('error') or f"Error creating update: {str(json_error)}"
                    }
                    logger.info("Using simplified update")
                
                # Publish to task-specific channel
                task_channel = f"task:{task_id}"
                logger.info(f"Publishing to channel {task_channel}")
                recipients = self.redis_client.publish(task_channel, update)
                logger.info(f"Published to task channel, recipients: {recipients}")
                
                # Also publish to global tasks channel
                global_channel = "tasks:all"
                logger.info(f"Publishing to channel {global_channel}")
                global_recipients = self.redis_client.publish(global_channel, update)
                logger.info(f"Published to global channel, recipients: {global_recipients}")
                
                return recipients > 0 or global_recipients > 0
            
            except Exception as format_error:
                logger.error(f"Error formatting task update: {str(format_error)}")
                logger.error(traceback.format_exc())
                
                # Try with a minimal update
                try:
                    minimal_update = {
                        'id': task_id,
                        'type': task_type,
                        'status': task.get('status', ''),
                        'error': f"Error formatting update: {str(format_error)}"
                    }
                    
                    task_channel = f"task:{task_id}"
                    recipients = self.redis_client.publish(task_channel, minimal_update)
                    
                    global_channel = "tasks:all"
                    self.redis_client.publish(global_channel, minimal_update)
                    
                    return recipients > 0
                except Exception as minimal_error:
                    logger.error(f"Even minimal update failed: {str(minimal_error)}")
                    return False
            
        except Exception as e:
            logger.error(f"Top level error in publish_task_update: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
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
        logger.info(f"Publishing log for task {task_id}: [{level}] {message}")
        
        try:
            if task_id not in self.tasks:
                logger.warning(f"Cannot publish log for non-existent task {task_id}")
                return False
            
            log_entry = {
                'task_id': task_id,
                'timestamp': datetime.now().isoformat(),
                'level': level,
                'message': message
            }
            
            # Store the log entry locally first
            self.store_log(task_id, log_entry)
            
            # Continue with Redis publishing if available
            if not hasattr(self, 'redis_client') or self.redis_client is None:
                logger.warning("Redis client not available, cannot publish log")
                return True  # Return True because we stored the log locally
            
            # Publish to task-specific logs channel
            log_channel = f"logs:{task_id}"
            logger.info(f"Publishing to log channel {log_channel}")
            recipients = self.redis_client.publish(log_channel, log_entry)
            logger.info(f"Published to log channel, recipients: {recipients}")
            
            # Also publish to global logs channel
            global_log_channel = "logs:all"
            logger.info(f"Publishing to global log channel {global_log_channel}")
            global_recipients = self.redis_client.publish(global_log_channel, log_entry)
            logger.info(f"Published to global log channel, recipients: {global_recipients}")
            
            return recipients > 0 or global_recipients > 0 or True  # Return True even if Redis fails
            
        except Exception as e:
            logger.error(f"Error publishing log: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task status dictionary, or None if task doesn't exist
        """
        logger.info(f"Getting status for task {task_id}")
        
        try:
            with self.lock:
                task = self.tasks.get(task_id)
                if task is None:
                    logger.warning(f"Task {task_id} not found")
                else:
                    logger.info(f"Retrieved task {task_id}, status: {task.get('status')}")
                return task
        except Exception as e:
            logger.error(f"Error getting task status: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
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
        logger.info(f"Listing tasks with filters - type: {task_type}, status: {status}, limit: {limit}")
        
        try:
            with self.lock:
                filtered_tasks = list(self.tasks.values())
                
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
                
                logger.info(f"Found {len(sorted_tasks)} tasks, returning up to {limit}")
                return sorted_tasks[:limit]
        except Exception as e:
            logger.error(f"Error listing tasks: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    def cleanup_old_tasks(self, max_age_hours: int = 24) -> int:
        """
        Clean up tasks older than the specified age.
        
        Args:
            max_age_hours: Maximum age of tasks in hours
            
        Returns:
            Number of tasks cleaned up
        """
        logger.info(f"Cleaning up tasks older than {max_age_hours} hours")
        
        try:
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
                
                logger.info(f"Removed {len(tasks_to_remove)} old tasks")
            
            # Also clean up logs for removed tasks
            with self.logs_lock:
                for task_id in tasks_to_remove:
                    if task_id in self.task_logs:
                        del self.task_logs[task_id]
                        logger.info(f"Removed logs for task {task_id}")
                
            return len(tasks_to_remove)
        except Exception as e:
            logger.error(f"Error cleaning up old tasks: {str(e)}")
            logger.error(traceback.format_exc())
            return 0
    
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
        logger.info(f"Setting up background thread for task {task_id}")
        
        def _run_task():
            try:
                logger.info(f"Starting background execution of task {task_id}")
                self.update_task_status(task_id, status='running', progress=0.0)
                # Publish log that task is starting
                self.publish_log(task_id, f"Task {task_id} started", "info")
                
                logger.info(f"Executing task function for {task_id}")
                result = task_func(*args, **kwargs)
                
                logger.info(f"Task {task_id} completed successfully")
                self.update_task_status(task_id, status='completed', progress=100.0, result=result)
                # Publish log that task is complete
                self.publish_log(task_id, f"Task {task_id} completed successfully", "info")
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error running task {task_id}: {error_msg}")
                logger.error(traceback.format_exc())
                self.update_task_status(task_id, status='failed', error=error_msg)
                # Publish log that task failed
                self.publish_log(task_id, f"Task {task_id} failed: {error_msg}", "error")
        
        thread = threading.Thread(target=_run_task)
        thread.daemon = True
        logger.info(f"Starting background thread for task {task_id}")
        thread.start()
        logger.info(f"Background thread started for task {task_id}")
        
        return thread

# Create a global task manager instance
try:
    logger.info("Creating global TaskManager instance")
    task_manager = TaskManager()
    logger.info("Global TaskManager instance created successfully")
except Exception as e:
    logger.critical(f"Failed to create global TaskManager instance: {str(e)}")
    logger.critical(traceback.format_exc())
    raise