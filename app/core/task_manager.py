# app/core/task_manager.py

import uuid
import threading
import os
import logging
import time
import json
import traceback
import redis
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from celery.result import AsyncResult

from app.core.celery_app import celery_app
from app.core.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)

class TaskManager:
    """
    A class to manage Celery tasks with Redis-based storage.
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
            
            # Local cache (for backward compatibility)
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
            
            # Initialize Redis connection
            self.redis = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            
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
            
            # Create task object
            task_created_at = datetime.now().isoformat()
            logger.info(f"Creating task object with created_at={task_created_at}")
            
            task = {
                'id': task_id,
                'type': task_type,
                'status': 'created',
                'progress': 0.0,
                'params': params or {},
                'created_at': task_created_at,
                'updated_at': task_created_at,
                'completed_at': None,
                'result': None,
                'error': None,
                'celery_task_id': None  # Will store Celery task ID
            }
            
            # Store in Redis
            self.redis.set(f"task:{task_id}", json.dumps(task))
            
            # Store in memory for backward compatibility
            with self.lock:
                self.tasks[task_id] = task
                logger.info(f"Task {task_id} created in memory and Redis")
            
            # Initialize empty log list for this task
            with self.logs_lock:
                self.task_logs[task_id] = []
                logger.info(f"Initialized empty log list for task {task_id}")
            
            # Publish task creation notification
            try:
                logger.info(f"Publishing task {task_id} creation to Redis")
                self.redis.publish(
                    "tasks:events", 
                    json.dumps({"event": "task_created", "task_id": task_id, "task_type": task_type})
                )
            except Exception as redis_error:
                logger.error(f"Error publishing task creation to Redis: {str(redis_error)}")
            
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
        error: str = None,
        celery_task_id: str = None
    ) -> bool:
        """
        Update the status of a task.
        
        Args:
            task_id: ID of the task to update
            status: New status
            progress: Progress percentage (0-100)
            result: Task result
            error: Error message
            celery_task_id: ID of the associated Celery task
            
        Returns:
            True if task was updated, False if task doesn't exist
        """
        logger.info(f"Updating task {task_id} status: {status}, progress: {progress}")
        
        try:
            # Get task from Redis
            task_json = self.redis.get(f"task:{task_id}")
            if task_json:
                task = json.loads(task_json)
            else:
                # Try to get from memory as fallback
                with self.lock:
                    task = self.tasks.get(task_id)
                
            if not task:
                logger.warning(f"Task {task_id} not found for update")
                return False
            
            # Update task
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
            
            if celery_task_id is not None:
                task['celery_task_id'] = celery_task_id
                logger.info(f"Updated task {task_id} with Celery task ID: {celery_task_id}")
            
            task['updated_at'] = datetime.now().isoformat()
            
            # Save back to Redis
            self.redis.set(f"task:{task_id}", json.dumps(task))
            
            # Update in-memory too
            with self.lock:
                self.tasks[task_id] = task
            
            # Publish task update event
            try:
                logger.info(f"Publishing task {task_id} update to Redis")
                self.redis.publish(
                    f"task:{task_id}",
                    json.dumps({
                        "id": task_id,
                        "status": status,
                        "progress": progress,
                        "error": error,
                        "updated_at": task['updated_at']
                    })
                )
                # Also publish to the global channel
                self.redis.publish(
                    "tasks:updates",
                    json.dumps({
                        "id": task_id,
                        "status": status,
                        "progress": progress,
                        "error": error,
                        "updated_at": task['updated_at']
                    })
                )
            except Exception as redis_error:
                logger.error(f"Error publishing task update to Redis: {str(redis_error)}")
            
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
    
    def publish_log(self, task_id: str, message: str, level: str = "info") -> bool:
        """
        Publish a log message for a task.
        
        Args:
            task_id: ID of the task
            message: Log message
            level: Log level (debug, info, warning, error)
            
        Returns:
            True if log was published, False otherwise
        """
        logger.info(f"Publishing log for task {task_id}: [{level}] {message}")
        
        try:
            # Check if task exists in Redis
            if not self.redis.exists(f"task:{task_id}"):
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

            # Also publish to Redis for real-time updates
            try:
                # Publish to task-specific logs channel
                self.redis.publish(f"logs:{task_id}", json.dumps(log_entry))
                
                # Also publish to global logs channel
                self.redis.publish("logs:all", json.dumps(log_entry))
            except Exception as redis_error:
                logger.error(f"Error publishing log to Redis: {str(redis_error)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error publishing log: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a task from Redis.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task status dictionary, or None if task doesn't exist
        """
        logger.info(f"Getting status for task {task_id}")
        
        try:
            # Get task from Redis
            task_json = self.redis.get(f"task:{task_id}")
            if task_json:
                task = json.loads(task_json)
            else:
                # Try to get from memory as fallback
                with self.lock:
                    task = self.tasks.get(task_id)
            
            if not task:
                logger.warning(f"Task {task_id} not found")
                return None
                
            # Check if we need to update from Celery
            celery_task_id = task.get('celery_task_id')
            if celery_task_id:
                # Get updated status from Celery
                celery_result = AsyncResult(celery_task_id, app=celery_app)
                if celery_result.state != task.get('status'):
                    # Update our task status from Celery
                    if celery_result.state == 'SUCCESS':
                        self.update_task_status(
                            task_id,
                            status='completed',
                            progress=100.0,
                            result=celery_result.result
                        )
                    elif celery_result.state == 'FAILURE':
                        self.update_task_status(
                            task_id,
                            status='failed',
                            error=str(celery_result.result)
                        )
                    else:
                        self.update_task_status(
                            task_id,
                            status=celery_result.state.lower()
                        )
                    # Refresh task data
                    task_json = self.redis.get(f"task:{task_id}")
                    if task_json:
                        task = json.loads(task_json)
                    
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
            # Get tasks from Redis
            all_tasks = []
            for key in self.redis.keys("task:*"):
                task_json = self.redis.get(key)
                if task_json:
                    try:
                        task = json.loads(task_json)
                        all_tasks.append(task)
                    except json.JSONDecodeError:
                        pass
            
            # If no tasks found in Redis, use in-memory tasks
            if not all_tasks:
                with self.lock:
                    all_tasks = list(self.tasks.values())
            
            # Apply filters
            filtered_tasks = all_tasks
            
            if task_type:
                filtered_tasks = [task for task in filtered_tasks if task.get('type') == task_type]
            
            if status:
                filtered_tasks = [task for task in filtered_tasks if task.get('status') == status]
            
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
            
            # Check for old tasks in Redis and memory
            for key in self.redis.keys("task:*"):
                task_json = self.redis.get(key)
                if task_json:
                    try:
                        task = json.loads(task_json)
                        task_id = task.get('id')
                        created_at_str = task.get('created_at')
                        if created_at_str:
                            created_at = datetime.fromisoformat(created_at_str)
                            age_hours = (current_time - created_at).total_seconds() / 3600
                            if age_hours > max_age_hours:
                                tasks_to_remove.append(task_id)
                    except (json.JSONDecodeError, ValueError):
                        pass
            
            # Remove from Redis and memory
            for task_id in tasks_to_remove:
                # Remove from Redis
                self.redis.delete(f"task:{task_id}")
                
                # Remove from memory
                with self.lock:
                    if task_id in self.tasks:
                        del self.tasks[task_id]
                
                # Remove logs
                with self.logs_lock:
                    if task_id in self.task_logs:
                        del self.task_logs[task_id]
                        logger.info(f"Removed logs for task {task_id}")
                
            logger.info(f"Removed {len(tasks_to_remove)} old tasks")
            return len(tasks_to_remove)
        except Exception as e:
            logger.error(f"Error cleaning up old tasks: {str(e)}")
            logger.error(traceback.format_exc())
            return 0
    
    def run_task(
        self, 
        task_type: str, 
        task_func: Callable, 
        params: Dict[str, Any] = None
    ) -> str:
        """
        Run a task in Celery and track its status.
        
        Args:
            task_type: Type of task (e.g., 'crawl', 'scrape')
            task_func: Celery task function to run
            params: Parameters for the task
            
        Returns:
            Task ID
        """
        # Create a task
        task_id = self.create_task(task_type, params)
        
        try:
            # Update task status to queued
            self.update_task_status(task_id, status='queued', progress=0.0)
            
            # Log task start
            self.publish_log(task_id, f"Task {task_id} queued for execution", "info")
            
            # Run the task in Celery
            celery_task = task_func.delay(task_id=task_id, **params) if params else task_func.delay(task_id=task_id)
            
            # Update task with Celery task ID
            self.update_task_status(
                task_id,
                status='pending',
                celery_task_id=celery_task.id
            )
            
            logger.info(f"Task {task_id} started with Celery task ID {celery_task.id}")
            
            return task_id
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error running task {task_id}: {error_msg}")
            logger.error(traceback.format_exc())
            
            # Update task status to failed
            self.update_task_status(
                task_id,
                status='failed',
                error=error_msg
            )
            
            # Log task failure
            self.publish_log(task_id, f"Task {task_id} failed: {error_msg}", "error")
            
            return task_id

# Create a global task manager instance
task_manager = TaskManager()