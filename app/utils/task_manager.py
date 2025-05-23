import uuid
import threading
import logging
import json
import traceback
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class TaskManager:
    
    def __init__(self, data_dir: str = "apollo_data"):
        
        logger.info(f"Initializing TaskManager with data_dir={data_dir}")
        self.data_dir = data_dir
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.task_logs: Dict[str, List[Dict[str, Any]]] = {}
        self.max_logs_per_task = 1000
        self.lock = threading.Lock()
        self.logs_lock = threading.Lock()
        
        Path(data_dir).mkdir(exist_ok=True, parents=True)
    
    def create_task(self, task_type: str, params: Dict[str, Any] = None) -> str:
        
        task_id = str(uuid.uuid4())
        logger.info(f"Creating new task of type '{task_type}' with ID: {task_id}")
        
        sanitized_params = self._sanitize_params(params, task_type)
        
        with self.lock:
            task_created_at = datetime.now().isoformat()
            self.tasks[task_id] = {
                'id': task_id,
                'type': task_type,
                'status': 'created',
                'progress': 0.0,
                'params': sanitized_params,
                'created_at': task_created_at,
                'updated_at': task_created_at,
                'completed_at': None,
                'result': None,
                'error': None
            }
        
        with self.logs_lock:
            self.task_logs[task_id] = []
        
        return task_id
    
    def _sanitize_params(self, params: Optional[Dict[str, Any]], task_type: str) -> Dict[str, Any]:
      
        if not params:
            return {}
            
        try:
            json.dumps(params)
            return params
        except (TypeError, ValueError):
            logger.warning(f"Task params are not JSON serializable, using simplified params")
            return {"error": "Original params not serializable", "task_type": task_type}
    
    def update_task_status(
        self, 
        task_id: str, 
        status: str = None, 
        progress: float = None, 
        result: Any = None, 
        error: str = None
    ) -> bool:
        
        with self.lock:
            if task_id not in self.tasks:
                logger.warning(f"Task {task_id} not found for update")
                return False
            
            task = self.tasks[task_id]
            
            if status is not None:
                task['status'] = status
                if status in ['completed', 'failed', 'error', 'stopped']:
                    task['completed_at'] = datetime.now().isoformat()
            
            if progress is not None:
                task['progress'] = progress
            
            if result is not None:
                self._update_task_result(task, result)
            
            if error is not None:
                task['error'] = error
                logger.error(f"Task {task_id} error: {error}")
            
            task['updated_at'] = datetime.now().isoformat()
            
            return True
    
    def _update_task_result(self, task: Dict[str, Any], new_result: Any) -> None:

        current_result = task['result']
        
        if current_result is None:
            task['result'] = new_result
        elif isinstance(current_result, dict) and isinstance(new_result, dict):
            current_result.update(new_result)
        else:
            task['result'] = new_result
    
    def store_log(self, task_id: str, log_entry: Dict[str, Any]) -> None:
        
        with self.logs_lock:
            if task_id not in self.task_logs:
                self.task_logs[task_id] = []
            
            self.task_logs[task_id].append(log_entry)
            
            if len(self.task_logs[task_id]) > self.max_logs_per_task:
                self.task_logs[task_id] = self.task_logs[task_id][-self.max_logs_per_task:]
    
    def get_and_clear_logs(self, task_id: str) -> List[Dict[str, Any]]:
        
        with self.logs_lock:
            logs = self.task_logs.get(task_id, [])
            
            if task_id in self.task_logs:
                self.task_logs[task_id] = []
            
            return logs
    
    def publish_log(self, task_id: str, message: str, level: str = "info") -> bool:
        
        if task_id not in self.tasks:
            logger.warning(f"Cannot publish log for non-existent task {task_id}")
            return False
        
        log_entry = {
            'task_id': task_id,
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        }
        
        self.store_log(task_id, log_entry)
            
        if level == "debug":
            logger.debug(message)
        elif level == "info":
            logger.info(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)
            
        return True
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
 
        with self.lock:
            return self.tasks.get(task_id)
    
    def list_tasks(self, task_type: str = None, status: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        
        with self.lock:
            filtered_tasks = list(self.tasks.values())
            
            if task_type:
                filtered_tasks = [task for task in filtered_tasks if task['type'] == task_type]
            
            if status:
                filtered_tasks = [task for task in filtered_tasks if task['status'] == status]
            
            sorted_tasks = sorted(
                filtered_tasks, 
                key=lambda task: task.get('created_at', ''), 
                reverse=True
            )
            
            return sorted_tasks[:limit]
    
    def cleanup_old_tasks(self, max_age_hours: int = 24) -> int:
        
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
        
        with self.logs_lock:
            for task_id in tasks_to_remove:
                if task_id in self.task_logs:
                    del self.task_logs[task_id]
            
        return len(tasks_to_remove)
    
    def run_task_in_background(
        self, 
        task_id: str, 
        task_func: Callable[..., Any], 
        *args, 
        **kwargs
    ) -> threading.Thread:
        
        def _run_task():
            try:
                self.update_task_status(task_id, status='running', progress=0.0)
                self.publish_log(task_id, f"Task {task_id} started", "info")

                result = task_func(*args, **kwargs)

                self.update_task_status(task_id, status='completed', progress=100.0, result=result)
                self.publish_log(task_id, f"Task {task_id} completed successfully", "info")
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error running task {task_id}: {error_msg}")
                logger.error(traceback.format_exc())

                self.update_task_status(task_id, status='failed', error=error_msg)
                self.publish_log(task_id, f"Task {task_id} failed: {error_msg}", "error")
        
        thread = threading.Thread(target=_run_task)
        thread.daemon = True
        thread.start()
        
        return thread

try:
    task_manager = TaskManager()
except Exception as e:
    logger.critical(f"Failed to create global TaskManager instance: {str(e)}")
    logger.critical(traceback.format_exc())
    raise