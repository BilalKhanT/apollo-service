import asyncio
import logging
from typing import Dict, Any, Optional, Set
from datetime import datetime, timedelta
from app.utils.task_manager import task_manager
from app.utils.socket_manager import socket_manager

logger = logging.getLogger(__name__)

class RealtimePublisher:
    """
    Service for managing real-time publishing of task updates via WebSocket.
    Automatically handles publishing for active tasks and cleans up completed ones.
    """
    
    def __init__(self):
        self.publishing_tasks: Dict[str, asyncio.Task] = {}
        self.task_intervals: Dict[str, float] = {}
        self.running = False
        self.cleanup_task: Optional[asyncio.Task] = None
        self.cleanup_interval = 300  # 5 minutes
        
    async def start(self):
        """Start the real-time publisher service"""
        if self.running:
            logger.warning("RealtimePublisher is already running")
            return
            
        self.running = True
        logger.info("Starting RealtimePublisher service")
        
        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
    async def stop(self):
        """Stop the real-time publisher service"""
        if not self.running:
            return
            
        logger.info("Stopping RealtimePublisher service")
        self.running = False
        
        # Stop all publishing tasks
        for task_id in list(self.publishing_tasks.keys()):
            await self.stop_publishing(task_id)
        
        # Stop cleanup task
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
            self.cleanup_task = None
        
        logger.info("RealtimePublisher service stopped")
    
    async def start_publishing(self, task_id: str, interval: float = 2.0):
        """
        Start real-time publishing for a task
        
        Args:
            task_id: ID of the task to publish updates for
            interval: Update interval in seconds
        """
        if not task_id:
            logger.warning("Cannot start publishing for empty task_id")
            return
            
        if task_id in self.publishing_tasks:
            logger.debug(f"Task {task_id} is already being published")
            return
        
        # Verify task exists
        task_status = task_manager.get_task_status(task_id)
        if not task_status:
            logger.warning(f"Cannot start publishing for non-existent task {task_id}")
            return
        
        # Check if task is already completed
        if task_status.get('status') in ['completed', 'failed', 'stopped', 'error']:
            logger.debug(f"Task {task_id} is already completed, not starting publisher")
            return
        
        try:
            self.task_intervals[task_id] = interval
            self.publishing_tasks[task_id] = asyncio.create_task(
                self._publish_loop(task_id, interval)
            )
            logger.info(f"Started real-time publishing for task {task_id} (interval: {interval}s)")
        except Exception as e:
            logger.error(f"Error starting publishing for task {task_id}: {str(e)}")
            # Clean up if task creation failed
            if task_id in self.task_intervals:
                del self.task_intervals[task_id]
    
    async def stop_publishing(self, task_id: str):
        """Stop real-time publishing for a task"""
        if not task_id:
            logger.warning("Cannot stop publishing for empty task_id")
            return
            
        if task_id in self.publishing_tasks:
            try:
                task = self.publishing_tasks[task_id]
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                del self.publishing_tasks[task_id]
            except Exception as e:
                logger.error(f"Error stopping publishing task for {task_id}: {str(e)}")
                # Still try to clean up the dictionary entry
                if task_id in self.publishing_tasks:
                    del self.publishing_tasks[task_id]
            
        if task_id in self.task_intervals:
            del self.task_intervals[task_id]
            
        logger.info(f"Stopped real-time publishing for task {task_id}")
    
    async def _publish_loop(self, task_id: str, interval: float):
        """Main publishing loop for a specific task"""
        logger.debug(f"Starting publish loop for task {task_id}")
        
        try:
            last_status = None
            last_log_count = 0
            consecutive_same_status = 0
            
            while self.running:
                # Get current task status
                task_status = task_manager.get_task_status(task_id)
                if not task_status:
                    logger.warning(f"Task {task_id} no longer exists, stopping publisher")
                    break
                
                current_status = task_status.get('status')
                
                # Check if task is completed
                if current_status in ['completed', 'failed', 'stopped', 'error']:
                    logger.info(f"Task {task_id} completed with status '{current_status}', stopping publisher")
                    
                    # Send final status update
                    try:
                        await socket_manager.emit_task_status(task_id, task_status)
                    except Exception as e:
                        logger.error(f"Error sending final status for task {task_id}: {str(e)}")
                    
                    # Send completion notification
                    try:
                        await socket_manager.emit_task_completion(task_id, task_status)
                    except Exception as e:
                        logger.error(f"Error sending completion notification for task {task_id}: {str(e)}")
                    
                    # Send any remaining logs
                    try:
                        remaining_logs = task_manager.get_and_clear_logs(task_id)
                        if remaining_logs:
                            await socket_manager.emit_task_logs(task_id, remaining_logs)
                    except Exception as e:
                        logger.error(f"Error sending final logs for task {task_id}: {str(e)}")
                    
                    break
                
                # Optimize publishing frequency based on status changes
                status_changed = (last_status != current_status)
                if status_changed:
                    consecutive_same_status = 0
                else:
                    consecutive_same_status += 1
                
                # Always send status updates for active tasks
                try:
                    await socket_manager.emit_task_status(task_id, task_status)
                except Exception as e:
                    logger.error(f"Error sending status update for task {task_id}: {str(e)}")
                
                # Get and send new logs
                try:
                    logs = task_manager.get_and_clear_logs(task_id)
                    if logs:
                        await socket_manager.emit_task_logs(task_id, logs)
                        last_log_count = len(logs)
                except Exception as e:
                    logger.error(f"Error sending logs for task {task_id}: {str(e)}")
                
                # Dynamic interval adjustment
                actual_interval = interval
                
                # Reduce frequency if status hasn't changed for a while
                if consecutive_same_status > 10:
                    actual_interval = min(interval * 2, 10.0)  # Max 10 seconds
                elif consecutive_same_status > 5:
                    actual_interval = interval * 1.5
                
                # Increase frequency if there's high activity (many logs)
                if last_log_count > 5:
                    actual_interval = max(interval * 0.5, 0.5)  # Min 0.5 seconds
                
                last_status = current_status
                await asyncio.sleep(actual_interval)
                
        except asyncio.CancelledError:
            logger.debug(f"Publishing cancelled for task {task_id}")
        except Exception as e:
            logger.error(f"Error in publishing loop for task {task_id}: {str(e)}")
        finally:
            # Clean up
            try:
                if task_id in self.publishing_tasks:
                    del self.publishing_tasks[task_id]
                if task_id in self.task_intervals:
                    del self.task_intervals[task_id]
            except Exception as e:
                logger.error(f"Error cleaning up publishing resources for task {task_id}: {str(e)}")
            logger.debug(f"Publish loop ended for task {task_id}")
    
    async def _cleanup_loop(self):
        """Periodic cleanup of completed tasks and stale connections"""
        logger.info("Starting RealtimePublisher cleanup loop")
        
        try:
            while self.running:
                await self._perform_cleanup()
                await asyncio.sleep(self.cleanup_interval)
        except asyncio.CancelledError:
            logger.debug("Cleanup loop cancelled")
        except Exception as e:
            logger.error(f"Error in cleanup loop: {str(e)}")
    
    async def _perform_cleanup(self):
        """Perform cleanup operations"""
        try:
            # Clean up publishers for completed tasks
            tasks_to_stop = []
            for task_id in list(self.publishing_tasks.keys()):
                try:
                    task_status = task_manager.get_task_status(task_id)
                    if not task_status or task_status.get('status') in ['completed', 'failed', 'stopped', 'error']:
                        tasks_to_stop.append(task_id)
                except Exception as e:
                    logger.error(f"Error checking task status for {task_id} during cleanup: {str(e)}")
                    tasks_to_stop.append(task_id)  # Remove problematic tasks
            
            for task_id in tasks_to_stop:
                try:
                    await self.stop_publishing(task_id)
                    logger.debug(f"Cleaned up publisher for completed task {task_id}")
                except Exception as e:
                    logger.error(f"Error stopping publisher for task {task_id} during cleanup: {str(e)}")
            
            # Log stats
            if tasks_to_stop:
                logger.info(f"Cleanup completed: stopped {len(tasks_to_stop)} publishers")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the publisher service"""
        active_publishers = len(self.publishing_tasks)
        
        # Count publishers by task type
        type_counts = {}
        for task_id in self.publishing_tasks:
            try:
                task_status = task_manager.get_task_status(task_id)
                if task_status:
                    task_type = task_status.get('type', 'unknown')
                    type_counts[task_type] = type_counts.get(task_type, 0) + 1
            except Exception as e:
                logger.error(f"Error getting task type for {task_id}: {str(e)}")
        
        return {
            "running": self.running,
            "active_publishers": active_publishers,
            "publishers_by_type": type_counts,
            "cleanup_interval_seconds": self.cleanup_interval,
            "publishing_tasks": list(self.publishing_tasks.keys())
        }
    
    def is_publishing(self, task_id: str) -> bool:
        """Check if a task is currently being published"""
        if not task_id:
            return False
        return task_id in self.publishing_tasks and not self.publishing_tasks[task_id].done()
    
    async def force_update(self, task_id: str):
        """Force an immediate update for a specific task"""
        if not task_id:
            logger.warning("Cannot force update for empty task_id")
            return False
            
        try:
            task_status = task_manager.get_task_status(task_id)
            if task_status:
                await socket_manager.emit_task_status(task_id, task_status)
                
                # Also send any pending logs
                logs = task_manager.get_and_clear_logs(task_id)
                if logs:
                    await socket_manager.emit_task_logs(task_id, logs)
                
                logger.info(f"Forced update sent for task {task_id}")
                return True
            else:
                logger.warning(f"Cannot force update for non-existent task {task_id}")
                return False
        except Exception as e:
            logger.error(f"Error forcing update for task {task_id}: {str(e)}")
            return False

# Global publisher instance
realtime_publisher = RealtimePublisher()