import socketio
import asyncio
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime

logger = logging.getLogger(__name__)

class SocketManager:
    def __init__(self):
        self.sio = socketio.AsyncServer(
            cors_allowed_origins="*",
            async_mode='asgi',
            logger=False,  
            engineio_logger=False
        )
        self.connected_clients: Dict[str, Dict[str, Any]] = {}
        self.task_subscribers: Dict[str, Set[str]] = {}  
        self.client_tasks: Dict[str, Set[str]] = {}  
        self._setup_event_handlers()
        
    def _setup_event_handlers(self):
        """Setup Socket.IO event handlers"""
        
        @self.sio.event
        async def connect(sid, environ):
            await self._handle_connect(sid, environ)
            
        @self.sio.event
        async def disconnect(sid):
            await self._handle_disconnect(sid)
            
        @self.sio.event
        async def subscribe_task(sid, data):
            await self._handle_subscribe_task(sid, data)
            
        @self.sio.event
        async def unsubscribe_task(sid, data):
            await self._handle_unsubscribe_task(sid, data)
            
        @self.sio.event
        async def get_task_status(sid, data):
            await self._handle_get_task_status(sid, data)
            
        @self.sio.event
        async def ping(sid):
            await self.sio.emit('pong', {'timestamp': datetime.utcnow().isoformat()}, room=sid)
        
    async def _handle_connect(self, sid: str, environ: Dict):
        self.connected_clients[sid] = {
            'connected_at': datetime.utcnow(),
            'last_seen': datetime.utcnow(),
            'user_agent': environ.get('HTTP_USER_AGENT', 'Unknown'),
            'ip': environ.get('REMOTE_ADDR', 'Unknown')
        }
        self.client_tasks[sid] = set()
        
        logger.info(f"Client {sid} connected from {self.connected_clients[sid]['ip']}")
        
        await self.sio.emit('connection_established', {
            'client_id': sid,
            'server_time': datetime.utcnow().isoformat(),
            'message': 'Connected to Apollo WebSocket Server'
        }, room=sid)
        
    async def _handle_disconnect(self, sid: str):
        if sid in self.connected_clients:
            if sid in self.client_tasks:
                subscribed_tasks = self.client_tasks[sid].copy()
                for task_id in subscribed_tasks:
                    self._unsubscribe_from_task(sid, task_id)
                del self.client_tasks[sid]
            
            logger.info(f"Client {sid} disconnected after {datetime.utcnow() - self.connected_clients[sid]['connected_at']}")
            del self.connected_clients[sid]
    
    async def _handle_subscribe_task(self, sid: str, data: Dict[str, Any]):
        try:
            task_id = data.get('task_id')
            if not task_id:
                await self.sio.emit('error', {
                    'event': 'subscribe_task',
                    'message': 'task_id is required',
                    'code': 'MISSING_TASK_ID'
                }, room=sid)
                return

            from app.utils.task_manager import task_manager
            task_status = task_manager.get_task_status(task_id)
            if not task_status:
                await self.sio.emit('error', {
                    'event': 'subscribe_task',
                    'message': f'Task {task_id} not found',
                    'code': 'TASK_NOT_FOUND',
                    'task_id': task_id
                }, room=sid)
                return

            self._subscribe_to_task(sid, task_id)

            await self.sio.emit('task_status_update', {
                'task_id': task_id,
                'timestamp': datetime.utcnow().isoformat(),
                'data': task_status,
                'type': 'initial'
            }, room=sid)

            logs = task_manager.get_and_clear_logs(task_id)
            if logs:
                await self.sio.emit('task_logs_update', {
                    'task_id': task_id,
                    'timestamp': datetime.utcnow().isoformat(),
                    'logs': logs,
                    'count': len(logs),
                    'type': 'initial'
                }, room=sid)
            
            await self.sio.emit('subscription_confirmed', {
                'task_id': task_id,
                'message': f'Successfully subscribed to task {task_id}',
                'status': task_status.get('status'),
                'task_type': task_status.get('type')
            }, room=sid)
            
            logger.info(f"Client {sid} subscribed to task {task_id}")
            
        except Exception as e:
            logger.error(f"Error handling subscribe_task for client {sid}: {str(e)}")
            await self.sio.emit('error', {
                'event': 'subscribe_task',
                'message': f'Subscription failed: {str(e)}',
                'code': 'SUBSCRIPTION_ERROR'
            }, room=sid)

    async def _handle_unsubscribe_task(self, sid: str, data: Dict[str, Any]):
        try:
            task_id = data.get('task_id')
            if not task_id:
                await self.sio.emit('error', {
                    'event': 'unsubscribe_task',
                    'message': 'task_id is required',
                    'code': 'MISSING_TASK_ID'
                }, room=sid)
                return
            
            self._unsubscribe_from_task(sid, task_id)
            
            await self.sio.emit('unsubscription_confirmed', {
                'task_id': task_id,
                'message': f'Successfully unsubscribed from task {task_id}'
            }, room=sid)
            
            logger.info(f"Client {sid} unsubscribed from task {task_id}")
            
        except Exception as e:
            logger.error(f"Error handling unsubscribe_task for client {sid}: {str(e)}")
            await self.sio.emit('error', {
                'event': 'unsubscribe_task',
                'message': f'Unsubscription failed: {str(e)}',
                'code': 'UNSUBSCRIPTION_ERROR'
            }, room=sid)

    async def _handle_get_task_status(self, sid: str, data: Dict[str, Any]):
        try:
            task_id = data.get('task_id')
            if not task_id:
                await self.sio.emit('error', {
                    'event': 'get_task_status',
                    'message': 'task_id is required',
                    'code': 'MISSING_TASK_ID'
                }, room=sid)
                return
            
            from app.utils.task_manager import task_manager
            task_status = task_manager.get_task_status(task_id)
            
            if task_status:
                await self.sio.emit('task_status_response', {
                    'task_id': task_id,
                    'timestamp': datetime.utcnow().isoformat(),
                    'data': task_status,
                    'found': True
                }, room=sid)
            else:
                await self.sio.emit('task_status_response', {
                    'task_id': task_id,
                    'timestamp': datetime.utcnow().isoformat(),
                    'data': None,
                    'found': False,
                    'message': f'Task {task_id} not found'
                }, room=sid)
                
        except Exception as e:
            logger.error(f"Error handling get_task_status for client {sid}: {str(e)}")
            await self.sio.emit('error', {
                'event': 'get_task_status',
                'message': f'Failed to get task status: {str(e)}',
                'code': 'STATUS_ERROR'
            }, room=sid)
    
    def _subscribe_to_task(self, client_id: str, task_id: str):
        if task_id not in self.task_subscribers:
            self.task_subscribers[task_id] = set()
        
        self.task_subscribers[task_id].add(client_id)
        
        if client_id in self.client_tasks:
            self.client_tasks[client_id].add(task_id)

        if client_id in self.connected_clients:
            self.connected_clients[client_id]['last_seen'] = datetime.utcnow()
    
    def _unsubscribe_from_task(self, client_id: str, task_id: str):
        """Internal method to unsubscribe client from task updates"""
        if task_id in self.task_subscribers:
            self.task_subscribers[task_id].discard(client_id)

            if not self.task_subscribers[task_id]:
                del self.task_subscribers[task_id]
        
        if client_id in self.client_tasks:
            self.client_tasks[client_id].discard(task_id)
    
    async def emit_task_status(self, task_id: str, status_data: Dict[str, Any]):
        if task_id in self.task_subscribers and self.task_subscribers[task_id]:
            subscribers = list(self.task_subscribers[task_id])

            active_subscribers = [sid for sid in subscribers if sid in self.connected_clients]
            
            if active_subscribers:
                try:
                    await self.sio.emit(
                        'task_status_update',
                        {
                            'task_id': task_id,
                            'timestamp': datetime.utcnow().isoformat(),
                            'data': status_data,
                            'type': 'update'
                        },
                        room=active_subscribers
                    )
                    logger.debug(f"Emitted status update for task {task_id} to {len(active_subscribers)} clients")
                except Exception as e:
                    logger.error(f"Error emitting status update for task {task_id}: {str(e)}")
    
    async def emit_task_logs(self, task_id: str, logs: List[Dict[str, Any]]):
        if task_id in self.task_subscribers and self.task_subscribers[task_id] and logs:
            subscribers = list(self.task_subscribers[task_id])

            active_subscribers = [sid for sid in subscribers if sid in self.connected_clients]
            
            if active_subscribers:
                try:
                    await self.sio.emit(
                        'task_logs_update',
                        {
                            'task_id': task_id,
                            'timestamp': datetime.utcnow().isoformat(),
                            'logs': logs,
                            'count': len(logs),
                            'type': 'update'
                        },
                        room=active_subscribers
                    )
                    logger.debug(f"Emitted {len(logs)} logs for task {task_id} to {len(active_subscribers)} clients")
                except Exception as e:
                    logger.error(f"Error emitting logs for task {task_id}: {str(e)}")
    
    async def emit_task_completion(self, task_id: str, final_status: Dict[str, Any]):
        if task_id in self.task_subscribers and self.task_subscribers[task_id]:
            subscribers = list(self.task_subscribers[task_id])

            active_subscribers = [sid for sid in subscribers if sid in self.connected_clients]
            
            if active_subscribers:
                try:
                    await self.sio.emit(
                        'task_completed',
                        {
                            'task_id': task_id,
                            'timestamp': datetime.utcnow().isoformat(),
                            'final_status': final_status,
                            'type': 'completion'
                        },
                        room=active_subscribers
                    )
                    logger.info(f"Emitted completion for task {task_id} to {len(active_subscribers)} clients")

                    asyncio.create_task(self._cleanup_completed_task(task_id, delay=30))
                    
                except Exception as e:
                    logger.error(f"Error emitting completion for task {task_id}: {str(e)}")
    
    async def _cleanup_completed_task(self, task_id: str, delay: int = 30):
        await asyncio.sleep(delay)
        if task_id in self.task_subscribers:
            subscribers = list(self.task_subscribers[task_id])
            for client_id in subscribers:
                self._unsubscribe_from_task(client_id, task_id)
            logger.info(f"Cleaned up {len(subscribers)} subscribers for completed task {task_id}")
    
    async def broadcast_server_message(self, message: str, level: str = "info"):
        if self.connected_clients:
            await self.sio.emit(
                'server_message',
                {
                    'message': message,
                    'level': level,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
            logger.info(f"Broadcasted server message to {len(self.connected_clients)} clients: {message}")
    
    def get_stats(self) -> Dict[str, Any]:
        total_subscriptions = sum(len(subs) for subs in self.task_subscribers.values())

        current_time = datetime.utcnow()
        active_clients = 0
        for client_data in self.connected_clients.values():
            time_diff = current_time - client_data['last_seen']
            if time_diff.total_seconds() < 300:  
                active_clients += 1
        
        return {
            'connected_clients': len(self.connected_clients),
            'active_clients': active_clients,
            'active_task_subscriptions': len(self.task_subscribers),
            'total_subscriptions': total_subscriptions,
            'tasks_with_subscribers': list(self.task_subscribers.keys()),
            'server_uptime': datetime.utcnow().isoformat()
        }
    
    def get_client_info(self, client_id: str) -> Optional[Dict[str, Any]]:
        if client_id in self.connected_clients:
            client_data = self.connected_clients[client_id].copy()
            client_data['subscribed_tasks'] = list(self.client_tasks.get(client_id, []))
            return client_data
        return None
    
    def get_task_subscribers(self, task_id: str) -> List[str]:
        return list(self.task_subscribers.get(task_id, set()))

socket_manager = SocketManager()