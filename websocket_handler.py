"""
WebSocket handler for real-time collaboration features.
Enables live data streaming and multi-user collaboration.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Set, Optional, Any, List
from dataclasses import dataclass, asdict
import websockets
from websockets.server import WebSocketServerProtocol
from threading import Thread

from event_bus import get_event_bus, EventType, ScheduleEvent


@dataclass
class ConnectedUser:
    """Information about a connected user"""
    user_id: str
    websocket: WebSocketServerProtocol
    connected_at: datetime
    last_activity: datetime
    permissions: List[str] = None
    
    def __post_init__(self):
        if self.permissions is None:
            self.permissions = ["read", "write"]  # Default permissions


class WebSocketHandler:
    """Handles WebSocket connections for real-time collaboration"""
    
    def __init__(self, scheduler, host: str = "localhost", port: int = 8765):
        """
        Initialize WebSocket handler
        
        Args:
            scheduler: The main Scheduler instance
            host: WebSocket server host
            port: WebSocket server port
        """
        self.scheduler = scheduler
        self.host = host
        self.port = port
        
        # Connected users
        self.connected_users: Dict[str, ConnectedUser] = {}
        self.websocket_to_user: Dict[WebSocketServerProtocol, str] = {}
        
        # Event bus integration
        self.event_bus = get_event_bus()
        self._setup_event_listeners()
        
        # Server instance
        self.server = None
        self.server_thread = None
        
        logging.info(f"WebSocketHandler initialized for {host}:{port}")
    
    def _setup_event_listeners(self):
        """Set up event listeners to broadcast events to connected clients"""
        self.event_bus.subscribe(EventType.SHIFT_ASSIGNED, self._broadcast_event)
        self.event_bus.subscribe(EventType.SHIFT_UNASSIGNED, self._broadcast_event)
        self.event_bus.subscribe(EventType.SHIFT_SWAPPED, self._broadcast_event)
        self.event_bus.subscribe(EventType.CONSTRAINT_VIOLATION, self._broadcast_event)
        self.event_bus.subscribe(EventType.VALIDATION_RESULT, self._broadcast_event)
        self.event_bus.subscribe(EventType.BULK_UPDATE, self._broadcast_event)
    
    async def handle_client(self, websocket: WebSocketServerProtocol, path: str):
        """Handle a new WebSocket client connection"""
        user_id = None
        try:
            # Wait for authentication message
            auth_message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
            auth_data = json.loads(auth_message)
            
            if auth_data.get('type') != 'authenticate':
                await self._send_error(websocket, "First message must be authentication")
                return
            
            user_id = auth_data.get('user_id')
            if not user_id:
                await self._send_error(websocket, "user_id required for authentication")
                return
            
            # Register the user
            await self._register_user(user_id, websocket)
            
            # Send welcome message
            await self._send_message(websocket, {
                'type': 'authenticated',
                'user_id': user_id,
                'server_time': datetime.now().isoformat(),
                'connected_users': list(self.connected_users.keys())
            })
            
            # Broadcast user connection
            await self._broadcast_to_others(user_id, {
                'type': 'user_connected',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat()
            })
            
            # Handle messages from this client
            async for message in websocket:
                await self._handle_message(user_id, websocket, message)
                
        except websockets.exceptions.ConnectionClosed:
            logging.info(f"Client {user_id} disconnected")
        except asyncio.TimeoutError:
            logging.warning("Client authentication timeout")
            await self._send_error(websocket, "Authentication timeout")
        except Exception as e:
            logging.error(f"Error handling client {user_id}: {e}")
        finally:
            if user_id:
                await self._unregister_user(user_id)
    
    async def _register_user(self, user_id: str, websocket: WebSocketServerProtocol):
        """Register a new connected user"""
        # Remove existing connection if user reconnects
        if user_id in self.connected_users:
            old_websocket = self.connected_users[user_id].websocket
            if old_websocket in self.websocket_to_user:
                del self.websocket_to_user[old_websocket]
        
        # Register new connection
        user = ConnectedUser(
            user_id=user_id,
            websocket=websocket,
            connected_at=datetime.now(),
            last_activity=datetime.now()
        )
        
        self.connected_users[user_id] = user
        self.websocket_to_user[websocket] = user_id
        
        # Emit user connected event
        self.event_bus.emit(EventType.USER_CONNECTED, user_id=user_id)
        
        logging.info(f"User {user_id} connected via WebSocket")
    
    async def _unregister_user(self, user_id: str):
        """Unregister a disconnected user"""
        if user_id in self.connected_users:
            user = self.connected_users[user_id]
            
            # Clean up references
            if user.websocket in self.websocket_to_user:
                del self.websocket_to_user[user.websocket]
            del self.connected_users[user_id]
            
            # Broadcast disconnection
            await self._broadcast_to_others(user_id, {
                'type': 'user_disconnected',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat()
            })
            
            # Emit user disconnected event
            self.event_bus.emit(EventType.USER_DISCONNECTED, user_id=user_id)
            
            logging.info(f"User {user_id} disconnected")
    
    async def _handle_message(self, user_id: str, websocket: WebSocketServerProtocol, message: str):
        """Handle a message from a connected client"""
        try:
            data = json.loads(message)
            message_type = data.get('type')
            
            # Update last activity
            if user_id in self.connected_users:
                self.connected_users[user_id].last_activity = datetime.now()
            
            if message_type == 'ping':
                await self._send_message(websocket, {'type': 'pong'})
            
            elif message_type == 'assign_worker':
                await self._handle_assign_worker(user_id, websocket, data)
            
            elif message_type == 'unassign_worker':
                await self._handle_unassign_worker(user_id, websocket, data)
            
            elif message_type == 'swap_workers':
                await self._handle_swap_workers(user_id, websocket, data)
            
            elif message_type == 'validate_schedule':
                await self._handle_validate_schedule(user_id, websocket, data)
            
            elif message_type == 'get_analytics':
                await self._handle_get_analytics(user_id, websocket, data)
            
            elif message_type == 'undo':
                await self._handle_undo(user_id, websocket, data)
            
            elif message_type == 'redo':
                await self._handle_redo(user_id, websocket, data)
            
            else:
                await self._send_error(websocket, f"Unknown message type: {message_type}")
                
        except json.JSONDecodeError:
            await self._send_error(websocket, "Invalid JSON message")
        except Exception as e:
            logging.error(f"Error handling message from {user_id}: {e}")
            await self._send_error(websocket, f"Message handling error: {str(e)}")
    
    async def _handle_assign_worker(self, user_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle worker assignment request"""
        try:
            worker_id = data.get('worker_id')
            shift_date = datetime.fromisoformat(data.get('shift_date'))
            post_index = data.get('post_index')
            
            if not all([worker_id, shift_date is not None, post_index is not None]):
                await self._send_error(websocket, "Missing required fields for assignment")
                return
            
            # Perform assignment using scheduler's real-time method
            result = self.scheduler.assign_worker_real_time(
                worker_id, shift_date, post_index, user_id
            )
            
            # Send response to requesting client
            await self._send_message(websocket, {
                'type': 'assign_worker_response',
                'request_id': data.get('request_id'),
                'result': result
            })
            
        except Exception as e:
            await self._send_error(websocket, f"Assignment error: {str(e)}")
    
    async def _handle_unassign_worker(self, user_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle worker unassignment request"""
        try:
            shift_date = datetime.fromisoformat(data.get('shift_date'))
            post_index = data.get('post_index')
            
            if not all([shift_date is not None, post_index is not None]):
                await self._send_error(websocket, "Missing required fields for unassignment")
                return
            
            # Perform unassignment using scheduler's real-time method
            result = self.scheduler.unassign_worker_real_time(
                shift_date, post_index, user_id
            )
            
            # Send response to requesting client
            await self._send_message(websocket, {
                'type': 'unassign_worker_response',
                'request_id': data.get('request_id'),
                'result': result
            })
            
        except Exception as e:
            await self._send_error(websocket, f"Unassignment error: {str(e)}")
    
    async def _handle_swap_workers(self, user_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle worker swap request"""
        try:
            shift_date1 = datetime.fromisoformat(data.get('shift_date1'))
            post_index1 = data.get('post_index1')
            shift_date2 = datetime.fromisoformat(data.get('shift_date2'))
            post_index2 = data.get('post_index2')
            
            if not all([shift_date1 is not None, post_index1 is not None,
                       shift_date2 is not None, post_index2 is not None]):
                await self._send_error(websocket, "Missing required fields for swap")
                return
            
            # Perform swap using scheduler's real-time method
            result = self.scheduler.swap_workers_real_time(
                shift_date1, post_index1, shift_date2, post_index2, user_id
            )
            
            # Send response to requesting client
            await self._send_message(websocket, {
                'type': 'swap_workers_response',
                'request_id': data.get('request_id'),
                'result': result
            })
            
        except Exception as e:
            await self._send_error(websocket, f"Swap error: {str(e)}")
    
    async def _handle_validate_schedule(self, user_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle schedule validation request"""
        try:
            quick_check = data.get('quick_check', False)
            
            # Perform validation using scheduler's real-time method
            result = self.scheduler.validate_schedule_real_time(quick_check)
            
            # Send response to requesting client
            await self._send_message(websocket, {
                'type': 'validate_schedule_response',
                'request_id': data.get('request_id'),
                'result': result
            })
            
        except Exception as e:
            await self._send_error(websocket, f"Validation error: {str(e)}")
    
    async def _handle_get_analytics(self, user_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle analytics request"""
        try:
            # Get analytics using scheduler's real-time method
            analytics = self.scheduler.get_real_time_analytics()
            
            # Send response to requesting client
            await self._send_message(websocket, {
                'type': 'analytics_response',
                'request_id': data.get('request_id'),
                'analytics': analytics
            })
            
        except Exception as e:
            await self._send_error(websocket, f"Analytics error: {str(e)}")
    
    async def _handle_undo(self, user_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle undo request"""
        try:
            # Perform undo using scheduler's method
            result = self.scheduler.undo_last_change(user_id)
            
            # Send response to requesting client
            await self._send_message(websocket, {
                'type': 'undo_response',
                'request_id': data.get('request_id'),
                'result': result
            })
            
        except Exception as e:
            await self._send_error(websocket, f"Undo error: {str(e)}")
    
    async def _handle_redo(self, user_id: str, websocket: WebSocketServerProtocol, data: Dict[str, Any]):
        """Handle redo request"""
        try:
            # Perform redo using scheduler's method
            result = self.scheduler.redo_last_change(user_id)
            
            # Send response to requesting client
            await self._send_message(websocket, {
                'type': 'redo_response',
                'request_id': data.get('request_id'),
                'result': result
            })
            
        except Exception as e:
            await self._send_error(websocket, f"Redo error: {str(e)}")
    
    async def _send_message(self, websocket: WebSocketServerProtocol, message: Dict[str, Any]):
        """Send a message to a specific WebSocket client"""
        try:
            await websocket.send(json.dumps(message, default=str))
        except websockets.exceptions.ConnectionClosed:
            pass  # Client disconnected
        except Exception as e:
            logging.error(f"Error sending message: {e}")
    
    async def _send_error(self, websocket: WebSocketServerProtocol, error_message: str):
        """Send an error message to a client"""
        await self._send_message(websocket, {
            'type': 'error',
            'message': error_message,
            'timestamp': datetime.now().isoformat()
        })
    
    async def _broadcast_to_all(self, message: Dict[str, Any]):
        """Broadcast a message to all connected clients"""
        if not self.connected_users:
            return
        
        # Send to all connected users
        tasks = []
        for user in self.connected_users.values():
            tasks.append(self._send_message(user.websocket, message))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _broadcast_to_others(self, excluding_user_id: str, message: Dict[str, Any]):
        """Broadcast a message to all clients except one"""
        tasks = []
        for user_id, user in self.connected_users.items():
            if user_id != excluding_user_id:
                tasks.append(self._send_message(user.websocket, message))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def _broadcast_event(self, event: ScheduleEvent):
        """Broadcast a schedule event to all connected clients"""
        if not self.connected_users:
            return
        
        # Convert event to WebSocket message
        message = {
            'type': 'schedule_event',
            'event': event.to_dict()
        }
        
        # Schedule the broadcast (since this might be called from a sync context)
        if hasattr(asyncio, '_get_running_loop'):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._broadcast_to_all(message))
            except RuntimeError:
                # No running loop, skip broadcast
                pass
    
    def start_server(self):
        """Start the WebSocket server in a separate thread"""
        if self.server_thread and self.server_thread.is_alive():
            logging.warning("WebSocket server already running")
            return
        
        def run_server():
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
            
            start_server = websockets.serve(
                self.handle_client,
                self.host,
                self.port
            )
            
            self.server = loop.run_until_complete(start_server)
            logging.info(f"WebSocket server started on {self.host}:{self.port}")
            
            try:
                loop.run_forever()
            except KeyboardInterrupt:
                pass
            finally:
                loop.run_until_complete(self.server.close())
                loop.close()
        
        self.server_thread = Thread(target=run_server, daemon=True)
        self.server_thread.start()
        
        logging.info("WebSocket server thread started")
    
    def stop_server(self):
        """Stop the WebSocket server"""
        if self.server:
            # This is a simplified stop - in practice you'd need more coordination
            logging.info("WebSocket server stop requested")
    
    def get_connected_users(self) -> List[Dict[str, Any]]:
        """Get information about connected users"""
        return [
            {
                'user_id': user.user_id,
                'connected_at': user.connected_at.isoformat(),
                'last_activity': user.last_activity.isoformat(),
                'permissions': user.permissions
            }
            for user in self.connected_users.values()
        ]
    
    def is_user_connected(self, user_id: str) -> bool:
        """Check if a user is currently connected"""
        return user_id in self.connected_users