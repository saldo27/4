"""
Collaboration manager for multi-user real-time editing.
Handles conflict resolution and coordination between multiple users.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
import logging
from dataclasses import dataclass, field
from threading import Lock
import uuid

from event_bus import get_event_bus, EventType


@dataclass
class UserSession:
    """Information about an active user session"""
    user_id: str
    session_id: str
    started_at: datetime
    last_activity: datetime
    active_operations: Set[str] = field(default_factory=set)
    locked_resources: Set[str] = field(default_factory=set)


@dataclass
class ResourceLock:
    """Represents a lock on a schedule resource"""
    resource_id: str
    user_id: str
    session_id: str
    locked_at: datetime
    lock_type: str  # 'read', 'write', 'exclusive'
    expires_at: Optional[datetime] = None


@dataclass
class ConflictResolution:
    """Information about how a conflict was resolved"""
    conflict_id: str
    conflict_type: str
    resolution_method: str
    resolved_by: str
    resolution_data: Dict[str, Any] = field(default_factory=dict)


class CollaborationManager:
    """Manages multi-user collaboration and conflict resolution"""
    
    def __init__(self, scheduler):
        """
        Initialize the collaboration manager
        
        Args:
            scheduler: The main Scheduler instance
        """
        self.scheduler = scheduler
        self.event_bus = get_event_bus()
        
        # User session management
        self.active_sessions: Dict[str, UserSession] = {}
        self.session_to_user: Dict[str, str] = {}
        
        # Resource locking
        self.resource_locks: Dict[str, ResourceLock] = {}
        self._lock_mutex = Lock()
        
        # Conflict tracking
        self.pending_conflicts: Dict[str, Dict[str, Any]] = {}
        self.resolved_conflicts: List[ConflictResolution] = []
        
        # Configuration
        self.lock_timeout = timedelta(minutes=5)  # Default lock timeout
        self.session_timeout = timedelta(hours=8)  # Default session timeout
        
        # Setup event listeners
        self._setup_event_listeners()
        
        logging.info("CollaborationManager initialized")
    
    def _setup_event_listeners(self):
        """Set up event listeners for collaboration events"""
        self.event_bus.subscribe(EventType.USER_CONNECTED, self._on_user_connected)
        self.event_bus.subscribe(EventType.USER_DISCONNECTED, self._on_user_disconnected)
        self.event_bus.subscribe(EventType.SHIFT_ASSIGNED, self._on_schedule_change)
        self.event_bus.subscribe(EventType.SHIFT_UNASSIGNED, self._on_schedule_change)
        self.event_bus.subscribe(EventType.SHIFT_SWAPPED, self._on_schedule_change)
    
    def start_user_session(self, user_id: str) -> str:
        """
        Start a new user session
        
        Args:
            user_id: ID of the user
            
        Returns:
            Session ID
        """
        session_id = str(uuid.uuid4())
        
        # End any existing session for this user
        self.end_user_session(user_id)
        
        # Create new session
        session = UserSession(
            user_id=user_id,
            session_id=session_id,
            started_at=datetime.now(),
            last_activity=datetime.now()
        )
        
        self.active_sessions[user_id] = session
        self.session_to_user[session_id] = user_id
        
        logging.info(f"Started session {session_id} for user {user_id}")
        return session_id
    
    def end_user_session(self, user_id: str) -> bool:
        """
        End a user session and release all locks
        
        Args:
            user_id: ID of the user
            
        Returns:
            True if session was ended
        """
        if user_id not in self.active_sessions:
            return False
        
        session = self.active_sessions[user_id]
        
        # Release all locks held by this user
        self._release_user_locks(user_id)
        
        # Clean up session references
        if session.session_id in self.session_to_user:
            del self.session_to_user[session.session_id]
        del self.active_sessions[user_id]
        
        logging.info(f"Ended session for user {user_id}")
        return True
    
    def update_user_activity(self, user_id: str) -> bool:
        """
        Update the last activity time for a user
        
        Args:
            user_id: ID of the user
            
        Returns:
            True if activity was updated
        """
        if user_id in self.active_sessions:
            self.active_sessions[user_id].last_activity = datetime.now()
            return True
        return False
    
    def acquire_resource_lock(self, user_id: str, resource_id: str, lock_type: str = 'write') -> bool:
        """
        Acquire a lock on a schedule resource
        
        Args:
            user_id: ID of the user requesting the lock
            resource_id: ID of the resource to lock
            lock_type: Type of lock ('read', 'write', 'exclusive')
            
        Returns:
            True if lock was acquired
        """
        with self._lock_mutex:
            # Check if user has an active session
            if user_id not in self.active_sessions:
                logging.warning(f"User {user_id} has no active session")
                return False
            
            session = self.active_sessions[user_id]
            
            # Check if resource is already locked
            if resource_id in self.resource_locks:
                existing_lock = self.resource_locks[resource_id]
                
                # Check if lock has expired
                if (existing_lock.expires_at and 
                    datetime.now() > existing_lock.expires_at):
                    self._release_resource_lock(resource_id)
                else:
                    # Check if same user
                    if existing_lock.user_id == user_id:
                        # User already has the lock
                        return True
                    
                    # Check lock compatibility
                    if not self._are_locks_compatible(existing_lock.lock_type, lock_type):
                        logging.info(f"Lock conflict: {resource_id} locked by {existing_lock.user_id}")
                        return False
            
            # Acquire the lock
            lock = ResourceLock(
                resource_id=resource_id,
                user_id=user_id,
                session_id=session.session_id,
                locked_at=datetime.now(),
                lock_type=lock_type,
                expires_at=datetime.now() + self.lock_timeout
            )
            
            self.resource_locks[resource_id] = lock
            session.locked_resources.add(resource_id)
            
            # Emit lock event
            self.event_bus.emit(
                EventType.SCHEDULE_LOCKED,
                user_id=user_id,
                resource_id=resource_id,
                lock_type=lock_type
            )
            
            logging.debug(f"User {user_id} acquired {lock_type} lock on {resource_id}")
            return True
    
    def release_resource_lock(self, user_id: str, resource_id: str) -> bool:
        """
        Release a resource lock
        
        Args:
            user_id: ID of the user releasing the lock
            resource_id: ID of the resource to unlock
            
        Returns:
            True if lock was released
        """
        with self._lock_mutex:
            if resource_id not in self.resource_locks:
                return False
            
            lock = self.resource_locks[resource_id]
            
            # Check if user owns the lock
            if lock.user_id != user_id:
                logging.warning(f"User {user_id} tried to release lock owned by {lock.user_id}")
                return False
            
            return self._release_resource_lock(resource_id)
    
    def _release_resource_lock(self, resource_id: str) -> bool:
        """Internal method to release a resource lock"""
        if resource_id not in self.resource_locks:
            return False
        
        lock = self.resource_locks[resource_id]
        
        # Remove from user's locked resources
        if lock.user_id in self.active_sessions:
            session = self.active_sessions[lock.user_id]
            session.locked_resources.discard(resource_id)
        
        # Remove the lock
        del self.resource_locks[resource_id]
        
        # Emit unlock event
        self.event_bus.emit(
            EventType.SCHEDULE_UNLOCKED,
            user_id=lock.user_id,
            resource_id=resource_id,
            lock_type=lock.lock_type
        )
        
        logging.debug(f"Released lock on {resource_id}")
        return True
    
    def _release_user_locks(self, user_id: str):
        """Release all locks held by a user"""
        if user_id not in self.active_sessions:
            return
        
        session = self.active_sessions[user_id]
        locked_resources = session.locked_resources.copy()
        
        for resource_id in locked_resources:
            self._release_resource_lock(resource_id)
    
    def _are_locks_compatible(self, existing_lock_type: str, requested_lock_type: str) -> bool:
        """Check if two lock types are compatible"""
        # Read locks are compatible with other read locks
        if existing_lock_type == 'read' and requested_lock_type == 'read':
            return True
        
        # Exclusive locks are never compatible
        if existing_lock_type == 'exclusive' or requested_lock_type == 'exclusive':
            return False
        
        # Write locks are not compatible with anything
        if existing_lock_type == 'write' or requested_lock_type == 'write':
            return False
        
        return False
    
    def get_resource_lock_info(self, resource_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a resource lock
        
        Args:
            resource_id: ID of the resource
            
        Returns:
            Lock information or None if not locked
        """
        if resource_id not in self.resource_locks:
            return None
        
        lock = self.resource_locks[resource_id]
        return {
            'resource_id': lock.resource_id,
            'user_id': lock.user_id,
            'session_id': lock.session_id,
            'locked_at': lock.locked_at.isoformat(),
            'lock_type': lock.lock_type,
            'expires_at': lock.expires_at.isoformat() if lock.expires_at else None
        }
    
    def detect_operation_conflict(self, user_id: str, operation_type: str, operation_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect if an operation would conflict with other users' activities
        
        Args:
            user_id: ID of user performing the operation
            operation_type: Type of operation
            operation_data: Operation details
            
        Returns:
            Conflict information or None if no conflict
        """
        resource_ids = self._get_operation_resource_ids(operation_type, operation_data)
        
        conflicts = []
        for resource_id in resource_ids:
            if resource_id in self.resource_locks:
                lock = self.resource_locks[resource_id]
                if lock.user_id != user_id:
                    conflicts.append({
                        'resource_id': resource_id,
                        'locked_by': lock.user_id,
                        'lock_type': lock.lock_type,
                        'locked_at': lock.locked_at.isoformat()
                    })
        
        if conflicts:
            return {
                'conflict_type': 'resource_lock',
                'operation_type': operation_type,
                'conflicts': conflicts
            }
        
        return None
    
    def _get_operation_resource_ids(self, operation_type: str, operation_data: Dict[str, Any]) -> List[str]:
        """Get the resource IDs affected by an operation"""
        resource_ids = []
        
        if operation_type in ['assign_worker', 'unassign_worker']:
            # Resource ID for a specific shift slot
            shift_date = operation_data.get('shift_date')
            post_index = operation_data.get('post_index')
            if shift_date and post_index is not None:
                resource_ids.append(f"shift_{shift_date}_{post_index}")
        
        elif operation_type == 'swap_workers':
            # Resource IDs for both shift slots
            shift_date1 = operation_data.get('shift_date1')
            post_index1 = operation_data.get('post_index1')
            shift_date2 = operation_data.get('shift_date2')
            post_index2 = operation_data.get('post_index2')
            
            if all([shift_date1, post_index1 is not None, shift_date2, post_index2 is not None]):
                resource_ids.append(f"shift_{shift_date1}_{post_index1}")
                resource_ids.append(f"shift_{shift_date2}_{post_index2}")
        
        return resource_ids
    
    def resolve_conflict(self, conflict_id: str, resolution_method: str, 
                        resolved_by: str, resolution_data: Dict[str, Any] = None) -> bool:
        """
        Resolve a pending conflict
        
        Args:
            conflict_id: ID of the conflict to resolve
            resolution_method: Method used to resolve the conflict
            resolved_by: User who resolved the conflict
            resolution_data: Additional resolution data
            
        Returns:
            True if conflict was resolved
        """
        if conflict_id not in self.pending_conflicts:
            return False
        
        conflict = self.pending_conflicts[conflict_id]
        
        # Create resolution record
        resolution = ConflictResolution(
            conflict_id=conflict_id,
            conflict_type=conflict.get('type', 'unknown'),
            resolution_method=resolution_method,
            resolved_by=resolved_by,
            resolution_data=resolution_data or {}
        )
        
        self.resolved_conflicts.append(resolution)
        del self.pending_conflicts[conflict_id]
        
        logging.info(f"Conflict {conflict_id} resolved by {resolved_by} using {resolution_method}")
        return True
    
    def cleanup_expired_sessions(self):
        """Clean up expired user sessions and locks"""
        current_time = datetime.now()
        expired_users = []
        
        # Find expired sessions
        for user_id, session in self.active_sessions.items():
            if current_time - session.last_activity > self.session_timeout:
                expired_users.append(user_id)
        
        # Clean up expired sessions
        for user_id in expired_users:
            logging.info(f"Session expired for user {user_id}")
            self.end_user_session(user_id)
        
        # Clean up expired locks
        with self._lock_mutex:
            expired_locks = []
            for resource_id, lock in self.resource_locks.items():
                if lock.expires_at and current_time > lock.expires_at:
                    expired_locks.append(resource_id)
            
            for resource_id in expired_locks:
                logging.info(f"Lock expired for resource {resource_id}")
                self._release_resource_lock(resource_id)
    
    def get_collaboration_stats(self) -> Dict[str, Any]:
        """Get collaboration statistics"""
        current_time = datetime.now()
        
        # Active sessions stats
        active_session_count = len(self.active_sessions)
        recent_activity_count = sum(
            1 for session in self.active_sessions.values()
            if current_time - session.last_activity < timedelta(minutes=15)
        )
        
        # Lock stats
        active_lock_count = len(self.resource_locks)
        lock_types = {}
        for lock in self.resource_locks.values():
            lock_types[lock.lock_type] = lock_types.get(lock.lock_type, 0) + 1
        
        return {
            'active_sessions': active_session_count,
            'recent_activity': recent_activity_count,
            'active_locks': active_lock_count,
            'lock_types': lock_types,
            'pending_conflicts': len(self.pending_conflicts),
            'resolved_conflicts': len(self.resolved_conflicts),
            'timestamp': current_time.isoformat()
        }
    
    def get_user_info(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific user"""
        if user_id not in self.active_sessions:
            return None
        
        session = self.active_sessions[user_id]
        
        return {
            'user_id': user_id,
            'session_id': session.session_id,
            'started_at': session.started_at.isoformat(),
            'last_activity': session.last_activity.isoformat(),
            'active_operations': list(session.active_operations),
            'locked_resources': list(session.locked_resources)
        }
    
    def _on_user_connected(self, event):
        """Handle user connected events"""
        user_id = event.data.get('user_id')
        if user_id:
            self.start_user_session(user_id)
    
    def _on_user_disconnected(self, event):
        """Handle user disconnected events"""
        user_id = event.data.get('user_id')
        if user_id:
            self.end_user_session(user_id)
    
    def _on_schedule_change(self, event):
        """Handle schedule change events"""
        user_id = event.user_id
        if user_id:
            self.update_user_activity(user_id)