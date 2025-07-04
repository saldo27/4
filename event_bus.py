"""
Event-driven architecture implementation for real-time scheduler features.
Provides centralized event handling for all schedule changes.
"""

from datetime import datetime
from typing import Dict, List, Callable, Any, Optional
from enum import Enum
import logging
from threading import Lock
from dataclasses import dataclass, field
import json


class EventType(Enum):
    """Define different types of schedule events"""
    SHIFT_ASSIGNED = "shift_assigned"
    SHIFT_UNASSIGNED = "shift_unassigned"
    SHIFT_SWAPPED = "shift_swapped"
    WORKER_ADDED = "worker_added"
    WORKER_REMOVED = "worker_removed"
    SCHEDULE_GENERATED = "schedule_generated"
    CONSTRAINT_VIOLATION = "constraint_violation"
    BULK_UPDATE = "bulk_update"
    VALIDATION_RESULT = "validation_result"
    USER_CONNECTED = "user_connected"
    USER_DISCONNECTED = "user_disconnected"
    SCHEDULE_LOCKED = "schedule_locked"
    SCHEDULE_UNLOCKED = "schedule_unlocked"
    REAL_TIME_ACTIVATED = "real_time_activated"


@dataclass
class ScheduleEvent:
    """Represents a schedule change event"""
    event_type: EventType
    timestamp: datetime = field(default_factory=datetime.now)
    user_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(datetime.now().timestamp()))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization"""
        return {
            'event_type': self.event_type.value,
            'timestamp': self.timestamp.isoformat(),
            'user_id': self.user_id,
            'data': self.data,
            'event_id': self.event_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScheduleEvent':
        """Create event from dictionary"""
        return cls(
            event_type=EventType(data['event_type']),
            timestamp=datetime.fromisoformat(data['timestamp']),
            user_id=data.get('user_id'),
            data=data.get('data', {}),
            event_id=data['event_id']
        )


class EventBus:
    """Centralized event bus for handling all schedule-related events"""
    
    def __init__(self, max_history: int = 1000):
        """
        Initialize the event bus
        
        Args:
            max_history: Maximum number of events to keep in history
        """
        self._listeners: Dict[EventType, List[Callable]] = {}
        self._event_history: List[ScheduleEvent] = []
        self._max_history = max_history
        self._lock = Lock()
        
        logging.info("EventBus initialized")
    
    def subscribe(self, event_type: EventType, callback: Callable[[ScheduleEvent], None]) -> None:
        """
        Subscribe to an event type
        
        Args:
            event_type: Type of event to listen for
            callback: Function to call when event occurs
        """
        with self._lock:
            if event_type not in self._listeners:
                self._listeners[event_type] = []
            self._listeners[event_type].append(callback)
            
        logging.debug(f"Subscribed to {event_type.value}")
    
    def unsubscribe(self, event_type: EventType, callback: Callable[[ScheduleEvent], None]) -> None:
        """
        Unsubscribe from an event type
        
        Args:
            event_type: Type of event to stop listening for
            callback: Function to remove from listeners
        """
        with self._lock:
            if event_type in self._listeners:
                try:
                    self._listeners[event_type].remove(callback)
                    if not self._listeners[event_type]:
                        del self._listeners[event_type]
                except ValueError:
                    pass  # Callback wasn't in the list
        
        logging.debug(f"Unsubscribed from {event_type.value}")
    
    def publish(self, event: ScheduleEvent) -> None:
        """
        Publish an event to all subscribers
        
        Args:
            event: Event to publish
        """
        with self._lock:
            # Add to history
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history.pop(0)
            
            # Notify listeners
            listeners = self._listeners.get(event.event_type, []).copy()
        
        # Call listeners outside of lock to prevent deadlocks
        for listener in listeners:
            try:
                listener(event)
            except Exception as e:
                logging.error(f"Error in event listener for {event.event_type.value}: {e}")
        
        logging.debug(f"Published event: {event.event_type.value}")
    
    def emit(self, event_type: EventType, user_id: Optional[str] = None, **data) -> None:
        """
        Emit an event with the given data
        
        Args:
            event_type: Type of event to emit
            user_id: ID of user who triggered the event
            **data: Event data as keyword arguments
        """
        event = ScheduleEvent(
            event_type=event_type,
            user_id=user_id,
            data=data
        )
        self.publish(event)
    
    def get_event_history(self, 
                         event_type: Optional[EventType] = None, 
                         since: Optional[datetime] = None,
                         limit: Optional[int] = None) -> List[ScheduleEvent]:
        """
        Get event history with optional filtering
        
        Args:
            event_type: Filter by event type
            since: Filter events since this timestamp
            limit: Limit number of events returned
            
        Returns:
            List of events matching the criteria
        """
        with self._lock:
            events = self._event_history.copy()
        
        # Apply filters
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if since:
            events = [e for e in events if e.timestamp >= since]
        
        # Sort by timestamp (newest first)
        events.sort(key=lambda x: x.timestamp, reverse=True)
        
        if limit:
            events = events[:limit]
        
        return events
    
    def clear_history(self) -> None:
        """Clear all event history"""
        with self._lock:
            self._event_history.clear()
        logging.info("Event history cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the event bus"""
        with self._lock:
            total_events = len(self._event_history)
            event_counts = {}
            
            for event in self._event_history:
                event_type = event.event_type.value
                event_counts[event_type] = event_counts.get(event_type, 0) + 1
            
            listener_counts = {
                event_type.value: len(listeners) 
                for event_type, listeners in self._listeners.items()
            }
        
        return {
            'total_events': total_events,
            'event_type_counts': event_counts,
            'listener_counts': listener_counts,
            'max_history': self._max_history
        }


# Global event bus instance
_global_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Get the global event bus instance"""
    global _global_event_bus
    if _global_event_bus is None:
        _global_event_bus = EventBus()
    return _global_event_bus


def reset_event_bus() -> None:
    """Reset the global event bus (useful for testing)"""
    global _global_event_bus
    _global_event_bus = None