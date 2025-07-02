"""
Change tracking system for schedule modifications.
Provides undo/redo functionality and audit trail for real-time changes.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import logging
from dataclasses import dataclass, field
from enum import Enum
import json
from copy import deepcopy

from event_bus import get_event_bus, EventType


class ChangeType(Enum):
    """Types of schedule changes"""
    ASSIGNMENT = "assignment"
    UNASSIGNMENT = "unassignment"
    SWAP = "swap"
    BULK_UPDATE = "bulk_update"
    SCHEDULE_GENERATION = "schedule_generation"


@dataclass
class ScheduleChange:
    """Represents a single schedule change"""
    change_id: str
    change_type: ChangeType
    timestamp: datetime = field(default_factory=datetime.now)
    user_id: Optional[str] = None
    description: str = ""
    
    # Before state
    before_state: Dict[str, Any] = field(default_factory=dict)
    
    # After state
    after_state: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Rollback information
    can_rollback: bool = True
    rollback_applied: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert change to dictionary for serialization"""
        return {
            'change_id': self.change_id,
            'change_type': self.change_type.value,
            'timestamp': self.timestamp.isoformat(),
            'user_id': self.user_id,
            'description': self.description,
            'before_state': self.before_state,
            'after_state': self.after_state,
            'metadata': self.metadata,
            'can_rollback': self.can_rollback,
            'rollback_applied': self.rollback_applied
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScheduleChange':
        """Create change from dictionary"""
        return cls(
            change_id=data['change_id'],
            change_type=ChangeType(data['change_type']),
            timestamp=datetime.fromisoformat(data['timestamp']),
            user_id=data.get('user_id'),
            description=data.get('description', ''),
            before_state=data.get('before_state', {}),
            after_state=data.get('after_state', {}),
            metadata=data.get('metadata', {}),
            can_rollback=data.get('can_rollback', True),
            rollback_applied=data.get('rollback_applied', False)
        )


class ChangeTracker:
    """Tracks schedule changes and provides undo/redo functionality"""
    
    def __init__(self, scheduler, max_history: int = 100):
        """
        Initialize the change tracker
        
        Args:
            scheduler: The main Scheduler instance
            max_history: Maximum number of changes to keep in history
        """
        self.scheduler = scheduler
        self.event_bus = get_event_bus()
        self.max_history = max_history
        
        # Change tracking
        self._changes: List[ScheduleChange] = []
        self._current_position = -1  # Position in undo/redo stack
        
        # Setup event listeners
        self._setup_event_listeners()
        
        logging.info("ChangeTracker initialized")
    
    def _setup_event_listeners(self):
        """Set up event listeners to track changes"""
        self.event_bus.subscribe(EventType.SHIFT_ASSIGNED, self._on_shift_assigned)
        self.event_bus.subscribe(EventType.SHIFT_UNASSIGNED, self._on_shift_unassigned)
        self.event_bus.subscribe(EventType.SHIFT_SWAPPED, self._on_shift_swapped)
        self.event_bus.subscribe(EventType.BULK_UPDATE, self._on_bulk_update)
        self.event_bus.subscribe(EventType.SCHEDULE_GENERATED, self._on_schedule_generated)
    
    def record_change(self, change: ScheduleChange) -> None:
        """
        Record a schedule change
        
        Args:
            change: The change to record
        """
        # Remove any changes after current position (for redo)
        if self._current_position < len(self._changes) - 1:
            self._changes = self._changes[:self._current_position + 1]
        
        # Add the new change
        self._changes.append(change)
        self._current_position = len(self._changes) - 1
        
        # Maintain maximum history
        if len(self._changes) > self.max_history:
            self._changes.pop(0)
            self._current_position -= 1
        
        logging.debug(f"Recorded change: {change.change_id}")
    
    def undo(self, user_id: Optional[str] = None) -> Optional[ScheduleChange]:
        """
        Undo the last change
        
        Args:
            user_id: ID of user performing the undo
            
        Returns:
            The change that was undone, or None if no change to undo
        """
        if not self.can_undo():
            return None
        
        change = self._changes[self._current_position]
        
        if not change.can_rollback or change.rollback_applied:
            logging.warning(f"Cannot undo change {change.change_id}: rollback not allowed or already applied")
            return None
        
        try:
            # Apply the rollback
            success = self._apply_rollback(change)
            
            if success:
                change.rollback_applied = True
                self._current_position -= 1
                
                # Record the undo operation
                undo_change = ScheduleChange(
                    change_id=f"undo_{change.change_id}_{datetime.now().timestamp()}",
                    change_type=change.change_type,
                    user_id=user_id,
                    description=f"Undo: {change.description}",
                    before_state=change.after_state,
                    after_state=change.before_state,
                    metadata={'is_undo': True, 'original_change_id': change.change_id},
                    can_rollback=False  # Prevent undo of undo
                )
                
                # Don't record undo in normal history to avoid infinite loops
                logging.info(f"Undid change: {change.change_id}")
                return change
            else:
                logging.error(f"Failed to undo change: {change.change_id}")
                return None
                
        except Exception as e:
            logging.error(f"Error undoing change {change.change_id}: {e}")
            return None
    
    def redo(self, user_id: Optional[str] = None) -> Optional[ScheduleChange]:
        """
        Redo a previously undone change
        
        Args:
            user_id: ID of user performing the redo
            
        Returns:
            The change that was redone, or None if no change to redo
        """
        if not self.can_redo():
            return None
        
        self._current_position += 1
        change = self._changes[self._current_position]
        
        try:
            # Reapply the change
            success = self._reapply_change(change)
            
            if success:
                change.rollback_applied = False
                
                # Record the redo operation
                redo_change = ScheduleChange(
                    change_id=f"redo_{change.change_id}_{datetime.now().timestamp()}",
                    change_type=change.change_type,
                    user_id=user_id,
                    description=f"Redo: {change.description}",
                    before_state=change.before_state,
                    after_state=change.after_state,
                    metadata={'is_redo': True, 'original_change_id': change.change_id},
                    can_rollback=False  # Prevent undo of redo
                )
                
                logging.info(f"Redid change: {change.change_id}")
                return change
            else:
                self._current_position -= 1
                logging.error(f"Failed to redo change: {change.change_id}")
                return None
                
        except Exception as e:
            logging.error(f"Error redoing change {change.change_id}: {e}")
            self._current_position -= 1
            return None
    
    def can_undo(self) -> bool:
        """Check if undo is possible"""
        return (self._current_position >= 0 and 
                self._current_position < len(self._changes) and
                self._changes[self._current_position].can_rollback and
                not self._changes[self._current_position].rollback_applied)
    
    def can_redo(self) -> bool:
        """Check if redo is possible"""
        return (self._current_position < len(self._changes) - 1 and
                self._changes[self._current_position + 1].rollback_applied)
    
    def get_change_history(self, 
                          limit: Optional[int] = None,
                          user_id: Optional[str] = None,
                          change_type: Optional[ChangeType] = None) -> List[ScheduleChange]:
        """
        Get change history with optional filtering
        
        Args:
            limit: Maximum number of changes to return
            user_id: Filter by user ID
            change_type: Filter by change type
            
        Returns:
            List of changes matching criteria
        """
        changes = self._changes.copy()
        
        # Apply filters
        if user_id:
            changes = [c for c in changes if c.user_id == user_id]
        
        if change_type:
            changes = [c for c in changes if c.change_type == change_type]
        
        # Sort by timestamp (newest first)
        changes.sort(key=lambda x: x.timestamp, reverse=True)
        
        if limit:
            changes = changes[:limit]
        
        return changes
    
    def get_current_state_info(self) -> Dict[str, Any]:
        """Get information about current undo/redo state"""
        return {
            'total_changes': len(self._changes),
            'current_position': self._current_position,
            'can_undo': self.can_undo(),
            'can_redo': self.can_redo(),
            'next_undo': self._changes[self._current_position].description if self.can_undo() else None,
            'next_redo': self._changes[self._current_position + 1].description if self.can_redo() else None
        }
    
    def clear_history(self) -> None:
        """Clear all change history"""
        self._changes.clear()
        self._current_position = -1
        logging.info("Change history cleared")
    
    def export_audit_trail(self, filepath: str) -> bool:
        """
        Export audit trail to file
        
        Args:
            filepath: Path to export file
            
        Returns:
            True if export successful
        """
        try:
            audit_data = {
                'export_timestamp': datetime.now().isoformat(),
                'total_changes': len(self._changes),
                'changes': [change.to_dict() for change in self._changes]
            }
            
            with open(filepath, 'w') as f:
                json.dump(audit_data, f, indent=2)
            
            logging.info(f"Audit trail exported to {filepath}")
            return True
            
        except Exception as e:
            logging.error(f"Error exporting audit trail: {e}")
            return False
    
    def _apply_rollback(self, change: ScheduleChange) -> bool:
        """Apply rollback for a specific change"""
        try:
            if change.change_type == ChangeType.ASSIGNMENT:
                return self._rollback_assignment(change)
            elif change.change_type == ChangeType.UNASSIGNMENT:
                return self._rollback_unassignment(change)
            elif change.change_type == ChangeType.SWAP:
                return self._rollback_swap(change)
            elif change.change_type == ChangeType.BULK_UPDATE:
                return self._rollback_bulk_update(change)
            else:
                logging.warning(f"Rollback not implemented for change type: {change.change_type}")
                return False
                
        except Exception as e:
            logging.error(f"Error applying rollback for {change.change_id}: {e}")
            return False
    
    def _reapply_change(self, change: ScheduleChange) -> bool:
        """Reapply a previously undone change"""
        try:
            if change.change_type == ChangeType.ASSIGNMENT:
                return self._reapply_assignment(change)
            elif change.change_type == ChangeType.UNASSIGNMENT:
                return self._reapply_unassignment(change)
            elif change.change_type == ChangeType.SWAP:
                return self._reapply_swap(change)
            elif change.change_type == ChangeType.BULK_UPDATE:
                return self._reapply_bulk_update(change)
            else:
                logging.warning(f"Reapply not implemented for change type: {change.change_type}")
                return False
                
        except Exception as e:
            logging.error(f"Error reapplying change {change.change_id}: {e}")
            return False
    
    def _rollback_assignment(self, change: ScheduleChange) -> bool:
        """Rollback a worker assignment"""
        before_state = change.before_state
        shift_date = datetime.fromisoformat(before_state['shift_date'])
        post_index = before_state['post_index']
        previous_worker = before_state.get('previous_worker')
        worker_id = change.after_state['worker_id']
        
        # Remove current assignment
        if shift_date in self.scheduler.schedule:
            self.scheduler.schedule[shift_date][post_index] = previous_worker
            
            # Update tracking data
            self.scheduler._update_tracking_data(worker_id, shift_date, post_index, removing=True)
            if previous_worker:
                self.scheduler._update_tracking_data(previous_worker, shift_date, post_index, removing=False)
        
        return True
    
    def _rollback_unassignment(self, change: ScheduleChange) -> bool:
        """Rollback a worker unassignment"""
        before_state = change.before_state
        shift_date = datetime.fromisoformat(before_state['shift_date'])
        post_index = before_state['post_index']
        worker_id = before_state['worker_id']
        
        # Restore assignment
        if shift_date not in self.scheduler.schedule:
            self.scheduler.schedule[shift_date] = [None] * self.scheduler.num_shifts
        
        self.scheduler.schedule[shift_date][post_index] = worker_id
        
        # Update tracking data
        self.scheduler._update_tracking_data(worker_id, shift_date, post_index, removing=False)
        
        return True
    
    def _rollback_swap(self, change: ScheduleChange) -> bool:
        """Rollback a worker swap"""
        before_state = change.before_state
        shift1 = before_state['shift1']
        shift2 = before_state['shift2']
        
        shift_date1 = datetime.fromisoformat(shift1['date'])
        shift_date2 = datetime.fromisoformat(shift2['date'])
        
        # Restore original assignments
        if shift_date1 not in self.scheduler.schedule:
            self.scheduler.schedule[shift_date1] = [None] * self.scheduler.num_shifts
        if shift_date2 not in self.scheduler.schedule:
            self.scheduler.schedule[shift_date2] = [None] * self.scheduler.num_shifts
        
        self.scheduler.schedule[shift_date1][shift1['post']] = shift1['worker']
        self.scheduler.schedule[shift_date2][shift2['post']] = shift2['worker']
        
        # Update tracking data
        after_state = change.after_state
        
        # Remove current assignments and restore original ones
        if shift1['worker']:
            self.scheduler._update_tracking_data(shift1['worker'], shift_date2, shift2['post'], removing=True)
            self.scheduler._update_tracking_data(shift1['worker'], shift_date1, shift1['post'], removing=False)
        
        if shift2['worker'] and shift2['worker'] != shift1['worker']:
            self.scheduler._update_tracking_data(shift2['worker'], shift_date1, shift1['post'], removing=True)
            self.scheduler._update_tracking_data(shift2['worker'], shift_date2, shift2['post'], removing=False)
        
        return True
    
    def _rollback_bulk_update(self, change: ScheduleChange) -> bool:
        """Rollback a bulk update"""
        # For bulk updates, we would need to rollback each individual operation
        # This is complex and depends on the specific implementation
        logging.warning("Bulk update rollback not fully implemented")
        return False
    
    def _reapply_assignment(self, change: ScheduleChange) -> bool:
        """Reapply a worker assignment"""
        after_state = change.after_state
        shift_date = datetime.fromisoformat(after_state['shift_date'])
        post_index = after_state['post_index']
        worker_id = after_state['worker_id']
        
        # Apply assignment
        if shift_date not in self.scheduler.schedule:
            self.scheduler.schedule[shift_date] = [None] * self.scheduler.num_shifts
        
        previous_worker = self.scheduler.schedule[shift_date][post_index]
        self.scheduler.schedule[shift_date][post_index] = worker_id
        
        # Update tracking data
        if previous_worker:
            self.scheduler._update_tracking_data(previous_worker, shift_date, post_index, removing=True)
        self.scheduler._update_tracking_data(worker_id, shift_date, post_index, removing=False)
        
        return True
    
    def _reapply_unassignment(self, change: ScheduleChange) -> bool:
        """Reapply a worker unassignment"""
        after_state = change.after_state
        shift_date = datetime.fromisoformat(after_state['shift_date'])
        post_index = after_state['post_index']
        worker_id = change.before_state['worker_id']
        
        # Remove assignment
        if shift_date in self.scheduler.schedule:
            self.scheduler.schedule[shift_date][post_index] = None
            
            # Update tracking data
            self.scheduler._update_tracking_data(worker_id, shift_date, post_index, removing=True)
        
        return True
    
    def _reapply_swap(self, change: ScheduleChange) -> bool:
        """Reapply a worker swap"""
        after_state = change.after_state
        shift1 = after_state['shift1']
        shift2 = after_state['shift2']
        
        shift_date1 = datetime.fromisoformat(shift1['date'])
        shift_date2 = datetime.fromisoformat(shift2['date'])
        
        # Apply swap
        if shift_date1 not in self.scheduler.schedule:
            self.scheduler.schedule[shift_date1] = [None] * self.scheduler.num_shifts
        if shift_date2 not in self.scheduler.schedule:
            self.scheduler.schedule[shift_date2] = [None] * self.scheduler.num_shifts
        
        # Get original workers
        before_state = change.before_state
        original_worker1 = before_state['shift1']['worker']
        original_worker2 = before_state['shift2']['worker']
        
        # Apply the swap
        self.scheduler.schedule[shift_date1][shift1['post']] = original_worker2
        self.scheduler.schedule[shift_date2][shift2['post']] = original_worker1
        
        # Update tracking data
        if original_worker1:
            self.scheduler._update_tracking_data(original_worker1, shift_date1, shift1['post'], removing=True)
            self.scheduler._update_tracking_data(original_worker1, shift_date2, shift2['post'], removing=False)
        
        if original_worker2 and original_worker2 != original_worker1:
            self.scheduler._update_tracking_data(original_worker2, shift_date2, shift2['post'], removing=True)
            self.scheduler._update_tracking_data(original_worker2, shift_date1, shift1['post'], removing=False)
        
        return True
    
    def _reapply_bulk_update(self, change: ScheduleChange) -> bool:
        """Reapply a bulk update"""
        logging.warning("Bulk update reapply not fully implemented")
        return False
    
    def _on_shift_assigned(self, event):
        """Handle shift assignment events"""
        change = ScheduleChange(
            change_id=f"assign_{event.event_id}",
            change_type=ChangeType.ASSIGNMENT,
            user_id=event.user_id,
            description=f"Assigned {event.data['worker_id']} to {event.data['shift_date']} post {event.data['post_index']}",
            before_state={
                'shift_date': event.data['shift_date'],
                'post_index': event.data['post_index'],
                'previous_worker': event.data.get('previous_worker')
            },
            after_state={
                'shift_date': event.data['shift_date'],
                'post_index': event.data['post_index'],
                'worker_id': event.data['worker_id']
            }
        )
        self.record_change(change)
    
    def _on_shift_unassigned(self, event):
        """Handle shift unassignment events"""
        change = ScheduleChange(
            change_id=f"unassign_{event.event_id}",
            change_type=ChangeType.UNASSIGNMENT,
            user_id=event.user_id,
            description=f"Unassigned {event.data['worker_id']} from {event.data['shift_date']} post {event.data['post_index']}",
            before_state={
                'shift_date': event.data['shift_date'],
                'post_index': event.data['post_index'],
                'worker_id': event.data['worker_id']
            },
            after_state={
                'shift_date': event.data['shift_date'],
                'post_index': event.data['post_index'],
                'worker_id': None
            }
        )
        self.record_change(change)
    
    def _on_shift_swapped(self, event):
        """Handle shift swap events"""
        change = ScheduleChange(
            change_id=f"swap_{event.event_id}",
            change_type=ChangeType.SWAP,
            user_id=event.user_id,
            description=f"Swapped {event.data.get('worker1', 'empty')} and {event.data.get('worker2', 'empty')}",
            before_state={
                'shift1': {
                    'date': event.data['shift_date1'],
                    'post': event.data['post_index1'],
                    'worker': event.data.get('worker1')
                },
                'shift2': {
                    'date': event.data['shift_date2'],
                    'post': event.data['post_index2'],
                    'worker': event.data.get('worker2')
                }
            },
            after_state={
                'shift1': {
                    'date': event.data['shift_date1'],
                    'post': event.data['post_index1'],
                    'worker': event.data.get('worker2')
                },
                'shift2': {
                    'date': event.data['shift_date2'],
                    'post': event.data['post_index2'],
                    'worker': event.data.get('worker1')
                }
            }
        )
        self.record_change(change)
    
    def _on_bulk_update(self, event):
        """Handle bulk update events"""
        change = ScheduleChange(
            change_id=f"bulk_{event.event_id}",
            change_type=ChangeType.BULK_UPDATE,
            user_id=event.user_id,
            description=f"Bulk update: {event.data['successful_operations']}/{event.data['total_operations']} operations",
            metadata=event.data,
            can_rollback=False  # Bulk updates are complex to rollback
        )
        self.record_change(change)
    
    def _on_schedule_generated(self, event):
        """Handle schedule generation events"""
        change = ScheduleChange(
            change_id=f"generate_{event.event_id}",
            change_type=ChangeType.SCHEDULE_GENERATION,
            user_id=event.user_id,
            description="Generated new schedule",
            metadata=event.data,
            can_rollback=False  # Schedule generation creates new state
        )
        self.record_change(change)