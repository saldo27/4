"""
Data structures for the scheduler system.

This module provides dataclasses and type-safe data structures to improve
code clarity and reduce the use of dictionaries for structured data.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Set, Optional, Any, Union
from enum import Enum


@dataclass
class WorkerData:
    """Type-safe representation of worker data"""
    
    id: str
    work_percentage: float = 100.0
    target_shifts: int = 0
    
    # Optional fields
    work_periods: str = ""
    days_off: str = ""
    mandatory_days: str = ""
    is_incompatible: bool = False
    incompatible_with: List[str] = field(default_factory=list)
    
    # Calculated fields (will be populated by scheduler)
    available_slots: int = 0
    actual_shifts: int = 0
    
    def __post_init__(self):
        """Validate and normalize worker data"""
        if not self.id:
            raise ValueError("Worker ID cannot be empty")
        
        # Normalize work percentage
        self.work_percentage = max(0.0, min(100.0, self.work_percentage))
        
        # Ensure target_shifts is non-negative
        self.target_shifts = max(0, self.target_shifts)
    
    @property
    def work_percentage_decimal(self) -> float:
        """Get work percentage as decimal (0.0 to 1.0)"""
        return self.work_percentage / 100.0
    
    @property
    def utilization_rate(self) -> float:
        """Calculate utilization rate based on target vs available slots"""
        if self.available_slots == 0:
            return 0.0
        return min(1.0, self.target_shifts / self.available_slots)
    
    @property
    def target_deviation(self) -> int:
        """Calculate deviation from target shifts"""
        return abs(self.actual_shifts - self.target_shifts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility"""
        return {
            'id': self.id,
            'work_percentage': self.work_percentage,
            'target_shifts': self.target_shifts,
            'work_periods': self.work_periods,
            'days_off': self.days_off,
            'mandatory_days': self.mandatory_days,
            'is_incompatible': self.is_incompatible,
            'incompatible_with': self.incompatible_with
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkerData':
        """Create WorkerData from dictionary"""
        return cls(
            id=data['id'],
            work_percentage=float(data.get('work_percentage', 100.0)),
            target_shifts=int(data.get('target_shifts', 0)),
            work_periods=data.get('work_periods', ''),
            days_off=data.get('days_off', ''),
            mandatory_days=data.get('mandatory_days', ''),
            is_incompatible=bool(data.get('is_incompatible', False)),
            incompatible_with=list(data.get('incompatible_with', []))
        )


@dataclass
class ShiftAssignment:
    """Represents a single shift assignment"""
    
    date: Union[datetime, date]
    worker_id: str
    post_index: int
    
    # Additional metadata
    is_weekend: bool = False
    is_holiday: bool = False
    is_mandatory: bool = False
    
    def __post_init__(self):
        """Validate assignment data"""
        if not self.worker_id:
            raise ValueError("Worker ID cannot be empty")
        
        if self.post_index < 0:
            raise ValueError("Post index must be non-negative")
        
        # Convert datetime to date if needed
        if isinstance(self.date, datetime):
            self.date = self.date.date()
    
    @property
    def date_str(self) -> str:
        """Get date as string in YYYY-MM-DD format"""
        return self.date.strftime('%Y-%m-%d')
    
    @property
    def weekday(self) -> int:
        """Get weekday (0=Monday, 6=Sunday)"""
        return self.date.weekday()
    
    @property
    def is_special_day(self) -> bool:
        """Check if this is a weekend or holiday"""
        return self.is_weekend or self.is_holiday


@dataclass
class ScheduleDay:
    """Represents all shifts for a single day"""
    
    date: Union[datetime, date]
    assignments: List[Optional[str]] = field(default_factory=list)
    expected_shifts: int = 1
    
    def __post_init__(self):
        """Initialize and validate day data"""
        # Convert datetime to date if needed
        if isinstance(self.date, datetime):
            self.date = self.date.date()
        
        # Ensure assignments list has correct length
        while len(self.assignments) < self.expected_shifts:
            self.assignments.append(None)
    
    @property
    def filled_shifts(self) -> int:
        """Count of non-None assignments"""
        return sum(1 for assignment in self.assignments if assignment is not None)
    
    @property
    def empty_shifts(self) -> int:
        """Count of None assignments"""
        return sum(1 for assignment in self.assignments if assignment is None)
    
    @property
    def fill_rate(self) -> float:
        """Percentage of shifts filled"""
        if self.expected_shifts == 0:
            return 0.0
        return self.filled_shifts / self.expected_shifts
    
    @property
    def assigned_workers(self) -> List[str]:
        """List of assigned worker IDs (excluding None)"""
        return [worker_id for worker_id in self.assignments if worker_id is not None]
    
    def assign_worker(self, worker_id: str, post_index: int) -> bool:
        """
        Assign a worker to a specific post.
        
        Args:
            worker_id: ID of worker to assign
            post_index: Index of post to assign to
            
        Returns:
            True if assignment successful, False if post already occupied
        """
        if post_index >= len(self.assignments):
            # Extend assignments list if needed
            while len(self.assignments) <= post_index:
                self.assignments.append(None)
        
        if self.assignments[post_index] is not None:
            return False  # Post already occupied
        
        self.assignments[post_index] = worker_id
        return True
    
    def remove_worker(self, post_index: int) -> Optional[str]:
        """
        Remove worker from a specific post.
        
        Args:
            post_index: Index of post to clear
            
        Returns:
            ID of removed worker, or None if post was already empty
        """
        if post_index >= len(self.assignments):
            return None
        
        removed_worker = self.assignments[post_index]
        self.assignments[post_index] = None
        return removed_worker
    
    def get_worker_post(self, worker_id: str) -> Optional[int]:
        """
        Get the post index for a specific worker.
        
        Args:
            worker_id: Worker to find
            
        Returns:
            Post index if worker is assigned, None otherwise
        """
        try:
            return self.assignments.index(worker_id)
        except ValueError:
            return None


@dataclass  
class ConstraintViolation:
    """Represents a constraint violation"""
    
    worker_id: str
    constraint_type: str
    date: Union[datetime, date]
    description: str
    severity: str = "warning"  # "error", "warning", "info"
    
    # Additional context
    other_worker_id: Optional[str] = None
    post_index: Optional[int] = None
    expected_value: Optional[Any] = None
    actual_value: Optional[Any] = None
    
    def __post_init__(self):
        """Validate violation data"""
        if not self.worker_id:
            raise ValueError("Worker ID cannot be empty")
        
        if not self.constraint_type:
            raise ValueError("Constraint type cannot be empty")
        
        # Convert datetime to date if needed
        if isinstance(self.date, datetime):
            self.date = self.date.date()
    
    @property
    def is_critical(self) -> bool:
        """Check if this is a critical violation"""
        return self.severity == "error"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'worker_id': self.worker_id,
            'constraint_type': self.constraint_type,
            'date': self.date.isoformat(),
            'description': self.description,
            'severity': self.severity,
            'other_worker_id': self.other_worker_id,
            'post_index': self.post_index,
            'expected_value': self.expected_value,
            'actual_value': self.actual_value
        }


@dataclass
class ScheduleStatistics:
    """Container for schedule statistics"""
    
    total_shifts: int = 0
    filled_shifts: int = 0
    empty_shifts: int = 0
    
    # Worker statistics
    worker_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Constraint violations
    violations: List[ConstraintViolation] = field(default_factory=list)
    
    # Performance metrics
    balance_score: float = 0.0
    coverage_percentage: float = 0.0
    satisfaction_score: float = 0.0
    
    @property
    def fill_rate(self) -> float:
        """Overall fill rate percentage"""
        if self.total_shifts == 0:
            return 0.0
        return (self.filled_shifts / self.total_shifts) * 100.0
    
    @property
    def violation_rate(self) -> float:
        """Violations per filled shift"""
        if self.filled_shifts == 0:
            return 0.0
        return len(self.violations) / self.filled_shifts
    
    @property
    def critical_violations(self) -> List[ConstraintViolation]:
        """List of critical violations only"""
        return [v for v in self.violations if v.is_critical]
    
    def add_violation(self, violation: ConstraintViolation):
        """Add a constraint violation"""
        self.violations.append(violation)
    
    def get_worker_stats(self, worker_id: str) -> Dict[str, Any]:
        """Get statistics for a specific worker"""
        return self.worker_stats.get(worker_id, {})
    
    def set_worker_stats(self, worker_id: str, stats: Dict[str, Any]):
        """Set statistics for a specific worker"""
        self.worker_stats[worker_id] = stats


class ScheduleIndex:
    """
    Efficient indexing for schedule lookups.
    
    Provides O(1) lookups for common schedule queries.
    """
    
    def __init__(self):
        # Core indexes
        self.worker_assignments: Dict[str, Set[date]] = {}
        self.date_assignments: Dict[date, List[Optional[str]]] = {}
        self.worker_posts: Dict[str, Set[int]] = {}
        
        # Weekend and holiday indexes
        self.weekend_assignments: Dict[str, Set[date]] = {}
        self.holiday_assignments: Dict[str, Set[date]] = {}
        
        # Post rotation index
        self.post_worker_counts: Dict[int, Dict[str, int]] = {}
        
        # Constraint violation index
        self.violations_by_worker: Dict[str, List[ConstraintViolation]] = {}
        self.violations_by_date: Dict[date, List[ConstraintViolation]] = {}
    
    def add_assignment(self, worker_id: str, assignment_date: date, post_index: int, 
                      is_weekend: bool = False, is_holiday: bool = False):
        """Add an assignment to all relevant indexes"""
        
        # Core indexes
        if worker_id not in self.worker_assignments:
            self.worker_assignments[worker_id] = set()
        self.worker_assignments[worker_id].add(assignment_date)
        
        if assignment_date not in self.date_assignments:
            self.date_assignments[assignment_date] = []
        
        # Extend list if needed
        while len(self.date_assignments[assignment_date]) <= post_index:
            self.date_assignments[assignment_date].append(None)
        
        self.date_assignments[assignment_date][post_index] = worker_id
        
        # Post tracking
        if worker_id not in self.worker_posts:
            self.worker_posts[worker_id] = set()
        self.worker_posts[worker_id].add(post_index)
        
        # Weekend/holiday tracking
        if is_weekend:
            if worker_id not in self.weekend_assignments:
                self.weekend_assignments[worker_id] = set()
            self.weekend_assignments[worker_id].add(assignment_date)
        
        if is_holiday:
            if worker_id not in self.holiday_assignments:
                self.holiday_assignments[worker_id] = set()
            self.holiday_assignments[worker_id].add(assignment_date)
        
        # Post rotation tracking
        if post_index not in self.post_worker_counts:
            self.post_worker_counts[post_index] = {}
        if worker_id not in self.post_worker_counts[post_index]:
            self.post_worker_counts[post_index][worker_id] = 0
        self.post_worker_counts[post_index][worker_id] += 1
    
    def remove_assignment(self, worker_id: str, assignment_date: date, post_index: int,
                         is_weekend: bool = False, is_holiday: bool = False):
        """Remove an assignment from all relevant indexes"""
        
        # Core indexes
        if worker_id in self.worker_assignments:
            self.worker_assignments[worker_id].discard(assignment_date)
        
        if assignment_date in self.date_assignments:
            if post_index < len(self.date_assignments[assignment_date]):
                self.date_assignments[assignment_date][post_index] = None
        
        # Weekend/holiday tracking
        if is_weekend and worker_id in self.weekend_assignments:
            self.weekend_assignments[worker_id].discard(assignment_date)
        
        if is_holiday and worker_id in self.holiday_assignments:
            self.holiday_assignments[worker_id].discard(assignment_date)
        
        # Post rotation tracking
        if (post_index in self.post_worker_counts and 
            worker_id in self.post_worker_counts[post_index]):
            self.post_worker_counts[post_index][worker_id] -= 1
            if self.post_worker_counts[post_index][worker_id] <= 0:
                del self.post_worker_counts[post_index][worker_id]
    
    def get_worker_assignments(self, worker_id: str) -> Set[date]:
        """Get all assignment dates for a worker"""
        return self.worker_assignments.get(worker_id, set())
    
    def get_date_assignments(self, assignment_date: date) -> List[Optional[str]]:
        """Get all worker assignments for a specific date"""
        return self.date_assignments.get(assignment_date, [])
    
    def get_worker_posts(self, worker_id: str) -> Set[int]:
        """Get all posts worked by a worker"""
        return self.worker_posts.get(worker_id, set())
    
    def get_weekend_assignments(self, worker_id: str) -> Set[date]:
        """Get weekend assignments for a worker"""
        return self.weekend_assignments.get(worker_id, set())
    
    def get_post_distribution(self, post_index: int) -> Dict[str, int]:
        """Get worker distribution for a specific post"""
        return self.post_worker_counts.get(post_index, {})
    
    def clear(self):
        """Clear all indexes"""
        self.worker_assignments.clear()
        self.date_assignments.clear()
        self.worker_posts.clear()
        self.weekend_assignments.clear()
        self.holiday_assignments.clear()
        self.post_worker_counts.clear()
        self.violations_by_worker.clear()
        self.violations_by_date.clear()