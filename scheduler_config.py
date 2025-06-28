"""
Configuration constants and enums for the scheduler system.

This module centralizes all magic numbers, default values, and configuration
constants to improve maintainability and eliminate scattered hardcoded values.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional


class ConstraintType(Enum):
    """Enumeration of different constraint types in the scheduler"""
    GAP_BETWEEN_SHIFTS = "gap_between_shifts"
    INCOMPATIBILITY = "incompatibility"
    REDUCED_GAP = "reduced_gap"
    WEEKEND_LIMIT = "weekend_limit"
    CONSECUTIVE_WEEKENDS = "consecutive_weekends"
    POST_ROTATION = "post_rotation"
    WORK_PERCENTAGE = "work_percentage"
    MANDATORY_DAYS = "mandatory_days"
    DAYS_OFF = "days_off"


class SchedulerDefaults:
    """Default values for scheduler configuration"""
    
    # Time constraints
    GAP_BETWEEN_SHIFTS = 3
    MAX_CONSECUTIVE_WEEKENDS = 3
    
    # Worker constraints
    DEFAULT_WORK_PERCENTAGE = 100
    DEFAULT_TARGET_SHIFTS = 0
    MAX_SHIFTS_BUFFER = 2  # Buffer added to max_shifts_per_worker calculation
    
    # Performance thresholds
    MIN_COVERAGE_THRESHOLD = 95.0  # Minimum acceptable schedule coverage percentage
    MAX_DEVIATION_ALLOWED = 2  # Maximum shift deviation from target before warning
    
    # Optimization parameters
    DEFAULT_MAX_IMPROVEMENT_LOOPS = 70
    SCORE_IMPROVEMENT_THRESHOLD = 1.0
    
    # Validation thresholds
    MAX_IMBALANCE_RATIO = 0.3  # Maximum post imbalance ratio before warning
    
    # File and logging
    LOG_DIRECTORY = "logs"
    LOG_FILENAME = "scheduler.log"
    DEFAULT_USER = "saldo27"


class ValidationThresholds:
    """Thresholds for various validation checks"""
    
    MIN_WORK_PERCENTAGE = 0
    MAX_WORK_PERCENTAGE = 100
    MIN_SHIFTS = 1
    MIN_GAP_BETWEEN_SHIFTS = 0
    MIN_CONSECUTIVE_WEEKENDS = 1
    
    # Statistical thresholds
    GOOD_BALANCE_SCORE = 80.0
    ACCEPTABLE_BALANCE_SCORE = 60.0
    CRITICAL_VIOLATION_THRESHOLD = 5
    
    # Performance thresholds
    MAX_CONSTRAINT_VIOLATIONS_RATIO = 0.1
    MIN_WORKER_SATISFACTION_SCORE = 70.0


@dataclass
class SchedulerConfig:
    """
    Configuration class for scheduler parameters.
    
    Centralizes all configuration options with type hints and default values.
    """
    
    # Required parameters (no defaults)
    start_date: Any  # datetime
    end_date: Any    # datetime  
    num_shifts: int
    workers_data: list
    
    # Optional parameters with defaults
    gap_between_shifts: int = SchedulerDefaults.GAP_BETWEEN_SHIFTS
    max_consecutive_weekends: int = SchedulerDefaults.MAX_CONSECUTIVE_WEEKENDS
    variable_shifts: list = None
    holidays: list = None
    
    # Performance tuning
    max_improvement_loops: int = SchedulerDefaults.DEFAULT_MAX_IMPROVEMENT_LOOPS
    coverage_threshold: float = SchedulerDefaults.MIN_COVERAGE_THRESHOLD
    
    # Validation settings
    strict_validation: bool = True
    enable_constraint_caching: bool = True
    enable_performance_metrics: bool = False
    
    def __post_init__(self):
        """Post-initialization validation and setup"""
        if self.variable_shifts is None:
            self.variable_shifts = []
        if self.holidays is None:
            self.holidays = []
            
    def validate(self) -> None:
        """
        Validate configuration parameters
        
        Raises:
            ValueError: If any configuration parameter is invalid
        """
        if not isinstance(self.num_shifts, int) or self.num_shifts < ValidationThresholds.MIN_SHIFTS:
            raise ValueError(f"num_shifts must be an integer >= {ValidationThresholds.MIN_SHIFTS}")
            
        if not isinstance(self.gap_between_shifts, int) or self.gap_between_shifts < ValidationThresholds.MIN_GAP_BETWEEN_SHIFTS:
            raise ValueError(f"gap_between_shifts must be an integer >= {ValidationThresholds.MIN_GAP_BETWEEN_SHIFTS}")
            
        if not isinstance(self.max_consecutive_weekends, int) or self.max_consecutive_weekends < ValidationThresholds.MIN_CONSECUTIVE_WEEKENDS:
            raise ValueError(f"max_consecutive_weekends must be an integer >= {ValidationThresholds.MIN_CONSECUTIVE_WEEKENDS}")
            
        if not isinstance(self.workers_data, list) or len(self.workers_data) == 0:
            raise ValueError("workers_data must be a non-empty list")
            
        # Validate worker data
        for i, worker in enumerate(self.workers_data):
            if not isinstance(worker, dict):
                raise ValueError(f"Worker {i} must be a dictionary")
            if 'id' not in worker:
                raise ValueError(f"Worker {i} must have an 'id' field")
                
            work_percentage = worker.get('work_percentage', SchedulerDefaults.DEFAULT_WORK_PERCENTAGE)
            if not isinstance(work_percentage, (int, float)):
                raise ValueError(f"Worker {worker['id']} work_percentage must be a number")
            if work_percentage <= ValidationThresholds.MIN_WORK_PERCENTAGE or work_percentage > ValidationThresholds.MAX_WORK_PERCENTAGE:
                raise ValueError(f"Worker {worker['id']} work_percentage must be between {ValidationThresholds.MIN_WORK_PERCENTAGE} and {ValidationThresholds.MAX_WORK_PERCENTAGE}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary format for legacy compatibility"""
        return {
            'start_date': self.start_date,
            'end_date': self.end_date,
            'num_shifts': self.num_shifts,
            'workers_data': self.workers_data,
            'gap_between_shifts': self.gap_between_shifts,
            'max_consecutive_weekends': self.max_consecutive_weekends,
            'variable_shifts': self.variable_shifts,
            'holidays': self.holidays,
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'SchedulerConfig':
        """Create SchedulerConfig from dictionary (for legacy compatibility)"""
        return cls(**config_dict)


class PerformanceMetrics:
    """Constants for performance monitoring and metrics"""
    
    # Timing thresholds (in seconds)
    SLOW_OPERATION_THRESHOLD = 1.0
    VERY_SLOW_OPERATION_THRESHOLD = 5.0
    
    # Memory thresholds
    LARGE_OBJECT_SIZE_MB = 10
    
    # Algorithm complexity thresholds
    MAX_ACCEPTABLE_O_N_SQUARED_SIZE = 1000
    CACHE_SIZE_LIMIT = 10000
    
    # Metrics collection intervals
    METRICS_COLLECTION_INTERVAL = 100  # operations
    LOG_PERFORMANCE_EVERY_N_OPERATIONS = 1000


class CacheConfig:
    """Configuration for caching mechanisms"""
    
    # Cache sizes
    CONSTRAINT_CACHE_SIZE = 10000
    WORKER_LOOKUP_CACHE_SIZE = 1000
    DATE_CALCULATION_CACHE_SIZE = 5000
    
    # Cache TTL (time to live) in seconds
    DEFAULT_CACHE_TTL = 3600  # 1 hour
    
    # Cache enable/disable flags
    ENABLE_CONSTRAINT_CACHING = True
    ENABLE_WORKER_LOOKUP_CACHING = True
    ENABLE_DATE_CACHING = True