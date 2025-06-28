"""
Schedule validation module for the scheduler system.

This module contains all validation logic extracted from the main Scheduler class
to improve separation of concerns and maintainability.
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple, Set, Any, Optional
from dataclasses import dataclass

from scheduler_config import ConstraintType, ValidationThresholds, SchedulerDefaults
from exceptions import SchedulerError


@dataclass
class ValidationResult:
    """Result of a validation operation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    details: Dict[str, Any]
    
    def add_error(self, error: str):
        """Add an error to the result"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """Add a warning to the result"""
        self.warnings.append(warning)
    
    def merge(self, other: 'ValidationResult'):
        """Merge another validation result into this one"""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.details.update(other.details)
        if not other.is_valid:
            self.is_valid = False


class ScheduleValidator:
    """
    Handles all schedule validation logic.
    
    Extracted from the main Scheduler class to improve separation of concerns.
    """
    
    def __init__(self, scheduler):
        """
        Initialize the validator with a reference to the scheduler.
        
        Args:
            scheduler: Reference to the main Scheduler instance
        """
        self.scheduler = scheduler
        self.validation_cache = {}  # Cache for expensive validation operations
        
    def validate_final_schedule(self) -> ValidationResult:
        """
        Comprehensive validation of the final schedule.
        
        Returns:
            ValidationResult: Complete validation results with errors, warnings, and details
        """
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            details={}
        )
        
        try:
            # 1. Basic structure validation
            structure_result = self._validate_schedule_structure()
            result.merge(structure_result)
            
            # 2. Worker constraint validation
            constraints_result = self._validate_all_worker_constraints()
            result.merge(constraints_result)
            
            # 3. Coverage validation
            coverage_result = self._validate_coverage()
            result.merge(coverage_result)
            
            # 4. Balance validation
            balance_result = self._validate_balance()
            result.merge(balance_result)
            
            # 5. Constraint violations check
            violations_result = self._validate_constraint_violations()
            result.merge(violations_result)
            
            logging.info(f"Schedule validation completed: {len(result.errors)} errors, {len(result.warnings)} warnings")
            
        except Exception as e:
            result.add_error(f"Validation failed with exception: {str(e)}")
            logging.error(f"Validation exception: {str(e)}", exc_info=True)
        
        return result
    
    def _validate_schedule_structure(self) -> ValidationResult:
        """Validate the basic structure of the schedule"""
        result = ValidationResult(True, [], [], {})
        
        # Check if schedule exists and is properly initialized
        if not hasattr(self.scheduler, 'schedule') or not self.scheduler.schedule:
            result.add_error("Schedule is not initialized")
            return result
        
        # Check date range coverage
        expected_dates = set()
        current_date = self.scheduler.start_date
        while current_date <= self.scheduler.end_date:
            expected_dates.add(current_date)
            current_date += datetime.timedelta(days=1)
        
        actual_dates = set(self.scheduler.schedule.keys())
        missing_dates = expected_dates - actual_dates
        extra_dates = actual_dates - expected_dates
        
        if missing_dates:
            result.add_error(f"Missing dates in schedule: {sorted(missing_dates)}")
        
        if extra_dates:
            result.add_warning(f"Extra dates in schedule: {sorted(extra_dates)}")
        
        # Check shift structure
        total_slots = 0
        empty_slots = 0
        
        for date, shifts in self.scheduler.schedule.items():
            expected_shifts = self._get_expected_shifts_for_date(date)
            if len(shifts) != expected_shifts:
                result.add_error(f"Date {date} has {len(shifts)} shifts, expected {expected_shifts}")
            
            total_slots += len(shifts)
            empty_slots += shifts.count(None)
        
        result.details['total_slots'] = total_slots
        result.details['empty_slots'] = empty_slots
        result.details['fill_rate'] = (total_slots - empty_slots) / total_slots if total_slots > 0 else 0
        
        return result
    
    def _validate_all_worker_constraints(self) -> ValidationResult:
        """Validate constraints for all workers"""
        result = ValidationResult(True, [], [], {})
        
        worker_details = {}
        
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            worker_result = self._validate_worker_constraints(worker_id)
            result.merge(worker_result)
            worker_details[worker_id] = worker_result.details
        
        result.details['workers'] = worker_details
        return result
    
    def _validate_worker_constraints(self, worker_id: str) -> ValidationResult:
        """Validate all constraints for a specific worker"""
        result = ValidationResult(True, [], [], {})
        
        # Get worker assignments
        assignments = sorted(list(self.scheduler.worker_assignments.get(worker_id, set())))
        
        if not assignments:
            result.add_warning(f"Worker {worker_id} has no assignments")
            return result
        
        # Validate gap between shifts
        gap_result = self._validate_gap_constraints(worker_id, assignments)
        result.merge(gap_result)
        
        # Validate incompatibility constraints
        incomp_result = self._validate_incompatibility_constraints(worker_id, assignments)
        result.merge(incomp_result)
        
        # Validate weekend limits
        weekend_result = self._validate_weekend_constraints(worker_id, assignments)
        result.merge(weekend_result)
        
        # Validate post rotation
        post_result = self._validate_post_rotation(worker_id, assignments)
        result.merge(post_result)
        
        # Validate shift targets
        target_result = self._validate_shift_targets(worker_id, assignments)
        result.merge(target_result)
        
        return result
    
    def _validate_gap_constraints(self, worker_id: str, assignments: List[Any]) -> ValidationResult:
        """Validate gap between shifts constraints"""
        result = ValidationResult(True, [], [], {})
        
        gap_violations = 0
        min_gap = self.scheduler.gap_between_shifts + 1
        
        for i in range(len(assignments) - 1):
            date1 = assignments[i]
            date2 = assignments[i + 1]
            days_between = (date2 - date1).days
            
            if days_between < min_gap:
                gap_violations += 1
                result.add_error(
                    f"Worker {worker_id}: Gap violation between {date1} and {date2} "
                    f"({days_between} days, minimum required: {min_gap})"
                )
        
        result.details['gap_violations'] = gap_violations
        return result
    
    def _validate_incompatibility_constraints(self, worker_id: str, assignments: List[Any]) -> ValidationResult:
        """Validate worker incompatibility constraints"""
        result = ValidationResult(True, [], [], {})
        
        worker = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        if not worker:
            result.add_error(f"Worker {worker_id} not found in workers_data")
            return result
        
        incompatible_with = worker.get('incompatible_with', [])
        incompatibility_violations = 0
        
        for date in assignments:
            if date not in self.scheduler.schedule:
                continue
                
            assigned_workers = [w for w in self.scheduler.schedule[date] if w is not None]
            
            for other_worker in assigned_workers:
                if other_worker != worker_id and other_worker in incompatible_with:
                    incompatibility_violations += 1
                    result.add_error(
                        f"Worker {worker_id}: Incompatibility violation with {other_worker} on {date}"
                    )
        
        result.details['incompatibility_violations'] = incompatibility_violations
        return result
    
    def _validate_weekend_constraints(self, worker_id: str, assignments: List[Any]) -> ValidationResult:
        """Validate weekend and consecutive weekend constraints"""
        result = ValidationResult(True, [], [], {})
        
        weekend_assignments = [
            date for date in assignments 
            if self.scheduler.date_utils.is_weekend_day(date) or date in self.scheduler.holidays
        ]
        
        # Check consecutive weekends
        consecutive_weekends = self._count_consecutive_weekends(worker_id, weekend_assignments)
        max_consecutive = self.scheduler.max_consecutive_weekends
        
        if consecutive_weekends > max_consecutive:
            result.add_error(
                f"Worker {worker_id}: Exceeds maximum consecutive weekends "
                f"({consecutive_weekends} > {max_consecutive})"
            )
        
        result.details['weekend_assignments'] = len(weekend_assignments)
        result.details['consecutive_weekends'] = consecutive_weekends
        return result
    
    def _validate_post_rotation(self, worker_id: str, assignments: List[Any]) -> ValidationResult:
        """Validate post rotation distribution"""
        result = ValidationResult(True, [], [], {})
        
        post_counts = {}
        for date in assignments:
            if date in self.scheduler.schedule:
                shifts = self.scheduler.schedule[date]
                for post_idx, assigned_worker in enumerate(shifts):
                    if assigned_worker == worker_id:
                        post_counts[post_idx] = post_counts.get(post_idx, 0) + 1
        
        if not post_counts:
            result.add_warning(f"Worker {worker_id}: No post assignments found")
            return result
        
        # Calculate post distribution balance
        total_assignments = sum(post_counts.values())
        expected_per_post = total_assignments / self.scheduler.num_shifts
        
        max_deviation = 0
        for post in range(self.scheduler.num_shifts):
            actual_count = post_counts.get(post, 0)
            deviation = abs(actual_count - expected_per_post)
            max_deviation = max(max_deviation, deviation)
        
        imbalance_ratio = max_deviation / total_assignments if total_assignments > 0 else 0
        
        if imbalance_ratio > ValidationThresholds.MAX_IMBALANCE_RATIO:
            result.add_warning(
                f"Worker {worker_id}: Poor post rotation balance "
                f"(imbalance ratio: {imbalance_ratio:.2f})"
            )
        
        result.details['post_counts'] = post_counts
        result.details['imbalance_ratio'] = imbalance_ratio
        return result
    
    def _validate_shift_targets(self, worker_id: str, assignments: List[Any]) -> ValidationResult:
        """Validate shift targets vs actual assignments"""
        result = ValidationResult(True, [], [], {})
        
        worker = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        if not worker:
            return result
        
        target_shifts = worker.get('target_shifts', 0)
        actual_shifts = len(assignments)
        deviation = abs(actual_shifts - target_shifts)
        
        if deviation > SchedulerDefaults.MAX_DEVIATION_ALLOWED:
            result.add_warning(
                f"Worker {worker_id}: Significant deviation from target shifts "
                f"(actual: {actual_shifts}, target: {target_shifts}, deviation: {deviation})"
            )
        
        result.details['target_shifts'] = target_shifts
        result.details['actual_shifts'] = actual_shifts
        result.details['deviation'] = deviation
        return result
    
    def _validate_coverage(self) -> ValidationResult:
        """Validate schedule coverage"""
        result = ValidationResult(True, [], [], {})
        
        coverage = self._calculate_coverage()
        
        if coverage < SchedulerDefaults.MIN_COVERAGE_THRESHOLD:
            result.add_error(f"Low schedule coverage: {coverage:.1f}% (minimum: {SchedulerDefaults.MIN_COVERAGE_THRESHOLD}%)")
        elif coverage < 98.0:  # Warning threshold
            result.add_warning(f"Suboptimal schedule coverage: {coverage:.1f}%")
        
        result.details['coverage'] = coverage
        return result
    
    def _validate_balance(self) -> ValidationResult:
        """Validate overall schedule balance"""
        result = ValidationResult(True, [], [], {})
        
        # This would integrate with the statistics calculator
        if hasattr(self.scheduler, 'stats'):
            try:
                balance_score = self.scheduler.stats._calculate_balance_score()
                
                if balance_score < ValidationThresholds.ACCEPTABLE_BALANCE_SCORE:
                    result.add_warning(f"Poor schedule balance: {balance_score:.1f}")
                elif balance_score < ValidationThresholds.GOOD_BALANCE_SCORE:
                    result.add_warning(f"Suboptimal schedule balance: {balance_score:.1f}")
                
                result.details['balance_score'] = balance_score
            except Exception as e:
                result.add_warning(f"Could not calculate balance score: {str(e)}")
        
        return result
    
    def _validate_constraint_violations(self) -> ValidationResult:
        """Validate overall constraint violations"""
        result = ValidationResult(True, [], [], {})
        
        total_violations = 0
        violation_types = {}
        
        # Count violations by type
        for worker_id, skips in self.scheduler.constraint_skips.items():
            for constraint_type, violations in skips.items():
                if violations:
                    count = len(violations) if isinstance(violations, list) else 1
                    total_violations += count
                    violation_types[constraint_type] = violation_types.get(constraint_type, 0) + count
        
        total_assignments = sum(len(assignments) for assignments in self.scheduler.worker_assignments.values())
        violation_ratio = total_violations / total_assignments if total_assignments > 0 else 0
        
        if violation_ratio > ValidationThresholds.MAX_CONSTRAINT_VIOLATIONS_RATIO:
            result.add_error(
                f"High constraint violation ratio: {violation_ratio:.2f} "
                f"({total_violations} violations out of {total_assignments} assignments)"
            )
        
        result.details['total_violations'] = total_violations
        result.details['violation_types'] = violation_types
        result.details['violation_ratio'] = violation_ratio
        
        return result
    
    def _calculate_coverage(self) -> float:
        """Calculate schedule coverage percentage"""
        total_slots = sum(len(shifts) for shifts in self.scheduler.schedule.values())
        filled_slots = sum(
            sum(1 for worker in shifts if worker is not None)
            for shifts in self.scheduler.schedule.values()
        )
        
        return (filled_slots / total_slots * 100) if total_slots > 0 else 0
    
    def _get_expected_shifts_for_date(self, date) -> int:
        """Get expected number of shifts for a specific date"""
        # Check if this date has variable shifts configured
        for shift_config in self.scheduler.variable_shifts:
            if shift_config['start_date'] <= date <= shift_config['end_date']:
                return shift_config['shifts']
        
        # Return default number of shifts
        return self.scheduler.num_shifts
    
    def _count_consecutive_weekends(self, worker_id: str, weekend_assignments: List[Any]) -> int:
        """Count maximum consecutive weekends for a worker"""
        if not weekend_assignments:
            return 0
        
        # Group weekend assignments by weekend periods
        weekend_periods = []
        current_period = []
        
        for date in sorted(weekend_assignments):
            weekend_start = self.scheduler.date_utils._get_weekend_start(date)
            
            if not current_period or weekend_start == current_period[-1]:
                current_period.append(weekend_start)
            else:
                if current_period:
                    weekend_periods.append(len(current_period))
                current_period = [weekend_start]
        
        if current_period:
            weekend_periods.append(len(current_period))
        
        return max(weekend_periods) if weekend_periods else 0
    
    def clear_cache(self):
        """Clear validation cache"""
        self.validation_cache.clear()