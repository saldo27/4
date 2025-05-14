# Imports
from datetime import datetime, timedelta
import logging
from typing import TYPE_CHECKING
from exceptions import SchedulerError
if TYPE_CHECKING:
    from scheduler import Scheduler


class ConstraintChecker:
    """Handles all constraint checking logic for the scheduler"""
    
    # Methods
    def __init__(self, scheduler):
        """
        Initialize the constraint checker
    
        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
    
        # Store references to frequently accessed attributes
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        self.holidays = scheduler.holidays
        self.num_shifts = scheduler.num_shifts
        self.date_utils = scheduler.date_utils  # Add reference to date_utils
        self.gap_between_shifts = scheduler.gap_between_shifts
        self.max_consecutive_weekends = scheduler.max_consecutive_weekends
        self.max_shifts_per_worker = scheduler.max_shifts_per_worker
    
        logging.info("ConstraintChecker initialized")
    
    def _are_workers_incompatible(self, worker1_id, worker2_id):
        """
        Check if two workers are incompatible based SOLELY on the 'incompatible_with' list.
        """
        try:
            if worker1_id == worker2_id:
                return False

            worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
            worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)

            if not worker1 or not worker2:
                logging.warning(f"Could not find worker data for {worker1_id} or {worker2_id} during incompatibility check.")
                return False # Cannot determine incompatibility

            # Check 'incompatible_with' list in both directions
            # Ensure the lists contain IDs (handle potential variations if needed)
            incompatible_list1 = worker1.get('incompatible_with', [])
            incompatible_list2 = worker2.get('incompatible_with', [])

            # Perform the check - assuming IDs are stored directly in the list
            is_incompatible = (worker2_id in incompatible_list1) or \
                              (worker1_id in incompatible_list2)

            # Optional: Add debug log if needed
            if is_incompatible:
                logging.debug(f"Workers {worker1_id} and {worker2_id} are incompatible ('incompatible_with').")

            return is_incompatible

        except Exception as e:
            logging.error(f"Error checking worker incompatibility between {worker1_id} and {worker2_id}: {str(e)}")
            return False # Default to compatible on error
 

    def _check_incompatibility(self, worker_id, date):
        """Check if worker is incompatible with already assigned workers on a specific date"""
        try:
            # Use the schedule reference from self.scheduler
            if date not in self.scheduler.schedule:
                return True # No one assigned, compatible

            # Get the list of workers already assigned
            assigned_workers_list = self.scheduler.schedule.get(date, [])

            # Check the target worker against each assigned worker
            for assigned_id in assigned_workers_list:
                if assigned_id is None or assigned_id == worker_id:
                    continue

                # Use the corrected core incompatibility check
                if self._are_workers_incompatible(worker_id, assigned_id):
                    logging.debug(f"Incompatibility Violation: {worker_id} cannot work with {assigned_id} on {date}")
                    return False # Found incompatibility

            return True # No incompatibilities found

        except Exception as e:
            logging.error(f"Error checking incompatibility for worker {worker_id} on {date}: {str(e)}")
            return False # Fail safe - assume incompatible on error


    def _check_gap_constraint(self, worker_id, date, min_gap):
        """Check minimum gap between assignments"""
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        work_percentage = worker.get('work_percentage', 100) if worker else 100

        # Use consistent gap rules
        actual_min_gap = 3 if work_percentage < 100 else 2

        assignments = sorted(self.worker_assignments.get(worker_id, []))
        if assignments:
            for prev_date in assignments:
                days_between = abs((date - prev_date).days)
        
                # Basic gap check - Fixed to only check non-zero gaps
                if 0 < days_between < actual_min_gap:
                    return False
            
                # Special rule for full-time workers: Prevent Friday + Monday assignments
                if work_percentage >= 10:
                    if ((prev_date.weekday() == 4 and date.weekday() == 0) or 
                        (date.weekday() == 4 and prev_date.weekday() == 0)):
                        if days_between == 3:  # The gap between Friday and Monday
                            return False
        
                # Prevent same day of week in consecutive weeks
                if days_between in [7, 14]:
                    return False
            
        return True
    
    def _would_exceed_weekend_limit(self, worker_id, date, relaxation_level=0):
        """
        Check if assigning this date would exceed the weekend limit
        Modified to allow greater flexibility at higher relaxation levels
        """
        try:
            # If it's not a weekend day or holiday, no need to check
            if not (date.weekday() >= 4 or date in self.holidays):
                return False
    
            # Get all weekend assignments INCLUDING the new date
            weekend_assignments = [
                d for d in self.worker_assignments.get(worker_id, [])
                if (d.weekday() >= 4 or d in self.holidays)
            ]
        
            if date not in weekend_assignments:
                weekend_assignments.append(date)
        
            weekend_assignments.sort()  # Sort by date
        
            # CRITICAL: Still maintain the overall limit, but with more flexibility
            # at higher relaxation levels
            max_window_size = 21  # 3 weeks is the default
            max_weekend_count = self.max_consecutive_weekends  # Use the configured value
        
            # At relaxation level 1 or 2, allow adjacent weekends more easily
            if relaxation_level >= 1:
                # Adjust to looking at a floating window rather than centered window
                for i in range(len(weekend_assignments)):
                    # Check a window starting at this weekend assignment
                    window_start = weekend_assignments[i]
                    window_end = window_start + timedelta(days=max_window_size)
                
                    # Count weekend days in this window
                    window_weekend_count = sum(
                        1 for d in weekend_assignments
                        if window_start <= d <= window_end
                    )
                
                    if window_weekend_count > max_weekend_count:
                        return True
            else:
                # Traditional centered window check for strict enforcement
                for check_date in weekend_assignments:
                    window_start = check_date - timedelta(days=10)
                    window_end = check_date + timedelta(days=10)
                
                    # Count weekend days in this window
                    window_weekend_count = sum(
                        1 for d in weekend_assignments
                        if window_start <= d <= window_end
                    )    
                
                    if window_weekend_count > max_weekend_count:
                        return True
    
            return False
        
        except Exception as e:
            logging.error(f"Error checking weekend limit: {str(e)}")
            return True  # Fail safe
            
    def _is_worker_unavailable(self, worker_id, date):
        """
        Check if worker is unavailable on a specific date
        """
        try:
            worker = next(w for w in self.workers_data if w['id'] == worker_id)
        
            # Check days off
            if worker.get('days_off'):
                off_periods = self.date_utils.parse_date_ranges(worker['days_off'])
                if any(start <= date <= end for start, end in off_periods):
                    logging.debug(f"Worker {worker_id} is off on {date}")
                    return True

            # Check work periods
            if worker.get('work_periods'):
                work_periods = self.date_utils.parse_date_ranges(worker['work_periods'])
                if not any(start <= date <= end for start, end in work_periods):
                    logging.debug(f"Worker {worker_id} is not in work period on {date}")
                    return True

            # Check if worker is already assigned for this date
            if date in self.worker_assignments.get(worker_id, []):
                logging.debug(f"Worker {worker_id} is already assigned on {date}")
                return True

            # Check weekend constraints (replacing the custom weekend check)
            # Only check if this is a weekend day or holiday to improve performance
            if date.weekday() >= 4 or date in self.holidays:
                if self._would_exceed_weekend_limit(worker_id, date):
                    logging.debug(f"Worker {worker_id} would exceed weekend limit if assigned on {date}")
                    return True

            return False

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
            return True  # Default to unavailable in case of error
        
    def _can_assign_worker(self, worker_id, date, post):
        """
        Check if a worker can be assigned to a shift
        """
        try:
            # Log all constraint checks
            logging.debug(f"\nChecking worker {worker_id} for {date}, post {post}")

            # 1. First check - Incompatibility
            if not self._check_incompatibility(worker_id, date):
                logging.debug(f"- Failed: Worker {worker_id} is incompatible with assigned workers")
                return False

            # 2. Check max shifts
            if len(self.worker_assignments.get(worker_id, [])) >= self.max_shifts_per_worker:
                logging.debug(f"- Failed: Max shifts reached ({self.max_shifts_per_worker})")
                return False

            # 3. Check availability
            if self._is_worker_unavailable(worker_id, date):
                logging.debug(f"- Failed: Worker unavailable")
                return False

            # 4. CRITICAL: Check minimum gap - NEVER RELAX THIS
            assignments = sorted(list(self.worker_assignments.get(worker_id, [])))
            if assignments:
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                    if 0 < days_between < self.gap_between_shifts + 1:  # Fixed to only check non-zero gaps within range
                        logging.debug(f"- Failed: Insufficient gap ({days_between} days)")
                        return False

            # 6. CRITICAL: Check weekend limit - NEVER RELAX THIS
            if self._would_exceed_weekend_limit(worker_id, date):
                logging.debug(f"- Failed: Would exceed weekend limit")
                return False
        
            return True

        except Exception as e:
            logging.error(f"Error in _can_assign_worker for worker {worker_id}: {str(e)}", exc_info=True)
            return False
              
    def _check_constraints(self, worker_id, date, skip_constraints=False, try_part_time=False):
        """
        Unified constraint checking
        Returns: (bool, str) - (passed, reason_if_failed)
        """
        try:
            worker = next(w for w in self.workers_data if w['id'] == worker_id)
            work_percentage = float(worker.get('work_percentage', 100))

            # Basic availability checks (never skipped)
            if date in self.worker_assignments.get(worker_id, []):
                return False, "already assigned"

            if self._is_worker_unavailable(worker_id, date):
                return False, "unavailable"

            # Gap constraints
            if not skip_constraints:
                min_gap = 3 if try_part_time and work_percentage < 100 else self.gap_between_shifts + 1
            
                assignments = sorted(list(self.worker_assignments.get(worker_id, [])))
                if assignments:
                    for prev_date in assignments:
                        days_between = abs((date - prev_date).days)
                        if 0 < days_between < min_gap:  # Fixed to only check non-zero gaps within range
                            return False, f"gap constraint ({min_gap} days)"

            # Incompatibility constraints
            if not skip_constraints and not self._check_incompatibility(worker_id, date):
                return False, "incompatibility"

            # Weekend constraints
            if date.weekday() >= 4 or date in self.holidays:  
                if self._would_exceed_weekend_limit(worker_id, date):
                    return False, "too many weekend shifts in period"

            return True, "" 
        except Exception as e:
            logging.error(f"Error checking constraints for worker {worker_id}: {str(e)}")
            return False, f"error: {str(e)}"

    def _check_day_compatibility(self, worker_id, date):
        """Check if worker is compatible with all workers already assigned to this date"""
        if date not in self.schedule:
            return True
        
        for assigned_worker in self.schedule[date]:
            if assigned_worker is not None and self._are_workers_incompatible(worker_id, assigned_worker):
                logging.debug(f"Worker {worker_id} is incompatible with assigned worker {assigned_worker}")
                return False
        return True

    def _check_weekday_balance(self, worker_id, date):
        """
        Check if assigning this date would maintain weekday balance
        
        Returns:
            bool: True if assignment maintains balance, False otherwise
        """
        try:
            # Get current weekday counts including the new date
            weekday = date.weekday()
            weekday_counts = self.scheduler.worker_weekdays.get(worker_id, {}).copy()
            if not weekday_counts:
                weekday_counts = {i: 0 for i in range(7)}  # Initialize if needed
                
            weekday_counts[weekday] = weekday_counts.get(weekday, 0) + 1

            # Calculate maximum difference
            max_count = max(weekday_counts.values())
            min_count = min(weekday_counts.values())
        
            # Strictly enforce maximum 1 shift difference between weekdays
            if max_count - min_count > 1:
                logging.debug(f"Weekday balance violated for worker {worker_id}: {weekday_counts}")
                return False

            return True

        except Exception as e:
            logging.error(f"Error checking weekday balance for worker {worker_id}: {str(e)}")
            return True
 
    def _get_post_counts(self, worker_id):
        """
        Get the count of assignments for each post for a specific worker
    
        Args:
            worker_id: ID of the worker
        
        Returns:
            dict: Dictionary with post numbers as keys and counts as values
        """
        post_counts = {post: 0 for post in range(self.num_shifts)}
    
        for date, shifts in self.schedule.items():
            for post, assigned_worker in enumerate(shifts):
                if assigned_worker == worker_id:
                    post_counts[post] = post_counts.get(post, 0) + 1
                
        return post_counts
    
    def is_weekend_day(self, date):
        """Check if a date is a weekend day or holiday"""
        try:
            return date.weekday() >= 4 or date in self.holidays
        except Exception as e:
            logging.error(f"Error checking if date is weekend: {str(e)}")
            return False
