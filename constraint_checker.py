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


    def _check_gap_constraint(self, worker_id, date): # Removed min_gap parameter
        """Check minimum gap between assignments, Friday-Monday, and 7/14 day patterns."""
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker: return False # Should not happen
        work_percentage = worker.get('work_percentage', 100)

        # Determine base minimum days required *between* shifts
        # if gap_between_shifts = 1, then 2 days must be between assignments.
        # if gap_between_shifts = 3, then 4 days must be between assignments.
        # So, days_between must be >= self.scheduler.gap_between_shifts + 1
        min_required_days_between = self.scheduler.gap_between_shifts + 1
    
        # Part-time workers might need a larger gap
        if work_percentage < 70: # Using a common threshold from ScheduleBuilder
            min_required_days_between = max(min_required_days_between, self.scheduler.gap_between_shifts + 2) # e.g. at least +1 more day

        assignments = sorted(list(self.scheduler.worker_assignments.get(worker_id, []))) # Use scheduler's live assignments

        for prev_date in assignments:
            if prev_date == date: continue # Should not happen if checking before assignment
            days_between = abs((date - prev_date).days)

            # Basic gap check
            if days_between < min_required_days_between:
                logging.debug(f"Constraint Check: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails basic gap with {prev_date.strftime('%Y-%m-%d')} ({days_between} < {min_required_days_between})")
                return False
    
            # Friday-Monday rule: typically only if base gap is small (e.g., allows for 3-day difference)
            # This rule means a worker doing Fri cannot do Mon, creating a 3-day diff.
            # If min_required_days_between is already > 3, this rule is implicitly covered.
            if self.scheduler.gap_between_shifts <= 1: # Only apply if basic gap could allow a 3-day span
                if days_between == 3:
                    if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                        (date.weekday() == 4 and prev_date.weekday() == 0)):
                        logging.debug(f"Constraint Check: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails Fri-Mon rule with {prev_date.strftime('%Y-%m-%d')}")
                        return False
        
            # Prevent same day of week in consecutive weeks (7 or 14 day pattern)
            if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                logging.debug(f"Constraint Check: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails 7/14 day pattern with {prev_date.strftime('%Y-%m-%d')}")
                return False
        
        return True
    
    def _would_exceed_weekend_limit(self, worker_id, date): # No relaxation_level
        """
        Check if assigning this date would exceed the weekend limit.
        This constraint is NOT subject to relaxation.
        Uses logic similar to ScheduleBuilder._would_exceed_weekend_limit_simulated
        """ 
        try:
            # If it's not a weekend day or holiday, no need to check
            is_target_weekend = (date.weekday() >= 4 or # Fri, Sat, Sun
                                 date in self.scheduler.holidays or
                                 (date + timedelta(days=1)) in self.scheduler.holidays)
            if not is_target_weekend:
                return False

            worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker_data: return True # Should not happen

            work_percentage = worker_data.get('work_percentage', 100)
        
            # Use scheduler's config for max_consecutive_weekends
            max_allowed_consecutive = self.scheduler.max_consecutive_weekends 
            if work_percentage < 70: # Or your specific threshold for part-time
                max_allowed_consecutive = max(1, int(self.scheduler.max_consecutive_weekends * work_percentage / 100))

            # Get all weekend assignments INCLUDING the new date from scheduler's live data
            current_worker_assignments = self.scheduler.worker_assignments.get(worker_id, set())
            weekend_dates = []
            for d_val in current_worker_assignments:
                if (d_val.weekday() >= 4 or 
                    d_val in self.scheduler.holidays or
                    (d_val + timedelta(days=1)) in self.scheduler.holidays):
                    weekend_dates.append(d_val)
        
            if date not in weekend_dates: # Add the date being checked
                weekend_dates.append(date)
        
            weekend_dates.sort()

            if not weekend_dates:
                return False

            # Check for consecutive weekends (logic from ScheduleBuilder's simulated version)
            # This identifies groups of weekends that are on consecutive calendar weeks.
            consecutive_groups = []
            current_group = []
        
            for i, d_val in enumerate(weekend_dates):
                if not current_group:
                    current_group = [d_val]
                else:
                    prev_weekend_day_in_group = current_group[-1]
                    # A common definition for consecutive weekends: next calendar weekend.
                    # This usually means 5-9 days apart (e.g. Sat to next Fri, or Fri to next Sun)
                    # The original code in ScheduleBuilder used 5-10 days. Let's be consistent.
                    if 5 <= (d_val - prev_weekend_day_in_group).days <= 10: 
                        current_group.append(d_val)
                    else:
                        if current_group: # Save previous group
                            consecutive_groups.append(current_group)
                        current_group = [d_val] # Start new group
        
            if current_group: # Add the last group
                consecutive_groups.append(current_group)

            # Find the longest sequence of such consecutive weekends
            max_found_consecutive = 0
            if not consecutive_groups: # No weekends assigned at all
                 max_found_consecutive = 0
            elif all(len(g) == 1 for g in consecutive_groups): # Only isolated weekends
                max_found_consecutive = 1 if any(g for g in consecutive_groups) else 0
            else: # At least one group has more than one weekend
                max_found_consecutive = max(len(group) for group in consecutive_groups if group) if any(g for g in consecutive_groups) else 0
        
            if max_found_consecutive == 0 and weekend_dates: # If there are weekends, min consecutive is 1
                max_found_consecutive = 1

            if max_found_consecutive > max_allowed_consecutive:
                logging.debug(f"Constraint Check: Worker {worker_id} on {date.strftime('%Y-%m-%d')} would have {max_found_consecutive} consecutive weekends (max: {max_allowed_consecutive}).")
                return True

            return False
    
        except Exception as e:
            logging.error(f"Error checking weekend limit for {worker_id} on {date}: {str(e)}", exc_info=True)
            return True  # Fail safe: assume limit exceeded on error
            
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
            is_special_day_for_unavailability_check = (date.weekday() >= 4 or
                                                        date in self.holidays or
                                                        (date + timedelta(days=1)) in self.holidays)
            if is_special_day_for_unavailability_check:
                if self._would_exceed_weekend_limit(worker_id, date): # This now calls the consistently defined limit
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

            # 4. Check gap constraints (including 7/14 day pattern)
            if not self._check_gap_constraint(worker_id, date): # This method includes the 7/14 day check
                logging.debug(f"- Failed: Gap or 7/14 day pattern constraint for worker {worker_id} on {date}")
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
            if not skip_constraints: # Assuming skip_constraints applies to this too
                if not self._check_gap_constraint(worker_id, date): # Use the comprehensive check
                    # _check_gap_constraint already logs, or you can get a reason from it if refactored
                    return False, "gap or 7/14 day pattern constraint"

            # Incompatibility constraints
            if not skip_constraints and not self._check_incompatibility(worker_id, date):
                return False, "incompatibility"

            # Weekend constraints
            is_special_day_for_constraints_check = (date.weekday() >= 4 or
                                                    date in self.holidays or
                                                    (date + timedelta(days=1)) in self.holidays)
            if is_special_day_for_constraints_check:  
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
        """Check if a date is a weekend day or holiday or day before holiday."""
        try:
            return (date.weekday() >= 4 or # Friday, Saturday, Sunday
                    date in self.holidays or
                    (date + timedelta(days=1)) in self.holidays) # Day before a holiday
        except Exception as e:
            logging.error(f"Error checking if date is weekend: {str(e)}")
            return False
