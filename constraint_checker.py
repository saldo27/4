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
    
    def _would_exceed_weekend_limit(self, worker_id, date):
        """
        Check if assigning this date would exceed the weekend/holiday constraints:
        1. Max consecutive weekend/holiday constraint (from UI, adjusted for part-time)
        2. Total weekend/holiday count proportional to work periods and percentage
        """ 
        try:
            # Check if the date is a weekend or holiday
            is_weekend_or_holiday = (date.weekday() >= 4 or  # Fri, Sat, Sun
                                    date in self.scheduler.holidays or
                                    (date + timedelta(days=1)) in self.scheduler.holidays)
            if not is_weekend_or_holiday:
                return False  # Not a weekend/holiday, no need to check

            # Get worker data
            worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker_data:
                return True  # Worker not found
        
            # Get work percentage
            work_percentage = float(worker_data.get('work_percentage', 100))
        
            # Get work periods or default to full schedule period
            work_periods = []
            if 'work_periods' in worker_data and worker_data['work_periods'].strip():
                work_periods = self.date_utils.parse_date_ranges(worker_data['work_periods'])
                # Check if current date is within any work period
                if not any(start <= date <= end for start, end in work_periods):
                    # Date is outside work periods, so it's not a weekend limit issue but an availability issue
                    return False
            else:
                # Default to full schedule if no work periods specified
                work_periods = [(self.scheduler.start_date, self.scheduler.end_date)]

            # PART 1: CHECK MAX CONSECUTIVE WEEKENDS/HOLIDAYS
            # Get the base max consecutive value from UI
            base_max_consecutive = self.scheduler.max_consecutive_weekends
        
            # Adjust max consecutive for part-time workers (<70%)
            if work_percentage < 70:
                # For part-time workers, reduce the max consecutive proportionally (but at least 1)
                adjusted_max_consecutive = max(1, int(base_max_consecutive * work_percentage / 100))
            else:
                adjusted_max_consecutive = base_max_consecutive
        
            # Get all weekend/holiday assignments including the prospective date
            current_assignments = self.scheduler.worker_assignments.get(worker_id, set())
            weekend_dates = []
            for d in current_assignments:
                if (d.weekday() >= 4 or 
                    d in self.scheduler.holidays or
                    (d + timedelta(days=1)) in self.scheduler.holidays):
                    weekend_dates.append(d)
        
            # Add the date being checked if it's not already included
            if date not in weekend_dates:
                weekend_dates.append(date)
        
            weekend_dates.sort()
        
            # Check consecutive weekend/holiday constraint
            if weekend_dates:
                # Group weekend dates that are on consecutive calendar weekends
                consecutive_groups = []
                current_group = []
            
                for i, weekend_date in enumerate(weekend_dates):
                    if not current_group:
                        current_group = [weekend_date]
                    else:
                        prev_date = current_group[-1]
                        days_between = (weekend_date - prev_date).days
                    
                        # Consecutive weekends typically have 5-10 days between them
                        # (e.g., Sunday to Friday/Saturday/Sunday of next week)
                        if 5 <= days_between <= 10:
                            current_group.append(weekend_date)
                        else:
                            consecutive_groups.append(current_group)
                            current_group = [weekend_date]
            
                if current_group:
                    consecutive_groups.append(current_group)
            
                # Find the longest consecutive sequence
                max_consecutive = 0
                if consecutive_groups:
                    max_consecutive = max(len(group) for group in consecutive_groups)
            
                # Check against adjusted max consecutive limit
                if max_consecutive > adjusted_max_consecutive:
                    logging.debug(f"Worker {worker_id} would exceed adjusted max consecutive weekends/holidays: "
                                 f"{max_consecutive} > {adjusted_max_consecutive} "
                                 f"(base: {base_max_consecutive}, work %: {work_percentage}%)")
                    return True
        
            # PART 2: CHECK PROPORTIONAL TOTAL WEEKEND/HOLIDAY COUNT
        
            # Calculate total weekend/holiday days in the schedule
            all_days = self.scheduler._get_date_range(self.scheduler.start_date, self.scheduler.end_date)
            total_weekend_days = sum(1 for d in all_days if (
                d.weekday() >= 4 or 
                d in self.scheduler.holidays or
                (d + timedelta(days=1)) in self.scheduler.holidays
            ))
        
            # Calculate weekend/holiday days within worker's work periods
            worker_weekend_days = 0
            for start, end in work_periods:
                period_days = self.scheduler._get_date_range(start, end)
                worker_weekend_days += sum(1 for d in period_days if (
                    d.weekday() >= 4 or 
                    d in self.scheduler.holidays or
                    (d + timedelta(days=1)) in self.scheduler.holidays
                ))
        
            # Calculate the proportion of weekends this worker should cover
            # based on their work periods and work percentage
            proportion_of_schedule = worker_weekend_days / total_weekend_days if total_weekend_days > 0 else 0
            work_percentage_factor = work_percentage / 100
        
            # Calculate target weekend/holiday count for this worker
            # The 0.9 factor assumes approximately 90% of weekend/holiday slots need to be filled
            target_weekend_count = round(total_weekend_days * proportion_of_schedule * work_percentage_factor * 0.9)
        
            # Ensure at least 1 if they have any work periods with weekends
            if worker_weekend_days > 0:
                target_weekend_count = max(1, target_weekend_count)
        
            # Check if this assignment would exceed the target count
            if len(weekend_dates) > target_weekend_count:
                logging.debug(f"Worker {worker_id} would exceed proportional weekend/holiday count: "
                             f"{len(weekend_dates)} > {target_weekend_count} "
                             f"(work periods: {proportion_of_schedule:.2f} of schedule, "
                             f"work %: {work_percentage}%)")
                return True
        
            # All checks passed
            return False
    
        except Exception as e:
            logging.error(f"Error checking weekend limit for {worker_id} on {date}: {str(e)}", exc_info=True)
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
              
    def _check_constraints(self, worker_id, date, skip_constraints=False, try_part_time=False): # try_part_time seems unused
        """
        Unified constraint checking.
        Returns: (bool, str) - (passed, reason_if_failed)
        """
        try:
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                return False, "worker_not_found"
            # work_percentage = float(worker.get('work_percentage', 100)) # Not used directly in this version

            # Basic availability checks (never skipped)
            # self.scheduler.worker_assignments refers to the main worker assignment tracking
            if date in self.scheduler.worker_assignments.get(worker_id, []): # Check against current assignments
                return False, "already_assigned_this_day" 

            if self._is_worker_unavailable(worker_id, date): # This checks days_off, work_periods
                # _is_worker_unavailable already logs
                return False, "unavailable_generic" 

            if not skip_constraints:
                # Incompatibility constraints (with workers already in self.scheduler.schedule for that date)
                # Assuming self.scheduler.constraint_checker is this instance or has the method
                if not self._check_incompatibility(worker_id, date): # Check against self.scheduler.schedule
                    # _check_incompatibility logs
                    return False, "incompatibility"

                # Gap constraints (including 7/14 day pattern, Fri-Mon)
                # This uses self.scheduler.worker_assignments for its checks
                if not self._check_gap_constraint(worker_id, date):
                    # _check_gap_constraint logs
                    return False, "gap_or_pattern_constraint"
            
                # Weekend constraints (Max consecutive weekends/special days)
                # This uses self.scheduler.worker_assignments
                if self._would_exceed_weekend_limit(worker_id, date):
                    # _would_exceed_weekend_limit logs
                    return False, "weekend_limit_exceeded"

            return True, "passed_all_checks"
        except Exception as e:
            logging.error(f"Error checking constraints for worker {worker_id} on {date}: {str(e)}", exc_info=True)
            return False, f"error_in_check_constraints: {str(e)}"

    def _check_day_compatibility(self, worker_id, date):
        """Check if worker is compatible with all workers already assigned to this date"""
        if date not in self.schedule:
            return True
        
        for assigned_worker in self.schedule[date]:
            if assigned_worker is not None and self._are_workers_incompatible(worker_id, assigned_worker):
                logging.debug(f"Worker {worker_id} is incompatible with assigned worker {assigned_worker}")
                return False
        return True

    def _check_weekday_balance(self, worker_id, date_to_assign, current_assignments_for_worker):
        """
        Check if assigning date_to_assign to worker_id would maintain weekday balance (+/- 1).
        Uses a hypothetical count.

        Args:
            worker_id: The ID of the worker.
            date_to_assign: The datetime.date object of the shift being considered.
            current_assignments_for_worker: A set of datetime.date objects representing
                                             the worker's current assignments.
        Returns:
            bool: True if assignment maintains balance, False otherwise.
        """
        try:
            # 1. Calculate hypothetical weekday counts
            hypothetical_weekday_counts = {day: 0 for day in range(7)}

            # Count existing assignments
            for assigned_date in current_assignments_for_worker:
                hypothetical_weekday_counts[assigned_date.weekday()] += 1
            
            # Add the new assignment
            hypothetical_weekday_counts[date_to_assign.weekday()] += 1

            # 2. Calculate spread
            min_count = min(hypothetical_weekday_counts.values())
            max_count = max(hypothetical_weekday_counts.values())
            spread = max_count - min_count

            # 3. Check balance: spread > 1 means the difference is 2 or more, violating +/-1
            # Example: counts {Mon:1, Tue:1, Wed:3} -> min=1, max=3, spread=2. (Violates +/-1)
            # Example: counts {Mon:1, Tue:2, Wed:2} -> min=1, max=2, spread=1. (OK for +/-1)
            if spread > 1: # This means the difference is 2 or more.
                logging.debug(f"Constraint Check (Weekday Balance): Worker {worker_id} for date {date_to_assign.strftime('%Y-%m-%d')}. "
                              f"Hypothetical counts: {hypothetical_weekday_counts}, Spread: {spread}. VIOLATES +/-1 rule.")
                return False

            logging.debug(f"Constraint Check (Weekday Balance): Worker {worker_id} for date {date_to_assign.strftime('%Y-%m-%d')}. "
                          f"Hypothetical counts: {hypothetical_weekday_counts}, Spread: {spread}. OK for +/-1 rule.")
            return True

        except Exception as e:
            logging.error(f"Error in ConstraintChecker._check_weekday_balance for worker {worker_id}, date {date_to_assign}: {str(e)}", exc_info=True)
            return False # Safer to return False on error
 
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
