# Imports
from datetime import datetime, timedelta
import copy
import logging
import random
import math
from typing import TYPE_CHECKING
from exceptions import SchedulerError
from adaptive_iterations import AdaptiveIterationManager
if TYPE_CHECKING:
    from scheduler import Scheduler

class AdaptiveIterationManager:
    """Manages iteration counts for scheduling optimization based on problem complexity"""
    
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.start_time = None
        self.convergence_threshold = 3
        self.max_time_minutes = 5
        
    def calculate_adaptive_iterations(self):
        """Calculate adaptive iteration counts for different optimization phases"""
        num_workers = len(self.scheduler.workers_data)
        shifts_per_day = self.scheduler.num_shifts
        total_days = (self.scheduler.end_date - self.scheduler.start_date).days + 1
        
        # Calculate complexity
        base_complexity = num_workers * shifts_per_day * total_days
        
        # Simple complexity-based iteration calculation
        if base_complexity < 1000:
            return {'max_optimization_loops': 5, 'last_post_max_iterations': 3}
        elif base_complexity < 5000:
            return {'max_optimization_loops': 8, 'last_post_max_iterations': 5}
        elif base_complexity < 15000:
            return {'max_optimization_loops': 12, 'last_post_max_iterations': 8}
        else:
            return {'max_optimization_loops': 20, 'last_post_max_iterations': 12}
    
    def start_timer(self):
        self.start_time = datetime.now()
    
    def should_continue(self, iteration, no_improvement_count):
        # Time check
        if self.start_time:
            elapsed_minutes = (datetime.now() - self.start_time).total_seconds() / 60
            if elapsed_minutes > self.max_time_minutes:
                return False
        
        # Convergence check
        return no_improvement_count < self.convergence_threshold

class ScheduleBuilder:
    """Handles schedule generation and improvement"""
    
    # 1. Initialization
    def __init__(self, scheduler):
        """
        Initialize the schedule builder

        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler

        # IMPORTANT: Use direct references, not copies
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule # self.schedule IS scheduler.schedule
        logging.debug(f"[ScheduleBuilder.__init__] self.schedule object ID: {id(self.schedule)}, Initial keys: {list(self.schedule.keys())}")
        self.config = scheduler.config
        self.worker_assignments = scheduler.worker_assignments  # Use the same reference
        self.num_shifts = scheduler.num_shifts
        self.holidays = scheduler.holidays
        self.constraint_checker = scheduler.constraint_checker
        self.best_schedule_data = None # Initialize the attribute to store the best state found
        self._locked_mandatory = set()
        # Keep track of which (worker_id, date) pairs are truly mandatory
        self.start_date = scheduler.start_date
        self.end_date = scheduler.end_date
        self.date_utils = scheduler.date_utils
        self.gap_between_shifts = scheduler.gap_between_shifts 
        self.max_shifts_per_worker = scheduler.max_shifts_per_worker
        self.max_consecutive_weekends = scheduler.max_consecutive_weekends 
        self.data_manager = scheduler.data_manager
        self.worker_posts = scheduler.worker_posts
        self.worker_weekdays = scheduler.worker_weekdays
        self.worker_weekends = scheduler.worker_weekends
        self.constraint_skips = scheduler.constraint_skips
        self.last_assigned_date = scheduler.last_assignment_date # Used in calculate_score
        self.consecutive_shifts = scheduler.consecutive_shifts # Used in calculate_score
        self.iteration_manager = AdaptiveIterationManager(scheduler)
        self.adaptive_config = self.iteration_manager.calculate_adaptive_iterations()
        logging.info(f"Adaptive config: {self.adaptive_config}")

        logging.debug(f"[ScheduleBuilder.__init__] self.schedule object ID: {id(self.schedule)}, Initial keys: {list(self.schedule.keys())[:5]}")
        logging.info("ScheduleBuilder initialized")
        
    # 2. Utility Methods
    def _parse_dates(self, date_str):
        """
        Parse semicolon-separated dates using the date_utils
    
        Args:
            date_str: String with semicolon-separated dates in DD-MM-YYYY format
        Returns:
            list: List of datetime objects
        """
        if not date_str:
            return []
    
        # Delegate to the DateTimeUtils class
        return self.date_utils.parse_dates(date_str)

    def _ensure_data_integrity(self):
        """
        Ensure all data structures are consistent - delegates to scheduler
        """
        # Let the scheduler handle the data integrity check as it has the primary data
        return self.scheduler._ensure_data_integrity()    

    def _verify_assignment_consistency(self):
        """
        Verify and fix data consistency between schedule and tracking data
        """
        # Check schedule against worker_assignments and fix inconsistencies
        for date, shifts in self.schedule.items():
            for post, worker_id in enumerate(shifts):
                if worker_id is None:
                    continue
                
                # Ensure worker is tracked for this date
                if date not in self.worker_assignments.get(worker_id, set()):
                    self.worker_assignments[worker_id].add(date)
    
        # Check worker_assignments against schedule
        for worker_id, assignments in self.worker_assignments.items():
            for date in list(assignments):  # Make a copy to safely modify during iteration
                # Check if this worker is actually in the schedule for this date
                if date not in self.schedule or worker_id not in self.schedule[date]:
                    # Remove this inconsistent assignment
                    self.worker_assignments[worker_id].remove(date)
                    logging.warning(f"Fixed inconsistency: Worker {worker_id} was tracked for {date} but not in schedule")

    # 3. Worker Constraint Check Methods

    def _is_mandatory(self, worker_id, date):
        # This is a placeholder for your actual implementation
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker: return False
        mandatory_days_str = worker.get('mandatory_days', '')
        if not mandatory_days_str: return False
        try:
            mandatory_dates = self.date_utils.parse_dates(mandatory_days_str)
            return date in mandatory_dates
        except:
            return False
            
    def _is_worker_unavailable(self, worker_id, date):
        """
        Check if a worker is unavailable on a specific date

        Args:
            worker_id: ID of the worker to check
            date: Date to check availability
    
        Returns:
            bool: True if worker is unavailable, False otherwise
        """
        # Get worker data
        worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None) # Corrected to worker_data as per user
        if not worker_data:
            return True
    
        # Debug log
        logging.debug(f"Checking availability for worker {worker_id} on {date.strftime('%d-%m-%Y')}")

        # Check work periods - if work_periods is empty, worker is available for all dates
        work_periods_str = worker_data.get('work_periods', '')
        if work_periods_str:
            try:
                work_ranges = self.date_utils.parse_date_ranges(work_periods_str)
                if not any(start <= date <= end for start, end in work_ranges):
                    logging.debug(f"Worker {worker_id} not available - date outside work periods")
                    return True # Not within any defined work period
            except Exception as e:
                logging.error(f"Error parsing work_periods for {worker_id}: {e}")
                return True # Fail safe
            # If we reach here, it means work_periods_str was present, parsed, and date is within a period.
            # So, the worker IS NOT unavailable due to work_periods. We proceed to check days_off.

        # Check days off
        days_off_str = worker_data.get('days_off', '')
        if days_off_str:
            try:
                off_ranges = self.date_utils.parse_date_ranges(days_off_str)
                if any(start <= date <= end for start, end in off_ranges):
                    logging.debug(f"Worker {worker_id} not available - date is a day off")
                    return True
            except Exception as e:
                logging.error(f"Error parsing days_off for {worker_id}: {e}")
                return True # Fail safe

        logging.debug(f"Worker {worker_id} is available on {date.strftime('%d-%m-%Y')}")
        return False
    
    def _check_incompatibility_with_list(self, worker_id_to_check, assigned_workers_list):
        """Checks if worker_id_to_check is incompatible with anyone in the list."""
        worker_to_check_data = next((w for w in self.workers_data if w['id'] == worker_id_to_check), None)
        if not worker_to_check_data: return True # Should not happen, but fail safe

        incompatible_with_candidate = set(worker_to_check_data.get('incompatible_with', []))

        # *** ADD TYPE LOGGING HERE ***
        logging.debug(f"  CHECKING_INTERNAL: Worker={worker_id_to_check} (Type: {type(worker_id_to_check)}), AgainstList={assigned_workers_list}, IncompListForCheckWorker={incompatible_with_candidate}") # Corrected variable name

        for assigned_id in assigned_workers_list:
            if assigned_id is None or assigned_id == worker_id_to_check:
                continue
            if str(assigned_id) in incompatible_with_candidate: # Ensure type consistency if IDs might be mixed
                return False # Candidate is incompatible with an already assigned worker

            # Bidirectional check
            assigned_worker_data = next((w for w in self.workers_data if w['id'] == assigned_id), None)
            if assigned_worker_data:
                if str(worker_id_to_check) in set(assigned_worker_data.get('incompatible_with', [])):
                    return False # Assigned worker is incompatible with the ca
        logging.debug(f"  CHECKING_INTERNAL: Worker={worker_id_to_check} vs List={assigned_workers_list} -> OK (No incompatibility detected)")
        return True # No incompatibilities found

    def _check_incompatibility(self, worker_id, date):
        # Placeholder using _check_incompatibility_with_list
        assigned_workers_on_date = [w for w in self.schedule.get(date, []) if w is not None]
        return self._check_incompatibility_with_list(worker_id, assigned_workers_on_date)

    def _are_workers_incompatible(self, worker1_id, worker2_id):
        """
        Check if two workers are incompatible with each other
    
        Args:
            worker1_id: ID of first worker
            worker2_id: ID of second worker
        
        Returns:
            bool: True if workers are incompatible, False otherwise
        """
        # Find the worker data for each worker
        worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
        worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
    
        if not worker1 or not worker2:
            return False
    
        # Check if either worker has the other in their incompatibility list
        incompatible_with_1 = worker1.get('incompatible_with', [])
        incompatible_with_2 = worker2.get('incompatible_with', [])
    
        return worker2_id in incompatible_with_1 or worker1_id in incompatible_with_2 

    def _would_exceed_weekend_limit(self, worker_id, date):
        """
        Check if adding this date would exceed the worker's weekend limit

        Args:
            worker_id: ID of the worker to check
            date: Date to potentially add
    
        Returns:
            bool: True if weekend limit would be exceeded, False otherwise
        """

        is_special_day = (date.weekday() >= 4 or # Friday, Saturday, Sunday
                  date in self.holidays or 
                  (date + timedelta(days=1)) in self.holidays)
        if not is_special_day:
            return False # Not a day relevant for this constraint
        
        # Skip if not a weekend
        if not self.date_utils.is_weekend_day(date) and date not in self.holidays:
            return False

        # Get worker data
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker:
            return True

        # Get weekend assignments for this worker
        current_worker_assignments = self.scheduler.worker_assignments.get(worker_id, set())
        weekend_dates = []
        for d_val in current_worker_assignments:
            if (d_val.weekday() >= 4 or 
                d_val in self.scheduler.holidays or
                (d_val + timedelta(days=1)) in self.scheduler.holidays):
                weekend_dates.append(d_val)

        # When checking the target 'date' itself:
        if (date.weekday() >= 4 or 
            date in self.holidays or 
            (date + timedelta(days=1)) in self.holidays):
            if date not in weekend_dates: # if it's a special day being added
                weekend_dates.append(date)

        # Calculate the maximum allowed weekend shifts based on work percentage
        work_percentage = worker.get('work_percentage', 100)
        max_weekend_shifts = self.max_consecutive_weekends  # Use the configurable parameter
        if work_percentage < 100:
            # For part-time workers, adjust max consecutive weekends proportionally
            max_weekend_shifts = max(1, int(self.max_consecutive_weekends * work_percentage / 100))

        # Check if adding this date would exceed the limit for any 3-week period
        if date in weekend_dates:
            return False  # Already counted

        # Add the date temporarily
        test_dates = weekend_dates + [date]
        test_dates.sort()

        # Check for any 3-week period with too many weekend shifts
        three_weeks = timedelta(days=21)
        for i, start_date_val in enumerate(test_dates): # Renamed start_date to avoid conflict
            end_date_val = start_date_val + three_weeks # Renamed end_date to avoid conflict
            count = sum(1 for d in test_dates[i:] if d <= end_date_val)
            if count > max_weekend_shifts:
                return True

        return False

    def _get_post_counts(self, worker_id):
        """
        Get the count of assignments for each post for a specific worker
    
        Args:
            worker_id: ID of the worker
        
        Returns:
            dict: Dictionary with post numbers as keys and counts as values
        """
        post_counts = {post: 0 for post in range(self.num_shifts)}
    
        for date_val, shifts in self.schedule.items(): # Renamed date to avoid conflict
            for post, assigned_worker in enumerate(shifts):
                if assigned_worker == worker_id:
                    post_counts[post] = post_counts.get(post, 0) + 1
    
        return post_counts

    def _update_worker_stats(self, worker_id, date, removing=False):
        """
        Update worker statistics when adding or removing an assignment
    
        Args:
            worker_id: ID of the worker
            date: The date of the assignment
            removing: Whether we're removing (True) or adding (False) an assignment
        """
        # Update weekday counts
        weekday = date.weekday()
        if worker_id in self.worker_weekdays:
            if removing:
                self.worker_weekdays[worker_id][weekday] = max(0, self.worker_weekdays[worker_id][weekday] - 1)
            else:
                self.worker_weekdays[worker_id][weekday] += 1
    
        # Update weekend tracking
        is_weekend = date.weekday() >= 4 or date in self.holidays  # Friday, Saturday, Sunday or holiday
        if is_weekend and worker_id in self.worker_weekends:
            if removing:
                if date in self.worker_weekends[worker_id]:
                    self.worker_weekends[worker_id].remove(date)
            else:
                if date not in self.worker_weekends[worker_id]:
                    self.worker_weekends[worker_id].append(date)
                    self.worker_weekends[worker_id].sort()

    def _verify_no_incompatibilities(self):
        """
        Verify that the final schedule doesn't have any incompatibility violations
        and fix any found violations.
        """
        logging.info("Performing final incompatibility verification check")
    
        violations_found = 0
        violations_fixed = 0
    
        # Check each date for incompatible worker assignments
        for date_val in sorted(self.schedule.keys()): # Renamed date to avoid conflict
            workers_today = [w for w in self.schedule[date_val] if w is not None]
        
            # Process all pairs to find incompatibilities
            for i in range(len(workers_today)):
                for j in range(i+1, len(workers_today)):
                    worker1_id = workers_today[i]
                    worker2_id = workers_today[j]
                
                    # Check if they are incompatible
                    if self._are_workers_incompatible(worker1_id, worker2_id):
                        violations_found += 1
                        logging.warning(f"Final verification found incompatibility violation: {worker1_id} and {worker2_id} on {date_val.strftime('%d-%m-%Y')}")
                    
                        # Find their positions
                        post1 = self.schedule[date_val].index(worker1_id)
                        post2 = self.schedule[date_val].index(worker2_id)
                    
                        # Remove one of the workers (choose the one with more shifts assigned)
                        w1_shifts = len(self.worker_assignments.get(worker1_id, set()))
                        w2_shifts = len(self.worker_assignments.get(worker2_id, set()))
                    
                        # Remove the worker with more shifts or the second worker if equal
                        if w1_shifts > w2_shifts:
                            self.schedule[date_val][post1] = None
                            self.worker_assignments[worker1_id].remove(date_val)
                            self.scheduler._update_tracking_data(worker1_id, date_val, post1, removing=True)
                            violations_fixed += 1
                            logging.info(f"Removed worker {worker1_id} from {date_val.strftime('%d-%m-%Y')} to fix incompatibility")
                        else:
                            self.schedule[date_val][post2] = None
                            self.worker_assignments[worker2_id].remove(date_val)
                            self.scheduler._update_tracking_data(worker2_id, date_val, post2, removing=True)
                            violations_fixed += 1
                            logging.info(f"Removed worker {worker2_id} from {date_val.strftime('%d-%m-%Y')} to fix incompatibility")
    
        logging.info(f"Final verification: found {violations_found} violations, fixed {violations_fixed}")
        return violations_fixed > 0

    # 4. Worker Assignment Methods

    def _can_assign_worker(self, worker_id, date, post):
        try:
            # Log all constraint checks
            logging.debug(f"\nChecking worker {worker_id} for {date}, post {post}")
        
            # Skip if already assigned to this date
            if worker_id in self.schedule.get(date, []):
                return False
            
            # Get worker data
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                return False
            
            # Check worker availability (days off)
            if self._is_worker_unavailable(worker_id, date):
                return False
            
            # Check for incompatibilities
            if not self._check_incompatibility(worker_id, date):
                return False
            
            # Check minimum gap and 7-14 day pattern
            assignments = sorted(list(self.worker_assignments.get(worker_id, [])))
            if assignments:
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                
                    # Check minimum gap
                    if 0 < days_between < self.gap_between_shifts + 1:
                        logging.debug(f"- Failed: Insufficient gap ({days_between} days)")
                        return False
                
                    # Check for 7-14 day pattern (same weekday in consecutive weeks)
                    if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                        logging.debug(f"- Failed: Would create {days_between} day pattern")
                        return False
            
            # Special case: Friday-Monday check if gap is only 1 day
            if self.gap_between_shifts == 1:
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                    if days_between == 3:
                        if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                            (date.weekday() == 4 and prev_date.weekday() == 0)):
                            return False

            # Check weekend limits
            if self._would_exceed_weekend_limit(worker_id, date):
                return False

            # Part-time workers need more days between shifts
            work_percentage = worker.get('work_percentage', 100)
            if work_percentage < 70:
                part_time_gap = max(3, self.gap_between_shifts + 2)
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                    if days_between < part_time_gap:
                        return False

            # If we've made it this far, the worker can be assigned
            return True
    
        except Exception as e:
            logging.error(f"Error in _can_assign_worker for worker {worker_id}: {str(e)}", exc_info=True)
            return False

    def assign_worker_to_shift(self, worker_id, date, post):
        """Assign a worker to a shift with proper incompatibility checking"""
    
        # Check if the date already exists in the schedule
        if date not in self.schedule:
            self.schedule[date] = [None] * self.num_shifts
        
        # Check for incompatibility with already assigned workers
        already_assigned = [w for w in self.schedule[date] if w is not None]
        if not self._check_incompatibility_with_list(worker_id, already_assigned):
            logging.warning(f"Cannot assign worker {worker_id} due to incompatibility on {date}")
            return False
        
        # Proceed with assignment if no incompatibility
        self.schedule[date][post] = worker_id
        self.scheduler._update_tracking_data(worker_id, date, post) # Corrected: self.scheduler._update_tracking_data
        return True
    
    def _can_swap_assignments(self, worker_id, date_from, post_from, date_to, post_to):
        """
        Checks if moving worker_id from (date_from, post_from) to (date_to, post_to) is valid.
        Uses deepcopy for safer simulation.
        """
        # --- Simulation Setup ---\
        # Create deep copies of the schedule and assignments
        try:
            # Use scheduler\'s references for deepcopy
            simulated_schedule = copy.deepcopy(self.scheduler.schedule)
            simulated_assignments = copy.deepcopy(self.scheduler.worker_assignments)

            # --- Simulate the Swap ---\
            # 1. Check if \'from\' state is valid before simulating removal
            if date_from not in simulated_schedule or \
               len(simulated_schedule[date_from]) <= post_from or \
               simulated_schedule[date_from][post_from] != worker_id or \
               worker_id not in simulated_assignments or \
               date_from not in simulated_assignments[worker_id]:
                    logging.warning(f"_can_swap_assignments: Initial state invalid for removing {worker_id} from {date_from}|P{post_from}. Aborting check.")
                    return False # Cannot simulate if initial state is wrong

            # 2. Simulate removing worker from \'from\' position
            simulated_schedule[date_from][post_from] = None
            simulated_assignments[worker_id].remove(date_from)
            # Clean up empty set for worker if needed
            if not simulated_assignments[worker_id]:
                 del simulated_assignments[worker_id]


            # 3. Simulate adding worker to \'to\' position
            # Ensure target list exists and is long enough in the simulation
            simulated_schedule.setdefault(date_to, [None] * self.num_shifts)
            while len(simulated_schedule[date_to]) <= post_to:
                simulated_schedule[date_to].append(None)

            # Check if target slot is empty in simulation before placing
            if simulated_schedule[date_to][post_to] is not None:
                logging.debug(f"_can_swap_assignments: Target slot {date_to}|P{post_to} is not empty in simulation. Aborting check.")
                return False

            simulated_schedule[date_to][post_to] = worker_id
            simulated_assignments.setdefault(worker_id, set()).add(date_to)
            
            # --- Check Constraints on Simulated State ---\
            # Check if the worker can be assigned to the target slot considering the simulated state
            can_assign_to_target = self._check_constraints_on_simulated(\
                worker_id, date_to, post_to, simulated_schedule, simulated_assignments\
            )

            # Also check if the source date is still valid *without* the worker
            # (e.g., maybe removing the worker caused an issue for others on date_from)
            source_date_still_valid = self._check_all_constraints_for_date_simulated(\
                date_from, simulated_schedule, simulated_assignments\
            )

            # Also check if the target date remains valid *with* the worker added
            target_date_still_valid = self._check_all_constraints_for_date_simulated(\
                 date_to, simulated_schedule, simulated_assignments\
            )


            is_valid_swap = can_assign_to_target and source_date_still_valid and target_date_still_valid

            # --- End Simulation ---\
            # No rollback needed as we operated on copies.

            logging.debug(f"Swap Check: {worker_id} from {date_from}|P{post_from} to {date_to}|P{post_to}. Valid: {is_valid_swap} (Target OK: {can_assign_to_target}, Source OK: {source_date_still_valid}, Target Date OK: {target_date_still_valid})") # Corrected log string
            return is_valid_swap

        except Exception as e:
            logging.error(f"Error during _can_swap_assignments simulation for {worker_id}: {e}", exc_info=True)
            return False # Fail safe


    def _check_constraints_on_simulated(self, worker_id, date, post, simulated_schedule, simulated_assignments):
        """Checks constraints for a worker on a specific date using simulated data."""
        try:
            # Get worker data for percentage check if needed later
            worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
            work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100

            # 1. Incompatibility (using simulated_schedule)
            if not self._check_incompatibility_simulated(worker_id, date, simulated_schedule):
                logging.debug(f"Sim Check Fail: Incompatible {worker_id} on {date}")
                return False

            # 2. Gap Constraint (using simulated_assignments)
            # This helper already includes basic gap logic
            if not self._check_gap_constraint_simulated(worker_id, date, simulated_assignments):
                logging.debug(f"Sim Check Fail: Gap constraint {worker_id} on {date}")
                return False

            # 3. Weekend Limit (using simulated_assignments)
            if self._would_exceed_weekend_limit_simulated(worker_id, date, simulated_assignments):
                 logging.debug(f"Sim Check Fail: Weekend limit {worker_id} on {date}")
                 return False

            # 4. Max Shifts (using simulated_assignments)
            # Use scheduler's max_shifts_per_worker config
            if len(simulated_assignments.get(worker_id, set())) > self.max_shifts_per_worker:
                 logging.debug(f"Sim Check Fail: Max shifts {worker_id}")
                 return False

            # 5. Basic Availability (Check if worker is unavailable fundamentally)
            if self._is_worker_unavailable(worker_id, date):
                 logging.debug(f"Sim Check Fail: Worker {worker_id} fundamentally unavailable on {date}")
                 return False

            # 6. Double Booking Check (using simulated_schedule)
            count = 0
            for assigned_post, assigned_worker in enumerate(simulated_schedule.get(date, [])):
                 if assigned_worker == worker_id:
                      if assigned_post != post: # Don't count the slot we are checking
                           count += 1
            if count > 0:
                 logging.debug(f"Sim Check Fail: Double booking {worker_id} on {date}")
                 return False

            sorted_sim_assignments = sorted(list(simulated_assignments.get(worker_id, [])))

            # 7. Friday-Monday Check (Only if gap constraint allows 3 days, i.e., gap_between_shifts == 1)
            # Apply strictly during simulation checks
            if self.scheduler.gap_between_shifts == 1: 
                 for prev_date in sorted_sim_assignments:
                      if prev_date == date: continue
                      days_between = abs((date - prev_date).days)
                      if days_between == 3:
                           # Check if one is Friday (4) and the other is Monday (0)
                           if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                               (date.weekday() == 4 and prev_date.weekday() == 0)):
                               logging.debug(f"Sim Check Fail: Friday-Monday conflict for {worker_id} between {prev_date} and {date}")
                               return False

            # 8. 7/14 Day Pattern Check (Same day of week in consecutive weeks)
            for prev_date in sorted_sim_assignments:
                if prev_date == date: 
                    continue
                days_between = abs((date - prev_date).days)
                # Check for exactly 7 or 14 days pattern AND same weekday
                if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                    logging.debug(f"Sim Check Fail: {days_between} day pattern conflict for {worker_id} between {prev_date} and {date}")
                    return False
                
            return True # All checks passed on simulated data        
        except Exception as e:
            logging.error(f"Error during _check_constraints_on_simulated for {worker_id} on {date}: {e}", exc_info=True)
            return False # Fail safe

    def _check_all_constraints_for_date_simulated(self, date, simulated_schedule, simulated_assignments):
         """ Checks all constraints for all workers assigned on a given date in the SIMULATED schedule. """
         if date not in simulated_schedule: return True # Date might not exist in sim if empty

         assignments_on_date = simulated_schedule[date]

         # Check pairwise incompatibility first for the whole date
         workers_present = [w for w in assignments_on_date if w is not None]
         for i in range(len(workers_present)):
              for j in range(i + 1, len(workers_present)):
                   worker1_id = workers_present[i]
                   worker2_id = workers_present[j]
                   if self._are_workers_incompatible(worker1_id, worker2_id):
                        logging.debug(f"Simulated state invalid: Incompatibility between {worker1_id} and {worker2_id} on {date}")
                        return False

         # Then check individual constraints for each worker
         for post, worker_id in enumerate(assignments_on_date):
              if worker_id is not None:
                   # Check this worker's assignment using the simulated state helper
                   if not self._check_constraints_on_simulated(worker_id, date, post, simulated_schedule, simulated_assignments):
                        # logging.debug(f"Simulated state invalid: Constraint fail for {worker_id} on {date} post {post}")
                        return False # Constraint failed for this worker in the simulated state
         return True
        
    def _check_incompatibility_simulated(self, worker_id, date, simulated_schedule):
        """Check incompatibility using the simulated schedule."""
        assigned_workers_list = simulated_schedule.get(date, [])
        # Use the existing helper, it only needs the list of workers on that day
        return self._check_incompatibility_with_list(worker_id, assigned_workers_list)

    def _check_gap_constraint_simulated(self, worker_id, date, simulated_assignments):
        """Check gap constraint using simulated assignments."""
        # Use scheduler's gap config
        min_days_between = self.scheduler.gap_between_shifts + 1
        # Add part-time adjustment if needed
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
        if work_percentage < 70: # Example threshold for part-time adjustment
            min_days_between = max(min_days_between, self.scheduler.gap_between_shifts + 2)

        assignments = sorted(list(simulated_assignments.get(worker_id, [])))

        for prev_date in assignments:
            if prev_date == date: continue # Don't compare date to itself
            days_between = abs((date - prev_date).days)
            if days_between < min_days_between:
                return False
            # Add Friday-Monday / 7-14 day checks if needed here too, using relaxation_level=0 logic
            if self.scheduler.gap_between_shifts == 1 and work_percentage >= 20: # Corrected: work_percentage from worker_data
                if days_between == 3:
                    if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                        (date.weekday() == 4 and prev_date.weekday() == 0)):
                        return False
            # Add check for weekly pattern (7/14 day)
            if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                return False
        return True

    def _would_exceed_weekend_limit_simulated(self, worker_id, date, simulated_assignments):
        """Check weekend limit using simulated assignments."""
        # Check if date is a weekend/holiday
        is_target_weekend = (date.weekday() >= 4 or 
                     date in self.scheduler.holidays or
                     (date + timedelta(days=1)) in self.scheduler.holidays)
        if not is_target_weekend:
            return False
    
        # Get worker data to check work_percentage
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
    
        # Calculate max_weekend_count based on work_percentage
        max_weekend_count = self.scheduler.max_consecutive_weekends
        if work_percentage < 70:
            max_weekend_count = max(1, int(self.scheduler.max_consecutive_weekends * work_percentage / 100))
    
        # Get weekend assignments and add the current date
        weekend_dates = []
        for d_val in simulated_assignments.get(worker_id, set()):
            if (d_val.weekday() >= 4 or 
                d_val in self.scheduler.holidays or
                (d_val + timedelta(days=1)) in self.scheduler.holidays):
                weekend_dates.append(d_val)
    
        # Add the date if it's not already in the list
        if date not in weekend_dates:
            weekend_dates.append(date)
    
        # Sort dates to ensure chronological order
        weekend_dates.sort()
    
        # Check for consecutive weekends
        consecutive_groups = []
        current_group = []
    
        for i, d_val in enumerate(weekend_dates): # Renamed d to d_val
            # Start a new group or add to the current one
            if not current_group:
                current_group = [d_val]
            else:
                # Get the previous weekend's date
                prev_weekend = current_group[-1]
                # Calculate days between this weekend and the previous one
                days_diff = (d_val - prev_weekend).days
            
                # Checking if they are adjacent weekend dates (7-10 days apart)
                # A weekend is consecutive to the previous if it's the next calendar weekend
                # This is typically 7 days apart, but could be 6-8 days depending on which weekend days
                if 5 <= days_diff <= 10:
                    current_group.append(d_val)
                else:
                    # Not consecutive, save the current group and start a new one
                    if len(current_group) > 1:  # Only save groups with more than 1 weekend
                        consecutive_groups.append(current_group)
                    current_group = [d_val]
    
        # Add the last group if it has more than 1 weekend
        if len(current_group) > 1:
            consecutive_groups.append(current_group)
    
        # Find the longest consecutive sequence
        max_consecutive = 0
        if consecutive_groups:
            max_consecutive = max(len(group) for group in consecutive_groups)
        else:
            max_consecutive = 1  # No consecutive weekends found, or only single weekends
    
        # Check if maximum consecutive weekend count is exceeded
        if max_consecutive > max_weekend_count:
            logging.debug(f"Weekend limit exceeded: Worker {worker_id} would have {max_consecutive} consecutive weekend shifts (max allowed: {max_weekend_count})")
            return True
    
        return False
    
    def _calculate_worker_score(self, worker, date, post, relaxation_level=0):
        """
        Calculate score for a worker assignment with optional relaxation of constraints
    
        Args:
            worker: The worker to evaluate
            date: The date to assign
            post: The post number to assign
            relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
    
        Returns:
            float: Score for this worker-date-post combination, higher is better
                  Returns float('-inf') if assignment is invalid
        """
        try:
            worker_id = worker['id']
            score = 0
        
            # --- Hard Constraints (never relaxed) ---\
        
            # Basic availability check
            if self._is_worker_unavailable(worker_id, date) or worker_id in self.schedule.get(date, []):
                return float('-inf')
            
            # Check incompatibility against workers already assigned on this date (excluding the current post being considered)
            already_assigned_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) if w is not None and idx != post]
            if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
                 logging.debug(f"Score check fail: Worker {worker_id} incompatible on {date}")
                 return float('-inf')
            
            # --- Check for mandatory shifts ---\
            worker_data = worker
            mandatory_days_str = worker_data.get('mandatory_days', '') # Corrected: mandatory_days_str
            mandatory_dates = self._parse_dates(mandatory_days_str) # Corrected: mandatory_days_str
        
            # If this is a mandatory date for this worker, give it maximum priority
            if date in mandatory_dates:
                return float('inf')  # Highest possible score to ensure mandatory shifts are assigned
        
            # --- Target Shifts Check (excluding mandatory shifts) ---\
            current_shifts = len(self.worker_assignments[worker_id])
            target_shifts = worker.get('target_shifts', 0)
        
            # Count mandatory shifts that are already assigned
            mandatory_shifts_assigned = sum(\
                1 for d in self.worker_assignments[worker_id] if d in mandatory_dates\
            )
        
            # Count mandatory shifts still to be assigned
            mandatory_shifts_remaining = sum(\
                1 for d in mandatory_dates \
                if d >= date and d not in self.worker_assignments[worker_id]\
            )
        
            # Calculate non-mandatory shifts target
            non_mandatory_target = target_shifts - len(mandatory_dates)
            non_mandatory_assigned = current_shifts - mandatory_shifts_assigned
        
            # Check if we've already met or exceeded non-mandatory target
            shift_difference = non_mandatory_target - non_mandatory_assigned
        
            # Reserve capacity for remaining mandatory shifts
            if non_mandatory_assigned + mandatory_shifts_remaining >= target_shifts and relaxation_level < 2:
                return float('-inf')  # Need to reserve remaining slots for mandatory shifts
        
            # Stop if worker already met or exceeded non-mandatory target (except at higher relaxation)
            if shift_difference <= 0:
                if relaxation_level == 0:
                    score -= 8000 * abs(shift_difference)  # Heavy penalty, not impossible
                elif relaxation_level == 1:
                    score -= 5000 * abs(shift_difference)  # Moderate penalty
                else:
                    score -= 2000 * abs(shift_difference)  # Light penalty at high relaxation
            else:
                # Prioritize workers who are furthest below target
                score += shift_difference * 2000  # Increased from 1000

            # --- MONTHLY TARGET CHECK ---
            month_key = f"{date.year}-{date.month:02d}"
            # Ensure monthly_targets is a dictionary; worker_config might not have it if not pre-calculated
            worker_config = next((w for w in self.workers_data if w['id'] == worker_id), None)
            monthly_targets_config = worker_config.get('monthly_targets', {}) if worker_config else {}
            
            target_this_month = monthly_targets_config.get(month_key, 0)

            # Calculate current shifts assigned in this month
            shifts_this_month = 0
            # Ensure worker_id is in worker_assignments before iterating
            if worker_id in self.scheduler.worker_assignments:
                 for assigned_date in self.scheduler.worker_assignments[worker_id]:
                      if assigned_date.year == date.year and assigned_date.month == date.month:
                           shifts_this_month += 1
            
            # Define a more flexible monthly max.
            # Allow at least target + buffer, or a slightly higher cap if target is very low.
            # BUFFER_FOR_MONTHLY_MAX should be defined in __init__, e.g., self.BUFFER_FOR_MONTHLY_MAX = 1
            # Defaulting here if not present, but it's better to set it in __init__
            buffer_monthly_max = getattr(self, 'BUFFER_FOR_MONTHLY_MAX', 1) 
            
            if target_this_month > 0:
                effective_max_monthly = target_this_month + buffer_monthly_max + relaxation_level
            else: # If monthly target is 0
                # Allow at least a small number, especially if overall target is non-zero
                # or if the schedule is very empty and we need to fill slots.
                # This allows workers with a 0 monthly target (due to proration perhaps) to still pick up a shift.
                overall_target_shifts = worker_config.get('target_shifts', 0)
                if overall_target_shifts > 0: # If they have an overall target, allow at least buffer + relax
                    effective_max_monthly = buffer_monthly_max + relaxation_level 
                else: # Overall target is also 0, be very restrictive
                    effective_max_monthly = relaxation_level # Only allow if relaxed

                # Potentially, always allow at least 1 if relax_level = 0 and overall_target > 0
                if overall_target_shifts > 0 and effective_max_monthly == 0 and relaxation_level == 0:
                    effective_max_monthly = 1


            logging.debug(f"  [Monthly Check W:{worker_id} M:{month_key}] ShiftsThisMonth:{shifts_this_month}, TargetThisMonth:{target_this_month}, EffectiveMaxMonthly:{effective_max_monthly}, RelaxLvl:{relaxation_level}")
            # If adding this shift would make the worker exceed their effective_max_monthly for this month
            if shifts_this_month + 1 > effective_max_monthly:
                # At lower relaxation levels, this is a hard stop.
                if relaxation_level < 1: # Stricter for relaxation 0
                    logging.debug(f"    Worker {worker_id} rejected for {date.strftime('%Y-%m-%d')}: Would exceed effective monthly max ({shifts_this_month + 1} > {effective_max_monthly}) at relax level {relaxation_level}")
                    return float('-inf')
                # At higher relaxation, it's a penalty but not a hard stop, unless it's way over.
                #elif relaxation_level < 2 and (shifts_this_month + 1 > effective_max_monthly + 1) : # Allow one more if relax_level = 1
                    #logging.debug(f"    Worker {worker_id} rejected for {date.strftime('%Y-%m-%d')}: Would significantly exceed effective monthly max ({shifts_this_month + 1} > {effective_max_monthly + 1}) at relax level {relaxation_level}")
                    #return float('-inf')
                #else: # relaxation_level == 2 or higher, or just slightly over at relax_level == 1
                    #score -= (shifts_this_month + 1 - effective_max_monthly) * 2000 # Heavy penalty
                    #logging.debug(f"    Worker {worker_id} penalized for {date.strftime('%Y-%m-%d')}: Exceeds effective monthly max ({shifts_this_month + 1} > {effective_max_monthly}). Score impact: {(shifts_this_month + 1 - effective_max_monthly) * -2000}")


            # Scoring based on being below target (positive score impact)
            if shifts_this_month < target_this_month:
                 score_bonus_monthly = (target_this_month - shifts_this_month) * 2000 # Strong incentive to meet monthly target
                 score += score_bonus_monthly
                 logging.debug(f"    Worker {worker_id} gets monthly bonus for {date.strftime('%Y-%m-%d')}: below target ({shifts_this_month} < {target_this_month}). Bonus: {score_bonus_monthly}")
            elif shifts_this_month == target_this_month and target_this_month > 0 : # Exactly at target
                 score += 500 # Small bonus for being at target
                 logging.debug(f"    Worker {worker_id} gets small monthly bonus for {date.strftime('%Y-%m-%d')}: at target ({shifts_this_month} == {target_this_month}). Bonus: 500")


            # --- Overall Target Shifts Check (Non-Mandatory Part) ---
            # This part seems okay but ensure 'target_shifts' in worker_config is the overall non-mandatory target
            current_total_shifts = len(self.worker_assignments.get(worker_id, set())) # Get current total assignments
            overall_target_shifts = worker_config.get('target_shifts', 0) # This should be the overall target for the period

            # If adding this shift makes the worker exceed their overall target
            if current_total_shifts + 1 > overall_target_shifts:
                if overall_target_shifts > 0 : # Only apply if target is not zero
                    if relaxation_level < 1: # Strict for relaxation 0
                        logging.debug(f"    Worker {worker_id} rejected for {date.strftime('%Y-%m-%d')}: Would exceed overall target ({current_total_shifts + 1} > {overall_target_shifts}) at relax level {relaxation_level}")
                        return float('-inf')
                    else: # Penalty for relaxation 1+
                        penalty_overall = (current_total_shifts + 1 - overall_target_shifts) * 1500
                        score -= penalty_overall
                        logging.debug(f"    Worker {worker_id} penalized for {date.strftime('%Y-%m-%d')}: Exceeds overall target ({current_total_shifts + 1} > {overall_target_shifts}). Score impact: {-penalty_overall}")
            else: # If below or at overall target
                score_bonus_overall = (overall_target_shifts - (current_total_shifts + 1)) * 500 # Bonus for being under overall target
                score += score_bonus_overall
                logging.debug(f"    Worker {worker_id} gets overall target bonus for {date.strftime('%Y-%m-%d')}: ({current_total_shifts + 1} vs {overall_target_shifts}). Bonus: {score_bonus_overall}")


            # --- Gap Constraints ---\
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if assignments:
                work_percentage = worker.get('work_percentage', 100)
                # Use configurable gap parameter (minimum gap is higher for part-time workers)
                min_gap = self.gap_between_shifts + 2 if work_percentage < 70 else self.gap_between_shifts + 1
    
                # Check if any previous assignment violates minimum gap
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
        
                    # Basic minimum gap check
                    if days_between < min_gap:
                        return float('-inf')
        
                    # Special rule No Friday + Monday (3-day gap)
                    if relaxation_level == 0 and self.gap_between_shifts == 1:
                        if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                            (date.weekday() == 4 and prev_date.weekday() == 0)):
                            if days_between == 3:
                                return float('-inf')
                
                    # Prevent same day of week in consecutive weeks (SHOULD NOT BE RELAXED)
                    # OLD: if relaxation_level < 2 and (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                    if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday(): # MADE NON-RELAXABLE
                        logging.debug(f"Score check fail (Hard Constraint): Worker {worker['id']} on {date.strftime('%Y-%m-%d')} fails 7/14 day pattern with {prev_date.strftime('%Y-%m-%d')}")
                        return float('-inf')
        
                    # --- Weekend Limits ---\
                    if self._would_exceed_weekend_limit(worker_id, date):
                        logging.debug(f"Score check fail: Worker {worker_id} would exceed weekend limit on {date.strftime('%Y-%m-%d')}")
                        return float('-inf')
        
                    # --- Weekday Balance Check (Strict +/- 1, meaning max spread is 1) ---
                    # This check uses self.scheduler.worker_weekdays which should be the source of truth
                    # as updated by self.scheduler._update_tracking_data
           
                    worker_id_str = str(worker['id']) # Ensure string ID
            
                    # self.worker_weekdays in ScheduleBuilder refers directly to scheduler.worker_weekdays
                    if worker_id_str not in self.worker_weekdays: # Check if key exists in the referenced dict
                        logging.warning(f"Worker {worker_id_str} not found in self.worker_weekdays during score calculation. Initializing.")
                        # Initialize it directly on the shared object if necessary, though robust init in Scheduler is better
                        self.worker_weekdays[worker_id_str] = {day_idx: 0 for day_idx in range(7)}

                    current_weekday_counts_from_scheduler = self.worker_weekdays[worker_id_str]
            
                    # Simulate adding the current assignment using a copy
                    hypothetical_weekday_counts = current_weekday_counts_from_scheduler.copy()
                    target_weekday = date.weekday()
                    hypothetical_weekday_counts[target_weekday] = hypothetical_weekday_counts.get(target_weekday, 0) + 1

                    min_hypothetical_count = min(hypothetical_weekday_counts.values())
                    max_hypothetical_count = max(hypothetical_weekday_counts.values())
            
                    spread = max_hypothetical_count - min_hypothetical_count
        
                    if spread > 1: # If spread is 2 or more, it violates the +/-1 balance.
                        if relaxation_level < 1: # Strict for relaxation_level 0
                            logging.debug(f"Score check fail (Hard Weekday Imbalance): Worker {worker_id_str} for date {date.strftime('%Y-%m-%d')}. "
                                          f"Hypothetical counts: {hypothetical_weekday_counts}, Spread: {spread} (>1). Relaxation: {relaxation_level}. Returning -inf.")
                            return float('-inf')
                        else:
                            penalty_weekday_balance = (spread - 1) * 250 
                            score -= penalty_weekday_balance
                            logging.debug(f"Score penalty (Relaxed Weekday Imbalance): Worker {worker_id_str} for date {date.strftime('%Y-%m-%d')}. "
                                          f"Hypothetical counts: {hypothetical_weekday_counts}, Spread: {spread}. Penalty: -{penalty_weekday_balance}. Relaxation: {relaxation_level}.")
                            
            # --- Scoring Components (softer constraints) ---\

            # 1. Overall Target Score (Reduced weight compared to monthly)
            if shift_difference > 0:
                 score += shift_difference * 500 # Reduced weight
            elif shift_difference <=0 and relaxation_level >= 2:
                 score -= 5000 # Keep penalty if over overall target at high relaxation
        
            # 2. Weekend Balance Score
            is_special_day_for_scoring = (date.weekday() >= 4 or 
                                          date in self.holidays or
                                          (date + timedelta(days=1)) in self.holidays)
            if is_special_day_for_scoring:
                special_day_assignments = sum(
                    1 for d in self.worker_assignments[worker_id]
                    if (d.weekday() >= 4 or 
                        d in self.holidays or
                        (d + timedelta(days=1)) in self.holidays)
                )
                score -= special_day_assignments * 300 

        
            # 4. Weekly Balance Score - avoid concentration in some weeks
            week_number = date.isocalendar()[1]
            week_counts = {}
            for d_val in self.worker_assignments[worker_id]: # Renamed d to d_val
                w = d_val.isocalendar()[1]
                week_counts[w] = week_counts.get(w, 0) + 1
        
            current_week_count = week_counts.get(week_number, 0)
            avg_week_count = len(assignments) / max(1, len(week_counts)) if week_counts else 0 # Added check for empty week_counts
        
            if current_week_count < avg_week_count:
                score += 500  # Bonus for weeks with fewer assignments
        
            # 5. Schedule Progression Score - adjust priority as schedule fills up
            schedule_completion = sum(len(s) for s in self.schedule.values()) / (\
                (self.end_date - self.start_date).days * self.num_shifts) if (self.end_date - self.start_date).days > 0 else 0 # Added check for zero days
        
            # Higher weight for target difference as schedule progresses
            score += shift_difference * 500 * schedule_completion
        
            # Log the score calculation
            logging.debug(f"Score for worker {worker_id}: {score} "\
                        f"(current: {current_shifts}, target: {target_shifts}, "\
                        f"relaxation: {relaxation_level})")
        
            return score
    
        except Exception as e:
            logging.error(f"Error calculating score for worker {worker['id']}: {str(e)}")
            return float('-inf')

    def _calculate_improvement_score(self, worker, date, post):
        """
        Calculate a score for a worker assignment during the improvement phase.
    
        This uses a more lenient scoring approach to encourage filling empty shifts.
        """
        worker_id = worker['id']
    
        # Base score from standard calculation
        base_score = self._calculate_worker_score(worker, date, post)
    
        # If base score is negative infinity, the assignment is invalid
        if base_score == float('-inf'):
            return float('-inf')
    
        # Bonus for balancing post rotation
        post_counts = self._get_post_counts(worker_id)
        total_assignments = sum(post_counts.values())
    
        # Skip post balance check for workers with few assignments
        if total_assignments >= self.num_shifts and self.num_shifts > 0: # Added check for num_shifts > 0
            expected_per_post = total_assignments / self.num_shifts
            current_count = post_counts.get(post, 0)
        
            # Give bonus if this post is underrepresented for this worker
            if current_count < expected_per_post:
                base_score += 10 * (expected_per_post - current_count)
    
        # Bonus for balancing workload
        work_percentage = worker.get('work_percentage', 100)
        current_assignments = len(self.worker_assignments[worker_id])
    
        # Calculate average assignments per worker, adjusted for work percentage
        total_assignments_all = sum(len(self.worker_assignments[w_data['id']]) for w_data in self.workers_data) # Corrected: w_data
        total_work_percentage = sum(w_data.get('work_percentage', 100) for w_data in self.workers_data) # Corrected: w_data
    
        # Expected assignments based on work percentage
        expected_assignments = (total_assignments_all / (total_work_percentage / 100)) * (work_percentage / 100) if total_work_percentage > 0 else 0 # Added check for total_work_percentage
    
        # Bonus for underloaded workers
        if current_assignments < expected_assignments:
            base_score += 5 * (expected_assignments - current_assignments)
    
        return base_score

# 5. Schedule Generation Methods
            
    def _assign_mandatory_guards(self):
        logging.info("Starting mandatory guard assignment…")
        assigned_count = 0
        for worker in self.workers_data: # Use self.workers_data
            worker_id = worker['id']
            mandatory_str = worker.get('mandatory_days', '')
            try:
                dates = self.date_utils.parse_dates(mandatory_str)
            except Exception as e:
                logging.error(f"Error parsing mandatory_days for worker {worker_id}: {e}")
                continue

            for date in dates:
                if not (self.start_date <= date <= self.end_date): continue

                if date not in self.schedule: # self.schedule is scheduler.schedule
                    self.schedule[date] = [None] * self.num_shifts
                
                # Try to place in any available post for that date
                placed_mandatory = False
                for post in range(self.num_shifts):
                    if len(self.schedule[date]) <= post: self.schedule[date].extend([None] * (post + 1 - len(self.schedule[date])))

                    if self.schedule[date][post] is None:
                        # Check incompatibility before placing
                        others_on_date = [w for i, w in enumerate(self.schedule.get(date, [])) if i != post and w is not None]
                        if not self._check_incompatibility_with_list(worker_id, others_on_date):
                            logging.debug(f"Mandatory shift for {worker_id} on {date.strftime('%Y-%m-%d')} post {post} incompatible. Trying next post.")
                            continue
                        
                        self.schedule[date][post] = worker_id
                        self.worker_assignments.setdefault(worker_id, set()).add(date) # Use self.worker_assignments
                        self.scheduler._update_tracking_data(worker_id, date, post, removing=False) # Call scheduler's central update
                        self._locked_mandatory.add((worker_id, date)) # Lock it
                        logging.debug(f"Assigned worker {worker_id} to {date.strftime('%Y-%m-%d')} post {post} (mandatory) and locked.")
                        assigned_count += 1
                        placed_mandatory = True
                        break 
                if not placed_mandatory:
                     logging.warning(f"Could not place mandatory shift for {worker_id} on {date.strftime('%Y-%m-%d')}. All posts filled or incompatible.")
        
        logging.info(f"Finished mandatory guard assignment. Assigned {assigned_count} shifts.")
        # No _save_current_as_best here; scheduler's generate_schedule will handle it after this.
        # self._synchronize_tracking_data() # Ensure builder's view is also synced if it has separate copies (it shouldn't for core data)
        return assigned_count > 0
    
    def _get_remaining_dates_to_process(self, forward):
        """Get remaining dates that need to be processed"""
        dates_to_process = []
        current = self.start_date
    
        # Get all dates in period that are not weekends or holidays
        # or that already have some assignments but need more
        while current <= self.end_date:
            # for each date, check if we need to generate more shifts
            if current not in self.schedule:
                dates_to_process.append(current)
            else:
                # compare actual slots vs configured for that date
                expected = self.scheduler._get_shifts_for_date(current)
                if len(self.schedule[current]) < expected:
                    dates_to_process.append(current)
            current += timedelta(days=1)
    
        # Sort based on direction
        if forward:
            dates_to_process.sort()
        else:
            dates_to_process.sort(reverse=True)
    
        return dates_to_process
    
    def _assign_day_shifts_with_relaxation(self, date, attempt_number=50, relaxation_level=0):
        """Assign shifts for a given date with optional constraint relaxation"""
        logging.debug(f"Assigning shifts for {date.strftime('%d-%m-%Y')} (attempt: {attempt_number}, initial relax: {relaxation_level})")

        # Ensure the date entry exists and is a list
        if date not in self.schedule:
            self.schedule[date] = []
        # Ensure it's padded to current length if it exists but is shorter than previous post assignments
        # (This shouldn't happen often but safeguards against potential inconsistencies)
        current_len = len(self.schedule.get(date, []))
        max_post_assigned_prev = -1
        if current_len > 0:
             max_post_assigned_prev = current_len -1


        # Determine how many slots this date actually has (supports variable shifts)
        start_post = len(self.schedule.get(date, []))
        total_slots = self.scheduler._get_shifts_for_date(date) # Corrected: Use scheduler method
        for post in range(start_post, total_slots):
            # ← NEW: never overwrite a locked mandatory shift
            # Check if self.schedule[date] is long enough before accessing by index
            if len(self.schedule.get(date,[])) > post and (self.schedule[date][post] is not None and (self.schedule[date][post], date) in self._locked_mandatory) :
                continue
            assigned_this_post = False
            for relax_level in range(relaxation_level + 1): 
                candidates = self._get_candidates(date, post, relax_level)

                logging.debug(f"Found {len(candidates)} candidates for {date.strftime('%d-%m-%Y')}, post {post}, relax level {relax_level}")

                if candidates:
                    # Log top candidates if needed
                    # for i, (worker, score) in enumerate(candidates[:3]):
                    #     logging.debug(f"  Candidate {i+1}: Worker {worker['id']} with score {score:.2f}")

                    # Sort candidates by score (descending)
                    candidates.sort(key=lambda x: x[1], reverse=True)

                    # --- Try assigning the first compatible candidate ---\
                    for candidate_worker, candidate_score in candidates:
                        worker_id = candidate_worker['id']

                        # *** DEBUG LOGGING - START ***\
                        current_assignments_on_date = [w for w in self.schedule.get(date, []) if w is not None]
                        logging.debug(f"CHECKING: Date={date}, Post={post}, Candidate={worker_id}, CurrentlyAssigned={current_assignments_on_date}")
                        # *** DEBUG LOGGING - END ***\

                        # *** EXPLICIT INCOMPATIBILITY CHECK ***\
                        # Temporarily add logging INSIDE the check function call might also help, or log its result explicitly
                        is_compatible = self._check_incompatibility_with_list(worker_id, current_assignments_on_date)
                        logging.debug(f"  -> Incompatibility Check Result: {is_compatible}") # Log the result

                        # if not self._check_incompatibility_with_list(worker_id, current_assignments_on_date):
                        if not is_compatible: # Use the variable to make logging easier
                            logging.debug(f"  Skipping candidate {worker_id} for post {post} on {date}: Incompatible with current assignments on this date.")
                            continue # Try next candidate

                        # *** If compatible, assign this worker ***\
                        # Ensure list is long enough before assigning by index
                        while len(self.schedule[date]) <= post:
                             self.schedule[date].append(None)

                        # Double check slot is still None before assigning (paranoid check)
                        if self.schedule[date][post] is None:
                            self.schedule[date][post] = worker_id # Assign to the correct post index
                            self.worker_assignments.setdefault(worker_id, set()).add(date)
                            self.scheduler._update_tracking_data(worker_id, date, post)

                            logging.info(f"Assigned worker {worker_id} to {date.strftime('%d-%m-%Y')}, post {post} (Score: {candidate_score:.2f}, Relax: {relax_level})")
                            assigned_this_post = True
                            break # Found a compatible worker for this post, break candidate loop
                        else:
                            # This case should be rare if logic is correct, but log it
                            logging.warning(f"  Slot {post} on {date} was unexpectedly filled before assigning candidate {worker_id}. Current value: {self.schedule[date][post]}")
                            # Continue to the next candidate, as this one cannot be placed here anymore


                    if assigned_this_post:
                        break # Success at this relaxation level, break relaxation loop
                    else:
                        # If loop finishes without assigning (no compatible candidates found at this relax level)
                        logging.debug(f"No compatible candidate found for post {post} at relax level {relax_level}")
                else:
                     logging.debug(f"No candidates found for post {post} at relax level {relax_level}")


            # --- Handle case where post remains unfilled after trying all relaxation levels ---\
            if not assigned_this_post:
                 # Ensure list is long enough before potentially assigning None
                 while len(self.schedule[date]) <= post:
                      self.schedule[date].append(None)

                 # Only log warning if the slot is genuinely still None
                 if self.schedule[date][post] is None:
                      logging.warning(f"No suitable worker found for {date.strftime('%d-%m-%Y')}, post {post} - shift unfilled after all checks.")
                 # Else: it might have been filled by a mandatory assignment earlier, which is fine.

        # --- Ensure schedule[date] list has the correct final length ---\
        # Pad with None if necessary, e.g., if initial assignment skipped posts
        while len(self.schedule.get(date, [])) < self.num_shifts:
             self.schedule.setdefault(date, []).append(None) # Use setdefault for safety if date somehow disappeared

    def _get_candidates(self, date, post, relaxation_level=0):
        """
        Get suitable candidates with their scores using the specified relaxation level
    
        Args:
            date: The date to assign
            post: The post number to assign
            relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
        """
        candidates = []
        logging.debug(f"Looking for candidates for {date.strftime('%d-%m-%Y')}, post {post}")

        # Get workers already assigned to other posts on this date
        already_assigned_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) if w is not None and idx != post]

        for worker in self.workers_data:
            worker_id = worker['id']
            logging.debug(f"Checking worker {worker_id} for {date.strftime('%d-%m-%Y')}")

            # --- PRE-FILTERING ---\
            # Skip if already assigned to this date (redundant with score check, but safe)
            if worker_id in self.schedule.get(date, []): # Check against all posts on this date
                 logging.debug(f"  Worker {worker_id} skipped - already assigned to {date.strftime('%d-%m-%Y')}")
                 continue

            # Skip if unavailable
            if self._is_worker_unavailable(worker_id, date):
                 logging.debug(f"  Worker {worker_id} skipped - unavailable on {date.strftime('%d-%m-%Y')}")
                 continue

            # *** ADDED: Explicit Incompatibility Check BEFORE scoring ***\
            # Never relax incompatibility constraint
            if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
                 logging.debug(f"  Worker {worker_id} skipped - incompatible with already assigned workers on {date.strftime('%d-%m-%Y')}")
                 continue
            # Skip if max shifts reached
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                logging.debug(f"Worker {worker_id} skipped - max shifts reached: {len(self.worker_assignments[worker_id])}/{self.max_shifts_per_worker}")
                continue

            # Calculate score using the main scoring function
            score = self._calculate_worker_score(worker, date, post, relaxation_level)
            
            if score > float('-inf'): # Only add valid candidates
                logging.debug(f"Worker {worker_id} added as candidate with score {score}")
                candidates.append((worker, score))

        return candidates

    # 6. Schedule Improvement Methods

    def _try_fill_empty_shifts(self):
        """
        Try to fill empty shifts in the authoritative self.schedule.
        Pass 1: Direct assignment, attempting with increasing relaxation levels.
        Pass 2: Attempt swaps for remaining empty shifts.
        """
        logging.debug(f"ENTERED _try_fill_empty_shifts. self.schedule ID: {id(self.schedule)}. Keys count: {len(self.schedule.keys())}. Sample: {dict(list(self.schedule.items())[:2])}")

        initial_empty_slots = []
        for date_val, workers_in_posts in self.schedule.items():
            for post_index, worker_in_post in enumerate(workers_in_posts):
                if worker_in_post is None:
                    initial_empty_slots.append((date_val, post_index))
        
        logging.debug(f"[_try_fill_empty_shifts] Initial identified empty_slots count: {len(initial_empty_slots)}")
        if not initial_empty_slots:
            logging.info(f"--- No initial empty shifts to fill. ---")
            return False

        logging.info(f"Attempting to fill {len(initial_empty_slots)} empty shifts...")
        initial_empty_slots.sort(key=lambda x: (x[0], x[1])) # Process chronologically, then by post

        shifts_filled_this_pass_total = 0
        made_change_overall = False
        remaining_empty_shifts_after_pass1 = []

        logging.info("--- Starting Pass 1: Direct Fill with Relaxation Iteration ---")
        for date_val, post_val in initial_empty_slots:
            if self.schedule[date_val][post_val] is not None:
                logging.debug(f"[Pass 1] Slot ({date_val.strftime('%Y-%m-%d')}, {post_val}) already filled by {self.schedule[date_val][post_val]}. Skipping.")
                continue
            
            assigned_this_post_pass1 = False
            
            # Iterate through relaxation levels for direct fill
            # Max relaxation level can be a config, e.g., self.scheduler.config.get('max_direct_fill_relaxation', 3)
            # For now, let's assume up to 2 (0, 1, 2)
            for relax_lvl_attempt in range(3): 
                pass1_candidates = []
                logging.debug(f"  [Pass 1 Attempt] Date: {date_val.strftime('%Y-%m-%d')}, Post: {post_val}, Relaxation Level: {relax_lvl_attempt}")

                for worker_data_val in self.workers_data:
                    worker_id_val = worker_data_val['id']
                    logging.debug(f"    [Pass 1 Candidate Check] Worker: {worker_id_val} for Date: {date_val.strftime('%Y-%m-%d')}, Post: {post_val}, Relax: {relax_lvl_attempt}")
                    
                    score = self._calculate_worker_score(worker_data_val, date_val, post_val, relaxation_level=relax_lvl_attempt)

                    if score > float('-inf'):
                        logging.debug(f"      -> Pass1 ACCEPTED as candidate: Worker {worker_id_val} with score {score} at relax {relax_lvl_attempt}")
                        pass1_candidates.append((worker_data_val, score))
                    else:
                        logging.debug(f"      -> Pass1 REJECTED (Score Check): Worker {worker_id_val} at relax {relax_lvl_attempt}")
            
                if pass1_candidates:
                    pass1_candidates.sort(key=lambda x: x[1], reverse=True)
                    logging.debug(f"    [Pass 1] Candidates for {date_val.strftime('%Y-%m-%d')} Post {post_val} (Relax {relax_lvl_attempt}): {[(c[0]['id'], c[1]) for c in pass1_candidates]}")
                    
                    # Try the top candidate that is valid at this relaxation level
                    candidate_worker_data, candidate_score = pass1_candidates[0]
                    worker_id_to_assign = candidate_worker_data['id']
                    
                    if self.schedule[date_val][post_val] is None: 
                        others_now = [w for i, w in enumerate(self.schedule.get(date_val, [])) if i != post_val and w is not None]
                        if not self._check_incompatibility_with_list(worker_id_to_assign, others_now):
                            logging.debug(f"      -> Pass1 Assignment REJECTED (Last Minute Incompat): W:{worker_id_to_assign} for {date_val.strftime('%Y-%m-%d')} P:{post_val} at Relax {relax_lvl_attempt}")
                        else:
                            self.schedule[date_val][post_val] = worker_id_to_assign
                            self.worker_assignments.setdefault(worker_id_to_assign, set()).add(date_val)
                            self.scheduler._update_tracking_data(worker_id_to_assign, date_val, post_val, removing=False)
                            logging.info(f"[Pass 1 Direct Fill] Filled empty shift on {date_val.strftime('%Y-%m-%d')} Post {post_val} with W:{worker_id_to_assign} (Score: {candidate_score:.2f}, Relax: {relax_lvl_attempt})")
                            shifts_filled_this_pass_total += 1
                            made_change_overall = True
                            assigned_this_post_pass1 = True
                            break # Break from the relaxation_level attempts for this slot, as it's filled
                    else: 
                        # Slot was filled by a previous iteration (should not happen if logic is sequential for a slot)
                        # or by another process if this method is called concurrently (not expected here)
                        logging.warning(f"    [Pass 1] Slot ({date_val.strftime('%Y-%m-%d')}, {post_val}) was unexpectedly filled before assignment at Relax {relax_lvl_attempt}. Current: {self.schedule[date_val][post_val]}")
                        assigned_this_post_pass1 = True # Consider it "handled" to break relaxation attempts
                        break 
            
            if not assigned_this_post_pass1 and self.schedule[date_val][post_val] is None:
                remaining_empty_shifts_after_pass1.append((date_val, post_val))
                logging.debug(f"Could not find compatible direct candidate in Pass 1 for {date_val.strftime('%Y-%m-%d')} Post {post_val} after all relaxation attempts.")

        if not remaining_empty_shifts_after_pass1:
            logging.info(f"--- Finished Pass 1. No remaining empty shifts for Pass 2. ---")
        else:
            logging.info(f"--- Finished Pass 1. Starting Pass 2: Attempting swaps for {len(remaining_empty_shifts_after_pass1)} empty shifts ---")
            for date_empty, post_empty in remaining_empty_shifts_after_pass1:
                if self.schedule[date_empty][post_empty] is not None:
                    logging.warning(f"[Pass 2 Swap] Slot ({date_empty.strftime('%Y-%m-%d')}, {post_empty}) no longer empty. Skipping.")
                    continue
                swap_found = False
                potential_W_data = list(self.workers_data); random.shuffle(potential_W_data)
                for worker_W_data in potential_W_data:
                    worker_W_id = worker_W_data['id']
                    if not self.worker_assignments.get(worker_W_id): continue
                    
                    original_W_assignments = list(self.worker_assignments[worker_W_id]); random.shuffle(original_W_assignments)
                    for date_conflict in original_W_assignments:
                        if (worker_W_id, date_conflict) in self._locked_mandatory: continue
                        try: 
                            post_conflict = self.schedule[date_conflict].index(worker_W_id)
                        except (ValueError, KeyError, IndexError): 
                            logging.warning(f"Could not find worker {worker_W_id} in schedule for date {date_conflict} during swap search. Assignments: {self.worker_assignments.get(worker_W_id)}, Schedule on date: {self.schedule.get(date_conflict)}")
                            continue

                        # Create a temporary state for checking W's move to empty
                        temp_schedule_for_W_check = copy.deepcopy(self.schedule)
                        temp_assignments_for_W_check = copy.deepcopy(self.worker_assignments)
                        
                        # Remove W from original conflict spot in temp
                        if date_conflict in temp_schedule_for_W_check and \
                           len(temp_schedule_for_W_check[date_conflict]) > post_conflict and \
                           temp_schedule_for_W_check[date_conflict][post_conflict] == worker_W_id:
                            temp_schedule_for_W_check[date_conflict][post_conflict] = None
                            if worker_W_id in temp_assignments_for_W_check and date_conflict in temp_assignments_for_W_check[worker_W_id]:
                                temp_assignments_for_W_check[worker_W_id].remove(date_conflict)
                                if not temp_assignments_for_W_check[worker_W_id]: # Clean up if set becomes empty
                                    del temp_assignments_for_W_check[worker_W_id]
                        else:
                            logging.warning(f"Swap pre-check: Worker {worker_W_id} not found at {date_conflict}|P{post_conflict} in temp_schedule for W check. Skipping.")
                            continue
                        
                        # Check if W can be assigned to the empty slot in this temp state (using strict constraints for the move itself)
                        can_W_take_empty_simulated = self._check_constraints_on_simulated(
                            worker_W_id, date_empty, post_empty, 
                            temp_schedule_for_W_check, temp_assignments_for_W_check
                        )

                        if not can_W_take_empty_simulated:
                            logging.debug(f"  Swap Check: Worker {worker_W_id} cannot take empty slot {date_empty}|P{post_empty} due to constraints in simulated state.")
                            continue 

                        # Now, find a worker X who can take W's original spot (date_conflict, post_conflict)
                        worker_X_id = self._find_swap_candidate(worker_W_id, date_conflict, post_conflict)

                        if worker_X_id:
                            logging.info(f"[Pass 2 Swap Attempt] W:{worker_W_id} ({date_conflict.strftime('%Y-%m-%d')},P{post_conflict}) -> ({date_empty.strftime('%Y-%m-%d')},P{post_empty}); X:{worker_X_id} takes W's original spot.")
                            
                            # 1. Remove W from original spot
                            self.schedule[date_conflict][post_conflict] = None
                            if worker_W_id in self.worker_assignments and date_conflict in self.worker_assignments[worker_W_id]:
                                self.worker_assignments[worker_W_id].remove(date_conflict)
                            self.scheduler._update_tracking_data(worker_W_id, date_conflict, post_conflict, removing=True)

                            # 2. Assign X to W's original spot
                            self.schedule[date_conflict][post_conflict] = worker_X_id
                            self.worker_assignments.setdefault(worker_X_id, set()).add(date_conflict)
                            self.scheduler._update_tracking_data(worker_X_id, date_conflict, post_conflict, removing=False)
                            
                            # 3. Assign W to the empty spot
                            self.schedule[date_empty][post_empty] = worker_W_id
                            self.worker_assignments.setdefault(worker_W_id, set()).add(date_empty) # Ensure setdefault here too
                            self.scheduler._update_tracking_data(worker_W_id, date_empty, post_empty, removing=False)
                            
                            shifts_filled_this_pass_total += 1
                            made_change_overall = True
                            swap_found = True
                            break # Break from date_conflict loop for worker_W
                    if swap_found: 
                        break # Break from worker_W_data loop
                if not swap_found: 
                    logging.debug(f"No swap for empty {date_empty.strftime('%Y-%m-%d')} P{post_empty}")
        
        logging.info(f"--- Finished _try_fill_empty_shifts. Total filled/swapped: {shifts_filled_this_pass_total} ---")
        if made_change_overall:
            self._synchronize_tracking_data() # Ensure builder's and scheduler's data are aligned
            self._save_current_as_best()
        return made_change_overall

    
    def _find_swap_candidate(self, worker_W_id, conflict_date, conflict_post):
        """
        Finds a worker (X) who can take the shift at (conflict_date, conflict_post),
        ensuring they are not worker_W_id and not already assigned on that date.
        Uses strict constraints (_can_assign_worker via constraint_checker or _calculate_worker_score).
        Assumes (conflict_date, conflict_post) is currently "empty" for the purpose of this check,
        as worker_W is hypothetically moved out.
        """
        potential_X_workers = [
            w_data for w_data in self.scheduler.workers_data 
            if w_data['id'] != worker_W_id and \
               w_data['id'] not in self.scheduler.schedule.get(conflict_date, []) 
        ]
        random.shuffle(potential_X_workers)

        for worker_X_data in potential_X_workers:
            worker_X_id = worker_X_data['id']
            
            # Check if X can strictly take W's old slot (which is now considered notionally empty)
            # We use _calculate_worker_score with relaxation_level=0 for a comprehensive check
            # The schedule state for this check should reflect W being absent from conflict_date/post
            
            # Simulate W's absence for X's check
            sim_schedule_for_X = copy.deepcopy(self.scheduler.schedule)
            if conflict_date in sim_schedule_for_X and len(sim_schedule_for_X[conflict_date]) > conflict_post:
                # Only set to None if it was W, to be safe, though it should be.
                if sim_schedule_for_X[conflict_date][conflict_post] == worker_W_id:
                     sim_schedule_for_X[conflict_date][conflict_post] = None
            
            # Temporarily use the simulated schedule for this specific score calculation for X
            original_schedule_ref = self.schedule # Keep original ref
            self.schedule = sim_schedule_for_X # Temporarily point to sim
            
            score_for_X = self._calculate_worker_score(worker_X_data, conflict_date, conflict_post, relaxation_level=0)
            
            self.schedule = original_schedule_ref # Restore original ref

            if score_for_X > float('-inf'): # If X can be assigned
                 logging.debug(f"Found valid swap candidate X={worker_X_id} for W={worker_W_id}'s slot ({conflict_date.strftime('%Y-%m-%d')},{conflict_post}) with score {score_for_X}")
                 return worker_X_id

        logging.debug(f"No suitable swap candidate X found for W={worker_W_id}'s slot ({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")
        return None
    
    def _balance_weekend_shifts(self):
        """
        Balance weekend/holiday shifts across workers based on their percentage of working days.
        Each worker should have approximately:
        (total_shifts_for_worker) * (total_weekend_days / total_days) shifts on weekends/holidays, ±1.
        """
        logging.info("Balancing weekend and holiday shifts among workers...")
        fixes_made = 0
    
        # Calculate the total days and weekend/holiday days in the schedule period
        total_days_in_period = (self.end_date - self.start_date).days + 1 # Renamed total_days
        weekend_days_in_period = sum(1 for d_val in self.date_utils.generate_date_range(self.start_date, self.end_date) # Renamed d, use generate_date_range
                      if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
    
        # Calculate the target percentage
        weekend_percentage = weekend_days_in_period / total_days_in_period if total_days_in_period > 0 else 0
        logging.info(f"Schedule period has {weekend_days_in_period} weekend/holiday days out of {total_days_in_period} total days ({weekend_percentage:.1%})")
    
        # Check each worker's current weekend shift allocation
        workers_to_check = self.workers_data.copy()
        random.shuffle(workers_to_check)  # Process in random order
    
        for worker_val in workers_to_check: # Renamed worker
            worker_id_val = worker_val['id'] # Renamed worker_id
            assignments = self.worker_assignments.get(worker_id_val, set())
            total_shifts = len(assignments)
        
            if total_shifts == 0:
                continue  # Skip workers with no assignments
            
            # Count weekend assignments for this worker
            weekend_shifts = sum(1 for date_val in assignments # Renamed date
                                if self.date_utils.is_weekend_day(date_val) or date_val in self.holidays)
        
            # Calculate target weekend shifts for this worker
            target_weekend_shifts = total_shifts * weekend_percentage
            deviation = weekend_shifts - target_weekend_shifts
            allowed_deviation = 0.75  # Tighten the tolerance

            ## And add priority scoring based on how far workers are from target:
            deviation_priority = abs(deviation)
            # Process workers with largest deviations first            logging.debug(f"Worker {worker_id_val}: {weekend_shifts} weekend shifts, target {target_weekend_shifts:.2f}, deviation {deviation:.2f}")
        
            # Case 1: Worker has too many weekend shifts
            if deviation > allowed_deviation:
                logging.info(f"Worker {worker_id_val} has too many weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
                swap_found = False
            
                # Find workers with too few weekend shifts to swap with
                potential_swap_partners = []
                for other_worker_val in self.workers_data: # Renamed other_worker
                    other_id = other_worker_val['id']
                    if other_id == worker_id_val:
                        continue
                
                    other_total = len(self.worker_assignments.get(other_id, []))
                    if other_total == 0:
                        continue
                    
                    other_weekend = sum(1 for d_val in self.worker_assignments.get(other_id, []) # Renamed d
                                       if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
                                    
                    other_target = other_total * weekend_percentage
                    other_deviation = other_weekend - other_target
                
                    if other_deviation < -allowed_deviation:
                        potential_swap_partners.append((other_id, other_deviation))
            
                # Sort potential partners by how under-assigned they are
                potential_swap_partners.sort(key=lambda x: x[1])
            
                # Try to swap a weekend shift from this worker to an under-assigned worker
                if potential_swap_partners:
                    for swap_partner_id, _ in potential_swap_partners:
                        # Find a weekend assignment from this worker to swap
                        possible_from_dates = [d_val for d_val in assignments # Renamed d
                                             if (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                             and not self._is_mandatory(worker_id_val, d_val)]
                    
                        if not possible_from_dates:
                            continue  # No swappable weekend shifts
                        
                        random.shuffle(possible_from_dates)
                    
                        for from_date in possible_from_dates:
                            # Find the post this worker is assigned to
                            from_post = self.schedule[from_date].index(worker_id_val)
                        
                            # Find a weekday assignment from the swap partner that could be exchanged
                            partner_assignments = self.worker_assignments.get(swap_partner_id, set())
                            possible_to_dates = [d_val for d_val in partner_assignments # Renamed d
                                               if not (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                               and not self._is_mandatory(swap_partner_id, d_val)]
                        
                            if not possible_to_dates:
                                continue  # No swappable weekday shifts for partner
                            
                            random.shuffle(possible_to_dates)
                        
                            for to_date in possible_to_dates:
                                # Find the post the partner is assigned to
                                to_post = self.schedule[to_date].index(swap_partner_id)
                            
                                # Check if swap is valid (worker1 <-> worker2)
                                if self._can_worker_swap(worker_id_val, from_date, from_post, swap_partner_id, to_date, to_post): # Corrected: _can_worker_swap
                                    # Execute worker-worker swap
                                    self._execute_worker_swap(worker_id_val, from_date, from_post, swap_partner_id, to_date, to_post)
                                    logging.info(f"Swapped weekend shift: Worker {worker_id_val} on {from_date.strftime('%Y-%m-%d')} with "\
                                               f"Worker {swap_partner_id} on {to_date.strftime('%Y-%m-%d')}")
                                    fixes_made += 1
                                    swap_found = True
                                    break
                        
                            if swap_found:
                                break
                    
                        if swap_found:
                            break
                        
            # Case 2: Worker has too few weekend shifts
            elif deviation < allowed_deviation:
                logging.info(f"Worker {worker_id_val} has too few weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
                swap_found = False
            
                # Find workers with too many weekend shifts to swap with
                potential_swap_partners = []
                for other_worker_val in self.workers_data: # Renamed other_worker
                    other_id = other_worker_val['id']
                    if other_id == worker_id_val:
                        continue
                
                    other_total = len(self.worker_assignments.get(other_id, []))
                    if other_total == 0:
                        continue
                    
                    other_weekend = sum(1 for d_val in self.worker_assignments.get(other_id, []) # Renamed d
                                       if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
                                    
                    other_target = other_total * weekend_percentage
                    other_deviation = other_weekend - other_target
                
                    if other_deviation > allowed_deviation:
                        potential_swap_partners.append((other_id, other_deviation))
            
                # Sort potential partners by how over-assigned they are
                potential_swap_partners.sort(key=lambda x: -x[1])
            
                # Implementation similar to above but with roles reversed
                if potential_swap_partners:
                    for swap_partner_id, _ in potential_swap_partners:
                        # Find a weekend assignment from the partner to swap
                        partner_assignments = self.worker_assignments.get(swap_partner_id, set())
                        possible_from_dates = [d_val for d_val in partner_assignments # Renamed d
                                             if (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                             and not self._is_mandatory(swap_partner_id, d_val)]
                    
                        if not possible_from_dates:
                            continue
                        
                        random.shuffle(possible_from_dates)
                    
                        for from_date in possible_from_dates:
                            from_post = self.schedule[from_date].index(swap_partner_id)
                        
                            # Find a weekday assignment from this worker
                            possible_to_dates = [d_val for d_val in assignments # Renamed d
                                               if not (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                               and not self._is_mandatory(worker_id_val, d_val)]
                        
                            if not possible_to_dates:
                                continue
                            
                            random.shuffle(possible_to_dates)
                        
                            for to_date in possible_to_dates:
                                to_post = self.schedule[to_date].index(worker_id_val)
                            
                                # Check if swap is valid (partner <-> this worker)
                                if self._can_worker_swap(swap_partner_id, from_date, from_post, worker_id_val, to_date, to_post): # Corrected: _can_worker_swap
                                    self._execute_worker_swap(swap_partner_id, from_date, from_post, worker_id_val, to_date, to_post)
                                    logging.info(f"Swapped weekend shift: Worker {swap_partner_id} on {from_date.strftime('%Y-%m-%d')} with "\
                                               f"Worker {worker_id_val} on {to_date.strftime('%Y-%m-%d')}")
                                    fixes_made += 1
                                    swap_found = True
                                    break
                        
                            if swap_found:
                                break
                    
                        if swap_found:
                            break
    
        logging.info(f"Weekend shift balancing: made {fixes_made} changes")
        if fixes_made > 0:
            self._save_current_as_best()
        return fixes_made > 0

    def _execute_worker_swap(self, worker1_id, date1, post1, worker2_id, date2, post2):
        """
        Swap two workers' assignments between dates/posts.
    
        Args:
            worker1_id: First worker's ID
            date1: First worker's date
            post1: First worker's post
            worker2_id: Second worker's ID
            date2: Second worker's date
            post2: Second worker's post
        """
        # Ensure both workers are currently assigned as expected
        if (self.schedule[date1][post1] != worker1_id or
            self.schedule[date2][post2] != worker2_id):
            logging.error(f"Worker swap failed: Workers not in expected positions")
            return False
    
        # Swap the workers in the schedule
        self.schedule[date1][post1] = worker2_id
        self.schedule[date2][post2] = worker1_id
    
        # Update worker_assignments for both workers
        self.worker_assignments[worker1_id].remove(date1)
        self.worker_assignments[worker1_id].add(date2)
        self.worker_assignments[worker2_id].remove(date2)
        self.worker_assignments[worker2_id].add(date1)
    
        # Update tracking data for both workers
        self.scheduler._update_tracking_data(worker1_id, date1, post1, removing=True)
        self.scheduler._update_tracking_data(worker1_id, date2, post2)
        self.scheduler._update_tracking_data(worker2_id, date2, post2, removing=True)
        self.scheduler._update_tracking_data(worker2_id, date1, post1)
    
        return True
        
    def _identify_imbalanced_posts(self, deviation_threshold=1.5):
        """
        Identifies workers with an imbalanced distribution of assigned posts.

        Args:
            deviation_threshold: How much the count for a single post can deviate
                                 from the average before considering the worker imbalanced.

        Returns:
            List of tuples: [(worker_id, post_counts, max_deviation), ...]
                           Sorted by max_deviation descending.
        """
        imbalanced_workers = []
        num_posts = self.num_shifts
        if num_posts == 0: return [] # Avoid division by zero

        # Use scheduler's worker data and post tracking
        for worker_val in self.scheduler.workers_data: # Renamed worker
            worker_id_val = worker_val['id'] # Renamed worker_id
            # Get post counts, defaulting to an empty dict if worker has no assignments yet
            actual_post_counts = self.scheduler.worker_posts.get(worker_id_val, {})
            total_assigned = sum(actual_post_counts.values())

            # If worker has no shifts or only one type of post, they can't be imbalanced yet
            if total_assigned == 0 or num_posts <= 1:
                continue

            target_per_post = total_assigned / num_posts
            max_deviation = 0
            post_deviations = {} # Store deviation per post

            for post_val in range(num_posts): # Renamed post
                actual_count = actual_post_counts.get(post_val, 0)
                deviation = actual_count - target_per_post
                post_deviations[post_val] = deviation
                if abs(deviation) > max_deviation:
                    max_deviation = abs(deviation)

            # Consider imbalanced if the count for any post is off by more than the threshold
            if max_deviation > deviation_threshold:
                # Store the actual counts, not the deviations map for simplicity
                imbalanced_workers.append((worker_id_val, actual_post_counts.copy(), max_deviation))
                logging.debug(f"Worker {worker_id_val} identified as imbalanced for posts. Max Deviation: {max_deviation:.2f}, Target/Post: {target_per_post:.2f}, Counts: {actual_post_counts}")


        # Sort by the magnitude of imbalance (highest deviation first)
        imbalanced_workers.sort(key=lambda x: x[2], reverse=True)
        return imbalanced_workers

    def _get_over_under_posts(self, post_counts, total_assigned, balance_threshold=1.0):
        """
        Given a worker's post counts, find which posts they have significantly
        more or less than the average.

        Args:
            post_counts (dict): {post_index: count} for the worker.
            total_assigned (int): Total shifts assigned to the worker.
            balance_threshold: How far from the average count triggers over/under.

        Returns:
            tuple: (list_of_overassigned_posts, list_of_underassigned_posts)
                   Each list contains tuples: [(post_index, count), ...]\
                   Sorted by deviation magnitude.
        """
        overassigned = []
        underassigned = []
        num_posts = self.num_shifts
        if num_posts <= 1 or total_assigned == 0:
            return [], [] # Cannot be over/under assigned

        target_per_post = total_assigned / num_posts

        for post_val in range(num_posts): # Renamed post
            actual_count = post_counts.get(post_val, 0)
            deviation = actual_count - target_per_post

            # Use a threshold slightly > 0 to avoid minor float issues
            # Consider overassigned if count is clearly higher than target
            if deviation > balance_threshold:
                overassigned.append((post_val, actual_count, deviation)) # Include deviation for sorting
            # Consider underassigned if count is clearly lower than target
            elif deviation < -balance_threshold:
                 underassigned.append((post_val, actual_count, deviation)) # Deviation is negative

        # Sort overassigned: highest count (most over) first
        overassigned.sort(key=lambda x: x[2], reverse=True)
        # Sort underassigned: lowest count (most under) first (most negative deviation)
        underassigned.sort(key=lambda x: x[2])

        # Return only (post, count) tuples
        overassigned_simple = [(p, c) for p, c, d_val in overassigned] # Renamed d to d_val
        underassigned_simple = [(p, c) for p, c, d_val in underassigned] # Renamed d to d_val

        return overassigned_simple, underassigned_simple
    
    def _balance_target_shifts_aggressively(self):
        """Balance workers to meet their exact target_shifts, focusing on largest deviations first"""
        logging.info("Starting aggressive target balancing...")
        changes_made = 0
    
        # Calculate deviations for all workers
        worker_deviations = []
        for worker in self.workers_data:
            worker_id = worker['id']
            target = worker['target_shifts']
            current = len(self.worker_assignments.get(worker_id, []))
            deviation = current - target
            if abs(deviation) > 0.5:  # Only process workers with meaningful deviation
                worker_deviations.append((worker_id, deviation, target, current))
    
        # Sort by absolute deviation (largest first)
        worker_deviations.sort(key=lambda x: abs(x[1]), reverse=True)
    
        for worker_id, deviation, target, current in worker_deviations:
            if deviation > 0:  # Worker has too many shifts
                changes_made += self._try_redistribute_excess_shifts(worker_id, int(deviation))
    
        return changes_made

    def _try_redistribute_excess_shifts(self, overloaded_worker_id, excess_count):
        """Try to move excess shifts from overloaded worker to underloaded workers"""
        changes = 0
        max_attempts = min(excess_count, 5)  # Limit attempts to avoid disruption
    
        # Find underloaded workers
        underloaded_workers = []
        for worker in self.workers_data:
            worker_id = worker['id']
            if worker_id == overloaded_worker_id:
                continue
            target = worker['target_shifts']
            current = len(self.worker_assignments.get(worker_id, []))
            if current < target:
                deficit = target - current
                underloaded_workers.append((worker_id, deficit))
    
        # Sort by largest deficit first
        underloaded_workers.sort(key=lambda x: x[1], reverse=True)
    
        if not underloaded_workers:
            return 0
    
        # Try to move shifts
        assignments = list(self.worker_assignments.get(overloaded_worker_id, []))
        random.shuffle(assignments)
    
        for date in assignments[:max_attempts]:
            if (overloaded_worker_id, date) in self._locked_mandatory:
                continue
            if self._is_mandatory(overloaded_worker_id, date):
                continue
            
            try:
                post = self.schedule[date].index(overloaded_worker_id)
            except (ValueError, KeyError):
                continue
            
            # Try to assign to an underloaded worker
            for under_worker_id, deficit in underloaded_workers:
                if under_worker_id in self.schedule.get(date, []):
                    continue  # Already assigned this date
                
                if self._can_assign_worker(under_worker_id, date, post):
                    # Make the transfer
                    self.schedule[date][post] = under_worker_id
                    self.worker_assignments[overloaded_worker_id].remove(date)
                    self.worker_assignments.setdefault(under_worker_id, set()).add(date)
                
                    # Update tracking
                    self.scheduler._update_tracking_data(overloaded_worker_id, date, post, removing=True)
                    self.scheduler._update_tracking_data(under_worker_id, date, post)
                
                    changes += 1
                    logging.info(f"Redistributed shift on {date.strftime('%Y-%m-%d')} from worker {overloaded_worker_id} to {under_worker_id}")
                    break
                
            if changes >= max_attempts:
                break
    
        return changes

    def _balance_workloads(self):
        """
        """
        logging.info("Attempting to balance worker workloads")
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()

        # First verify and fix data consistency
        self._verify_assignment_consistency()

        # Count total assignments for each worker
        assignment_counts = {}
        for worker_val in self.workers_data: # Renamed worker
            worker_id_val = worker_val['id'] # Renamed worker_id
            work_percentage = worker_val.get('work_percentage', 100)
    
            # Count assignments
            count = len(self.worker_assignments[worker_id_val])
    
            # Normalize by work percentage
            normalized_count = count * 100 / work_percentage if work_percentage > 0 else 0
    
            assignment_counts[worker_id_val] = {\
                'worker_id': worker_id_val,\
                'count': count,\
                'work_percentage': work_percentage,\
                'normalized_count': normalized_count\
            }    

        # Calculate average normalized count
        total_normalized = sum(data['normalized_count'] for data in assignment_counts.values())
        avg_normalized = total_normalized / len(assignment_counts) if assignment_counts else 0

        # Identify overloaded and underloaded workers
        overloaded = []
        underloaded = []

        for worker_id_val, data_val in assignment_counts.items(): # Renamed worker_id, data
            # Allow 10% deviation from average
            if data_val['normalized_count'] > avg_normalized * 1.1:
                overloaded.append((worker_id_val, data_val))
            elif data_val['normalized_count'] < avg_normalized * 0.9:
                underloaded.append((worker_id_val, data_val))

        # Sort by most overloaded/underloaded
        overloaded.sort(key=lambda x: x[1]['normalized_count'], reverse=True)
        underloaded.sort(key=lambda x: x[1]['normalized_count'])

        changes_made = 0
        max_changes = 30  # Limit number of changes to avoid disrupting the schedule too much

        # Try to redistribute shifts from overloaded to underloaded workers
        for over_worker_id, over_data in overloaded:
            if changes_made >= max_changes or not underloaded:
                break
        
            # Find shifts that can be reassigned from this overloaded worker
            possible_shifts = []
    
            for date_val in sorted(self.scheduler.worker_assignments.get(over_worker_id, set())): # Renamed date
                # never touch a locked mandatory
                if (over_worker_id, date_val) in self._locked_mandatory:
                    logging.debug(f"Skipping workload‐balance move for mandatory shift: {over_worker_id} on {date_val}")
                    continue

                # --- MANDATORY CHECK --- (you already had this, but now enforced globally)
                # skip if this date is mandatory for this worker
                if self._is_mandatory(over_worker_id, date_val):
                    continue

            
                # Make sure the worker is actually in the schedule for this date
                if date_val not in self.schedule:
                    # This date is in worker_assignments but not in schedule
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but date is not in schedule")
                    continue
                
                try:
                    # Find the post this worker is assigned to
                    if over_worker_id not in self.schedule[date_val]:
                        # Worker is supposed to be assigned to this date but isn't in the schedule
                        logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but is not in schedule")
                        continue
                    
                    post_val = self.schedule[date_val].index(over_worker_id) # Renamed post
                    possible_shifts.append((date_val, post_val))
                except ValueError:
                    # Worker not found in schedule for this date
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but is not in schedule")
                    continue
    
            # Shuffle to introduce randomness
            random.shuffle(possible_shifts)
    
            # Try each shift
            for date_val, post_val in possible_shifts: # Renamed date, post
                reassigned = False
                for under_worker_id, _ in underloaded:
                    # ... (check if under_worker already assigned) ...
                    if self._can_assign_worker(under_worker_id, date_val, post_val):
                        # remove only if it wasn't locked mandatory
                        if (over_worker_id, date_val) in self._locked_mandatory:
                            continue
                        self.scheduler.schedule[date_val][post_val] = under_worker_id
                        self.scheduler.worker_assignments[over_worker_id].remove(date_val)
                        # Ensure under_worker tracking exists
                        if under_worker_id not in self.scheduler.worker_assignments:
                             self.scheduler.worker_assignments[under_worker_id] = set()
                        self.scheduler.worker_assignments[under_worker_id].add(date_val)

                        # Update tracking data (Needs FIX: update for BOTH workers)
                        self.scheduler._update_tracking_data(over_worker_id, date_val, post_val, removing=True) # Remove stats for over_worker
                        self.scheduler._update_tracking_data(under_worker_id, date_val, post_val) # Add stats for under_worker

                        changes_made += 1
                        logging.info(f"Balanced workload: Moved shift on {date_val.strftime('%Y-%m-%d')} post {post_val} from {over_worker_id} to {under_worker_id}")
                        
                        # Update counts
                        assignment_counts[over_worker_id]['count'] -= 1
                        assignment_counts[over_worker_id]['normalized_count'] = (\
                            assignment_counts[over_worker_id]['count'] * 100 / \
                            assignment_counts[over_worker_id]['work_percentage']\
                        ) if assignment_counts[over_worker_id]['work_percentage'] > 0 else 0 # Added check for zero division
                
                        assignment_counts[under_worker_id]['count'] += 1
                        assignment_counts[under_worker_id]['normalized_count'] = (\
                            assignment_counts[under_worker_id]['count'] * 100 / \
                            assignment_counts[under_worker_id]['work_percentage']\
                        ) if assignment_counts[under_worker_id]['work_percentage'] > 0 else 0 # Added check for zero division
                
                        reassigned = True
                
                        # Check if workers are still overloaded/underloaded
                        if assignment_counts[over_worker_id]['normalized_count'] <= avg_normalized * 1.1:
                            # No longer overloaded
                            overloaded = [(w, d_val_loop) for w, d_val_loop in overloaded if w != over_worker_id] # Renamed d to d_val_loop
                
                        if assignment_counts[under_worker_id]['normalized_count'] >= avg_normalized * 0.9:
                            # No longer underloaded
                            underloaded = [(w, d_val_loop) for w, d_val_loop in underloaded if w != under_worker_id] # Renamed d to d_val_loop
                
                        break
        
                if reassigned:
                    break
            
                if changes_made >= max_changes:
                    break

        logging.info(f"Workload balancing: made {changes_made} changes")
        if changes_made > 0:
            self._save_current_as_best()
        return changes_made > 0

    def _optimize_schedule(self, iterations=None):
        """Enhanced optimization with adaptive iterations and convergence detection"""
        # Start the optimization timer
        self.iteration_manager.start_timer()
    
        # Use adaptive iterations if not specified
        if iterations is None:
            max_main_loops = self.adaptive_config['max_optimization_loops']
            max_post_iterations = self.adaptive_config['last_post_max_iterations']
            convergence_threshold = self.adaptive_config['convergence_threshold']
        else:
            # Fallback to legacy behavior if iterations specified
            max_main_loops = iterations
            max_post_iterations = max(3, iterations // 2)
            convergence_threshold = 3
    
        logging.info(f"Starting adaptive optimization with max {max_main_loops} loops, "
                    f"convergence threshold: {convergence_threshold}")
    
        # Ensure initial state is the best known if nothing better is found
        if self.best_schedule_data is None:
            self._save_current_as_best(initial=True)
    
        best_score = self._evaluate_schedule()
        if self.best_schedule_data and self.best_schedule_data['score'] > best_score:
            best_score = self.best_schedule_data['score']
            self._restore_best_schedule()
        else:
            self._save_current_as_best(initial=True)
            best_score = self.best_schedule_data['score']

        iterations_without_improvement = 0
    
        logging.info(f"Starting schedule optimization. Initial best score: {best_score:.2f}")

        # Main optimization loop with adaptive control
        for i in range(max_main_loops):
            logging.info(f"--- Main Optimization Loop Iteration: {i + 1}/{max_main_loops} ---")
            made_change_in_main_iteration = False
        
            # Check if we should continue optimization
            current_score = self._evaluate_schedule()
            if not self.iteration_manager.should_continue(i, iterations_without_improvement, current_score):
                logging.info("Early termination due to convergence, time limit, or quality threshold")
                break
            # 0. Priority phase: Focus on workers furthest from targets
            if i < 3:  # First 3 iterations focus heavily on targets
                if self._balance_target_shifts_aggressively():
                    logging.info(f"Improved target matching in iteration {i + 1}")
                    made_change_in_main_iteration = True
                    self._synchronize_tracking_data()
        
            # Synchronize data at the beginning of each major optimization iteration
            self._synchronize_tracking_data()

            # 1. Try to fill empty shifts with adaptive attempts
            fill_attempts = min(self.adaptive_config.get('max_fill_attempts', 5), 3 + i // 2)
            for attempt in range(fill_attempts):
                if self._try_fill_empty_shifts():
                    logging.info(f"Improved schedule by filling empty shifts (attempt {attempt + 1})")
                    made_change_in_main_iteration = True
                    self._synchronize_tracking_data()
                    break  # Success, move to next optimization phase

            # 2. Try to improve weekend distribution with adaptive passes
            weekend_passes = min(self.adaptive_config.get('max_weekend_passes', 3), 2 + i // 3)
            for pass_num in range(weekend_passes):
                if self._balance_weekend_shifts():
                    logging.info(f"Improved weekend distribution (pass {pass_num + 1}/{weekend_passes})")
                    made_change_in_main_iteration = True
                    self._synchronize_tracking_data()

            # 3. Try to balance overall workloads with adaptive iterations
            balance_iterations = min(self.adaptive_config.get('max_balance_iterations', 3), 2 + i // 4)
            for balance_iter in range(balance_iterations):
                if self._balance_workloads():
                    logging.info(f"Improved workload balance (iteration {balance_iter + 1}/{balance_iterations})")
                    made_change_in_main_iteration = True
                    self._synchronize_tracking_data()
                    break  # Success, move to next phase

            # 4. Iteratively adjust last post distribution
            if self._adjust_last_post_distribution(
                balance_tolerance=self.adaptive_config.get('last_post_balance_tolerance', 0.5), 
                max_iterations=max_post_iterations
            ):
                logging.info("Schedule potentially improved through iterative last post adjustments.")
                made_change_in_main_iteration = True

            # 5. Final verification of incompatibilities and attempt to fix them
            if self._verify_no_incompatibilities():
                logging.info("Fixed incompatibility violations during optimization.")
                made_change_in_main_iteration = True
                self._synchronize_tracking_data()

            # Evaluate the schedule after this full pass of optimizations
            current_score = self._evaluate_schedule()
            logging.info(f"Score after main optimization iteration {i + 1}: {current_score:.2f}. Previous best: {best_score:.2f}")

            # Check for improvement
            improvement = current_score - best_score
            improvement_threshold = self.adaptive_config.get('improvement_threshold', 0.1)
        
            if improvement > improvement_threshold:
                logging.info(f"Significant improvement: +{improvement:.2f} (threshold: {improvement_threshold})")
                best_score = current_score
                self._save_current_as_best()
                iterations_without_improvement = 0
            else:
                iterations_without_improvement += 1
                logging.info(f"No significant improvement in main iteration {i+1}. "
                            f"Score: {current_score:.2f}. "
                            f"Iterations without improvement: {iterations_without_improvement}/{convergence_threshold}")
            
                # Restore best if current is worse
                if self.best_schedule_data and self.schedule != self.best_schedule_data['schedule']:
                    if current_score < self.best_schedule_data['score']:
                        logging.info(f"Restoring to best known score: {self.best_schedule_data['score']:.2f}")
                        self._restore_best_schedule()

            # Early exit conditions
            if not made_change_in_main_iteration and iterations_without_improvement >= 2:
                logging.info(f"No changes made and no improvement for 2 iterations. Early exit consideration.")
        
            if iterations_without_improvement >= convergence_threshold:
                logging.info(f"Reached {convergence_threshold} main iterations without improvement. Stopping optimization.")
                break
    
        # Final check and restoration
        if self.best_schedule_data and self.schedule != self.best_schedule_data['schedule']:
            final_current_score = self._evaluate_schedule()
            if final_current_score < self.best_schedule_data['score']:
                logging.info(f"Final check: Restoring to best saved score {self.best_schedule_data['score']:.2f} "
                            f"as current ({final_current_score:.2f}) is worse.")
                self._restore_best_schedule()
            elif final_current_score > self.best_schedule_data['score']:
                logging.info(f"Final check: Current schedule score {final_current_score:.2f} "
                            f"is better than saved {self.best_schedule_data['score']:.2f}. Saving current.")
                self._save_current_as_best()
                best_score = final_current_score

        # Ensure we have a valid best schedule saved before returning
        if not self._ensure_best_schedule_saved():
            logging.error("Critical: Failed to ensure best schedule is saved")
            # Try one more time to save current state
            try:
                self._save_current_as_best()
                logging.info("Emergency save attempt completed")
            except Exception as e:
                logging.error(f"Emergency save failed: {str(e)}")
    
        elapsed_time = (datetime.now() - self.iteration_manager.start_time).total_seconds()
        logging.info(f"Adaptive optimization process complete in {elapsed_time:.1f}s. "
                    f"Final best score: {best_score:.2f}")
        logging.info(f"Completed {i + 1} optimization loops with {iterations_without_improvement} "
                    f"final iterations without improvement")
    
        # Final verification
        if hasattr(self, 'best_schedule_data') and self.best_schedule_data:
            logging.info(f"Optimization complete - best schedule confirmed with score: {self.best_schedule_data.get('score', 'unknown')}")
        else:
            logging.warning("Optimization complete but no best schedule data found")
    
        return best_score        

    def _can_worker_swap(self, worker1_id, date1, post1, worker2_id, date2, post2):
        """
        Check if two workers can swap their assignments between dates/posts.
        This method performs a comprehensive check of all constraints to ensure
        that the swap would be valid according to the system's rules.
    
        Args:
            worker1_id: First worker's ID
            date1: First worker's date
            post1: First worker's post
            worker2_id: Second worker's ID
            date2: Second worker's date
            post2: Second worker's post
    
        Returns:
            bool: True if the swap is valid, False otherwise
        """
        # First check: Make sure neither assignment is mandatory
        if self._is_mandatory(worker1_id, date1) or self._is_mandatory(worker2_id, date2):
            logging.debug(f"Swap rejected: Config-defined mandatory assignment detected by _is_mandatory. W1_mandatory: {self._is_mandatory(worker1_id, date1)}, W2_mandatory: {self._is_mandatory(worker2_id, date2)}") # Corrected log string
            return False
    
        # Make a copy of the schedule and assignments to simulate the swap
        schedule_copy = copy.deepcopy(self.schedule)
        assignments_copy = {}
        for worker_id_val, assignments_val in self.worker_assignments.items(): # Renamed worker_id, assignments
            assignments_copy[worker_id_val] = set(assignments_val)
    
        # Simulate the swap
        schedule_copy[date1][post1] = worker2_id
        schedule_copy[date2][post2] = worker1_id
    
        # Update worker_assignments copies
        assignments_copy[worker1_id].remove(date1)
        assignments_copy[worker1_id].add(date2)
        assignments_copy[worker2_id].remove(date2)
        assignments_copy[worker2_id].add(date1)
    
        # Check all constraints for both workers in the simulated state
    
        # 1. Check incompatibility constraints for worker1 on date2
        currently_assigned_date2 = [w for i, w in enumerate(schedule_copy[date2]) \
                                   if w is not None and i != post2]
        if not self._check_incompatibility_with_list(worker1_id, currently_assigned_date2):
            logging.debug(f"Swap rejected: Worker {worker1_id} incompatible with workers on {date2}")
            return False
    
        # 2. Check incompatibility constraints for worker2 on date1
        currently_assigned_date1 = [w for i, w in enumerate(schedule_copy[date1]) \
                                   if w is not None and i != post1]
        if not self._check_incompatibility_with_list(worker2_id, currently_assigned_date1):
            logging.debug(f"Swap rejected: Worker {worker2_id} incompatible with workers on {date1}")
            return False
    
        # 3. Check minimum gap constraints for worker1
        min_days_between = self.gap_between_shifts + 1
        worker1_dates = sorted(list(assignments_copy[worker1_id]))
    
        for assigned_date_val in worker1_dates: # Renamed assigned_date
            if assigned_date_val == date2:
                continue  # Skip the newly assigned date
        
            days_between = abs((date2 - assigned_date_val).days)
            if days_between < min_days_between:
                logging.debug(f"Swap rejected: Worker {worker1_id} would have insufficient gap between {assigned_date_val} and {date2}")
                return False
        
            # Special case for Friday-Monday if gap is only 1 day
            if self.gap_between_shifts == 1 and days_between == 3:
                if ((assigned_date_val.weekday() == 4 and date2.weekday() == 0) or \
                    (date2.weekday() == 4 and assigned_date_val.weekday() == 0)):
                    logging.debug(f"Swap rejected: Worker {worker1_id} would have Friday-Monday pattern")
                    return False
        
            # NEW: Check for 7/14 day pattern (same day of week in consecutive weeks)
            if (days_between == 7 or days_between == 14) and date2.weekday() == assigned_date_val.weekday():
                logging.debug(f"Swap rejected: Worker {worker1_id} would have {days_between} day pattern")
                return False
    
        # 4. Check minimum gap constraints for worker2
        worker2_dates = sorted(list(assignments_copy[worker2_id]))
    
        for assigned_date_val in worker2_dates: # Renamed assigned_date
            if assigned_date_val == date1:
                continue  # Skip the newly assigned date
        
            days_between = abs((date1 - assigned_date_val).days)
            if days_between < min_days_between:
                logging.debug(f"Swap rejected: Worker {worker2_id} would have insufficient gap between {assigned_date_val} and {date1}")
                return False
        
            # Special case for Friday-Monday if gap is only 1 day
            if self.gap_between_shifts == 1 and days_between == 3:
                if ((assigned_date_val.weekday() == 4 and date1.weekday() == 0) or \
                    (date1.weekday() == 4 and assigned_date_val.weekday() == 0)):
                    logging.debug(f"Swap rejected: Worker {worker2_id} would have Friday-Monday pattern")
                    return False
        
            # NEW: Check for 7/14 day pattern (same day of week in consecutive weeks)
            if (days_between == 7 or days_between == 14) and date1.weekday() == assigned_date_val.weekday():
                logging.debug(f"Swap rejected: Worker {worker2_id} would have {days_between} day pattern")
                return False
        
        # 5. Check weekend constraints for worker1
        worker1_data_val = next((w for w in self.workers_data if w['id'] == worker1_id), None) # Renamed worker1 to worker1_data_val
        if worker1_data_val:
            worker1_weekend_dates = [d_val for d_val in worker1_dates # Renamed d to d_val
                                    if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays]
        
            # If the new date is a weekend/holiday, add it to the list
            if self.date_utils.is_weekend_day(date2) or date2 in self.holidays:
                if date2 not in worker1_weekend_dates:
                    worker1_weekend_dates.append(date2)
                    worker1_weekend_dates.sort()
        
            # Check if this would violate max consecutive weekends
            max_weekend_count = self.max_consecutive_weekends
            work_percentage = worker1_data_val.get('work_percentage', 100)
            if work_percentage < 70:
                max_weekend_count = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
        
            for i, weekend_date_val in enumerate(worker1_weekend_dates): # Renamed weekend_date
                window_start = weekend_date_val - timedelta(days=10)
                window_end = weekend_date_val + timedelta(days=10)
            
                # Count weekend/holiday dates in this window
                window_count = sum(1 for d_val in worker1_weekend_dates # Renamed d to d_val
                                  if window_start <= d_val <= window_end)
            
                if window_count > max_weekend_count:
                    logging.debug(f"Swap rejected: Worker {worker1_id} would exceed weekend limit")
                    return False
    
        # 6. Check weekend constraints for worker2
        worker2_data_val = next((w for w in self.workers_data if w['id'] == worker2_id), None) # Renamed worker2 to worker2_data_val
        if worker2_data_val:
            worker2_weekend_dates = [d_val for d_val in worker2_dates # Renamed d to d_val
                                    if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays]
        
            # If the new date is a weekend/holiday, add it to the list
            if self.date_utils.is_weekend_day(date1) or date1 in self.holidays:
                if date1 not in worker2_weekend_dates:
                    worker2_weekend_dates.append(date1)
                    worker2_weekend_dates.sort()
        
            # Check if this would violate max consecutive weekends
            max_weekend_count = self.max_consecutive_weekends
            work_percentage = worker2_data_val.get('work_percentage', 100)
            if work_percentage < 100:
                max_weekend_count = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
        
            for i, weekend_date_val in enumerate(worker2_weekend_dates): # Renamed weekend_date
                window_start = weekend_date_val - timedelta(days=10)
                window_end = weekend_date_val + timedelta(days=10)
            
                # Count weekend/holiday dates in this window
                window_count = sum(1 for d_val in worker2_weekend_dates # Renamed d to d_val
                                  if window_start <= d_val <= window_end)
            
                if window_count > max_weekend_count:
                    logging.debug(f"Swap rejected: Worker {worker2_id} would exceed weekend limit")
                    return False
    
        # All constraints passed, the swap is valid
        logging.debug(f"Swap between Worker {worker1_id} ({date1}/{post1}) and Worker {worker2_id} ({date2}/{post2}) is valid")
        return True

    def _execute_swap(self, worker_id, date_from, post_from, worker_X_id, date_to, post_to):
        """ Helper to perform the actual swap updates. Can handle either a single worker swap or a swap between two workers. """
        # 1. Update schedule dictionary
        self.scheduler.schedule[date_from][post_from] = None if worker_X_id is None else worker_X_id
    
        # Ensure target list is long enough before assignment
        while len(self.scheduler.schedule[date_to]) <= post_to:
            self.scheduler.schedule[date_to].append(None)
        self.scheduler.schedule[date_to][post_to] = worker_id

        # 2. Update worker_assignments set for the first worker
        # Check if the date exists in the worker's assignments before removing
        if date_from in self.scheduler.worker_assignments.get(worker_id, set()):
            self.scheduler.worker_assignments[worker_id].remove(date_from)
    
        # Add the new date to the worker's assignments
        self.scheduler.worker_assignments.setdefault(worker_id, set()).add(date_to)

        # 3. Update worker_assignments for the second worker if present
        if worker_X_id is not None:
            # Check if the date exists in worker_X's assignments before removing
            if date_to in self.scheduler.worker_assignments.get(worker_X_id, set()):
                self.scheduler.worker_assignments[worker_X_id].remove(date_to)
        
            # Add the from_date to worker_X's assignments
            self.scheduler.worker_assignments.setdefault(worker_X_id, set()).add(date_from)

        # 4. Update detailed tracking stats for both workers
        # Only update tracking data for removal if the worker was actually assigned to that date
        if date_from in self.scheduler.worker_assignments.get(worker_id, set()) or (date_from in self.scheduler.schedule and self.scheduler.schedule[date_from].count(worker_id) > 0): # Corrected condition
            self.scheduler._update_tracking_data(worker_id, date_from, post_from, removing=True)
    
        self.scheduler._update_tracking_data(worker_id, date_to, post_to)
    
        if worker_X_id is not None:
            # Only update tracking data for removal if worker_X was actually assigned to that date
            if date_to in self.scheduler.worker_assignments.get(worker_X_id, set()) or (date_to in self.scheduler.schedule and self.scheduler.schedule[date_to].count(worker_X_id) > 0): # Corrected condition
                self.scheduler._update_tracking_data(worker_X_id, date_to, post_to, removing=True)
        
            self.scheduler._update_tracking_data(worker_X_id, date_from, post_from)          

    def _check_all_constraints_for_date(self, date):
        """ Checks all constraints for all workers assigned on a given date. """
        # Indent level 1
        if date not in self.scheduler.schedule:
            return True # No assignments, no violations

        assignments_on_date = self.scheduler.schedule[date]
        workers_present = [w for w in assignments_on_date if w is not None]

        # Direct check for pairwise incompatibility on this date
        for i in range(len(workers_present)):
            # Indent level 2
            for j in range(i + 1, len(workers_present)):
                # Indent level 3
                worker1_id = workers_present[i]
                worker2_id = workers_present[j]
                if self._are_workers_incompatible(worker1_id, worker2_id):
                    # Indent level 4
                    logging.debug(f"Constraint check failed (direct): Incompatibility between {worker1_id} and {worker2_id} on {date}")
                    return False

        # Now check individual worker constraints (gap, weekend limits, etc.)
        for post, worker_id in enumerate(assignments_on_date):
            # Indent level 2
            if worker_id is not None:
                # Indent level 3
                # Assuming _check_constraints uses live data from self.scheduler
                # Ensure the constraint checker method exists and is correctly referenced
                try:
                    passed, reason = self.scheduler.constraint_checker._check_constraints(\
                        worker_id,\
                        date,\
                        skip_constraints=False\
                    )
                    if not passed:
                        logging.debug(f"Constraint violation for worker {worker_id} on {date}: {reason}")
                        return False
                except AttributeError:
                    logging.error("Constraint checker or _check_constraints method not found during swap validation.")
                    return False
                except Exception as e_constr:
                    logging.error(f"Error calling constraint checker for {worker_id} on {date}: {e_constr}", exc_info=True)
                    return False

        # Indent level 1 (aligned with the initial 'if' and 'for' loops)
        return True

    def _improve_weekend_distribution(self):
        """
        Improve weekend distribution by balancing "special constraint days" 
        (Fri/Sat/Sun, Holiday, Day-before-Holiday) more evenly among workers
        and attempting to resolve overloads based on max_consecutive_weekends 
        interpreted as a monthly cap for these days.
        """
        logging.info("Attempting to improve special day (weekend/holiday/eve) distribution")
    
        # Ensure data consistency before proceeding
        self._ensure_data_integrity() # This should call scheduler's data sync if it exists
                                      # or be robust enough on its own.
                                      # For now, assuming scheduler's data is the source of truth.

        # Count "special constraint day" assignments for each worker per month
        special_day_counts_by_month = {} 
        months = {}
        current_date_iter = self.start_date
        while current_date_iter <= self.end_date:
            month_key = (current_date_iter.year, current_date_iter.month)
            if month_key not in months: months[month_key] = []
            months[month_key].append(current_date_iter)
            current_date_iter += timedelta(days=1)

        for month_key, dates_in_month in months.items():
            current_month_special_day_counts = {} 
            for worker_val in self.workers_data:
                worker_id_val = worker_val['id']
                
                count = 0
                for date_val in dates_in_month:
                    # MANUALLY EMBEDDED CHECK
                    is_special_day = (date_val.weekday() >= 4 or  # Friday, Saturday, Sunday
                                      date_val in self.holidays or
                                      (date_val + timedelta(days=1)) in self.holidays)

                    if date_val in self.scheduler.worker_assignments.get(worker_id_val, set()) and is_special_day:
                        count += 1
                current_month_special_day_counts[worker_id_val] = count
            special_day_counts_by_month[month_key] = current_month_special_day_counts
    
        changes_made = 0
    
        for month_key, current_month_counts in special_day_counts_by_month.items():
            overloaded_workers = []
            underloaded_workers = []

            for worker_val in self.workers_data:
                worker_id_val = worker_val['id']
                work_percentage = worker_val.get('work_percentage', 100)
                
                # Using max_consecutive_weekends as a type of monthly limit for these special days.
                # This value comes from the scheduler's config.
                max_special_days_limit_for_month = self.max_consecutive_weekends 
                if work_percentage < 100: # Apply part-time adjustment
                    max_special_days_limit_for_month = max(1, int(self.max_consecutive_weekends * work_percentage / 100))

                actual_special_days_this_month = current_month_counts.get(worker_id_val, 0)

                if actual_special_days_this_month > max_special_days_limit_for_month:
                    overloaded_workers.append((worker_id_val, actual_special_days_this_month, max_special_days_limit_for_month))
                elif actual_special_days_this_month < max_special_days_limit_for_month:
                    available_slots = max_special_days_limit_for_month - actual_special_days_this_month
                    underloaded_workers.append((worker_id_val, actual_special_days_this_month, available_slots))

            overloaded_workers.sort(key=lambda x: x[1] - x[2], reverse=True) 
            underloaded_workers.sort(key=lambda x: x[2], reverse=True) 

            month_dates_list = months[month_key]
            
            special_days_this_month_list = []
            for date_val in month_dates_list:
                # MANUALLY EMBEDDED CHECK
                is_special_day = (date_val.weekday() >= 4 or
                                  date_val in self.holidays or
                                  (date_val + timedelta(days=1)) in self.holidays)
                if is_special_day:
                    special_days_this_month_list.append(date_val)

            for over_worker_id, _, _ in overloaded_workers: # Removed unused over_count, over_limit
                if not underloaded_workers: break 

                # Iterate only through the worker's assigned special days in this month
                possible_dates_to_move_from = []
                for s_day in special_days_this_month_list: # Iterate only over actual special days
                    if s_day in self.scheduler.worker_assignments.get(over_worker_id, set()) and \
                       over_worker_id in self.scheduler.schedule.get(s_day, []): # Check if actually in schedule slot
                        possible_dates_to_move_from.append(s_day)
                
                random.shuffle(possible_dates_to_move_from)

                for special_day_to_reassign in possible_dates_to_move_from:
                    # --- MANDATORY CHECKS ---
                    if (over_worker_id, special_day_to_reassign) in self._locked_mandatory:
                        logging.debug(f"Cannot move worker {over_worker_id} from locked mandatory shift on {special_day_to_reassign.strftime('%Y-%m-%d')} for balancing.")
                        continue
                    if self._is_mandatory(over_worker_id, special_day_to_reassign): 
                        logging.debug(f"Cannot move worker {over_worker_id} from config-mandatory shift on {special_day_to_reassign.strftime('%Y-%m-%d')} for balancing.")
                        continue
                    # --- END MANDATORY CHECKS ---
                    
                    try:
                        # Ensure the worker is actually in the schedule for this date and find post
                        if special_day_to_reassign not in self.scheduler.schedule or \
                           over_worker_id not in self.scheduler.schedule[special_day_to_reassign]:
                            logging.warning(f"Data inconsistency: Worker {over_worker_id} tracked for {special_day_to_reassign} but not in schedule slot.")
                            continue
                        post_val = self.scheduler.schedule[special_day_to_reassign].index(over_worker_id)
                    except (ValueError, KeyError, IndexError) as e: # Added specific exception logging
                        logging.warning(f"Inconsistency finding post for {over_worker_id} on {special_day_to_reassign} during special day balance: {e}")
                        continue

                    swap_done_for_this_shift = False
                    for under_worker_id, _, _ in underloaded_workers: # Removed unused counts/slots
                        # Check if under_worker is already assigned on this special day
                        if special_day_to_reassign in self.scheduler.schedule and \
                           under_worker_id in self.scheduler.schedule.get(special_day_to_reassign, []):
                            continue

                        # _can_assign_worker MUST use the same consistent definition of special day for its internal checks
                        # (especially its call to _would_exceed_weekend_limit)
                        if self._can_assign_worker(under_worker_id, special_day_to_reassign, post_val):
                            # Perform the assignment change
                            self.scheduler.schedule[special_day_to_reassign][post_val] = under_worker_id
                            self.scheduler.worker_assignments[over_worker_id].remove(special_day_to_reassign)
                            self.scheduler.worker_assignments.setdefault(under_worker_id, set()).add(special_day_to_reassign)

                            self.scheduler._update_tracking_data(over_worker_id, special_day_to_reassign, post_val, removing=True)
                            self.scheduler._update_tracking_data(under_worker_id, special_day_to_reassign, post_val) # Default is adding=False
                            
                            # Update local counts for the current month
                            current_month_counts[over_worker_id] -= 1
                            current_month_counts[under_worker_id] = current_month_counts.get(under_worker_id, 0) + 1
                            
                            changes_made += 1
                            logging.info(f"Improved special day distribution: Moved shift on {special_day_to_reassign.strftime('%Y-%m-%d')} "
                                         f"from worker {over_worker_id} to worker {under_worker_id}")

                            # --- Re-evaluate overloaded/underloaded lists locally ---
                            # Check if 'over_worker_id' is still overloaded
                            over_worker_new_count = current_month_counts[over_worker_id]
                            over_worker_obj = next((w for w in self.workers_data if w['id'] == over_worker_id), None)
                            over_worker_limit_this_month = self.max_consecutive_weekends
                            if over_worker_obj and over_worker_obj.get('work_percentage', 100) < 100:
                                over_worker_limit_this_month = max(1, int(self.max_consecutive_weekends * over_worker_obj.get('work_percentage',100) / 100))
                            
                            if over_worker_new_count <= over_worker_limit_this_month:
                                overloaded_workers = [(w, c, l) for w, c, l in overloaded_workers if w != over_worker_id]

                            # Check if 'under_worker_id' is still underloaded or became full
                            under_worker_new_count = current_month_counts[under_worker_id]
                            under_worker_obj = next((w for w in self.workers_data if w['id'] == under_worker_id), None)
                            under_worker_limit_this_month = self.max_consecutive_weekends
                            if under_worker_obj and under_worker_obj.get('work_percentage', 100) < 100:
                                under_worker_limit_this_month = max(1, int(self.max_consecutive_weekends * under_worker_obj.get('work_percentage',100) / 100))

                            if under_worker_new_count >= under_worker_limit_this_month:
                                underloaded_workers = [(w, c, s) for w, c, s in underloaded_workers if w != under_worker_id]
                            # --- End re-evaluation ---
                            
                            swap_done_for_this_shift = True
                            break # Found a swap for this special_day_to_reassign, move to next overloaded worker or next date
                    
                    if swap_done_for_this_shift:
                        # If a swap was made for this overloaded worker's shift,
                        # it's often good to re-evaluate the most overloaded worker.
                        # For simplicity here, we break and let the outer loop pick the next most overloaded.
                        break 
            
        logging.info(f"Special day (weekend/holiday/eve) distribution improvement: made {changes_made} changes")
        if changes_made > 0:
            self._synchronize_tracking_data() 
            self._save_current_as_best() 
        return changes_made > 0
    
    def _fix_incompatibility_violations(self):
        """
        Check the entire schedule for incompatibility violations and fix them
        by reassigning incompatible workers to different days
        """
        logging.info("Checking and fixing incompatibility violations")
    
        violations_fixed = 0
        violations_found = 0
    
        # Check each date for incompatible worker assignments
        for date_val in sorted(self.schedule.keys()): # Renamed date
            workers_today = [w for w in self.schedule[date_val] if w is not None]
        
            # Check each pair of workers
            for i, worker1_id in enumerate(workers_today):
                for worker2_id in workers_today[i+1:]:
                    # Check if these workers are incompatible
                    if self._are_workers_incompatible(worker1_id, worker2_id):
                        violations_found += 1
                        logging.warning(f"Found incompatibility violation: {worker1_id} and {worker2_id} on {date_val}")
                    
                        # Try to fix the violation by moving one of the workers
                        # Let's try to move the second worker first
                        if self._try_reassign_worker(worker2_id, date_val):
                            violations_fixed += 1
                            logging.info(f"Fixed by reassigning {worker2_id} from {date_val}")
                        # If that didn't work, try moving the first worker
                        elif self._try_reassign_worker(worker1_id, date_val):
                            violations_fixed += 1
                            logging.info(f"Fixed by reassigning {worker1_id} from {date_val}")
    
        logging.info(f"Incompatibility check: found {violations_found} violations, fixed {violations_fixed}")
        return violations_fixed > 0
        
    def _try_reassign_worker(self, worker_id, date):
        """
        Try to find a new date to assign this worker to fix an incompatibility
        """
        # --- ADD MANDATORY CHECK ---\
        if (worker_id, date) in self._locked_mandatory:
            logging.warning(f"Cannot reassign worker {worker_id} from locked mandatory shift on {date.strftime('%Y-%m-%d')} to fix incompatibility.")
            return False
        if self._is_mandatory(worker_id, date): # Existing check
            logging.warning(f"Cannot reassign worker {worker_id} from config-mandatory shift on {date.strftime('%Y-%m-%d')} to fix incompatibility.")
            return False
        # --- END MANDATORY CHECK ---\
        # Find the position this worker is assigned to
        try:
           post_val = self.schedule[date].index(worker_id) # Renamed post
        except ValueError:
            return False
    
        # First, try to find a date with an empty slot for the same post
        current_date_iter = self.start_date # Renamed current_date
        while current_date_iter <= self.end_date:
            # Skip the current date
            if current_date_iter == date:
                current_date_iter += timedelta(days=1)
                continue
            
            # Check if this date has an empty slot at the same post
            if (current_date_iter in self.schedule and \
                len(self.schedule[current_date_iter]) > post_val and \
                self.schedule[current_date_iter][post_val] is None):
            
                # Check if worker can be assigned to this date
                if self._can_assign_worker(worker_id, current_date_iter, post_val):
                    # Remove from original date
                    self.schedule[date][post_val] = None
                    self.worker_assignments[worker_id].remove(date)
                
                    # Assign to new date
                    self.schedule[current_date_iter][post_val] = worker_id
                    self.worker_assignments[worker_id].add(current_date_iter)
                
                    # Update tracking data
                    self._update_worker_stats(worker_id, date, removing=True)
                    self.scheduler._update_tracking_data(worker_id, current_date_iter, post_val) # Corrected: was under_worker_id, weekend_date
                
                    return True
                
            current_date_iter += timedelta(days=1)
    
        # If we couldn't find a new assignment, just remove this worker
        self.schedule[date][post_val] = None
        self.worker_assignments[worker_id].remove(date)
        self._update_worker_stats(worker_id, date, removing=True)
    
        return True

    def validate_mandatory_shifts(self):
        """Validate that all mandatory shifts have been assigned"""
        missing_mandatory = []
    
        for worker_val in self.workers_data: # Renamed worker
            worker_id_val = worker_val['id'] # Renamed worker_id
            mandatory_days_str = worker_val.get('mandatory_days', '') # Renamed mandatory_days
        
            if not mandatory_days_str:
                continue
            
            mandatory_dates_list = self.date_utils.parse_dates(mandatory_days_str) # Renamed mandatory_dates
            for date_val in mandatory_dates_list: # Renamed date
                if date_val < self.start_date or date_val > self.end_date:
                    continue  # Skip dates outside scheduling period
                
                # Check if worker is assigned on this date
                assigned = False
                if date_val in self.schedule:
                    if worker_id_val in self.schedule[date_val]:
                        assigned = True
                    
                if not assigned:
                    missing_mandatory.append((worker_id_val, date_val))
    
        return missing_mandatory

    def _apply_targeted_improvements(self, attempt_number):
        """
        Apply targeted improvements to the schedule. Runs multiple improvement steps.
        Returns True if ANY improvement step made a change, False otherwise.
        """
        random.seed(1000 + attempt_number)
        any_change_made = False

        logging.info(f"--- Starting Improvement Attempt {attempt_number} ---")

        # 1. Try to fill empty shifts (using direct fill and swaps)
        if self._try_fill_empty_shifts():
            logging.info(f"Attempt {attempt_number}: Filled some empty shifts.")
            any_change_made = True
            # Re-verify integrity after potentially complex swaps
            self._verify_assignment_consistency()

        # 3. Try to improve weekend distribution
        if self._improve_weekend_distribution():
            logging.info(f"Attempt {attempt_number}: Improved weekend distribution.")
            any_change_made = True
            self._verify_assignment_consistency()


        # 4. Try to balance workload distribution
        if self._balance_workloads():
            logging.info(f"Attempt {attempt_number}: Balanced workloads.")
            any_change_made = True
            self._verify_assignment_consistency()

        # 5. Final Incompatibility Check (Important after swaps/reassignments)
        # It might be better to run this *last* to clean up any issues created by other steps.
        if self._verify_no_incompatibilities(): # Assuming this tries to fix them
             logging.info(f"Attempt {attempt_number}: Fixed incompatibility violations.")
             any_change_made = True
             # No need to verify consistency again, as this function should handle it


        logging.info(f"--- Finished Improvement Attempt {attempt_number}. Changes made: {any_change_made} ---")
        return any_change_made # Return True if any step made a change

    def _synchronize_tracking_data(self):
        # Placeholder for your method in ScheduleBuilder if it exists, or call scheduler\'s
        if hasattr(self.scheduler, '_synchronize_tracking_data'):
            self.scheduler._synchronize_tracking_data()
        else:
            logging.warning("Scheduler\'_synchronize_tracking_data not found by builder.")
            # Fallback or simplified sync if necessary:
            new_worker_assignments = {w['id']: set() for w in self.workers_data}
            new_worker_posts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data}
            for date_val, shifts_on_date in self.schedule.items(): # Renamed date
                for post_idx, worker_id_in_post in enumerate(shifts_on_date):
                    if worker_id_in_post is not None:
                        new_worker_assignments.setdefault(worker_id_in_post, set()).add(date_val)
                        new_worker_posts.setdefault(worker_id_in_post, {p: 0 for p in range(self.num_shifts)})[post_idx] += 1
            self.worker_assignments = new_worker_assignments # Update builder\'s reference
            self.scheduler.worker_assignments = new_worker_assignments # Update scheduler\'s reference
            self.worker_posts = new_worker_posts
            self.scheduler.worker_posts = new_worker_posts
            self.scheduler.worker_shift_counts = {wid: len(dates_val) for wid, dates_val in new_worker_assignments.items()} # Renamed worker_shift_counts, dates
            # self.scheduler.worker_shift_counts = self.worker_shift_counts # This line is redundant
            # Add other tracking data sync if needed (weekends, etc.)
            
    def _is_date_in_variable_shift_period(self, date_to_check):
        """
        Checks if a given date falls into any defined variable shift period.
        """
        # This leverages the existing logic in the scheduler to determine actual shifts for a date.
        # If the number of shifts for the date is different from the default self.num_shifts,
        # then it's considered within a variable shift period.
        
        # Ensure scheduler reference and its attributes are available
        if not hasattr(self, 'scheduler') or not hasattr(self.scheduler, '_get_shifts_for_date') or not hasattr(self.scheduler, 'num_shifts'):
            logging.warning("_is_date_in_variable_shift_period: Scheduler or required attributes not available.")
            return True # Fail safe: assume it's variable if we can't check

        actual_shifts_for_date = self.scheduler._get_shifts_for_date(date_to_check)
        
        # If variable_shifts is empty, no date is in a variable period by this definition.
        if not self.scheduler.variable_shifts:
             return False

        is_variable = actual_shifts_for_date != self.scheduler.num_shifts
        if is_variable:
            logging.debug(f"Date {date_to_check.strftime('%Y-%m-%d')} is in a variable shift period (actual: {actual_shifts_for_date}, default: {self.scheduler.num_shifts}).")
        else:
            logging.debug(f"Date {date_to_check.strftime('%Y-%m-%d')} is NOT in a variable shift period (standard shifts: {self.scheduler.num_shifts}).")
        return is_variable

    def _adjust_last_post_distribution(self, balance_tolerance=1.0, max_iterations=10): # balance_tolerance of 1 means +/-1
        """
        Adjusts the distribution of last-post slots among workers for days NOT in variable_shifts periods.
        The goal is to have the total count of last posts per worker be within +/- balance_tolerance of each other.
        Swaps are only performed intra-day between workers already assigned on that day.

        Args:
            balance_tolerance (float): Allowed deviation from the average number of last posts.
                                     A tolerance of 1.0 aims for a +/-1 overall balance.
            max_iterations (int): Maximum number of full passes to attempt balancing.

        Returns:
            bool: True if any swap was made across all iterations, False otherwise.
        """
        overall_swaps_made_across_iterations = False
        logging.info(f"Starting iterative last post distribution adjustment (max_iterations={max_iterations}, tolerance={balance_tolerance}).")
        logging.info("This will only apply to days NOT within a variable shift period.")

        for iteration in range(max_iterations):
            logging.info(f"--- Last Post Adjustment Iteration: {iteration + 1}/{max_iterations} ---")
            made_swap_in_this_iteration = False
            
            # Ensure all tracking data is perfectly up-to-date before counting
            self._synchronize_tracking_data()

            # 1. Count actual last-post assignments for each worker ON NON-VARIABLE SHIFT DAYS
            last_post_counts_total = {str(w['id']): 0 for w in self.workers_data} # Use string IDs
            total_last_slots_in_non_variable_periods = 0
            
            # Store (date, index_of_last_assigned_post, worker_in_that_post) for swappable days
            swappable_days_with_last_post_info = []

            for date_val, shifts_on_day in self.schedule.items():
                if not shifts_on_day or not any(s is not None for s in shifts_on_day):
                    continue

                # Crucial Check: Skip this day if it's in a variable shift period
                if self._is_date_in_variable_shift_period(date_val):
                    logging.debug(f"Skipping date {date_val.strftime('%Y-%m-%d')} for last post balancing as it's in a variable shift period.")
                    continue

                # Find the actual last *assigned* post index for the day
                actual_last_assigned_idx = -1
                for i in range(len(shifts_on_day) - 1, -1, -1):
                    if shifts_on_day[i] is not None:
                        actual_last_assigned_idx = i
                        break
                
                if actual_last_assigned_idx != -1:
                    total_last_slots_in_non_variable_periods += 1
                    worker_in_last_actual_post = str(shifts_on_day[actual_last_assigned_idx]) # Ensure string ID
                    
                    last_post_counts_total[worker_in_last_actual_post] = last_post_counts_total.get(worker_in_last_actual_post, 0) + 1
                    swappable_days_with_last_post_info.append((date_val, actual_last_assigned_idx, worker_in_last_actual_post))

            if total_last_slots_in_non_variable_periods == 0:
                logging.info(f"[AdjustLastPost Iter {iteration+1}] No last posts assigned in non-variable shift periods. Nothing to balance.")
                break 

            num_eligible_workers = len(self.workers_data)
            avg_last_posts_per_worker = total_last_slots_in_non_variable_periods / num_eligible_workers if num_eligible_workers > 0 else 0
            
            logging.debug(f"[AdjustLastPost Iter {iteration+1}] Total Last Post Counts (Non-Variable Periods): {last_post_counts_total}")
            logging.debug(f"  Total Last Slots (Non-Variable): {total_last_slots_in_non_variable_periods}, Avg Last Posts/Worker: {avg_last_posts_per_worker:.2f}")

            # Sort workers by how many last posts they have (most to least)
            sorted_workers_by_last_posts = sorted(
                last_post_counts_total.items(),
                key=lambda item: item[1],
                reverse=True
            )

            # Shuffle the days to attempt swaps on to avoid bias
            random.shuffle(swappable_days_with_last_post_info)

            for date_to_adjust, last_post_idx_on_day, worker_currently_in_last_post_id in swappable_days_with_last_post_info:
                worker_A_id = str(worker_currently_in_last_post_id) # The one currently having the last post
                worker_A_last_post_count = last_post_counts_total.get(worker_A_id, 0)

                # Check if worker_A is "overloaded" with last posts
                if worker_A_last_post_count > avg_last_posts_per_worker + balance_tolerance:
                    logging.debug(f"  Attempting to rebalance: Worker {worker_A_id} (LPs: {worker_A_last_post_count}) has > avg ({avg_last_posts_per_worker:.2f}) + tolerance on {date_to_adjust.strftime('%Y-%m-%d')}")

                    # Find another worker (worker_B) on the SAME DAY in an EARLIER post
                    # who is "underloaded" or less loaded with last posts than worker_A
                    
                    shifts_on_this_specific_day = self.schedule[date_to_adjust]
                    potential_swap_partners_on_day = [] # List of (worker_B_id, worker_B_original_post_idx, worker_B_last_post_count)

                    for earlier_post_idx in range(last_post_idx_on_day): # Iterate posts *before* the current last_post_idx_on_day
                        worker_B_id_str = str(shifts_on_this_specific_day[earlier_post_idx])
                        
                        if worker_B_id_str != "None" and worker_B_id_str != worker_A_id :
                            worker_B_last_post_count = last_post_counts_total.get(worker_B_id_str, 0)
                            
                            # Worker B is a candidate if they have fewer last posts than A,
                            # AND ideally are "underloaded" or just less loaded than A.
                            # The primary condition is that swapping improves balance for A.
                            if worker_B_last_post_count < worker_A_last_post_count:
                                potential_swap_partners_on_day.append((worker_B_id_str, earlier_post_idx, worker_B_last_post_count))
                    
                    if not potential_swap_partners_on_day:
                        logging.debug(f"    No suitable intra-day swap partners found for {worker_A_id} on {date_to_adjust.strftime('%Y-%m-%d')}.")
                        continue

                    # Sort candidates: prioritize those with the fewest last posts
                    potential_swap_partners_on_day.sort(key=lambda x: x[2]) 

                    for worker_B_id, worker_B_original_post_idx, _ in potential_swap_partners_on_day:
                        # Simulate the intra-day swap:
                        # Worker A (overloaded with LPs) moves to worker_B_original_post_idx
                        # Worker B (less loaded with LPs) moves to last_post_idx_on_day
                        
                        # Check for new incompatibilities created by this intra-day swap
                        #   - A in B's old slot vs others on day (excluding B's old slot)
                        #   - B in A's old slot vs others on day (excluding A's old slot)
                        #   - A vs B (should not be incompatible if they are already working on the same day,
                        #            but good to have a general check if _are_workers_incompatible is robust)

                        temp_schedule_for_day = list(shifts_on_this_specific_day)
                        temp_schedule_for_day[last_post_idx_on_day] = worker_B_id
                        temp_schedule_for_day[worker_B_original_post_idx] = worker_A_id
                        
                        valid_swap_incompatibility_check = True
                        # Check A in B's old slot
                        others_at_B_slot = [str(w) for i, w in enumerate(temp_schedule_for_day) if i != worker_B_original_post_idx and w is not None]
                        if not self._check_incompatibility_with_list(worker_A_id, others_at_B_slot): # from schedule_builder
                            valid_swap_incompatibility_check = False
                        
                        if valid_swap_incompatibility_check:
                            # Check B in A's old slot
                            others_at_A_slot = [str(w) for i, w in enumerate(temp_schedule_for_day) if i != last_post_idx_on_day and w is not None]
                            if not self._check_incompatibility_with_list(worker_B_id, others_at_A_slot): # from schedule_builder
                                valid_swap_incompatibility_check = False
                        
                        # Explicitly check if A and B are incompatible (should be rare if they work same day, but good check)
                        # This uses schedule_builder's _are_workers_incompatible
                        if valid_swap_incompatibility_check and self._are_workers_incompatible(worker_A_id, worker_B_id):
                             valid_swap_incompatibility_check = False
                        
                        if valid_swap_incompatibility_check:
                            # Perform the actual swap in self.schedule
                            logging.info(f"  [AdjustLastPost Iter {iteration+1}] Swapping on {date_to_adjust.strftime('%Y-%m-%d')}: "
                                         f"Worker {worker_A_id} (P{last_post_idx_on_day} -> P{worker_B_original_post_idx}) with "
                                         f"Worker {worker_B_id} (P{worker_B_original_post_idx} -> P{last_post_idx_on_day})")
                            
                            self.schedule[date_to_adjust][last_post_idx_on_day] = worker_B_id
                            self.schedule[date_to_adjust][worker_B_original_post_idx] = worker_A_id
                            
                            # Update local last_post_counts for this iteration's subsequent checks
                            last_post_counts_total[worker_A_id] -= 1
                            last_post_counts_total[worker_B_id] = last_post_counts_total.get(worker_B_id, 0) + 1
                            
                            # Worker assignments (dates) don't change, only their posts on this day.
                            # Full _update_tracking_data for posts will be handled by _synchronize_tracking_data
                            # at the start of the next iteration or after all adjustments.
                            
                            made_swap_in_this_iteration = True
                            overall_swaps_made_across_iterations = True
                            
                            # Break from iterating swap partners for this day, as a swap was made.
                            # Then, the outer loop will continue to the next day in swappable_days_with_last_post_info.
                            break 
                
                # If a swap was made, the average and counts changed, so it might be beneficial to
                # re-evaluate from the start of the worker list for the current iteration.
                # For simplicity in this iterative version, we continue with the current sorted list of days,
                # and the next full iteration (outer loop) will re-calculate averages and counts.
            
            if not made_swap_in_this_iteration:
                logging.info(f"[AdjustLastPost Iter {iteration+1}/{max_iterations}] No beneficial last post swaps found in this full pass over days.")
                break # Break the outer loop (iterations) if no swaps were made in a full pass

        if overall_swaps_made_across_iterations:
            self._synchronize_tracking_data() # Ensure worker_posts and other scheduler tracking is up-to-date
            self._save_current_as_best() 
            logging.info(f"Finished last post adjustments. Total iterations run: {iteration + 1}. Overall swaps made: {overall_swaps_made_across_iterations}.")
        else:
            logging.info(f"No last post adjustments made after {iteration + 1} iteration(s).")
            
        return overall_swaps_made_across_iterations

    # 7. Backup and Restore Methods

    def _backup_best_schedule(self):
        """Save a backup of the current best schedule by delegating to scheduler"""
        return self.scheduler._backup_best_schedule()

    def _save_current_as_best(self, initial=False):
        """
        Save the current schedule as the best one found so far.
    
        Args:
            initial: Whether this is the initial best schedule (default: False)
        """
        try:
            best_score = self.calculate_score(self.schedule, self.worker_assignments)
        
            # Log what we're doing
            if initial:
                logging.info(f"Initializing best schedule with score {best_score}")
            else:
                current_best = self.best_schedule.get('score', 0)
                if best_score > current_best:
                    logging.info(f"Saving new best schedule: score {best_score} (previous: {current_best})")
                else:
                    logging.debug(f"Current score {best_score} not better than best {current_best}")
                    return False

            # Create a deep copy of the current state
            self.best_schedule = {
                'schedule': copy.deepcopy(self.schedule),
                'worker_assignments': copy.deepcopy(self.worker_assignments),
                'score': best_score,
                'worker_shift_counts': copy.deepcopy(self.scheduler.worker_shift_counts),
                'worker_weekend_counts': copy.deepcopy(self.scheduler.worker_weekend_counts),  # FIXED: Use worker_weekend_counts instead of worker_weekend_shifts
                'worker_posts': copy.deepcopy(self.scheduler.worker_posts),
                'last_assignment_date': copy.deepcopy(self.scheduler.last_assignment_date),
                'consecutive_shifts': copy.deepcopy(self.scheduler.consecutive_shifts)
            }
        
            return True
        except Exception as e:
            logging.error(f"Error saving best schedule: {str(e)}", exc_info=True)
            return False

    def _restore_best_schedule(self):
        """Restore backup by delegating to scheduler"""
        return self.scheduler._restore_best_schedule()

    def _ensure_best_schedule_saved(self):
        """
        Ensure that a best schedule is always saved during optimization
        """
        try:
            if not hasattr(self, 'best_schedule_data') or self.best_schedule_data is None:
                logging.warning("No best schedule saved, creating one from current state")
                current_score = self.scheduler.calculate_score()
                if current_score > float('-inf'):
                    self._save_current_as_best()
                    logging.info(f"Emergency save: Created best schedule with score {current_score}")
                else:
                    logging.error("Cannot save current state - invalid score")
                    return False
        
            # Verify the saved data is valid
            if 'schedule' not in self.best_schedule_data or not self.best_schedule_data['schedule']:
                logging.error("Saved best schedule data is invalid - no schedule found")
                return False
            
            return True
        
        except Exception as e:
            logging.error(f"Error ensuring best schedule saved: {str(e)}", exc_info=True)
            return False
        
    def get_best_schedule(self):
        """
        Get the best schedule found during optimization with enhanced safety checks
        """
        try:
            # Check if we have a saved best schedule
            if hasattr(self, 'best_schedule_data') and self.best_schedule_data is not None:
                if 'schedule' in self.best_schedule_data and self.best_schedule_data['schedule']:
                    logging.info(f"Returning saved best schedule with score: {self.best_schedule_data.get('score', 'unknown')}")
                    return self.best_schedule_data
                else:
                    logging.warning("best_schedule_data exists but contains no valid schedule data")
            else:
                logging.warning("No best_schedule_data found")
        
            # Fallback: Create best schedule from current state if it has assignments
            current_schedule = getattr(self, 'schedule', {})
            if current_schedule and any(any(worker is not None for worker in shifts) 
                                      for shifts in current_schedule.values()):
                logging.info("Creating best schedule data from current state")
            
                # Ensure we have all required tracking data
                if not hasattr(self, 'worker_assignments') or not self.worker_assignments:
                    self._synchronize_tracking_data()
            
            # Create the best schedule data structure
                best_data = {
                    'schedule': current_schedule,
                    'worker_assignments': getattr(self, 'worker_assignments', {}),
                    'worker_shift_counts': getattr(self, 'worker_shift_counts', {}),
                    'worker_weekend_counts': getattr(self, 'worker_weekend_counts', {}),
                    'worker_posts': getattr(self, 'worker_posts', {}),
                    'last_assignment_date': getattr(self, 'last_assignment_date', {}),
                    'consecutive_shifts': getattr(self, 'consecutive_shifts', {}),
                    'score': self.scheduler.calculate_score()
                }
            
                # Save this as our best schedule
                self.best_schedule_data = best_data
                logging.info(f"Created and saved best schedule data with score: {best_data['score']}")
                return best_data
        
            # If we reach here, we have no valid schedule data
            logging.error("No valid schedule data found in current state or saved best")
            return None
        
        except Exception as e:
            logging.error(f"Error in get_best_schedule: {str(e)}", exc_info=True)
            return None

    def calculate_score(self, schedule_to_score=None, assignments_to_score=None):
        # Placeholder - use scheduler\'s score calculation for consistency
        return self.scheduler.calculate_score(schedule_to_score or self.schedule, assignments_to_score or self.worker_assignments)

