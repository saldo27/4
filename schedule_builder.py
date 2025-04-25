# Imports
from datetime import datetime, timedelta
import copy
import logging
import random
from typing import TYPE_CHECKING
from exceptions import SchedulerError
if TYPE_CHECKING:
    from scheduler import Scheduler

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
        self.schedule = scheduler.schedule  # Use the same reference
        self.config = scheduler.config
        self.worker_assignments = scheduler.worker_assignments  # Use the same reference
        self.num_shifts = scheduler.num_shifts
        self.holidays = scheduler.holidays
        self.constraint_checker = scheduler.constraint_checker
        self.best_schedule_data = None # Initialize the attribute to store the best state found

        # Add references to the configurable parameters
        self.gap_between_shifts = scheduler.gap_between_shifts
        self.max_consecutive_weekends = scheduler.max_consecutive_weekends

        # Add these lines:
        self.start_date = scheduler.start_date
        self.end_date = scheduler.end_date
        self.date_utils = scheduler.date_utils
        self.data_manager = scheduler.data_manager
        self.worker_posts = scheduler.worker_posts
        self.worker_weekdays = scheduler.worker_weekdays
        self.worker_weekends = scheduler.worker_weekends
        self.constraint_skips = scheduler.constraint_skips
        self.max_shifts_per_worker = scheduler.max_shifts_per_worker

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
        """Checks if a given date is mandatory for the worker."""
        # Find the worker data from the scheduler's list
        worker = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        if not worker:
            logging.warning(f"_is_mandatory check failed: Worker {worker_id} not found.")
            return False # Worker not found, cannot be mandatory

        mandatory_days_str = worker.get('mandatory_days', '')
        if not mandatory_days_str:
            return False # No mandatory days defined

        # Use the utility to parse dates, handle potential errors
        try:
            # Access date_utils via the scheduler reference
            mandatory_dates = self.scheduler.date_utils.parse_dates(mandatory_days_str)
            is_mand = date in mandatory_dates
            # Add debug log
            # logging.debug(f"Checking mandatory for {worker_id} on {date.strftime('%Y-%m-%d')}: Result={is_mand} (List: {mandatory_dates})")
            return is_mand
        except ValueError:
             # Log error if parsing fails, treat as not mandatory for safety
             logging.error(f"Could not parse mandatory_days for worker {worker_id}: '{mandatory_days_str}'")
             return False
        except AttributeError:
             # Handle case where date_utils might not be initialized yet (shouldn't happen here, but safety)
             logging.error("date_utils not available in scheduler during _is_mandatory check.")
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
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker:
            return True
    
        # Debug log
        logging.debug(f"Checking availability for worker {worker_id} on {date.strftime('%d-%m-%Y')}")

        # Check work periods - if work_periods is empty, worker is available for all dates
        work_periods = worker.get('work_periods', '').strip()
        if work_periods:
            date_str = date.strftime('%d-%m-%Y')
            if 'work_dates' in worker:
                # If we've precomputed work dates, use those
                if date_str not in worker.get('work_dates', set()):
                    logging.debug(f"Worker {worker_id} not available - date {date_str} not in work_dates")
                    return True
            else:
                # Otherwise, parse the work periods
                try:
                    date_ranges = self.date_utils.parse_date_ranges(work_periods)
                    within_work_period = False
                
                    for start_date, end_date in date_ranges:
                        if start_date <= date <= end_date:
                            within_work_period = True
                            break
                
                    if not within_work_period:
                        logging.debug(f"Worker {worker_id} not available - date outside work periods")
                        return True
                except Exception as e:
                    logging.error(f"Error parsing work periods for worker {worker_id}: {str(e)}")
    
        # Check days off
        days_off = worker.get('days_off', '')
        if days_off:
            day_ranges = self.date_utils.parse_date_ranges(days_off)
            for start_date, end_date in day_ranges:
                if start_date <= date <= end_date:
                    logging.debug(f"Worker {worker_id} not available - date in days off")
                    return True

        logging.debug(f"Worker {worker_id} is available on {date.strftime('%d-%m-%Y')}")
        return False
    
    def _check_incompatibility_with_list(self, worker_id_to_check, assigned_workers_list):
        """Checks if worker_id_to_check is incompatible with anyone in the list."""
        worker_to_check_data = next((w for w in self.workers_data if w['id'] == worker_id_to_check), None)
        if not worker_to_check_data: return True # Should not happen, but fail safe

        incompatible_with = worker_to_check_data.get('incompatible_with', [])

        # *** ADD TYPE LOGGING HERE ***
        logging.debug(f"  CHECKING_INTERNAL: Worker={worker_id_to_check} (Type: {type(worker_id_to_check)}), AgainstList={assigned_workers_list}, IncompListForCheckWorker={incompatible_with}")

        for assigned_id in assigned_workers_list:
             if assigned_id is None: # Removed redundant check assigned_id == worker_id_to_check due to type mismatch possibility
                  continue

             # *** ADD TYPE LOGGING FOR LOOP VARIABLES ***
             logging.debug(f"    Comparing: Candidate {worker_id_to_check} (Type: {type(worker_id_to_check)}) vs Assigned {assigned_id} (Type: {type(assigned_id)})")

             # Check 1: Is the assigned worker in the candidate's incompatible list?
             # *** ADD TYPE LOGGING FOR CHECK 1 ***
             logging.debug(f"      Check 1: Is {assigned_id} (Type: {type(assigned_id)}) in {incompatible_with}?")
             if assigned_id in incompatible_with:
                  logging.debug(f"      -> YES. Incompatibility found.")
                  return False # Found incompatibility

             # Check 2: Is the candidate in the assigned worker's incompatible list?
             assigned_worker_data = next((w for w in self.workers_data if w['id'] == assigned_id), None)
             if assigned_worker_data:
                 assigned_incompatible_list = assigned_worker_data.get('incompatible_with', [])
                 # *** ADD TYPE LOGGING FOR CHECK 2 ***
                 logging.debug(f"      Check 2: Is {worker_id_to_check} (Type: {type(worker_id_to_check)}) in {assigned_incompatible_list}?")
                 if worker_id_to_check in assigned_incompatible_list:
                     logging.debug(f"      -> YES. Incompatibility found.")
                     return False # Found incompatibility
             else:
                 logging.warning(f"    Could not find data for assigned worker ID: {assigned_id}")


        logging.debug(f"  CHECKING_INTERNAL: Worker={worker_id_to_check} vs List={assigned_workers_list} -> OK (No incompatibility detected)")
        return True # No incompatibilities found

    def _check_incompatibility(self, worker_id, date):
        """Check if worker is incompatible with already assigned workers on a specific date"""
        try:
            if date not in self.schedule:
                return True # No one assigned yet, so compatible

            assigned_workers_list = self.schedule.get(date, [])
            return self._check_incompatibility_with_list(worker_id, assigned_workers_list)

        except Exception as e:
            logging.error(f"Error checking incompatibility for worker {worker_id} on {date}: {str(e)}")
            return False # Fail safe - assume incompatible on error
        
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
        # Skip if not a weekend
        if not self.date_utils.is_weekend_day(date) and date not in self.holidays:
            return False

        # Get worker data
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker:
            return True

        # Get weekend assignments for this worker
        weekend_dates = self.worker_weekends.get(worker_id, [])

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
        for i, start_date in enumerate(test_dates):
            end_date = start_date + three_weeks
            count = sum(1 for d in test_dates[i:] if d <= end_date)
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
    
        for date, shifts in self.schedule.items():
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
        for date in sorted(self.schedule.keys()):
            workers_today = [w for w in self.schedule[date] if w is not None]
        
            # Process all pairs to find incompatibilities
            for i in range(len(workers_today)):
                for j in range(i+1, len(workers_today)):
                    worker1_id = workers_today[i]
                    worker2_id = workers_today[j]
                
                    # Check if they are incompatible
                    if self._are_workers_incompatible(worker1_id, worker2_id):
                        violations_found += 1
                        logging.warning(f"Final verification found incompatibility violation: {worker1_id} and {worker2_id} on {date.strftime('%d-%m-%Y')}")
                    
                        # Find their positions
                        post1 = self.schedule[date].index(worker1_id)
                        post2 = self.schedule[date].index(worker2_id)
                    
                        # Remove one of the workers (choose the one with more shifts assigned)
                        w1_shifts = len(self.worker_assignments.get(worker1_id, set()))
                        w2_shifts = len(self.worker_assignments.get(worker2_id, set()))
                    
                        # Remove the worker with more shifts or the second worker if equal
                        if w1_shifts > w2_shifts:
                            self.schedule[date][post1] = None
                            self.worker_assignments[worker1_id].remove(date)
                            self.scheduler._update_tracking_data(worker1_id, date, post1, removing=True)
                            violations_fixed += 1
                            logging.info(f"Removed worker {worker1_id} from {date.strftime('%d-%m-%Y')} to fix incompatibility")
                        else:
                            self.schedule[date][post2] = None
                            self.worker_assignments[worker2_id].remove(date)
                            self.scheduler._update_tracking_data(worker2_id, date, post2, removing=True)
                            violations_fixed += 1
                            logging.info(f"Removed worker {worker2_id} from {date.strftime('%d-%m-%Y')} to fix incompatibility")
    
        logging.info(f"Final verification: found {violations_found} violations, fixed {violations_fixed}")
        return violations_fixed > 0

    # 4. Worker Assignment Methods

    def _can_assign_worker(self, worker_id, date, post):
        """
        Check if a worker can be assigned to a specific date and post

        Args:
            worker_id: ID of the worker to check
            date: The date to assign
            post: The post number to assign
    
        Returns:
            bool: True if the worker can be assigned, False otherwise
        """
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

        # Check minimum gap between shifts based on configurable parameter
        assignments = sorted(self.worker_assignments[worker_id])
        min_days_between = self.gap_between_shifts + 1  # +1 because we need days_between > gap
    
        for prev_date in assignments:
            days_between = abs((date - prev_date).days)
            if days_between < min_days_between:  # Use configurable gap
                return False

        # Special case: Friday-Monday check if gap is only 1 day
        if self.gap_between_shifts == 1:
            for prev_date in assignments:
                days_between = abs((date - prev_date).days)
                if days_between == 3:
                    if ((prev_date.weekday() == 4 and date.weekday() == 0) or 
                        (date.weekday() == 4 and prev_date.weekday() == 0)):
                        return False

        # Check weekend limits
        if self._would_exceed_weekend_limit(worker_id, date):
            return False

        # Check if this worker can swap these assignments
        work_percentage = worker.get('work_percentage', 100)

        # Part-time workers need more days between shifts
        if work_percentage < 100:
            part_time_gap = max(3, self.gap_between_shifts + 2)  # At least 3 days, or gap+2
            for prev_date in assignments:
                days_between = abs((date - prev_date).days)
                if days_between < part_time_gap:
                    return False

        # Check for consecutive week patterns
        for prev_date in assignments:
            days_between = abs((date - prev_date).days)
            # Avoid same day of week in consecutive weeks when possible
            if days_between in [7, 14] and date.weekday() == prev_date.weekday():
                return False

        # If we've made it this far, the worker can be assigned
        return True

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
        self._update_tracking_data(worker_id, date, post)
        return True
    
    def _can_swap_assignments(self, worker_id, date_from, post_from, date_to, post_to):
        """
        Checks if moving worker_id from (date_from, post_from) to (date_to, post_to) is valid.
        Uses deepcopy for safer simulation.
        """
        # --- Simulation Setup ---
        # Create deep copies of the schedule and assignments
        try:
            # Use scheduler's references for deepcopy
            simulated_schedule = copy.deepcopy(self.scheduler.schedule)
            simulated_assignments = copy.deepcopy(self.scheduler.worker_assignments)

            # --- Simulate the Swap ---
            # 1. Check if 'from' state is valid before simulating removal
            if date_from not in simulated_schedule or \
               len(simulated_schedule[date_from]) <= post_from or \
               simulated_schedule[date_from][post_from] != worker_id or \
               worker_id not in simulated_assignments or \
               date_from not in simulated_assignments[worker_id]:
                    logging.warning(f"_can_swap_assignments: Initial state invalid for removing {worker_id} from {date_from}|P{post_from}. Aborting check.")
                    return False # Cannot simulate if initial state is wrong

            # 2. Simulate removing worker from 'from' position
            simulated_schedule[date_from][post_from] = None
            simulated_assignments[worker_id].remove(date_from)
            # Clean up empty set for worker if needed
            if not simulated_assignments[worker_id]:
                 del simulated_assignments[worker_id]


            # 3. Simulate adding worker to 'to' position
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

            # --- Check Constraints on Simulated State ---
            # Check if the worker can be assigned to the target slot considering the simulated state
            can_assign_to_target = self._check_constraints_on_simulated(
                worker_id, date_to, post_to, simulated_schedule, simulated_assignments
            )

            # Also check if the source date is still valid *without* the worker
            # (e.g., maybe removing the worker caused an issue for others on date_from)
            source_date_still_valid = self._check_all_constraints_for_date_simulated(
                date_from, simulated_schedule, simulated_assignments
            )

            # Also check if the target date remains valid *with* the worker added
            target_date_still_valid = self._check_all_constraints_for_date_simulated(
                 date_to, simulated_schedule, simulated_assignments
            )


            is_valid_swap = can_assign_to_target and source_date_still_valid and target_date_still_valid

            # --- End Simulation ---
            # No rollback needed as we operated on copies.

            logging.debug(f"Swap Check: {worker_id} from {date_from}|P{post_from} to {date_to}|P{post_to}. Valid: {is_valid_swap} (Target OK: {can_assign_to_target}, Source OK: {source_date_still_valid}, TargetDate OK: {target_date_still_valid})")
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
            if self.scheduler.gap_between_shifts == 1 and work_percentage >= 100: # Typically only for full-time
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
            # Apply strictly during simulation checks (as relaxation isn't usually passed here)
            for prev_date in sorted_sim_assignments:
                 if prev_date == date: continue
                 days_between = abs((date - prev_date).days)
                 # Check for 7, 14, 21 etc. day differences AND same weekday
                 if days_between > 0 and days_between % 7 == 0 and date.weekday() == prev_date.weekday():
                      logging.debug(f"Sim Check Fail: 7/14 day pattern conflict for {worker_id} between {prev_date} and {date}")
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
              if self.scheduler.gap_between_shifts == 1 and work_percentage >= 100:
                   if days_between == 3:
                       if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                           (date.weekday() == 4 and prev_date.weekday() == 0)):
                           return False
              # if days_between in [7, 14, 21]: return False # Optional strict weekly pattern check

         return True

    def _would_exceed_weekend_limit_simulated(self, worker_id, date, simulated_assignments):
        """Check weekend limit using simulated assignments."""
        # Check if date is a weekend/holiday
        # Use scheduler's holidays
        is_target_weekend = self.scheduler.date_utils.is_weekend_day(date) or date in self.scheduler.holidays
        if not is_target_weekend:
            return False

        # Get simulated weekend assignments including the target date
        sim_weekend_assignments = {
            d for d in simulated_assignments.get(worker_id, [])
            if (self.scheduler.date_utils.is_weekend_day(d) or d in self.scheduler.holidays)
        }
        # Add the current date being checked if it's a weekend/holiday
        sim_weekend_assignments.add(date)
        sorted_sim_weekends = sorted(list(sim_weekend_assignments))


        # Apply window check logic (e.g., max N in 21 days)
        # Use scheduler's max weekend config
        max_weekend_count = self.scheduler.max_consecutive_weekends
        # Add part-time adjustment if needed
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
        if work_percentage < 100:
             max_weekend_count = max(1, int(self.scheduler.max_consecutive_weekends * work_percentage / 100))

        # Check different window sizes if needed, e.g., 3 weekends in a row?
        # Simple check: count consecutive weekend *days*
        max_consecutive_days = 3 # Example: Allow Sat/Sun/Mon-Holiday but not Fri/Sat/Sun
        consecutive_count = 0
        for i, d in enumerate(sorted_sim_weekends):
             if i > 0 and (d - sorted_sim_weekends[i-1]).days == 1:
                  consecutive_count += 1
             else:
                  consecutive_count = 1
             if consecutive_count > max_consecutive_days:
                  # This check might be too strict, depending on rules
                  # return True # Exceeds consecutive weekend day limit
                  pass # Disable strict consecutive day check for now


        # Check: Max weekends in any 3-week (21 day) rolling window
        window_days = 21
        for i in range(len(sorted_sim_weekends)):
            window_start_date = sorted_sim_weekends[i]
            # Find weekends within the window starting from window_start_date
            count_in_window = 0
            for d in sorted_sim_weekends[i:]: # Only need to check dates from i onwards
                 if (d - window_start_date).days < window_days:
                      count_in_window += 1
                 else:
                      break # Dates are sorted, no need to check further
            if count_in_window > max_weekend_count:
                logging.debug(f"Sim Check Fail: Weekend limit exceeded for {worker_id}. Found {count_in_window} in window starting {window_start_date} (Limit: {max_weekend_count})")
                return True # Exceeds limit

        return False # Limit not exceeded
    
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
        
            # --- Hard Constraints (never relaxed) ---
        
            # Basic availability check
            if self._is_worker_unavailable(worker_id, date) or worker_id in self.schedule.get(date, []):
                return float('-inf')
            
            # Check incompatibility against workers already assigned on this date (excluding the current post being considered)
            already_assigned_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) if w is not None and idx != post]
            if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
                 logging.debug(f"Score check fail: Worker {worker_id} incompatible on {date}")
                 return float('-inf')
            
            # --- Check for mandatory shifts ---
            worker_data = worker
            mandatory_days = worker_data.get('mandatory_days', [])
            mandatory_dates = self._parse_dates(mandatory_days)
        
            # If this is a mandatory date for this worker, give it maximum priority
            if date in mandatory_dates:
                return float('inf')  # Highest possible score to ensure mandatory shifts are assigned
        
            # --- Target Shifts Check (excluding mandatory shifts) ---
            current_shifts = len(self.worker_assignments[worker_id])
            target_shifts = worker.get('target_shifts', 0)
        
            # Count mandatory shifts that are already assigned
            mandatory_shifts_assigned = sum(
                1 for d in self.worker_assignments[worker_id] if d in mandatory_dates
            )
        
            # Count mandatory shifts still to be assigned
            mandatory_shifts_remaining = sum(
                1 for d in mandatory_dates 
                if d >= date and d not in self.worker_assignments[worker_id]
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
                if relaxation_level < 2:
                    return float('-inf')  # Strict limit at relaxation levels 0-1
                else:
                    score -= 10000  # Severe penalty but still possible at highest relaxation
            else:
                # Higher priority for workers further from their non-mandatory target
                score += shift_difference * 1000

            # --- MONTHLY TARGET CHECK ---
            month_key = f"{date.year}-{date.month:02d}"
            monthly_targets = worker.get('monthly_targets', {})
            target_this_month = monthly_targets.get(month_key, 0)

            # Calculate current shifts assigned in this month
            shifts_this_month = 0
            if worker_id in self.scheduler.worker_assignments: # Use scheduler reference
                 for assigned_date in self.scheduler.worker_assignments[worker_id]:
                      if assigned_date.year == date.year and assigned_date.month == date.month:
                           shifts_this_month += 1

            # Define the acceptable range (+/- 1 from target)
            min_monthly = max(0, target_this_month - 1)
            max_monthly = target_this_month + 1

            monthly_diff = target_this_month - shifts_this_month

            # Penalize HARD if assignment goes over max_monthly + 1 (allow max+1 only)
            if shifts_this_month >= max_monthly + 1 and relaxation_level < 2:
                 logging.debug(f"Worker {worker_id} rejected for {date.strftime('%Y-%m-%d')}: Would exceed monthly max+1 ({shifts_this_month + 1} > {max_monthly})")
                 return float('-inf')
            elif shifts_this_month >= max_monthly and relaxation_level < 1:
                 logging.debug(f"Worker {worker_id} rejected for {date.strftime('%Y-%m-%d')}: Would exceed monthly max ({shifts_this_month + 1} > {max_monthly}) at relax level {relaxation_level}")
                 return float('-inf')


            # Strong bonus if worker is below min_monthly for this month
            if shifts_this_month < min_monthly:
                 # Bonus increases the further below min they are
                 score += (min_monthly - shifts_this_month) * 2500 # High weight for monthly need
                 logging.debug(f"Worker {worker_id} gets monthly bonus: below min ({shifts_this_month} < {min_monthly})")
            # Moderate bonus if worker is within the target range but below target
            elif shifts_this_month < target_this_month:
                 score += 500 # Bonus for needing shifts this month
                 logging.debug(f"Worker {worker_id} gets monthly bonus: below target ({shifts_this_month} < {target_this_month})")
            # Penalty if worker is already at or above max_monthly
            elif shifts_this_month >= max_monthly:
                 score -= (shifts_this_month - max_monthly + 1) * 1500 # Penalty increases the further above max they go
                 logging.debug(f"Worker {worker_id} gets monthly penalty: at/above max ({shifts_this_month} >= {max_monthly})")

            # --- Gap Constraints ---
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
        
                    # Special rule for full-time workers with gap=1: No Friday + Monday (3-day gap)
                    if work_percentage >= 100 and relaxation_level == 0 and self.gap_between_shifts == 1:
                        if ((prev_date.weekday() == 4 and date.weekday() == 0) or 
                            (date.weekday() == 4 and prev_date.weekday() == 0)):
                            if days_between == 3:
                                return float('-inf')
                
                    # Prevent same day of week in consecutive weeks (can be relaxed)
                    if relaxation_level < 2 and days_between in [7, 14, 21]:
                        return float('-inf')
        
            # --- Weekend Limits ---
            if relaxation_level < 2 and self._would_exceed_weekend_limit(worker_id, date):
                return float('-inf')
        
            # --- Weekday Balance Check ---
            weekday = date.weekday()
            weekday_counts = self.worker_weekdays[worker_id].copy()
            weekday_counts[weekday] += 1  # Simulate adding this assignment
        
            max_weekday = max(weekday_counts.values())
            min_weekday = min(weekday_counts.values())
        
            # If this assignment would create more than 1 day difference, reject it
            if (max_weekday - min_weekday) > 1 and relaxation_level < 1:
                return float('-inf')
        
            # --- Scoring Components (softer constraints) ---

            # 1. Overall Target Score (Reduced weight compared to monthly)
            if shift_difference > 0:
                 score += shift_difference * 500 # Reduced weight
            elif shift_difference <=0 and relaxation_level >= 2:
                 score -= 5000 # Keep penalty if over overall target at high relaxation
        
            # 2. Weekend Balance Score
            if date.weekday() >= 4:  # Friday, Saturday, Sunday
                weekend_assignments = sum(
                    1 for d in self.worker_assignments[worker_id]
                    if d.weekday() >= 4
                )
                # Lower score for workers with more weekend assignments
                score -= weekend_assignments * 300
        
            # 3. Post Rotation Score - focus especially on last post distribution
            last_post = self.num_shifts - 1
            if post == last_post:  # Special handling for the last post
                post_counts = self._get_post_counts(worker_id)
                total_assignments = sum(post_counts.values()) + 1  # +1 for this potential assignment
                target_last_post = total_assignments * (1 / self.num_shifts)
                current_last_post = post_counts.get(last_post, 0)
            
                # Encourage assignments when below target
                if current_last_post < target_last_post - 1:
                    score += 1000
                # Discourage assignments when above target
                elif current_last_post > target_last_post + 1:
                    score -= 1000
        
            # 4. Weekly Balance Score - avoid concentration in some weeks
            week_number = date.isocalendar()[1]
            week_counts = {}
            for d in self.worker_assignments[worker_id]:
                w = d.isocalendar()[1]
                week_counts[w] = week_counts.get(w, 0) + 1
        
            current_week_count = week_counts.get(week_number, 0)
            avg_week_count = len(assignments) / max(1, len(week_counts))
        
            if current_week_count < avg_week_count:
                score += 500  # Bonus for weeks with fewer assignments
        
            # 5. Schedule Progression Score - adjust priority as schedule fills up
            schedule_completion = sum(len(shifts) for shifts in self.schedule.values()) / (
                (self.end_date - self.start_date).days * self.num_shifts)
        
            # Higher weight for target difference as schedule progresses
            score += shift_difference * 500 * schedule_completion
        
            # Log the score calculation
            logging.debug(f"Score for worker {worker_id}: {score} "
                        f"(current: {current_shifts}, target: {target_shifts}, "
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
        if total_assignments >= self.num_shifts:
            expected_per_post = total_assignments / self.num_shifts
            current_count = post_counts.get(post, 0)
        
            # Give bonus if this post is underrepresented for this worker
            if current_count < expected_per_post:
                base_score += 10 * (expected_per_post - current_count)
    
        # Bonus for balancing workload
        work_percentage = worker.get('work_percentage', 100)
        current_assignments = len(self.worker_assignments[worker_id])
    
        # Calculate average assignments per worker, adjusted for work percentage
        total_assignments_all = sum(len(self.worker_assignments[w['id']]) for w in self.workers_data)
        total_work_percentage = sum(w.get('work_percentage', 100) for w in self.workers_data)
    
        # Expected assignments based on work percentage
        expected_assignments = (total_assignments_all / (total_work_percentage / 100)) * (work_percentage / 100)
    
        # Bonus for underloaded workers
        if current_assignments < expected_assignments:
            base_score += 5 * (expected_assignments - current_assignments)
    
        return base_score

    # 5. Schedule Generation Methods
            
    def _assign_mandatory_guards(self):
        """Assigns mandatory shifts based on both configuration and worker mandatory_days."""
        logging.info("Starting mandatory guard assignment...")
        assigned_count = 0

        # PART 1: Process mandatory shifts from configuration (existing logic)
        mandatory_shifts_config = self.scheduler.config.get('mandatory_shifts', {})
        worker_ids_set = set(w['id'] for w in self.workers_data) # Ensure workers_data is correct reference

        processed_mandatory = {} # Initialize before the if block
        logging.debug("Processing mandatory shifts from config (if any)...")
        if mandatory_shifts_config:
            for date_str, assignments in mandatory_shifts_config.items(): # CORRECTED variable name here
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    processed_assignments = {int(post): worker for post, worker in assignments.items()}
                    processed_mandatory[date_obj] = processed_assignments
                    logging.debug(f"  Added config mandatory assignments for {date_obj}")
                except ValueError as e:
                    logging.error(f"Invalid date format or post index in mandatory_shifts for '{date_str}': {e}")
                    continue # Skip invalid entries
        logging.debug("Finished processing mandatory shifts from config.")

        # Iterate through the schedule date range
        logging.debug("Starting date iteration for config mandatory shifts...")
        current_date = self.scheduler.start_date
        loop_count = 0 # Add loop counter for safety
        max_loops = (self.scheduler.end_date - self.scheduler.start_date).days + 2 # Safety break

        while current_date <= self.scheduler.end_date:
            loop_count += 1
            if loop_count > max_loops:
                 logging.error("!!!! SAFETY BREAK: Exited mandatory assignment Part 1 loop due to excessive iterations.")
                 break

            # --- Log current date being processed ---
            logging.debug(f"  Checking date: {current_date.strftime('%Y-%m-%d')}")
            # -----------------------------------------

            if current_date in processed_mandatory:
                # --- Start of indented block ---
                assignments_for_date = processed_mandatory[current_date]
                logging.debug(f"    Found config mandatory assignments for {current_date}: {assignments_for_date}")

                for post_idx, worker_id in assignments_for_date.items():
                    logging.debug(f"      Checking assignment: Post {post_idx} -> Worker {worker_id}")
                    # Basic validation
                    # Use self.scheduler.worker_ids if available, otherwise rebuild set
                    if worker_id not in self.scheduler.worker_ids:
                        logging.warning(f"      Mandatory shift worker '{worker_id}' not found. Skipping.")
                        continue
                    if post_idx < 0 or post_idx >= self.num_shifts:
                         logging.warning(f"      Mandatory shift post index {post_idx} out of range. Skipping.")
                         continue

                    # Initialize schedule for the date if needed
                    if current_date not in self.scheduler.schedule:
                        self.scheduler.schedule[current_date] = [None] * self.num_shifts
                        logging.debug(f"      Initialized schedule for {current_date}")

                    # Check if slot is already taken
                    if self.scheduler.schedule[current_date][post_idx] is not None:
                        logging.warning(f"      Mandatory shift slot {current_date} (Post {post_idx}) already filled by {self.scheduler.schedule[current_date][post_idx]}. Skipping.")
                        continue

                    # Check constraints and assign
                    logging.debug(f"      Checking constraints for Worker {worker_id} on {current_date} Post {post_idx}...")
                    # Ensure constraint checker method exists and is correct
                    try:
                        # Assuming check_constraints is the correct method name and location
                        can_assign = self.scheduler.constraint_checker._check_constraints(worker_id, current_date, post_idx=post_idx, skip_constraints=False)
                    except AttributeError:
                         logging.error("AttributeError: Could not find _check_constraints method on constraint_checker.")
                         can_assign = False # Fail safe
                    except Exception as e_check:
                         logging.error(f"Exception during constraint check for {worker_id} on {current_date}: {e_check}", exc_info=True)
                         can_assign = False # Fail safe


                    if can_assign:
                         logging.info(f"      Assigning config mandatory shift: {current_date} Post {post_idx} -> Worker {worker_id}")
                         self.scheduler.schedule[current_date][post_idx] = worker_id
                         # Ensure _update_tracking_data exists and has correct signature
                         try:
                            self.scheduler._update_tracking_data(worker_id, current_date, post_idx)
                         except AttributeError:
                             logging.error("AttributeError: Could not find _update_tracking_data method on scheduler.")
                         except Exception as e_update:
                             logging.error(f"Exception during tracking update for {worker_id} on {current_date}: {e_update}", exc_info=True)

                         assigned_count += 1
                    else:
                         logging.warning(f"      Could not assign config mandatory shift for {worker_id} on {current_date} (Post {post_idx}) due to constraints.")
                # --- End of indented block ---

            # This line must be OUTSIDE the 'if' block, but INSIDE the 'while' loop
            current_date += timedelta(days=1)
        logging.debug("Finished date iteration for config mandatory shifts.")


        # PART 2: NEW LOGIC - Process mandatory_days from individual worker data
        logging.info("Processing mandatory shifts from worker mandatory_days...")
        mandatory_assignments = []

        logging.debug("Collecting mandatory assignments from worker data...")
        for worker in self.workers_data: # Ensure correct reference
            worker_id = worker['id']
            mandatory_days = worker.get('mandatory_days', '')
            if not mandatory_days:
                continue
            try:
                # Ensure date_utils is correct reference
                mandatory_dates = self.date_utils.parse_dates(mandatory_days)
                for date in mandatory_dates:
                    if self.start_date <= date <= self.end_date: # Ensure correct references
                        mandatory_assignments.append((worker_id, date))
                        logging.debug(f"  Identified worker mandatory shift: Worker {worker_id} on {date.strftime('%Y-%m-%d')}")
            except Exception as e:
                logging.error(f"Error parsing mandatory days for worker {worker_id}: {str(e)}")
        logging.debug(f"Collected {len(mandatory_assignments)} mandatory assignments from worker data.")

        # Second pass: assign all mandatory shifts
        logging.debug("Assigning collected worker mandatory shifts...")
        for worker_id, date in mandatory_assignments:
            logging.debug(f"  Attempting assignment for Worker {worker_id} on {date.strftime('%Y-%m-%d')}")
            # Check if already assigned (handle potential KeyError if date not in schedule)
            if date in self.schedule and worker_id in self.schedule.get(date, []):
                logging.debug(f"    Worker {worker_id} already assigned on mandatory date {date}. Skipping.")
                continue

            # Initialize the date in the schedule if needed
            if date not in self.schedule:
                self.schedule[date] = [None] * self.num_shifts
                logging.debug(f"    Initialized schedule for {date}")

            # Try to assign the worker to any available post on the mandatory date
            success = False
            for post in range(self.num_shifts): # Ensure correct reference
                logging.debug(f"    Checking post {post}...")
                # Ensure list is long enough (should be handled by init)
                if post >= len(self.schedule[date]):
                    logging.warning(f"    Schedule list for {date} is unexpectedly short. Extending.")
                    self.schedule[date].extend([None] * (post - len(self.schedule[date]) + 1))

                if self.schedule[date][post] is None:
                    logging.debug(f"      Post {post} is empty. Checking incompatibility...")
                    # Check for incompatibility with already assigned workers
                    try:
                         # Ensure _check_incompatibility exists and is correct
                         is_compatible = self._check_incompatibility(worker_id, date)
                    except Exception as e_incomp:
                         logging.error(f"Exception during incompatibility check for {worker_id} on {date}: {e_incomp}", exc_info=True)
                         is_compatible = False # Fail safe

                    if not is_compatible:
                        logging.warning(f"      Cannot assign mandatory shift for worker {worker_id} on {date}: incompatibility detected at post {post}.")
                        continue # Try next post

                    # Assign the worker to this post
                    logging.info(f"      Assigning worker mandatory shift: Worker {worker_id} on {date} to post {post}")
                    self.schedule[date][post] = worker_id
                    # Ensure worker_assignments is correct reference and worker_id exists
                    self.worker_assignments.setdefault(worker_id, set()).add(date)
                    # Ensure _update_tracking_data exists and is correct
                    try:
                        self.scheduler._update_tracking_data(worker_id, date, post)
                    except AttributeError:
                         logging.error("AttributeError: Could not find _update_tracking_data method on scheduler.")
                    except Exception as e_update2:
                         logging.error(f"Exception during tracking update for {worker_id} on {date}: {e_update2}", exc_info=True)

                    success = True
                    assigned_count += 1
                    break # Found a spot for this mandatory assignment
                else:
                    logging.debug(f"      Post {post} is already filled by {self.schedule[date][post]}.")

            if not success:
                logging.warning(f"    Failed to assign mandatory shift for worker {worker_id} on {date}: All posts filled or incompatible.")

        logging.info(f"Finished mandatory guard assignment. Assigned {assigned_count} shifts.")
        # The original code returned assigned_count > 0, let's keep that
        return assigned_count > 0
    
        # First pass: collect all mandatory shift requirements
        for worker in self.workers_data:
            worker_id = worker['id']
            mandatory_days = worker.get('mandatory_days', '')
        
            if not mandatory_days:
                continue
            
            try:
                # Parse the mandatory days
                mandatory_dates = self.date_utils.parse_dates(mandatory_days)
            
                for date in mandatory_dates:
                    if date < self.start_date or date > self.end_date:
                        continue  # Skip dates outside scheduling period
                    
                    mandatory_assignments.append((worker_id, date))
                    logging.info(f"Mandatory shift identified: Worker {worker_id} on {date}")
                
            except Exception as e:
                logging.error(f"Error parsing mandatory days for worker {worker_id}: {str(e)}")
    
        # Second pass: assign all mandatory shifts
        for worker_id, date in mandatory_assignments:
            # Check if already assigned
            if date in self.schedule and worker_id in self.schedule[date]:
                logging.debug(f"Worker {worker_id} already assigned on mandatory date {date}")
                continue
            
            # Initialize the date in the schedule if needed
            if date not in self.schedule:
                self.schedule[date] = [None] * self.num_shifts
            
            # Try to assign the worker to any available post on the mandatory date
            success = False
            for post in range(self.num_shifts):
                if post >= len(self.schedule[date]):
                    # Extend list if needed
                    self.schedule[date].extend([None] * (post - len(self.schedule[date]) + 1))
                
                if self.schedule[date][post] is None:
                    # Check for incompatibility with already assigned workers
                    if not self._check_incompatibility(worker_id, date):
                        logging.warning(f"Cannot assign mandatory shift for worker {worker_id} on {date}: incompatibility")
                        continue
                    
                    # Assign the worker to this post
                    self.schedule[date][post] = worker_id
                    self.worker_assignments[worker_id].add(date)
                    self.scheduler._update_tracking_data(worker_id, date, post)
                    logging.info(f"Assigned mandatory shift: Worker {worker_id} on {date} to post {post}")
                    success = True
                    assigned_count += 1
                    break
                
            if not success:
                logging.warning(f"Failed to assign mandatory shift for worker {worker_id} on {date}: all posts filled or incompatible")
    
        logging.info(f"Finished mandatory guard assignment. Assigned {assigned_count} shifts.")
        return assigned_count > 0

    def _assign_priority_days(self, forward):
        """Process weekend and holiday assignments first since they're harder to fill"""
        # First assign all mandatory shifts/guards
        self._assign_mandatory_guards()
    
        dates_to_process = []
        current = self.start_date
        """Process weekend and holiday assignments first since they're harder to fill"""
        dates_to_process = []
        current = self.start_date
    
        # Get all weekend and holiday dates in the period
        while current <= self.end_date:
            if self.date_utils.is_weekend_day(current) or current in self.holidays:
                dates_to_process.append(current)
            current += timedelta(days=1)
    
        # Sort based on direction
        if not forward:
            dates_to_process.reverse()
    
        logging.info(f"Processing {len(dates_to_process)} priority days (weekends & holidays)")
    
        # Process these dates first with strict constraints
        for date in dates_to_process:
            if date not in self.schedule:
                self.schedule[date] = []
        
            remaining_shifts = self.num_shifts - len(self.schedule[date])
            if remaining_shifts > 0:
                self._assign_day_shifts_with_relaxation(date, 0, 0)  # Use strict constraints

    def _get_remaining_dates_to_process(self, forward):
        """Get remaining dates that need to be processed"""
        dates_to_process = []
        current = self.start_date
    
        # Get all dates in period that are not weekends or holidays
        # or that already have some assignments but need more
        while current <= self.end_date:
            date_needs_processing = False
        
            if current not in self.schedule:
                # Date not in schedule at all
                date_needs_processing = True
            elif len(self.schedule[current]) < self.num_shifts:
                # Date in schedule but has fewer shifts than needed
                date_needs_processing = True
            
            if date_needs_processing:
                dates_to_process.append(current)
            
            current += timedelta(days=1)
    
        # Sort based on direction
        if forward:
            dates_to_process.sort()
        else:
            dates_to_process.sort(reverse=True)
    
        return dates_to_process
    
    def _assign_day_shifts_with_relaxation(self, date, attempt_number=0, relaxation_level=0):
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


        # Determine starting post index. If schedule[date] already has items, start from the next index.
        start_post = len(self.schedule.get(date, []))

        for post in range(start_post, self.num_shifts):
            assigned_this_post = False
            # Try each relaxation level until we succeed or run out of options
            for relax_level in range(relaxation_level + 1): # Start from specified level up to max (usually 0)
                candidates = self._get_candidates(date, post, relax_level)

                logging.debug(f"Found {len(candidates)} candidates for {date.strftime('%d-%m-%Y')}, post {post}, relax level {relax_level}")

                if candidates:
                    # Log top candidates if needed
                    # for i, (worker, score) in enumerate(candidates[:3]):
                    #     logging.debug(f"  Candidate {i+1}: Worker {worker['id']} with score {score:.2f}")

                    # Sort candidates by score (descending)
                    candidates.sort(key=lambda x: x[1], reverse=True)

                    # --- Try assigning the first compatible candidate ---
                    for candidate_worker, candidate_score in candidates:
                        worker_id = candidate_worker['id']

                        # *** DEBUG LOGGING - START ***
                        current_assignments_on_date = [w for w in self.schedule.get(date, []) if w is not None]
                        logging.debug(f"CHECKING: Date={date}, Post={post}, Candidate={worker_id}, CurrentlyAssigned={current_assignments_on_date}")
                        # *** DEBUG LOGGING - END ***

                        # *** EXPLICIT INCOMPATIBILITY CHECK ***
                        # Temporarily add logging INSIDE the check function call might also help, or log its result explicitly
                        is_compatible = self._check_incompatibility_with_list(worker_id, current_assignments_on_date)
                        logging.debug(f"  -> Incompatibility Check Result: {is_compatible}") # Log the result

                        # if not self._check_incompatibility_with_list(worker_id, current_assignments_on_date):
                        if not is_compatible: # Use the variable to make logging easier
                            logging.debug(f"  Skipping candidate {worker_id} for post {post} on {date}: Incompatible with current assignments on this date.")
                            continue # Try next candidate

                        # *** If compatible, assign this worker ***
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


            # --- Handle case where post remains unfilled after trying all relaxation levels ---
            if not assigned_this_post:
                 # Ensure list is long enough before potentially assigning None
                 while len(self.schedule[date]) <= post:
                      self.schedule[date].append(None)

                 # Only log warning if the slot is genuinely still None
                 if self.schedule[date][post] is None:
                      logging.warning(f"No suitable worker found for {date.strftime('%d-%m-%Y')}, post {post} - shift unfilled after all checks.")
                 # Else: it might have been filled by a mandatory assignment earlier, which is fine.

        # --- Ensure schedule[date] list has the correct final length ---
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

            # --- PRE-FILTERING ---
            # Skip if already assigned to this date (redundant with score check, but safe)
            if worker_id in self.schedule.get(date, []):
                 logging.debug(f"  Worker {worker_id} skipped - already assigned to {date.strftime('%d-%m-%Y')}")
                 continue

            # Skip if unavailable
            if self._is_worker_unavailable(worker_id, date):
                 logging.debug(f"  Worker {worker_id} skipped - unavailable on {date.strftime('%d-%m-%Y')}")
                 continue

            # *** ADDED: Explicit Incompatibility Check BEFORE scoring ***
            # Never relax incompatibility constraint
            if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
                 logging.debug(f"  Worker {worker_id} skipped - incompatible with already assigned workers on {date.strftime('%d-%m-%Y')}")
                 continue
            # Skip if max shifts reached
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                logging.debug(f"Worker {worker_id} skipped - max shifts reached: {len(self.worker_assignments[worker_id])}/{self.max_shifts_per_worker}")
                continue

            # Skip if already assigned to this date
            if worker_id in self.schedule.get(date, []):
                logging.debug(f"Worker {worker_id} skipped - already assigned to {date.strftime('%d-%m-%Y')}")
                continue
        
            # CRITICAL FIX: We'll relax all constraints for the first assignments
            # Skip constraints check for now to get initial assignments working
        
            # Calculate score - SIMPLIFIED for debugging
            score = 1000 - len(self.worker_assignments[worker_id]) * 10  # Prefer workers with fewer assignments
        
            # Special bonus for workers who need to meet their target
            worker_target = worker.get('target_shifts', 0)
            current_assignments = len(self.worker_assignments[worker_id])
            if current_assignments < worker_target:
                score += 500  # Priority to workers who need more shifts
            
            logging.debug(f"Worker {worker_id} added as candidate with score {score}")
            candidates.append((worker, score))

        return candidates

    # 6. Schedule Improvement Methods

    def _try_fill_empty_shifts(self):
        """
        Try to fill empty shifts.
        Pass 1: Direct assignment using RELAXED constraints.
        Pass 2: Attempt swaps using STRICT constraints for remaining empty shifts.
        """
        empty_shifts = []

        # Find all empty shifts (using scheduler's reference)
        for date, workers in self.scheduler.schedule.items():
            for post, worker in enumerate(workers):
                if worker is None:
                    empty_shifts.append((date, post))
        if not empty_shifts: return False
        logging.info(f"Attempting to fill {len(empty_shifts)} empty shifts...")
        empty_shifts.sort(key=lambda x: x[0])
        shifts_filled_count = 0
        made_change_in_pass = False
        remaining_empty_shifts = []


        # --- Pass 1: Direct Assignment (Relaxed Check + STRICT Incompatibility) ---
        logging.info("--- Starting Pass 1: Direct Fill (Relaxed Constraints + Strict Incompatibility) ---")
        for date, post in empty_shifts:
            # Use scheduler's schedule reference
            if date in self.scheduler.schedule and len(self.scheduler.schedule[date]) > post and self.scheduler.schedule[date][post] is None:
                candidates = []
                # Use scheduler's workers_data reference
                for worker in self.scheduler.workers_data:
                    worker_id = worker['id']

                    # --- Start Relaxed Check ---
                    is_valid_candidate = True

                    # 1. Check absolute unavailability
                    if self._is_worker_unavailable(worker_id, date):
                        is_valid_candidate = False

                    # 2. Check if already working on this date (different post)
                    # Use scheduler's schedule reference
                    if is_valid_candidate and worker_id in [w for i, w in enumerate(self.scheduler.schedule[date]) if i != post and w is not None]:
                        is_valid_candidate = False

                    # === ADDED: STRICT Incompatibility Check ===
                    # Incompatibility is NEVER relaxed.
                    if is_valid_candidate and not self._check_incompatibility(worker_id, date):
                        # logging.debug(f"  [Relaxed Check] Worker {worker_id} incompatible on {date}")
                        is_valid_candidate = False
                    # === END ADDED CHECK ===

                    # 4. Check if max total shifts reached
                    # Use scheduler's references
                    if is_valid_candidate and len(self.scheduler.worker_assignments.get(worker_id, set())) >= self.max_shifts_per_worker:
                        is_valid_candidate = False
                    # --- End Relaxed Check ---

                    if is_valid_candidate:
                        # Scoring logic remains the same
                        score = 1000 - len(self.scheduler.worker_assignments.get(worker_id, set())) * 10
                        worker_target = worker.get('target_shifts', 0)
                        current_assignments = len(self.scheduler.worker_assignments.get(worker_id, set()))
                        if current_assignments < worker_target:
                             score += 500
                        candidates.append((worker, score))
                        # logging.debug(f"  [Relaxed Check] Worker {worker_id} is a candidate for {date} post {post} with score {score}")


                # --- Start of Replacement Block ---
                assigned_this_post_pass1 = False
                if candidates:
                    candidates.sort(key=lambda x: x[1], reverse=True)

                    # --- Loop through candidates and perform final check ---
                    for candidate_worker, candidate_score in candidates:
                        worker_id = candidate_worker['id']

                        # *** START: THESE DEBUG LOGS ARE CRITICAL ***
                        current_assignments_on_date = [w for w in self.scheduler.schedule.get(date, []) if w is not None]
                        logging.debug(f"CHECKING: Date={date}, Post={post}, Candidate={worker_id}, CurrentlyAssigned={current_assignments_on_date}") # <<< LOG 1

                        is_compatible = self._check_incompatibility_with_list(worker_id, current_assignments_on_date)
                        logging.debug(f"  -> Incompatibility Check Result: {is_compatible}") # <<< LOG 2

                        if not is_compatible:
                            logging.debug(f"  [Relaxed Direct Fill] Skipping candidate {worker_id} for post {post} on {date}: Incompatible with current assignments.") # <<< LOG 3
                            continue # Try next candidate
                        # *** END: CRITICAL DEBUG LOGS ***

                        # *** If compatible, assign this worker ***
                        while len(self.scheduler.schedule[date]) <= post:
                            self.scheduler.schedule[date].append(None)

                        if self.scheduler.schedule[date][post] is None:
                            self.scheduler.schedule[date][post] = worker_id
                            self.scheduler.worker_assignments.setdefault(worker_id, set()).add(date)
                            self.scheduler._update_tracking_data(worker_id, date, post)

                            logging.info(f"[Relaxed Direct Fill] Filled empty shift on {date.strftime('%Y-%m-%d')} post {post} with worker {worker_id} (Score: {candidate_score:.2f})") # This log IS appearing
                            shifts_filled_count += 1
                            made_change_in_pass = True
                            assigned_this_post_pass1 = True
                            break
                        else:
                            logging.warning(f"  [Relaxed Direct Fill] Slot {post} on {date} was unexpectedly filled before assigning candidate {worker_id}.")

                if not assigned_this_post_pass1:
                    remaining_empty_shifts.append((date, post))
                    if date not in self.scheduler.schedule or len(self.scheduler.schedule[date]) <= post or self.scheduler.schedule[date][post] is None:
                         logging.debug(f"Could not find compatible direct candidate (relaxed+incomp) for {date.strftime('%Y-%m-%d')} post {post}.")
           
        # --- Pass 2: Swap Attempt (Uses Stricter Checks via _can_assign_worker and _find_swap_candidate) ---
        if not remaining_empty_shifts:
             logging.info(f"--- Finished Pass 1: Filled {shifts_filled_count} shifts directly. No remaining empty shifts. ---")
             if made_change_in_pass:
                 self._save_current_as_best()
             return made_change_in_pass # Return True if Pass 1 made changes

        logging.info(f"--- Finished Pass 1: Filled {shifts_filled_count} shifts directly. Starting Pass 2: Attempting swaps for {len(remaining_empty_shifts)} remaining empty shifts (Strict Constraints) ---")

        # Iterate through the empty shifts remaining after Pass 1
        for date, post in remaining_empty_shifts:
            # Ensure the slot is *still* empty before attempting swap
            if not (date in self.scheduler.schedule and len(self.scheduler.schedule[date]) > post and self.scheduler.schedule[date][post] is None):
                continue # Skip if it got filled somehow

            swap_found = False
            potential_swap_workers = [w for w in self.scheduler.workers_data if w['id'] not in self.scheduler.schedule.get(date, [])]
            random.shuffle(potential_swap_workers)

            for worker_W in potential_swap_workers:
                worker_W_id = worker_W['id']
                original_assignments_W = sorted(list(self.scheduler.worker_assignments.get(worker_W_id, set())))

                for conflict_date in original_assignments_W:
                    if self._is_mandatory(worker_W_id, conflict_date): continue # Skip mandatory

                    # --- Simulation Block (Strict Check) ---
                    removed_during_check = False
                    current_assignments_W = self.scheduler.worker_assignments.get(worker_W_id, set())
                    if conflict_date in current_assignments_W:
                        try:
                            self.scheduler.worker_assignments[worker_W_id].remove(conflict_date)
                            removed_during_check = True
                        except KeyError: continue
                    else: continue

                    can_W_take_empty_slot = self._can_assign_worker(worker_W_id, date, post) # Strict Check

                    if removed_during_check:
                        self.scheduler.worker_assignments[worker_W_id].add(conflict_date) # Restore state
                    # --- End Simulation Block ---

                    if not can_W_take_empty_slot:
                        continue # Removing this conflict_date didn't help or W is still invalid

                    # --- Find conflict_post and worker_X (using Strict Check) ---
                    try:
                        if conflict_date not in self.scheduler.schedule or worker_W_id not in self.scheduler.schedule[conflict_date]: continue
                        conflict_post = self.scheduler.schedule[conflict_date].index(worker_W_id)
                    except (ValueError, IndexError, KeyError): continue

                    worker_X_id = self._find_swap_candidate(worker_W_id, conflict_date, conflict_post) # Strict Check
                    
                    # --- Perform Swap if candidate X found ---
                    if worker_X_id:
                        logging.info(f"[Swap Fill] Found swap: W={worker_W_id}, X={worker_X_id}, EmptySlot=({date.strftime('%Y-%m-%d')},{post}), ConflictSlot=({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")

                         # --- Perform Swap if candidate X found ---
                    if worker_X_id:
                        logging.info(f"[Swap Fill] Found swap: W={worker_W_id}, X={worker_X_id}, EmptySlot=({date.strftime('%Y-%m-%d')},{post}), ConflictSlot=({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")
                        # Perform the Swap (directly on scheduler's data)
                        self._execute_swap(worker_W_id, date, post, worker_X_id, conflict_date, conflict_post) # Use helper if available

                        shifts_filled_count += 1
                        made_change_in_pass = True
                        swap_found = True
                        break # Break from conflict_date loop

                if swap_found: break # Break from worker_W loop

            if not swap_found:
                 # FIX: Removed extraneous text after the closing parenthesis
                logging.debug(f"Could not find direct fill or swap for empty shift on {date.strftime('%Y-%m-%d')} post {post}")
        # --- End of loop for remaining_empty_shifts ---

        logging.info(f"--- Finished Pass 2: Attempted swaps. Total shifts filled in this run (direct + swap): {shifts_filled_count} ---")

        if made_change_in_pass:
            self._save_current_as_best()
        return made_change_in_pa
    
    def _find_swap_candidate(self, worker_W_id, conflict_date, conflict_post):
        """
        Finds a worker (X) who can take the shift at (conflict_date, conflict_post),
        ensuring they are not worker_W_id and not already assigned on that date.
        Uses strict constraints (_can_assign_worker).
        """
        potential_X_workers = [
            w for w in self.workers_data
            if w['id'] != worker_W_id and w['id'] not in self.schedule.get(conflict_date, [])
        ]
        random.shuffle(potential_X_workers) # Avoid bias

        for worker_X in potential_X_workers:
            worker_X_id = worker_X['id']
            # Check if X can strictly take W's old slot
            if self._can_assign_worker(worker_X_id, conflict_date, conflict_post):
                 # Optionally, add scoring here to pick the 'best' X if multiple candidates exist
                 # For simplicity, we take the first valid one found.
                 logging.debug(f"Found valid swap candidate X={worker_X_id} for W={worker_W_id}'s slot ({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")
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
        total_days = (self.end_date - self.start_date).days + 1
        weekend_days = sum(1 for d in self._get_date_range(self.start_date, self.end_date) 
                      if self.date_utils.is_weekend_day(d) or d in self.holidays)
    
        # Calculate the target percentage
        weekend_percentage = weekend_days / total_days if total_days > 0 else 0
        logging.info(f"Schedule period has {weekend_days} weekend/holiday days out of {total_days} total days ({weekend_percentage:.1%})")
    
        # Check each worker's current weekend shift allocation
        workers_to_check = self.workers_data.copy()
        random.shuffle(workers_to_check)  # Process in random order
    
        for worker in workers_to_check:
            worker_id = worker['id']
            assignments = self.worker_assignments.get(worker_id, set())
            total_shifts = len(assignments)
        
            if total_shifts == 0:
                continue  # Skip workers with no assignments
            
            # Count weekend assignments for this worker
            weekend_shifts = sum(1 for date in assignments 
                                if self.date_utils.is_weekend_day(date) or date in self.holidays)
        
            # Calculate target weekend shifts for this worker
            target_weekend_shifts = total_shifts * weekend_percentage
            deviation = weekend_shifts - target_weekend_shifts
            allowed_deviation = 1.0  # Allow ±1 shift from perfect distribution
        
            logging.debug(f"Worker {worker_id}: {weekend_shifts} weekend shifts, target {target_weekend_shifts:.2f}, deviation {deviation:.2f}")
        
            # Case 1: Worker has too many weekend shifts
            if deviation > allowed_deviation:
                logging.info(f"Worker {worker_id} has too many weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
                swap_found = False
            
                # Find workers with too few weekend shifts to swap with
                potential_swap_partners = []
                for other_worker in self.workers_data:
                    other_id = other_worker['id']
                    if other_id == worker_id:
                        continue
                
                    other_total = len(self.worker_assignments.get(other_id, []))
                    if other_total == 0:
                        continue
                    
                    other_weekend = sum(1 for d in self.worker_assignments.get(other_id, []) 
                                       if self.date_utils.is_weekend_day(d) or d in self.holidays)
                                    
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
                        possible_from_dates = [d for d in assignments 
                                             if (self.date_utils.is_weekend_day(d) or d in self.holidays)
                                             and not self._is_mandatory(worker_id, d)]
                    
                        if not possible_from_dates:
                            continue  # No swappable weekend shifts
                        
                        random.shuffle(possible_from_dates)
                    
                        for from_date in possible_from_dates:
                            # Find the post this worker is assigned to
                            from_post = self.schedule[from_date].index(worker_id)
                        
                            # Find a weekday assignment from the swap partner that could be exchanged
                            partner_assignments = self.worker_assignments.get(swap_partner_id, set())
                            possible_to_dates = [d for d in partner_assignments 
                                               if not (self.date_utils.is_weekend_day(d) or d in self.holidays)
                                               and not self._is_mandatory(swap_partner_id, d)]
                        
                            if not possible_to_dates:
                                continue  # No swappable weekday shifts for partner
                            
                            random.shuffle(possible_to_dates)
                        
                            for to_date in possible_to_dates:
                                # Find the post the partner is assigned to
                                to_post = self.schedule[to_date].index(swap_partner_id)
                            
                                # Check if swap is valid
                                if self._can_swap_assignments(worker_id, from_date, from_post, swap_partner_id, to_date, to_post):
                                    # Execute worker-worker swap
                                    self._execute_worker_swap(worker_id, from_date, from_post, swap_partner_id, to_date, to_post)
                                    logging.info(f"Swapped weekend shift: Worker {worker_id} on {from_date.strftime('%Y-%m-%d')} with "
                                               f"Worker {swap_partner_id} on {to_date.strftime('%Y-%m-%d')}")
                                    fixes_made += 1
                                    swap_found = True
                                    break
                        
                            if swap_found:
                                break
                    
                        if swap_found:
                            break
                        
            # Case 2: Worker has too few weekend shifts
            elif deviation < -allowed_deviation:
                logging.info(f"Worker {worker_id} has too few weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
                swap_found = False
            
                # Find workers with too many weekend shifts to swap with
                potential_swap_partners = []
                for other_worker in self.workers_data:
                    other_id = other_worker['id']
                    if other_id == worker_id:
                        continue
                
                    other_total = len(self.worker_assignments.get(other_id, []))
                    if other_total == 0:
                        continue
                    
                    other_weekend = sum(1 for d in self.worker_assignments.get(other_id, []) 
                                       if self.date_utils.is_weekend_day(d) or d in self.holidays)
                                    
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
                        possible_from_dates = [d for d in partner_assignments 
                                             if (self.date_utils.is_weekend_day(d) or d in self.holidays)
                                             and not self._is_mandatory(swap_partner_id, d)]
                    
                        if not possible_from_dates:
                            continue
                        
                        random.shuffle(possible_from_dates)
                    
                        for from_date in possible_from_dates:
                            from_post = self.schedule[from_date].index(swap_partner_id)
                        
                            # Find a weekday assignment from this worker
                            possible_to_dates = [d for d in assignments 
                                               if not (self.date_utils.is_weekend_day(d) or d in self.holidays)
                                               and not self._is_mandatory(worker_id, d)]
                        
                            if not possible_to_dates:
                                continue
                            
                            random.shuffle(possible_to_dates)
                        
                            for to_date in possible_to_dates:
                                to_post = self.schedule[to_date].index(worker_id)
                            
                                if self._can_swap_assignments(swap_partner_id, from_date, from_post, worker_id, to_date, to_post):
                                    self._execute_worker_swap(swap_partner_id, from_date, from_post, worker_id, to_date, to_post)
                                    logging.info(f"Swapped weekend shift: Worker {swap_partner_id} on {from_date.strftime('%Y-%m-%d')} with "
                                               f"Worker {worker_id} on {to_date.strftime('%Y-%m-%d')}")
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
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            # Get post counts, defaulting to an empty dict if worker has no assignments yet
            actual_post_counts = self.scheduler.worker_posts.get(worker_id, {})
            total_assigned = sum(actual_post_counts.values())

            # If worker has no shifts or only one type of post, they can't be imbalanced yet
            if total_assigned == 0 or num_posts <= 1:
                continue

            target_per_post = total_assigned / num_posts
            max_deviation = 0
            post_deviations = {} # Store deviation per post

            for post in range(num_posts):
                actual_count = actual_post_counts.get(post, 0)
                deviation = actual_count - target_per_post
                post_deviations[post] = deviation
                if abs(deviation) > max_deviation:
                    max_deviation = abs(deviation)

            # Consider imbalanced if the count for any post is off by more than the threshold
            if max_deviation > deviation_threshold:
                # Store the actual counts, not the deviations map for simplicity
                imbalanced_workers.append((worker_id, actual_post_counts.copy(), max_deviation))
                logging.debug(f"Worker {worker_id} identified as imbalanced for posts. Max Deviation: {max_deviation:.2f}, Target/Post: {target_per_post:.2f}, Counts: {actual_post_counts}")


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
                   Each list contains tuples: [(post_index, count), ...]
                   Sorted by deviation magnitude.
        """
        overassigned = []
        underassigned = []
        num_posts = self.num_shifts
        if num_posts <= 1 or total_assigned == 0:
            return [], [] # Cannot be over/under assigned

        target_per_post = total_assigned / num_posts

        for post in range(num_posts):
            actual_count = post_counts.get(post, 0)
            deviation = actual_count - target_per_post

            # Use a threshold slightly > 0 to avoid minor float issues
            # Consider overassigned if count is clearly higher than target
            if deviation > balance_threshold:
                overassigned.append((post, actual_count, deviation)) # Include deviation for sorting
            # Consider underassigned if count is clearly lower than target
            elif deviation < -balance_threshold:
                 underassigned.append((post, actual_count, deviation)) # Deviation is negative

        # Sort overassigned: highest count (most over) first
        overassigned.sort(key=lambda x: x[2], reverse=True)
        # Sort underassigned: lowest count (most under) first (most negative deviation)
        underassigned.sort(key=lambda x: x[2])

        # Return only (post, count) tuples
        overassigned_simple = [(p, c) for p, c, d in overassigned]
        underassigned_simple = [(p, c) for p, c, d in underassigned]

        return overassigned_simple, underassigned_simple
    
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
        for worker in self.workers_data:
            worker_id = worker['id']
            work_percentage = worker.get('work_percentage', 100)
    
            # Count assignments
            count = len(self.worker_assignments[worker_id])
    
            # Normalize by work percentage
            normalized_count = count * 100 / work_percentage if work_percentage > 0 else 0
    
            assignment_counts[worker_id] = {
                'worker_id': worker_id,
                'count': count,
                'work_percentage': work_percentage,
                'normalized_count': normalized_count
            }    

        # Calculate average normalized count
        total_normalized = sum(data['normalized_count'] for data in assignment_counts.values())
        avg_normalized = total_normalized / len(assignment_counts) if assignment_counts else 0

        # Identify overloaded and underloaded workers
        overloaded = []
        underloaded = []

        for worker_id, data in assignment_counts.items():
            # Allow 10% deviation from average
            if data['normalized_count'] > avg_normalized * 1.1:
                overloaded.append((worker_id, data))
            elif data['normalized_count'] < avg_normalized * 0.9:
                underloaded.append((worker_id, data))

        # Sort by most overloaded/underloaded
        overloaded.sort(key=lambda x: x[1]['normalized_count'], reverse=True)
        underloaded.sort(key=lambda x: x[1]['normalized_count'])

        changes_made = 0
        max_changes = 10  # Limit number of changes to avoid disrupting the schedule too much

        # Try to redistribute shifts from overloaded to underloaded workers
        for over_worker_id, over_data in overloaded:
            if changes_made >= max_changes or not underloaded:
                break
        
            # Find shifts that can be reassigned from this overloaded worker
            possible_shifts = []
    
            for date in sorted(self.scheduler.worker_assignments.get(over_worker_id, set())):
                # --- MANDATORY CHECK ---
                # Skip if this date is mandatory for this worker
                if self._is_mandatory(over_worker_id, date):
                     logging.debug(f"Cannot move worker {over_worker_id} from mandatory shift on {date.strftime('%Y-%m-%d')} for balancing.")
                     continue
                # --- END MANDATORY CHECK ---
            
                # Make sure the worker is actually in the schedule for this date
                if date not in self.schedule:
                    # This date is in worker_assignments but not in schedule
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date} but date is not in schedule")
                    continue
                
                try:
                    # Find the post this worker is assigned to
                    if over_worker_id not in self.schedule[date]:
                        # Worker is supposed to be assigned to this date but isn't in the schedule
                        logging.warning(f"Worker {over_worker_id} has assignment for date {date} but is not in schedule")
                        continue
                    
                    post = self.schedule[date].index(over_worker_id)
                    possible_shifts.append((date, post))
                except ValueError:
                    # Worker not found in schedule for this date
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date} but is not in schedule")
                    continue
    
            # Shuffle to introduce randomness
            random.shuffle(possible_shifts)
    
            # Try each shift
            for date, post in possible_shifts:
                reassigned = False
                for under_worker_id, _ in underloaded:
                    # ... (check if under_worker already assigned) ...
                    if self._can_assign_worker(under_worker_id, date, post):
                        # Make the reassignment (directly modify scheduler's references)
                        self.scheduler.schedule[date][post] = under_worker_id
                        self.scheduler.worker_assignments[over_worker_id].remove(date)
                        # Ensure under_worker tracking exists
                        if under_worker_id not in self.scheduler.worker_assignments:
                             self.scheduler.worker_assignments[under_worker_id] = set()
                        self.scheduler.worker_assignments[under_worker_id].add(date)

                        # Update tracking data (Needs FIX: update for BOTH workers)
                        self.scheduler._update_tracking_data(over_worker_id, date, post, removing=True) # Remove stats for over_worker
                        self.scheduler._update_tracking_data(under_worker_id, date, post) # Add stats for under_worker

                        changes_made += 1
                        logging.info(f"Balanced workload: Moved shift on {date.strftime('%Y-%m-%d')} post {post} from {over_worker_id} to {under_worker_id}")
                        
                        # Update counts
                        assignment_counts[over_worker_id]['count'] -= 1
                        assignment_counts[over_worker_id]['normalized_count'] = (
                            assignment_counts[over_worker_id]['count'] * 100 / 
                            assignment_counts[over_worker_id]['work_percentage']
                        )
                
                        assignment_counts[under_worker_id]['count'] += 1
                        assignment_counts[under_worker_id]['normalized_count'] = (
                            assignment_counts[under_worker_id]['count'] * 100 / 
                            assignment_counts[under_worker_id]['work_percentage']
                        )
                
                        reassigned = True
                
                        # Check if workers are still overloaded/underloaded
                        if assignment_counts[over_worker_id]['normalized_count'] <= avg_normalized * 1.1:
                            # No longer overloaded
                            overloaded = [(w, d) for w, d in overloaded if w != over_worker_id]
                
                        if assignment_counts[under_worker_id]['normalized_count'] >= avg_normalized * 0.9:
                            # No longer underloaded
                            underloaded = [(w, d) for w, d in underloaded if w != under_worker_id]
                
                        break
        
                if reassigned:
                    break
            
                if changes_made >= max_changes:
                    break

        logging.info(f"Workload balancing: made {changes_made} changes")
        if changes_made > 0:
            self._save_current_as_best()
        return changes_made > 0

    def _optimize_schedule(self, iterations=3):
        """Main schedule optimization function"""
        best_score = self._evaluate_schedule()
        iterations_without_improvement = 0
        max_iterations_without_improvement = iterations

        logging.info(f"Starting schedule optimization. Initial score: {best_score:.2f}")
    
        while iterations_without_improvement < max_iterations_without_improvement:
            improved = False
        
            # 1. Try basic post rotation balance first
            if self._improve_post_rotation():
                logging.info("Improved schedule through post rotation")
                improved = True
        
            # 2. Try specific last post balancing 
            if self._balance_last_post():
                logging.info("Improved schedule through last post balancing")
                improved = True
            
            # 3. Try weekend/holiday balancing (new function)
            if self._balance_weekend_shifts():
                logging.info("Improved schedule through weekend shift balancing")
                improved = True
            
            # 4. Try shift target balancing
            if self._balance_workloads():
                logging.info("Improved schedule through shift target balancing")
                improved = True

            # Other optimization steps...
        
            # Check if we've improved
            current_score = self._evaluate_schedule()
            if current_score > best_score:
                logging.info(f"Schedule improved. New score: {current_score:.2f} (was {best_score:.2f})")
                best_score = current_score
                iterations_without_improvement = 0
            else:
                iterations_without_improvement += 1
                logging.info(f"No improvement in iteration. Score: {current_score:.2f}. "
                            f"{iterations_without_improvement}/{max_iterations_without_improvement}")
    
        logging.info(f"Optimization complete. Final score: {best_score:.2f}")
        return best_score

    def _improve_post_rotation(self):
        """Improve post rotation by swapping assignments"""
        logging.info("Attempting to improve post rotation...")
        fixes_made = 0
        # Use a threshold slightly above 1 to avoid swapping for minor imbalances
        imbalanced_workers = self._identify_imbalanced_posts(deviation_threshold=1.5)

        if not imbalanced_workers:
             logging.info("No workers identified with significant post imbalance.")
             return False
            
        for worker_id, post_counts, deviation in imbalanced_workers:
            goto_next_worker = False
            total_assigned = sum(post_counts.values()) # Recalculate or pass from identify? Pass is better.


            overassigned_posts, underassigned_posts = self._get_over_under_posts(post_counts, total_assigned, balance_threshold=1.0)
            if not overassigned_posts or not underassigned_posts:
                logging.debug(f"Worker {worker_id} is imbalanced (dev={deviation:.2f}) but no clear over/under posts found with threshold 1.0. Counts: {post_counts}")
                continue

            logging.debug(f"Improving post rotation for {worker_id}: Over={overassigned_posts}, Under={underassigned_posts}")

            for over_post, _ in overassigned_posts:
                for under_post, _ in underassigned_posts:
                    possible_swap_dates = []
                    # Use scheduler references
                    for date_loop, workers_loop in self.scheduler.schedule.items():
                         if len(workers_loop) > over_post and workers_loop[over_post] == worker_id:
                              if not self._is_mandatory(worker_id, date_loop):
                                   possible_swap_dates.append(date_loop)
                              # else: Skip mandatory

                    random.shuffle(possible_swap_dates)

                    for date in possible_swap_dates: # Date moving FROM
                        potential_other_dates = sorted([d for d in self.scheduler.schedule.keys() if d != date])

                        for other_date in potential_other_dates: # Date moving TO
                            # ... (checks: worker not already assigned, target slot exists/empty) ...
                            if worker_id in self.scheduler.schedule.get(other_date, []): continue

                            schedule_other_date = self.scheduler.schedule.get(other_date, [])
                            target_slot_exists = len(schedule_other_date) > under_post
                            target_slot_empty = target_slot_exists and schedule_other_date[under_post] is None

                            if not target_slot_exists and len(schedule_other_date) == under_post:
                                target_slot_empty = True

                            if not target_slot_empty: continue

                            if not self._can_swap_assignments(worker_id, date, over_post, other_date, under_post):
                                continue

                            # --- Perform the swap ---
                            logging.info(f"Post Rotation Swap: Moving {worker_id} from {date.strftime('%Y-%m-%d')}|P{over_post} to {other_date.strftime('%Y-%m-%d')}|P{under_post}")

                            # ... (Update schedule, assignments, tracking data) ...
                            # 1. Update schedule dictionary
                            self.scheduler.schedule[date][over_post] = None
                            while len(self.scheduler.schedule[other_date]) <= under_post:
                                self.scheduler.schedule[other_date].append(None)
                            self.scheduler.schedule[other_date][under_post] = worker_id
                            # 2. Update worker_assignments set
                            self.scheduler.worker_assignments.setdefault(worker_id, set()).remove(date)
                            self.scheduler.worker_assignments.setdefault(worker_id, set()).add(other_date)
                            # 3. Update detailed tracking stats
                            self.scheduler._update_tracking_data(worker_id, date, over_post, removing=True)
                            self.scheduler._update_tracking_data(worker_id, other_date, under_post)


                            fixes_made += 1
                            # Set flag to True since swap was made
                            goto_next_worker = True
                            break # Break from other_date loop

                        # Check flag and break from date loop (Error occurred here)
                        if goto_next_worker: break

                    # Check flag and break from under_post loop
                    if goto_next_worker: break

                # Check flag and break from over_post loop
                if goto_next_worker: break

            # No reset needed here as it's initialized at the start of the next worker's loop
            # goto_next_worker = False # Removed reset from here

        logging.info(f"Post rotation improvement: attempted fixes for {len(imbalanced_workers)} workers, made {fixes_made} changes")
        if fixes_made > 0:
            self._save_current_as_best()
        return fixes_made > 0

    def _balance_last_post(self, balance_tolerance=1.0):
        """
        Specifically attempts to balance the number of shifts assigned to the LAST post
        for each worker, making sure each worker has approximately:
        (total_shifts_for_worker / num_shifts) shifts in the last position, ±1.
        """
        logging.info("Balancing last post assignments among workers...")
        fixes_made = 0
        last_post_idx = self.num_shifts - 1

        if self.num_shifts <= 1:
            logging.info("Skipping last post balancing: Only 0 or 1 post defined.")
            return False # Cannot balance if only one post

        # First analyze all workers' current post distributions
        worker_post_stats = []
        for worker in self.workers_data:
            worker_id = worker['id']
            total_shifts = len(self.worker_assignments.get(worker_id, set()))
        
            if total_shifts == 0:
                continue  # Skip workers with no assignments
            
            # Get accurate post counts
            post_counts = {}
            for date, shifts in self.schedule.items():
                for post_idx, assigned_worker in enumerate(shifts):
                    if assigned_worker == worker_id:
                        post_counts[post_idx] = post_counts.get(post_idx, 0) + 1
        
            # Calculate target for last post based on total shifts / num_shifts
            target_last_post = total_shifts / self.num_shifts
            actual_last_post = post_counts.get(last_post_idx, 0)
            deviation = actual_last_post - target_last_post
        
            worker_post_stats.append({
                'worker_id': worker_id,
                'total_shifts': total_shifts,
                'last_post_count': actual_last_post,
                'target_last_post': target_last_post,
                'deviation': deviation,
                'post_counts': post_counts
            })
        
            logging.debug(f"Worker {worker_id}: {actual_last_post} last post shifts, "
                         f"target {target_last_post:.2f}, deviation {deviation:.2f}")
    
        # Sort workers by deviation (most overassigned first, then most underassigned)
        overassigned = sorted([w for w in worker_post_stats if w['deviation'] > balance_tolerance], 
                              key=lambda x: -x['deviation'])
        underassigned = sorted([w for w in worker_post_stats if w['deviation'] < -balance_tolerance], 
                              key=lambda x: x['deviation'])
    
        # Process overassigned workers first
        for over_worker in overassigned:
            worker_id = over_worker['worker_id']
            logging.info(f"Worker {worker_id} has too many last post shifts "
                        f"({over_worker['last_post_count']}, target {over_worker['target_last_post']:.2f})")
        
            # Find dates where this worker is on the last post
            possible_from_dates = []
            for date, shifts in self.schedule.items():
                if len(shifts) > last_post_idx and shifts[last_post_idx] == worker_id:
                    if not self._is_mandatory(worker_id, date):
                        possible_from_dates.append(date)
        
            random.shuffle(possible_from_dates)
            fixed_for_worker = False
        
            # Try to fix overassigned workers in two ways:
            # 1. First try direct swaps with underassigned workers when possible
            if underassigned:
                for under_worker in underassigned:
                    under_id = under_worker['worker_id']
                    if fixed_for_worker:
                        break
                
                    logging.debug(f"Attempting direct swap between overassigned {worker_id} and underassigned {under_id}")
                
                    for from_date in possible_from_dates:
                        # Find a non-last post assignment for the underassigned worker
                        under_worker_dates = self.worker_assignments.get(under_id, set())
                        possible_swap_dates = []
                    
                        for date in under_worker_dates:
                            if self._is_mandatory(under_id, date):
                                continue
                        
                            shifts = self.schedule.get(date, [])
                            if under_id in shifts and shifts.index(under_id) != last_post_idx:
                                possible_swap_dates.append(date)
                    
                        random.shuffle(possible_swap_dates)
                    
                        for to_date in possible_swap_dates:
                            # Find the post this underassigned worker is assigned to
                            to_post = self.schedule[to_date].index(under_id)
                        
                            # Try swapping workers
                            if self._can_worker_swap(worker_id, from_date, last_post_idx, under_id, to_date, to_post):
                                # Execute the swap
                                self._execute_worker_swap(worker_id, from_date, last_post_idx, under_id, to_date, to_post)
                                logging.info(f"Swapped last post positions: Worker {worker_id} on {from_date.strftime('%Y-%m-%d')} "
                                           f"with worker {under_id} on {to_date.strftime('%Y-%m-%d')}")
                                fixes_made += 1
                                fixed_for_worker = True
                                break
                    
                        if fixed_for_worker:
                            break
        
            # 2. If direct swaps don't work, try to move to an empty non-last post position
            if not fixed_for_worker:
                for from_date in possible_from_dates:
                    # Find an empty non-last post slot on any date
                    potential_to_dates = sorted(self.schedule.keys())
                    random.shuffle(potential_to_dates)
                
                    for to_date in potential_to_dates:
                        if to_date == from_date:
                            continue  # Don't try same date
                    
                        if worker_id in self.schedule.get(to_date, []):
                            continue  # Worker already assigned on this date
                    
                        # Find an empty non-last post
                        for to_post in range(self.num_shifts):
                            if to_post == last_post_idx:
                                continue  # Skip last post
                        
                            shifts = self.schedule.get(to_date, [])
                            # Ensure post exists in the shifts list
                            if len(shifts) <= to_post:
                                continue
                        
                            if shifts[to_post] is None:  # Empty slot found
                                if self._can_swap_assignments(worker_id, from_date, last_post_idx, to_date, to_post):
                                    # Execute the swap
                                    self._execute_swap(worker_id, from_date, last_post_idx, to_date, to_post)
                                    logging.info(f"Moved worker {worker_id} from last post on {from_date.strftime('%Y-%m-%d')} "
                                               f"to post {to_post} on {to_date.strftime('%Y-%m-%d')}")
                                    fixes_made += 1
                                    fixed_for_worker = True
                                    break
                    
                        if fixed_for_worker:
                            break
                
                    if fixed_for_worker:
                        break
    
        # Process underassigned workers if no overassigned workers were fixed yet
        for under_worker in underassigned:
            worker_id = under_worker['worker_id']
            logging.info(f"Worker {worker_id} has too few last post shifts "
                        f"({under_worker['last_post_count']}, target {under_worker['target_last_post']:.2f})")
        
            # Find dates where this worker is on a non-last post
            worker_dates = self.worker_assignments.get(worker_id, set())
            possible_from_dates = []
        
            for date in worker_dates:
                if self._is_mandatory(worker_id, date):
                    continue
            
                shifts = self.schedule.get(date, [])
                if worker_id in shifts and shifts.index(worker_id) != last_post_idx:
                    possible_from_dates.append(date)
        
            random.shuffle(possible_from_dates)
            fixed_for_worker = False
        
            # Try to find an empty last post position for this worker
            for from_date in possible_from_dates:
                from_post = self.schedule[from_date].index(worker_id)
            
                potential_to_dates = sorted(self.schedule.keys())
                random.shuffle(potential_to_dates)
            
                for to_date in potential_to_dates:
                    if to_date == from_date:
                        continue  # Don't try same date
                
                    if worker_id in self.schedule.get(to_date, []):
                        continue  # Worker already assigned on this date
                
                    shifts = self.schedule.get(to_date, [])
                    # Ensure last post exists in the shifts list
                    if len(shifts) <= last_post_idx:
                        continue
                
                    if shifts[last_post_idx] is None:  # Empty last post slot found
                        if self._can_swap_assignments(worker_id, from_date, from_post, to_date, last_post_idx):
                            # Execute the swap
                            self._execute_swap(worker_id, from_date, from_post, to_date, last_post_idx)
                            logging.info(f"Moved worker {worker_id} from post {from_post} on {from_date.strftime('%Y-%m-%d')} "
                                       f"to last post on {to_date.strftime('%Y-%m-%d')}")
                            fixes_made += 1
                            fixed_for_worker = True
                            break
            
                if fixed_for_worker:
                    break
    
        logging.info(f"Last post balancing completed: made {fixes_made} changes")
        if fixes_made > 0:
            self._save_current_as_best()
        return fixes_made > 0

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
            logging.debug(f"Swap rejected: Mandatory guard assignment detected")
            return False
    
        # Make a copy of the schedule and assignments to simulate the swap
        schedule_copy = copy.deepcopy(self.schedule)
        assignments_copy = {}
        for worker_id, assignments in self.worker_assignments.items():
            assignments_copy[worker_id] = set(assignments)
    
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
        currently_assigned_date2 = [w for i, w in enumerate(schedule_copy[date2]) 
                                   if w is not None and i != post2]
        if not self._check_incompatibility_with_list(worker1_id, currently_assigned_date2):
            logging.debug(f"Swap rejected: Worker {worker1_id} incompatible with workers on {date2}")
            return False
    
        # 2. Check incompatibility constraints for worker2 on date1
        currently_assigned_date1 = [w for i, w in enumerate(schedule_copy[date1]) 
                                   if w is not None and i != post1]
        if not self._check_incompatibility_with_list(worker2_id, currently_assigned_date1):
            logging.debug(f"Swap rejected: Worker {worker2_id} incompatible with workers on {date1}")
            return False
    
        # 3. Check minimum gap constraints for worker1
        min_days_between = self.gap_between_shifts + 1
        worker1_dates = sorted(list(assignments_copy[worker1_id]))
    
        for assigned_date in worker1_dates:
            if assigned_date == date2:
                continue  # Skip the newly assigned date
        
            days_between = abs((date2 - assigned_date).days)
            if days_between < min_days_between:
                logging.debug(f"Swap rejected: Worker {worker1_id} would have insufficient gap between {assigned_date} and {date2}")
                return False
        
            # Special case for Friday-Monday if gap is only 1 day
            if self.gap_between_shifts == 1 and days_between == 3:
                if ((assigned_date.weekday() == 4 and date2.weekday() == 0) or 
                    (date2.weekday() == 4 and assigned_date.weekday() == 0)):
                    logging.debug(f"Swap rejected: Worker {worker1_id} would have Friday-Monday pattern")
                    return False
    
        # 4. Check minimum gap constraints for worker2
        worker2_dates = sorted(list(assignments_copy[worker2_id]))
    
        for assigned_date in worker2_dates:
            if assigned_date == date1:
                continue  # Skip the newly assigned date
        
            days_between = abs((date1 - assigned_date).days)
            if days_between < min_days_between:
                logging.debug(f"Swap rejected: Worker {worker2_id} would have insufficient gap between {assigned_date} and {date1}")
                return False
        
            # Special case for Friday-Monday if gap is only 1 day
            if self.gap_between_shifts == 1 and days_between == 3:
                if ((assigned_date.weekday() == 4 and date1.weekday() == 0) or 
                    (date1.weekday() == 4 and assigned_date.weekday() == 0)):
                    logging.debug(f"Swap rejected: Worker {worker2_id} would have Friday-Monday pattern")
                    return False
    
        # 5. Check weekend constraints for worker1
        worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
        if worker1:
            worker1_weekend_dates = [d for d in worker1_dates 
                                    if self.date_utils.is_weekend_day(d) or d in self.holidays]
        
            # If the new date is a weekend/holiday, add it to the list
            if self.date_utils.is_weekend_day(date2) or date2 in self.holidays:
                if date2 not in worker1_weekend_dates:
                    worker1_weekend_dates.append(date2)
                    worker1_weekend_dates.sort()
        
            # Check if this would violate max consecutive weekends
            max_weekend_count = self.max_consecutive_weekends
            work_percentage = worker1.get('work_percentage', 100)
            if work_percentage < 100:
                max_weekend_count = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
        
            for i, weekend_date in enumerate(worker1_weekend_dates):
                window_start = weekend_date - timedelta(days=10)
                window_end = weekend_date + timedelta(days=10)
            
                # Count weekend/holiday dates in this window
                window_count = sum(1 for d in worker1_weekend_dates 
                                  if window_start <= d <= window_end)
            
                if window_count > max_weekend_count:
                    logging.debug(f"Swap rejected: Worker {worker1_id} would exceed weekend limit")
                    return False
    
        # 6. Check weekend constraints for worker2
        worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
        if worker2:
            worker2_weekend_dates = [d for d in worker2_dates 
                                    if self.date_utils.is_weekend_day(d) or d in self.holidays]
        
            # If the new date is a weekend/holiday, add it to the list
            if self.date_utils.is_weekend_day(date1) or date1 in self.holidays:
                if date1 not in worker2_weekend_dates:
                    worker2_weekend_dates.append(date1)
                    worker2_weekend_dates.sort()
        
            # Check if this would violate max consecutive weekends
            max_weekend_count = self.max_consecutive_weekends
            work_percentage = worker2.get('work_percentage', 100)
            if work_percentage < 100:
                max_weekend_count = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
        
            for i, weekend_date in enumerate(worker2_weekend_dates):
                window_start = weekend_date - timedelta(days=10)
                window_end = weekend_date + timedelta(days=10)
            
                # Count weekend/holiday dates in this window
                window_count = sum(1 for d in worker2_weekend_dates 
                                  if window_start <= d <= window_end)
            
                if window_count > max_weekend_count:
                    logging.debug(f"Swap rejected: Worker {worker2_id} would exceed weekend limit")
                    return False
    
        # All constraints passed, the swap is valid
        logging.debug(f"Swap between Worker {worker1_id} ({date1}/{post1}) and Worker {worker2_id} ({date2}/{post2}) is valid")
        return True

    def _execute_swap(self, worker_id, date_from, post_from, date_to, post_to):
        """ Helper to perform the actual swap updates """
        # 1. Update schedule dictionary
        self.scheduler.schedule[date_from][post_from] = None
        # Ensure target list is long enough before assignment
        while len(self.scheduler.schedule[date_to]) <= post_to:
            self.scheduler.schedule[date_to].append(None)
        self.scheduler.schedule[date_to][post_to] = worker_id

        # 2. Update worker_assignments set
        self.scheduler.worker_assignments.setdefault(worker_id, set()).remove(date_from)
        self.scheduler.worker_assignments.setdefault(worker_id, set()).add(date_to)

        # 3. Update detailed tracking stats
        self.scheduler._update_tracking_data(worker_id, date_from, post_from, removing=True)
        self.scheduler._update_tracking_data(worker_id, date_to, post_to)

    # Make sure _can_swap_assignments exists and is correctly implemented
    # It should check constraints after simulating the move. Example sketch:
    def _can_swap_assignments(self, worker_id, date_from, post_from, date_to, post_to):
         """ Checks if moving worker_id from (date_from, post_from) to (date_to, post_to) is valid """
         # 1. Temporarily apply the swap to copies or directly (need rollback)
         original_val_from = self.scheduler.schedule[date_from][post_from]
         # Ensure target list is long enough for check
         original_len_to = len(self.scheduler.schedule.get(date_to, []))
         original_val_to = None
         if original_len_to > post_to:
              original_val_to = self.scheduler.schedule[date_to][post_to]
         elif original_len_to == post_to: # Can append
              pass
         else: # Cannot place here if list isn't long enough and we aren't appending
              return False

         self.scheduler.schedule[date_from][post_from] = None
         # Ensure list exists and is long enough
         self.scheduler.schedule.setdefault(date_to, [None] * self.num_shifts) # Ensure list exists
         while len(self.scheduler.schedule[date_to]) <= post_to:
              self.scheduler.schedule[date_to].append(None)
         self.scheduler.schedule[date_to][post_to] = worker_id
         self.scheduler.worker_assignments[worker_id].remove(date_from)
         self.scheduler.worker_assignments[worker_id].add(date_to)

         # 2. Check constraints for BOTH dates with the new state
         valid_from = self._check_all_constraints_for_date(date_from)
         valid_to = self._check_all_constraints_for_date(date_to)

         # 3. Rollback the temporary changes
         self.scheduler.schedule[date_from][post_from] = original_val_from # Should be worker_id
         if original_len_to > post_to:
             self.scheduler.schedule[date_to][post_to] = original_val_to # Should be None
         elif original_len_to == post_to: # We appended, so remove
             self.scheduler.schedule[date_to].pop()
         # If list was shorter and not appendable, we returned False earlier

         # Adjust list length if needed after pop
         if date_to in self.scheduler.schedule and len(self.scheduler.schedule[date_to]) == 0:
             # Maybe don't delete empty dates? Or handle carefully.
             # Let's assume empty lists are okay.
             pass


         self.scheduler.worker_assignments[worker_id].add(date_from)
         self.scheduler.worker_assignments[worker_id].remove(date_to)


         return valid_from and valid_to

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
                    # Indent level 4
                    if not self.scheduler.constraint_checker._check_constraints(worker_id, date, post_idx=post, skip_constraints=False):
                        # Indent level 5
                        # logging.debug(f"LIVE Constraint check failed for {worker_id} on {date} post {post} during swap check.")
                        return False
                except AttributeError:
                    # Indent level 4 (aligned with try)
                    logging.error("Constraint checker or _check_constraints method not found during swap validation.")
                    return False # Fail validation if checker is missing
                except Exception as e_constr:
                    # Indent level 4 (aligned with try)
                    logging.error(f"Error calling constraint checker for {worker_id} on {date}: {e_constr}", exc_info=True)
                    return False # Fail validation on error

        # Indent level 1 (aligned with the initial 'if' and 'for' loops)
        return True

    def _improve_weekend_distribution(self):
        """
        Improve weekend distribution by balancing weekend shifts more evenly among workers
        and attempting to resolve weekend overloads
        """
        logging.info("Attempting to improve weekend distribution")
    
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()

        # Count weekend assignments for each worker by month
        weekend_counts_by_month = {}
        months = {}
        current_date = self.start_date
        while current_date <= self.end_date:
            month_key = (current_date.year, current_date.month)
            if month_key not in months: months[month_key] = []
            months[month_key].append(current_date)
            current_date += timedelta(days=1)

        for month_key, dates in months.items():
            weekend_counts = {}
            for worker in self.workers_data:
                worker_id = worker['id']
                # Use scheduler references
                weekend_count = sum(1 for date in dates if date in self.scheduler.worker_assignments.get(worker_id, set()) and self.date_utils.is_weekend_day(date))
                weekend_counts[worker_id] = weekend_count
            weekend_counts_by_month[month_key] = weekend_counts
    
        changes_made = 0
    
         # Identify months with overloaded workers
        for month_key, weekend_counts in weekend_counts_by_month.items():
            overloaded_workers = []
            underloaded_workers = []

            for worker in self.workers_data:
                worker_id = worker['id']
                work_percentage = worker.get('work_percentage', 100)
                max_weekends = self.max_consecutive_weekends # Use configured param
                if work_percentage < 100:
                    max_weekends = max(1, int(self.max_consecutive_weekends * work_percentage / 100))

                weekend_count = weekend_counts.get(worker_id, 0)

                if weekend_count > max_weekends:
                    overloaded_workers.append((worker_id, weekend_count, max_weekends))
                elif weekend_count < max_weekends:
                    available_slots = max_weekends - weekend_count
                    underloaded_workers.append((worker_id, weekend_count, available_slots))

            overloaded_workers.sort(key=lambda x: x[1] - x[2], reverse=True)
            underloaded_workers.sort(key=lambda x: x[2], reverse=True)

            month_dates = months[month_key]
            # Use scheduler reference for holidays
            weekend_dates = [date for date in month_dates if self.date_utils.is_weekend_day(date) or date in self.scheduler.holidays]

            # Try to redistribute weekend shifts
            for over_worker_id, over_count, over_limit in overloaded_workers:
                if not underloaded_workers: break # No one to give shifts to

                # Iterate through potential dates to move FROM
                possible_dates_to_move = [wd for wd in weekend_dates if over_worker_id in self.scheduler.schedule.get(wd, [])]
                random.shuffle(possible_dates_to_move) # Randomize

                for weekend_date in possible_dates_to_move:

                    # --- ADDED MANDATORY CHECK ---
                    if self._is_mandatory(over_worker_id, weekend_date):
                        logging.debug(f"Cannot move worker {over_worker_id} from mandatory weekend shift on {weekend_date.strftime('%Y-%m-%d')} for balancing.")
                        continue # Skip this date for this worker
                    # --- END ADDED CHECK ---

                    # Find the post this worker is assigned to
                    try:
                        # Use scheduler reference
                        post = self.scheduler.schedule[weekend_date].index(over_worker_id)
                    except (ValueError, KeyError, IndexError):
                         logging.warning(f"Inconsistency finding post for {over_worker_id} on {weekend_date} during weekend balance.")
                         continue # Skip if schedule state is inconsistent

                    swap_done_for_date = False
                    # Try to find a suitable replacement (underloaded worker X)
                    for under_worker_id, _, _ in underloaded_workers:
                        # Skip if X is already assigned on this date
                        # Use scheduler reference
                        if under_worker_id in self.scheduler.schedule[weekend_date]:
                            continue

                        # Check if X can be assigned to this shift (using strict check)
                        if self._can_assign_worker(under_worker_id, weekend_date, post):
                            # Make the swap (directly modify scheduler's data)
                            self.scheduler.schedule[weekend_date][post] = under_worker_id
                            self.scheduler.worker_assignments[over_worker_id].remove(weekend_date)
                            self.scheduler.worker_assignments.setdefault(under_worker_id, set()).add(weekend_date)

                            # Update tracking data for BOTH workers
                            self.scheduler._update_tracking_data(over_worker_id, weekend_date, post, removing=True)
                            self.scheduler._update_tracking_data(under_worker_id, weekend_date, post)

                            # Update local counts for this month
                            weekend_counts[over_worker_id] -= 1
                            weekend_counts[under_worker_id] += 1

                            changes_made += 1
                            logging.info(f"Improved weekend distribution: Moved weekend shift on {weekend_date.strftime('%Y-%m-%d')} "
                                         f"from worker {over_worker_id} to worker {under_worker_id}")

                            # Update overloaded/underloaded lists locally for this month
                            if weekend_counts[over_worker_id] <= over_limit:
                                overloaded_workers = [(w, c, l) for w, c, l in overloaded_workers if w != over_worker_id]

                            for i, (w_id, count, slots) in enumerate(underloaded_workers):
                                if w_id == under_worker_id:
                                    # Check if worker X is now at their max for the month
                                    worker_X_data = next((w for w in self.workers_data if w['id'] == under_worker_id), None)
                                    worker_X_max = self.max_consecutive_weekends
                                    if worker_X_data and worker_X_data.get('work_percentage', 100) < 100:
                                        worker_X_max = max(1, int(self.max_consecutive_weekends * worker_X_data.get('work_percentage', 100) / 100))

                                    if weekend_counts[w_id] >= worker_X_max:
                                        underloaded_workers.pop(i)
                                    break # Found worker X in the list

                            swap_done_for_date = True
                            break # Found a swap for this weekend_date, move to next overloaded worker

                # If a swap was done for this overloaded worker, break to check the next (potentially new) most overloaded worker
                if swap_done_for_date:
                     break # Break from the weekend_date loop for the current over_worker_id

        logging.info(f"Weekend distribution improvement: made {changes_made} changes")
        if changes_made > 0:
            self._save_current_as_best() # Save if changes were made
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
        for date in sorted(self.schedule.keys()):
            workers_today = [w for w in self.schedule[date] if w is not None]
        
            # Check each pair of workers
            for i, worker1_id in enumerate(workers_today):
                for worker2_id in workers_today[i+1:]:
                    # Check if these workers are incompatible
                    if self._are_workers_incompatible(worker1_id, worker2_id):
                        violations_found += 1
                        logging.warning(f"Found incompatibility violation: {worker1_id} and {worker2_id} on {date}")
                    
                        # Try to fix the violation by moving one of the workers
                        # Let's try to move the second worker first
                        if self._try_reassign_worker(worker2_id, date):
                            violations_fixed += 1
                            logging.info(f"Fixed by reassigning {worker2_id} from {date}")
                        # If that didn't work, try moving the first worker
                        elif self._try_reassign_worker(worker1_id, date):
                            violations_fixed += 1
                            logging.info(f"Fixed by reassigning {worker1_id} from {date}")
    
        logging.info(f"Incompatibility check: found {violations_found} violations, fixed {violations_fixed}")
        return violations_fixed > 0
        
    def _try_reassign_worker(self, worker_id, date):
        """
        Try to find a new date to assign this worker to fix an incompatibility
        """
        # --- ADD MANDATORY CHECK ---
        if self._is_mandatory(worker_id, date):
            logging.warning(f"Cannot reassign worker {worker_id} from mandatory shift on {date.strftime('%Y-%m-%d')} to fix incompatibility.")
            # Option 1: Try removing the *other* incompatible worker instead? (More complex logic)
            # Option 2: Just log and fail for now.
            return False # Cannot move from mandatory shift
        # --- END MANDATORY CHECK ---
        # Find the position this worker is assigned to
        try:
           post = self.schedule[date].index(worker_id)
        except ValueError:
            return False
    
        # First, try to find a date with an empty slot for the same post
        current_date = self.start_date
        while current_date <= self.end_date:
            # Skip the current date
            if current_date == date:
                current_date += timedelta(days=1)
                continue
            
            # Check if this date has an empty slot at the same post
            if (current_date in self.schedule and 
                len(self.schedule[current_date]) > post and 
                self.schedule[current_date][post] is None):
            
                # Check if worker can be assigned to this date
                if self._can_assign_worker(worker_id, current_date, post):
                    # Remove from original date
                    self.schedule[date][post] = None
                    self.worker_assignments[worker_id].remove(date)
                
                    # Assign to new date
                    self.schedule[current_date][post] = worker_id
                    self.worker_assignments[worker_id].add(current_date)
                
                    # Update tracking data
                    self._update_worker_stats(worker_id, date, removing=True)
                    self.scheduler._update_tracking_data(under_worker_id, weekend_date, post)
                
                    return True
                
            current_date += timedelta(days=1)
    
        # If we couldn't find a new assignment, just remove this worker
        self.schedule[date][post] = None
        self.worker_assignments[worker_id].remove(date)
        self._update_worker_stats(worker_id, date, removing=True)
    
        return True

    def validate_mandatory_shifts(self):
        """Validate that all mandatory shifts have been assigned"""
        missing_mandatory = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
            mandatory_days = worker.get('mandatory_days', '')
        
            if not mandatory_days:
                continue
            
            mandatory_dates = self.date_utils.parse_dates(mandatory_days)
            for date in mandatory_dates:
                if date < self.start_date or date > self.end_date:
                    continue  # Skip dates outside scheduling period
                
                # Check if worker is assigned on this date
                assigned = False
                if date in self.schedule:
                    if worker_id in self.schedule[date]:
                        assigned = True
                    
                if not assigned:
                    missing_mandatory.append((worker_id, date))
    
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

        # 2. Try to improve post rotation by swapping assignments
        if self._improve_post_rotation():
            logging.info(f"Attempt {attempt_number}: Improved post rotation.")
            any_change_made = True
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

    # 7. Backup and Restore Methods

    def _backup_best_schedule(self):
        """Save a backup of the current best schedule by delegating to scheduler"""
        return self.scheduler._backup_best_schedule()
    
    def _restore_best_schedule(self):
        """Restore backup by delegating to scheduler"""
        return self.scheduler._restore_best_schedule()

    def _save_current_as_best(self, initial=False):
        """
        Calculates the score of the current schedule state and saves it as the
        best schedule found so far if it's better than the current best, or if
        it's the initial save.
        """
        current_score = self.calculate_score()
        old_score = self.best_schedule_data['score'] if self.best_schedule_data is not None else float('-inf')

        if initial or self.best_schedule_data is None or current_score > old_score:
            log_prefix = "[Initial Save]" if initial else "[New Best]"
            logging.info(f"{log_prefix} Saving current state as best. Score: {current_score:.2f} (Previous best: {old_score:.2f})")

            # This part requires the 'copy' module to be imported
            self.best_schedule_data = {
                'schedule': copy.deepcopy(self.scheduler.schedule),
                'worker_assignments': copy.deepcopy(self.scheduler.worker_assignments),
                'worker_shift_counts': copy.deepcopy(self.scheduler.worker_shift_counts),
                'worker_weekend_shifts': copy.deepcopy(self.scheduler.worker_weekend_shifts),
                'worker_posts': copy.deepcopy(self.scheduler.worker_posts),
                'last_assigned_date': copy.deepcopy(self.scheduler.last_assigned_date),
                'consecutive_shifts': copy.deepcopy(self.scheduler.consecutive_shifts),
                'score': current_score
            }

    def get_best_schedule(self):
        """ Returns the best schedule data dictionary found. """
        if self.best_schedule_data is None:
             logging.warning("get_best_schedule called but no best schedule was saved.")
        return self.best_schedule_data

    def calculate_score(self, schedule_to_score=None, assignments_to_score=None):
         """
         Calculates the score of a given schedule state or the current state.
         Higher score is better. Penalties decrease the score.
         """
         # Use current state if not provided
         schedule = schedule_to_score if schedule_to_score is not None else self.scheduler.schedule
         assignments = assignments_to_score if assignments_to_score is not None else self.scheduler.worker_assignments

         base_score = 1000.0
         penalty = 0.0

         # --- Penalties ---
         # 1. Empty Shifts
         empty_shifts = 0
         for date, posts in schedule.items():
             empty_shifts += posts.count(None)
         penalty += empty_shifts * 50.0

         # 2. Workload Imbalance
         # Use worker_shift_counts from the scheduler's current state
         shift_counts = list(self.scheduler.worker_shift_counts.values())
         if shift_counts:
             min_shifts = min(shift_counts)
             max_shifts = max(shift_counts)
             penalty += (max_shifts - min_shifts) * 5.0

         # 3. Weekend Imbalance
         # Use worker_weekend_shifts from the scheduler's current state
         weekend_counts = list(self.scheduler.worker_weekend_shifts.values())
         if weekend_counts:
             min_weekends = min(weekend_counts)
             max_weekends = max(weekend_counts)
             penalty += (max_weekends - min_weekends) * 10.0

         # 4. Post Rotation Imbalance
         total_post_deviation_penalty = 0
         num_posts = self.num_shifts
         if num_posts > 1:
             # Use worker_posts from the scheduler's current state
             for worker_id, worker_post_counts in self.scheduler.worker_posts.items():
                 total_assigned = sum(worker_post_counts.values())
                 if total_assigned > 0:
                     target_per_post = total_assigned / num_posts
                     worker_deviation = 0
                     for post in range(num_posts):
                          actual_count = worker_post_counts.get(post, 0)
                          worker_deviation += abs(actual_count - target_per_post)
                     total_post_deviation_penalty += worker_deviation * 1.0
         penalty += total_post_deviation_penalty

         # 5. Consecutive Shifts
         consecutive_penalty = 0
         # --- CORRECTED LINE: Access config via self.scheduler ---
         max_allowed_consecutive = self.scheduler.config.get('max_consecutive_shifts', 3)
         # --- END CORRECTION ---
         # Use worker_assignments for calculation (or better tracking data if available)
         for worker_id, dates in sorted(assignments.items()):
              sorted_dates = sorted(list(dates))
              current_consecutive = 0
              for i, date in enumerate(sorted_dates):
                   if i > 0 and (date - sorted_dates[i-1]).days == 1:
                        current_consecutive += 1
                   else:
                        current_consecutive = 1
                   if current_consecutive > max_allowed_consecutive:
                        consecutive_penalty += 5.0
         penalty += consecutive_penalty

         # 6. Minimum Rest Violation (Example)
         min_rest_hours = self.scheduler.config.get('min_rest_hours', 10) # Get from config via scheduler
         rest_penalty = 0
         # Add logic here to check rest periods based on assignments
         # penalty += rest_penalty

         # --- Add other penalties ---

         final_score = base_score - penalty
         return final_score
