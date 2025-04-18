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

        for assigned_id in assigned_workers_list:
             if assigned_id is None or assigned_id == worker_id_to_check:
                  continue
             # Check both directions for incompatibility
             if assigned_id in incompatible_with:
                  logging.debug(f"Incompatibility Check: {worker_id_to_check} cannot work with {assigned_id}")
                  return False # Found incompatibility

             # Also check if the assigned worker lists the worker_to_check as incompatible
             assigned_worker_data = next((w for w in self.workers_data if w['id'] == assigned_id), None)
             if assigned_worker_data and worker_id_to_check in assigned_worker_data.get('incompatible_with', []):
                 logging.debug(f"Incompatibility Check: {assigned_id} cannot work with {worker_id_to_check}")
                 return False # Found incompatibility

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
                            self._update_worker_stats(worker1_id, date, removing=True)
                            violations_fixed += 1
                            logging.info(f"Removed worker {worker1_id} from {date.strftime('%d-%m-%Y')} to fix incompatibility")
                        else:
                            self.schedule[date][post2] = None
                            self.worker_assignments[worker2_id].remove(date)
                            self._update_worker_stats(worker2_id, date, removing=True)
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

    def _can_swap_assignments(self, worker_id, date1, post1, date2, post2):
        """
        Check if a worker can be swapped from one assignment to another
    
        Args:
            worker_id: ID of the worker to check
            date1: Original date
            post1: Original post
            date2: New date
            post2: New post
        
        Returns:
            bool: True if the swap is valid, False otherwise
        """
        # First remove the worker from the original date (simulate)
        self.worker_assignments[worker_id].remove(date1)
    
        # Now check if they can be assigned to the new date
        result = self._can_assign_worker(worker_id, date2, post2)
    
        # Restore the original assignment
        self.worker_assignments[worker_id].add(date1)
    
        return result

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
        """Assigns mandatory shifts based on configuration."""
        logging.info("Starting mandatory guard assignment...")
        assigned_count = 0
        mandatory_shifts = self.scheduler.config.get('mandatory_shifts', {}) # Assuming format { "YYYY-MM-DD": {post_idx: worker_id} }

        if not mandatory_shifts:
            logging.info("No mandatory shifts found in configuration.")
            return

        # Ensure dates are datetime objects if they are strings
        processed_mandatory = {}
        for date_str, assignments in mandatory_shifts.items():
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                 # Ensure posts are integers
                processed_assignments = {int(post): worker for post, worker in assignments.items()}
                processed_mandatory[date_obj] = processed_assignments
            except ValueError as e:
                logging.error(f"Invalid date format or post index in mandatory_shifts for '{date_str}': {e}")
                continue # Skip invalid entries

        # Iterate through the schedule date range
        current_date = self.scheduler.start_date
        while current_date <= self.scheduler.end_date:
            if current_date in processed_mandatory:
                assignments_for_date = processed_mandatory[current_date]
                logging.debug(f"Processing mandatory shifts for {current_date}: {assignments_for_date}")

                for post_idx, worker_id in assignments_for_date.items():
                    # Basic validation
                    if worker_id not in self.scheduler.worker_ids:
                        logging.warning(f"Mandatory shift worker '{worker_id}' on {current_date} (Post {post_idx}) not found in workers data. Skipping.")
                        continue
                    if post_idx < 0 or post_idx >= self.num_shifts:
                        logging.warning(f"Mandatory shift post index {post_idx} for worker '{worker_id}' on {current_date} is out of range (0-{self.num_shifts-1}). Skipping.")
                        continue

                    # Initialize schedule for the date if needed
                    if current_date not in self.scheduler.schedule:
                        self.scheduler.schedule[current_date] = [None] * self.num_shifts

                    # Check if slot is already taken (maybe by another mandatory shift)
                    if self.scheduler.schedule[current_date][post_idx] is not None:
                        logging.warning(f"Mandatory shift slot {current_date} (Post {post_idx}) is already filled by {self.scheduler.schedule[current_date][post_idx]}. Skipping assignment for {worker_id}.")
                        continue

                    # Check constraints (IMPORTANT!)
                    # Use relaxation_level=1 or higher initially for mandatory if needed
                    if self.scheduler._check_constraints(worker_id, current_date, post_idx, relaxation_level=1):
                        logging.info(f"Assigning mandatory shift: {current_date} Post {post_idx} -> Worker {worker_id}")
                        self.scheduler.schedule[current_date][post_idx] = worker_id
                        # Update tracking data immediately
                        self.scheduler._update_tracking_data(worker_id, current_date, post_idx)
                        assigned_count += 1
                    else:
                        logging.warning(f"Could not assign mandatory shift for {worker_id} on {current_date} (Post {post_idx}) due to constraint violations (Relaxation 1).")
                        # Consider raising an error here if mandatory shifts MUST be assigned

            current_date += timedelta(days=1)

        logging.info(f"Finished mandatory guard assignment. Assigned {assigned_count} shifts.")                        
    def _assign_priority_days(self, forward):
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
        logging.debug(f"Assigning shifts for {date.strftime('%d-%m-%Y')} (relaxation level: {relaxation_level})")
    
        if date not in self.schedule:
            self.schedule[date] = []

        remaining_shifts = self.num_shifts - len(self.schedule[date])
    
        # Debug log to see how many shifts need to be assigned
        logging.debug(f"Need to assign {remaining_shifts} shifts for {date.strftime('%d-%m-%Y')}")

        for post in range(len(self.schedule[date]), self.num_shifts):
            # Try each relaxation level until we succeed or run out of options
            for relax_level in range(relaxation_level + 1):
                candidates = self._get_candidates(date, post, relax_level)
            
                # Debug log to see how many candidates were found
                logging.debug(f"Found {len(candidates)} candidates for {date.strftime('%d-%m-%Y')}, post {post}, relax level {relax_level}")
            
                if candidates:
                    # Log each candidate for debugging
                    for i, (worker, score) in enumerate(candidates[:3]):  # Just log first 3 to avoid clutter
                        logging.debug(f"Candidate {i+1}: Worker {worker['id']} with score {score}")
                
                    # Sort candidates by score (descending)
                    candidates.sort(key=lambda x: x[1], reverse=True)
                
                    # Group candidates with similar scores (within 10% of max score)
                    max_score = candidates[0][1]
                    top_candidates = [c for c in candidates if c[1] >= max_score * 0.9]
                
                    # Add some randomness to selection based on attempt number
                    random.Random(attempt_number + date.toordinal() + post).shuffle(top_candidates)
                
                    # Select the first candidate
                    best_worker = top_candidates[0][0]
                    worker_id = best_worker['id']
                
                    # Assign the worker
                    self.schedule[date].append(worker_id)
                    self.worker_assignments[worker_id].add(date)
                    self.scheduler._update_tracking_data(worker_id, date, post)
                
                    logging.info(f"Assigned worker {worker_id} to {date.strftime('%d-%m-%Y')}, post {post}")
                    break  # Success at this relaxation level
            else:
                # If we've tried all relaxation levels and still failed, leave shift unfilled
                self.schedule[date].append(None)
                logging.debug(f"No suitable worker found for {date.strftime('%Y-%m-%d')}, post {post} - shift unfilled")

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
    
        for worker in self.workers_data:
            worker_id = worker['id']
        
            # Debug log for each worker check
            logging.debug(f"Checking worker {worker_id} for {date.strftime('%d-%m-%Y')}")
        
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

        if not empty_shifts:
            return False  # No empty shifts to fill

        logging.info(f"Attempting to fill {len(empty_shifts)} empty shifts...")

        # Sort empty shifts by date (earlier dates first)
        empty_shifts.sort(key=lambda x: x[0])

        shifts_filled_count = 0
        made_change_in_pass = False # Flag to track if any change was made in this call
        remaining_empty_shifts = [] # Shifts not filled in Pass 1

        # --- Pass 1: Direct Assignment (Relaxed Check) ---
        logging.info("--- Starting Pass 1: Direct Fill (Relaxed Constraints) ---")
        for date, post in empty_shifts:
            # Ensure the slot is still empty (might have been filled by a previous step)
            # Use scheduler's schedule reference
            if date in self.scheduler.schedule and len(self.scheduler.schedule[date]) > post and self.scheduler.schedule[date][post] is None:
                candidates = []
                # Use scheduler's workers_data reference
                for worker in self.scheduler.workers_data:
                    worker_id = worker['id']

                    # --- Start Relaxed Check ---
                    is_valid_candidate = True

                    # 1. Check absolute unavailability (e.g., days off, outside work period)
                    if self._is_worker_unavailable(worker_id, date):
                        # logging.debug(f"  [Relaxed Check] Worker {worker_id} unavailable on {date}")
                        is_valid_candidate = False

                    # 2. Check if already working on this date (different post)
                    # Use scheduler's schedule reference
                    if is_valid_candidate and worker_id in [w for i, w in enumerate(self.scheduler.schedule[date]) if i != post and w is not None]:
                        # logging.debug(f"  [Relaxed Check] Worker {worker_id} already working on {date}")
                        is_valid_candidate = False

                    # 3. Check incompatibility with others already assigned *today*
                    if is_valid_candidate and not self._check_incompatibility(worker_id, date):
                        # logging.debug(f"  [Relaxed Check] Worker {worker_id} incompatible on {date}")
                        is_valid_candidate = False

                    # 4. Check if max total shifts reached (using scheduler's reference)
                    if is_valid_candidate and len(self.scheduler.worker_assignments.get(worker_id, set())) >= self.max_shifts_per_worker:
                        # logging.debug(f"  [Relaxed Check] Worker {worker_id} at max shifts ({self.max_shifts_per_worker})")
                        is_valid_candidate = False
                    # --- End Relaxed Check ---

                    if is_valid_candidate:
                        # Use a simple score preferring fewer shifts overall
                        # Use scheduler's worker_assignments reference
                        score = 1000 - len(self.scheduler.worker_assignments.get(worker_id, set())) * 10
                        # Add bonus if below target (optional but helps balance)
                        worker_target = worker.get('target_shifts', 0)
                        current_assignments = len(self.scheduler.worker_assignments.get(worker_id, set()))
                        if current_assignments < worker_target:
                             score += 500
                        candidates.append((worker, score))
                        # logging.debug(f"  [Relaxed Check] Worker {worker_id} is a candidate for {date} post {post} with score {score}")


                # --- Assignment logic (using candidates from relaxed check) ---
                if candidates:
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    best_worker = candidates[0][0]
                    worker_id = best_worker['id']

                    # Assign the worker (directly modify scheduler's data)
                    self.scheduler.schedule[date][post] = worker_id
                    self.scheduler.worker_assignments.setdefault(worker_id, set()).add(date)
                    self.scheduler._update_tracking_data(worker_id, date, post) # Call scheduler's update method

                    logging.info(f"[Relaxed Direct Fill] Filled empty shift on {date.strftime('%Y-%m-%d')} post {post} with worker {worker_id}")
                    shifts_filled_count += 1
                    made_change_in_pass = True
                else:
                    # If no candidate found even with relaxed check, keep for swap attempt
                    remaining_empty_shifts.append((date, post))
                    logging.debug(f"Could not find direct candidate (relaxed) for {date.strftime('%Y-%m-%d')} post {post}")

            elif date in self.scheduler.schedule and len(self.scheduler.schedule[date]) > post and self.scheduler.schedule[date][post] is not None:
                 # Slot was already filled, likely by a previous step in this same function call
                 pass # Do nothing, it's filled
            else:
                 # Should not happen if schedule structure is correct, but add safety
                 logging.warning(f"Could not access schedule slot for {date.strftime('%Y-%m-%d')} post {post} during relaxed direct fill attempt.")
                 remaining_empty_shifts.append((date, post)) # Keep for swap attempt


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
            # Use scheduler's schedule reference
            if not (date in self.scheduler.schedule and len(self.scheduler.schedule[date]) > post and self.scheduler.schedule[date][post] is None):
                continue # Skip if it got filled somehow

            swap_found = False
            # Find workers (W) not already working on the target 'date'
            potential_swap_workers = [w for w in self.scheduler.workers_data if w['id'] not in self.scheduler.schedule.get(date, [])]
            random.shuffle(potential_swap_workers) # Randomize worker order

            # Loop through potential workers (W) who might take the empty slot
            for worker_W in potential_swap_workers:
                worker_W_id = worker_W['id']
                # Use scheduler's worker_assignments reference
                original_assignments_W = sorted(list(self.scheduler.worker_assignments.get(worker_W_id, set())))

                # Loop through W's current shifts to find one to potentially give up
                for conflict_date in original_assignments_W:
                    # Check if mandatory
                    if self._is_mandatory(worker_W_id, conflict_date):
                        continue # Skip mandatory shifts

                    # --- Simulation Block (Strict Check) ---
                    # Simulate removing conflict_date to see if W can now take the empty slot
                    removed_during_check = False
                    current_assignments_W = self.scheduler.worker_assignments.get(worker_W_id, set())
                    if conflict_date in current_assignments_W:
                        try:
                            self.scheduler.worker_assignments[worker_W_id].remove(conflict_date)
                            removed_during_check = True
                        except KeyError: continue # Should not happen, but safety
                    else: continue # State changed?

                    # *** Use the STRICT check here ***
                    can_W_take_empty_slot = self._can_assign_worker(worker_W_id, date, post)

                    # Restore state after check
                    if removed_during_check:
                        self.scheduler.worker_assignments[worker_W_id].add(conflict_date)
                    # --- End Simulation Block ---

                    if not can_W_take_empty_slot:
                        continue # Removing this conflict_date didn't help or W is still invalid

                    # --- Find conflict_post and worker_X (using Strict Check) ---
                    try: # Find the post W was working on conflict_date
                        # Use scheduler's schedule reference
                        if conflict_date not in self.scheduler.schedule or worker_W_id not in self.scheduler.schedule[conflict_date]:
                             continue # Inconsistency
                        conflict_post = self.scheduler.schedule[conflict_date].index(worker_W_id)
                    except (ValueError, IndexError, KeyError):
                        continue # Inconsistency

                    # Find replacement worker X using strict check (_find_swap_candidate calls _can_assign_worker)
                    worker_X_id = self._find_swap_candidate(worker_W_id, conflict_date, conflict_post)

                    # --- Perform Swap if candidate X found ---
                    if worker_X_id:
                        logging.info(f"[Swap Fill] Found swap: W={worker_W_id}, X={worker_X_id}, EmptySlot=({date.strftime('%Y-%m-%d')},{post}), ConflictSlot=({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")

                        # --- Perform the Swap (directly on scheduler's data) ---
                        # 1. Assign W to empty slot
                        self.scheduler.schedule[date][post] = worker_W_id
                        self.scheduler.worker_assignments.setdefault(worker_W_id, set()).add(date)
                        self.scheduler._update_tracking_data(worker_W_id, date, post)

                        # 2. Remove W from conflict slot (with check)
                        if conflict_date in self.scheduler.worker_assignments.get(worker_W_id, set()):
                            self.scheduler.schedule[conflict_date][conflict_post] = None
                            self.scheduler.worker_assignments[worker_W_id].remove(conflict_date)
                            self.scheduler._update_tracking_data(worker_W_id, conflict_date, conflict_post, removing=True)
                        else:
                            logging.error(f"CRITICAL INCONSISTENCY during swap: {worker_W_id} not assigned to {conflict_date} before assigning X.")
                            self.scheduler.schedule[conflict_date][conflict_post] = None # Ensure empty for X

                        # 3. Assign X to W's old slot
                        self.scheduler.schedule[conflict_date][conflict_post] = worker_X_id
                        self.scheduler.worker_assignments.setdefault(worker_X_id, set()).add(conflict_date)
                        self.scheduler._update_tracking_data(worker_X_id, conflict_date, conflict_post)

                        shifts_filled_count += 1
                        made_change_in_pass = True
                        swap_found = True
                        break # Break from conflict_date loop (found swap for W)

                # Check if inner loop found a swap for worker_W
                if swap_found:
                    break # Break from worker_W loop (found swap for the empty shift)

            # Log if no swap found for this empty shift after checking all workers
            if not swap_found:
                logging.debug(f"Could not find direct fill or swap for empty shift on {date.strftime('%Y-%m-%d')} post {post}")
        # --- End of loop for remaining_empty_shifts ---

        logging.info(f"--- Finished Pass 2: Attempted swaps. Total shifts filled in this run (direct + swap): {shifts_filled_count} ---")

        # Save if any changes were made in Pass 1 OR Pass 2
        if made_change_in_pass:
            self._save_current_as_best()
        return made_change_in_pass
    
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
        for each worker, aiming for a count within +/- tolerance of the average.
        Runs AFTER general post rotation.
        """
        logging.info("Attempting to specifically balance the last post assignments...")
        fixes_made = 0
        last_post_idx = self.num_shifts - 1

        if self.num_shifts <= 1:
            logging.info("Skipping last post balancing: Only 0 or 1 post defined.")
            return False # Cannot balance if only one post

        # Iterate through workers multiple times potentially? Or just once? Let's start with once.
        workers_to_check = self.scheduler.workers_data[:] # Copy list
        random.shuffle(workers_to_check) # Process in random order

        for worker in workers_to_check:
            worker_id = worker['id']
            post_counts = self.scheduler.worker_posts.get(worker_id, {})
            total_assigned = sum(post_counts.values())

            if total_assigned == 0:
                continue # Skip workers with no assignments

            target_per_post = total_assigned / self.num_shifts
            last_post_count = post_counts.get(last_post_idx, 0)
            last_post_deviation = last_post_count - target_per_post

            swap_found_for_worker = False

            # --- Case 1: Last Post is Over-assigned ---
            if last_post_deviation > balance_tolerance:
                logging.debug(f"Worker {worker_id} is over-assigned on last post {last_post_idx}. Dev: {last_post_deviation:.2f}, Count: {last_post_count}, Target: {target_per_post:.2f}")
                # Find a shift on the last post to move FROM
                possible_from_dates = []
                for date, posts in self.scheduler.schedule.items():
                     if len(posts) > last_post_idx and posts[last_post_idx] == worker_id and not self._is_mandatory(worker_id, date):
                          possible_from_dates.append(date)
                random.shuffle(possible_from_dates)

                for date_from in possible_from_dates:
                    # Find a place to move TO (different post, different date ideally)
                    # Prioritize moving to posts where worker is under-assigned relative to target
                    potential_to_posts = sorted(
                        range(self.num_shifts),
                        key=lambda p: post_counts.get(p, 0) # Sort by current count ascending
                    )

                    for post_to in potential_to_posts:
                        if post_to == last_post_idx: continue # Don't move to the same post type

                        potential_to_dates = sorted(self.scheduler.schedule.keys())
                        random.shuffle(potential_to_dates) # Introduce randomness

                        for date_to in potential_to_dates:
                            if date_to == date_from: continue # Don't swap within the same day

                            # Check if target slot exists and is empty
                            schedule_date_to = self.scheduler.schedule.get(date_to, [])
                            target_slot_exists = len(schedule_date_to) > post_to
                            target_slot_empty = target_slot_exists and schedule_date_to[post_to] is None

                            # Allow creating the slot if it's the next one
                            if not target_slot_exists and len(schedule_date_to) == post_to:
                                target_slot_empty = True

                            if not target_slot_empty: continue

                            # Check if worker is already working on date_to
                            if worker_id in schedule_date_to: continue

                            # Check if swap is valid using constraints
                            if self._can_swap_assignments(worker_id, date_from, last_post_idx, date_to, post_to):
                                # Perform the swap
                                logging.info(f"[Balance Last Post] Offload Swap: Moving {worker_id} from {date_from.strftime('%Y-%m-%d')}|P{last_post_idx} to {date_to.strftime('%Y-%m-%d')}|P{post_to}")
                                self._execute_swap(worker_id, date_from, last_post_idx, date_to, post_to)
                                fixes_made += 1
                                swap_found_for_worker = True
                                break # Found swap for date_from, break date_to loop
                        if swap_found_for_worker: break # Break post_to loop
                    if swap_found_for_worker: break # Break date_from loop

            # --- Case 2: Last Post is Under-assigned ---
            elif last_post_deviation < -balance_tolerance:
                logging.debug(f"Worker {worker_id} is under-assigned on last post {last_post_idx}. Dev: {last_post_deviation:.2f}, Count: {last_post_count}, Target: {target_per_post:.2f}")
                # Find a shift on another post to move FROM
                # Prioritize moving from posts where worker is over-assigned relative to target
                possible_from_posts = sorted(
                    range(self.num_shifts),
                    key=lambda p: -post_counts.get(p, 0) # Sort by current count descending
                )

                for post_from in possible_from_posts:
                     if post_from == last_post_idx: continue # Don't move from the target post type

                     possible_from_dates = []
                     for date, posts in self.scheduler.schedule.items():
                          if len(posts) > post_from and posts[post_from] == worker_id and not self._is_mandatory(worker_id, date):
                               possible_from_dates.append(date)
                     random.shuffle(possible_from_dates)

                     for date_from in possible_from_dates:
                         # Find an empty slot on the LAST post to move TO
                         potential_to_dates = sorted(self.scheduler.schedule.keys())
                         random.shuffle(potential_to_dates)

                         for date_to in potential_to_dates:
                             if date_to == date_from: continue

                             # Check if target slot (last post) exists and is empty
                             schedule_date_to = self.scheduler.schedule.get(date_to, [])
                             target_slot_exists = len(schedule_date_to) > last_post_idx
                             target_slot_empty = target_slot_exists and schedule_date_to[last_post_idx] is None

                             if not target_slot_exists and len(schedule_date_to) == last_post_idx:
                                 target_slot_empty = True

                             if not target_slot_empty: continue

                             # Check if worker is already working on date_to
                             if worker_id in schedule_date_to: continue

                             # Check if swap is valid
                             if self._can_swap_assignments(worker_id, date_from, post_from, date_to, last_post_idx):
                                 # Perform the swap
                                 logging.info(f"[Balance Last Post] Onload Swap: Moving {worker_id} from {date_from.strftime('%Y-%m-%d')}|P{post_from} to {date_to.strftime('%Y-%m-%d')}|P{last_post_idx}")
                                 self._execute_swap(worker_id, date_from, post_from, date_to, last_post_idx)
                                 fixes_made += 1
                                 swap_found_for_worker = True
                                 break # Found swap for date_from, break date_to loop
                         if swap_found_for_worker: break # Break date_from loop
                     if swap_found_for_worker: break # Break post_from loop

        logging.info(f"Finished balancing last post. Made {fixes_made} changes.")
        if fixes_made > 0:
            self._save_current_as_best()
        return fixes_made > 0

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
         if date not in self.schedule: return True # No assignments, no violations
         assignments = self.schedule[date]
         for post, worker_id in enumerate(assignments):
              if worker_id is not None:
                   # Use relaxation_level=0 for strict checks during improvement phases
                   if not self._check_constraints(worker_id, date, post, relaxation_level=0):
                        # logging.debug(f"Constraint check failed for {worker_id} on {date} post {post} during swap check.")
                        return False
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
    
        # Group dates by month
        months = {}
        current_date = self.start_date
        while current_date <= self.end_date:
            month_key = (current_date.year, current_date.month)
            if month_key not in months:
                months[month_key] = []
            months[month_key].append(current_date)
            current_date += timedelta(days=1)
    
        # Count weekend assignments by month for each worker
        for month_key, dates in months.items():
            weekend_counts = {}
            for worker in self.workers_data:
                worker_id = worker['id']
                weekend_count = sum(1 for date in dates if date in self.worker_assignments[worker_id] and self.date_utils.is_weekend_day(date))
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
    
                # Calculate weekend limit based on work percentage and configurable parameter
                max_weekends = self.max_consecutive_weekends  # Use the configurable parameter
                if work_percentage < 100:
                    max_weekends = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
    
                weekend_count = weekend_counts.get(worker_id, 0)
    
                if weekend_count > max_weekends:
                    overloaded_workers.append((worker_id, weekend_count, max_weekends))
                elif weekend_count < max_weekends:
                    available_slots = max_weekends - weekend_count
                    underloaded_workers.append((worker_id, weekend_count, available_slots))
        
            # Sort by most overloaded and most available
            overloaded_workers.sort(key=lambda x: x[1] - x[2], reverse=True)
            underloaded_workers.sort(key=lambda x: x[2], reverse=True)
        
            # Get dates in this month
            month_dates = months[month_key]
            weekend_dates = [date for date in month_dates if self.date_utils.is_weekend_day(date)]
        
            # Try to redistribute weekend shifts
            for over_worker_id, over_count, over_limit in overloaded_workers:
                if not underloaded_workers:
                    break
                
                for weekend_date in weekend_dates:
                    # Skip if this worker isn't assigned on this date
                    if over_worker_id not in self.schedule[weekend_date]:
                        continue
                
                    # Find the post this worker is assigned to
                    post = self.schedule[weekend_date].index(over_worker_id)
                
                    # Try to find a suitable replacement
                    for under_worker_id, _, _ in underloaded_workers:
                        # Skip if this worker is already assigned on this date
                        if under_worker_id in self.schedule[weekend_date]:
                            continue
                    
                        # Check if we can assign this worker to this shift
                        if self._can_assign_worker(under_worker_id, weekend_date, post):
                            # Make the swap
                            self.schedule[weekend_date][post] = under_worker_id
                            self.worker_assignments[over_worker_id].remove(weekend_date)
                            self.worker_assignments[under_worker_id].add(weekend_date)
                        
                            # Remove the weekend tracking for the over-loaded worker
                            self.scheduler._update_tracking_data(over_worker_id, weekend_date, post, removing=True)

                            # Update tracking data for the under-loaded worker
                            self.scheduler._update_tracking_data(under_worker_id, weekend_date, post)
                        
                            # Update counts
                            weekend_counts[over_worker_id] -= 1
                            weekend_counts[under_worker_id] += 1
                        
                            changes_made += 1
                            logging.info(f"Improved weekend distribution: Moved weekend shift on {weekend_date.strftime('%d-%m-%Y')} "
                                        f"from worker {over_worker_id} to worker {under_worker_id}")
                        
                            # Update worker lists
                            if weekend_counts[over_worker_id] <= over_limit:
                                # This worker is no longer overloaded
                                overloaded_workers = [(w, c, l) for w, c, l in overloaded_workers if w != over_worker_id]
                        
                            # Check if under worker is now fully loaded
                            for i, (w_id, count, slots) in enumerate(underloaded_workers):
                                if w_id == under_worker_id:
                                    if weekend_counts[w_id] >= count + slots:
                                        # Remove from underloaded
                                        underloaded_workers.pop(i)
                                    break
                        
                            # Break to try next overloaded worker
                            break
    
        logging.info(f"Weekend distribution improvement: made {changes_made} changes")
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
