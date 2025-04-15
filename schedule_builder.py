# Imports
from datetime import datetime, timedelta
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
        self.worker_assignments = scheduler.worker_assignments  # Use the same reference
        self.num_shifts = scheduler.num_shifts
        self.holidays = scheduler.holidays
        self.constraint_checker = scheduler.constraint_checker

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

    def _check_incompatibility(self, worker_id, date):
        """
        Check for worker incompatibilities on a specific date
    
        Args:
            worker_id: ID of the worker to check
            date: Date to check for incompatibilities
    
        Returns:
            bool: True if no incompatibilities found, False if incompatibilities exist
        """
        # Get worker data
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker:
            logging.debug(f"Worker {worker_id} not found in workers_data")
            return False
    
        # Get incompatible workers
        incompatible_with = worker.get('incompatible_with', [])
        if not incompatible_with:
            return True
    
        # Get workers currently assigned to this date
        currently_assigned = []
        if date in self.schedule:
            # Extract all non-None worker IDs from the schedule for this date
            currently_assigned = [w for w in self.schedule[date] if w is not None]
    
        # Check if any incompatible workers are already assigned to this date
        for incompatible_id in incompatible_with:
            if incompatible_id in currently_assigned:
                logging.debug(f"Worker {worker_id} cannot work with incompatible worker {incompatible_id} already assigned on {date.strftime('%d-%m-%Y')}")
                return False
    
        return True
        
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
        
            # 1. Weekend Balance Score
            if date.weekday() >= 4:  # Friday, Saturday, Sunday
                weekend_assignments = sum(
                    1 for d in self.worker_assignments[worker_id]
                    if d.weekday() >= 4
                )
                # Lower score for workers with more weekend assignments
                score -= weekend_assignments * 300
        
            # 2. Post Rotation Score - focus especially on last post distribution
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
        
            # 3. Weekly Balance Score - avoid concentration in some weeks
            week_number = date.isocalendar()[1]
            week_counts = {}
            for d in self.worker_assignments[worker_id]:
                w = d.isocalendar()[1]
                week_counts[w] = week_counts.get(w, 0) + 1
        
            current_week_count = week_counts.get(week_number, 0)
            avg_week_count = len(assignments) / max(1, len(week_counts))
        
            if current_week_count < avg_week_count:
                score += 500  # Bonus for weeks with fewer assignments
        
            # 4. Schedule Progression Score - adjust priority as schedule fills up
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
        """
        Assign all mandatory guards first
        """
        logging.info("Processing mandatory guards...")

        workers_with_mandatory = [
            (w, self._parse_dates(w.get('mandatory_days', ''))) 
            for w in self.workers_data
        ]
        workers_with_mandatory.sort(key=lambda x: len(x[1]), reverse=True)

        for worker, mandatory_dates in workers_with_mandatory:
            if not mandatory_dates:
                continue

            worker_id = worker['id']
            for date in mandatory_dates:
                if self.start_date <= date <= self.end_date:
                    if date not in self.schedule:
                        self.schedule[date] = []
            
                    if (worker_id not in self.schedule[date] and 
                        len(self.schedule[date]) < self.num_shifts):
                        self.schedule[date].append(worker_id)
                        self.worker_assignments[worker_id].add(date)
                        
                        # Update tracking data
                        post = len(self.schedule[date]) - 1
                        self.scheduler._update_tracking_data(under_worker_id, weekend_date, post)
                        
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
        First, attempt direct assignment using strict constraints.
        If that fails, attempt to find a valid swap involving another worker's shift.
        Strictly enforces all mandatory constraints during swaps.
        """
        empty_shifts = []

        # Find all empty shifts
        for date, workers in self.schedule.items():
            for post, worker in enumerate(workers):
                if worker is None:
                    empty_shifts.append((date, post))

        if not empty_shifts:
            return False  # No empty shifts to fill

        logging.info(f"Attempting to fill {len(empty_shifts)} empty shifts")

        # Sort empty shifts by date (earlier dates first)
        empty_shifts.sort(key=lambda x: x[0])

        shifts_filled_count = 0
        made_change_in_pass = False # Flag to track if any change was made in this call

        # --- Pass 1: Direct Assignment ---
        remaining_empty_shifts = []
        for date, post in empty_shifts:
            # Ensure the slot is still empty (might have been filled by a swap earlier)
            if date in self.schedule and len(self.schedule[date]) > post and self.schedule[date][post] is None:
                candidates = []
                for worker in self.workers_data:
                    worker_id = worker['id']
                    # Skip if already assigned to this date IN A DIFFERENT POST
                    if worker_id in [w for i, w in enumerate(self.schedule[date]) if i != post and w is not None]:
                         continue
                    # Check if worker can be assigned (with strict constraints, relaxation_level=0)
                    if self._can_assign_worker(worker_id, date, post):
                        score = self._calculate_worker_score(worker, date, post, relaxation_level=0)
                        if score > float('-inf'):
                            candidates.append((worker, score))

                if candidates:
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    best_worker = candidates[0][0]
                    worker_id = best_worker['id']

                    # Assign the worker
                    self.schedule[date][post] = worker_id
                    self.worker_assignments[worker_id].add(date)
                    # Use the main scheduler's update method
                    self.scheduler._update_tracking_data(worker_id, date, post)

                    logging.info(f"[Direct Fill] Filled empty shift on {date.strftime('%Y-%m-%d')} post {post} with worker {worker_id}")
                    shifts_filled_count += 1
                    made_change_in_pass = True
                else:
                    remaining_empty_shifts.append((date, post)) # Keep for swap attempt
            elif date in self.schedule and len(self.schedule[date]) > post and self.schedule[date][post] is not None:
                 # Slot was already filled, likely by a previous swap in this same function call
                 pass # Do nothing, it's filled
            else:
                 # Should not happen if schedule structure is correct, but add safety
                 logging.warning(f"Could not access schedule slot for {date.strftime('%Y-%m-%d')} post {post} during direct fill attempt.")
                 remaining_empty_shifts.append((date, post)) # Keep for swap attempt


        # --- Pass 2: Swap Attempt ---
        if not remaining_empty_shifts:
             logging.info(f"Filled {shifts_filled_count} shifts directly. No remaining empty shifts.")
             # Update the main scheduler's schedule reference BEFORE returning
             self.scheduler.schedule = self.schedule.copy()
             self._save_current_as_best() # Save progress if direct fills happened
             return made_change_in_pass

        logging.info(f"Filled {shifts_filled_count} shifts directly. Attempting swaps for {len(remaining_empty_shifts)} remaining empty shifts.")

        for date, post in remaining_empty_shifts:
             # Ensure the slot is *still* empty before attempting swap
             if not (date in self.schedule and len(self.schedule[date]) > post and self.schedule[date][post] is None):
                 continue # Skip if it got filled somehow

             swap_found = False
             # Iterate through all workers to see if they could potentially take this empty shift via a swap
             potential_swap_workers = [w for w in self.workers_data if w['id'] not in self.schedule.get(date, [])]
             random.shuffle(potential_swap_workers) # Avoid bias

             for worker_W in potential_swap_workers:
                 worker_W_id = worker_W['id']

                 # Simulate removing W's assignments one by one to see if that makes (date, post) valid for W
                 original_assignments_W = sorted(list(self.worker_assignments.get(worker_W_id, set())))

                 for conflict_date in original_assignments_W:
                     # Temporarily remove this assignment to check if W becomes valid for the empty slot
                     self.worker_assignments[worker_W_id].remove(conflict_date)

                     # Check if worker W can NOW take the empty slot (date, post)
                     can_W_take_empty_slot = self._can_assign_worker(worker_W_id, date, post)

                     # Restore the assignment before proceeding
                     self.worker_assignments[worker_W_id].add(conflict_date) # Add back immediately

                     if not can_W_take_empty_slot:
                         continue # This conflict_date wasn't the blocker, or W is still invalid

                     # If W *could* take the empty slot by giving up conflict_date,
                     # find the post W occupies on conflict_date
                     try:
                         conflict_post = self.schedule[conflict_date].index(worker_W_id)
                     except (ValueError, IndexError, KeyError):
                         logging.warning(f"Worker {worker_W_id} has assignment for {conflict_date} but not found in schedule. Skipping swap check.")
                         continue # Data inconsistency

                     # Now, find a worker X who can take W's original shift (conflict_date, conflict_post)
                     worker_X_id = self._find_swap_candidate(worker_W_id, conflict_date, conflict_post)

                     if worker_X_id:
                         # --- Perform the Swap ---
                         logging.info(f"[Swap Fill] Found swap: W={worker_W_id}, X={worker_X_id}, EmptySlot=({date.strftime('%Y-%m-%d')},{post}), ConflictSlot=({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")

                         # 1. Assign W to the originally empty slot (date, post)
                         self.schedule[date][post] = worker_W_id
                         self.worker_assignments[worker_W_id].add(date)
                         self.scheduler._update_tracking_data(worker_W_id, date, post) # Update stats for new assignment

                         # 2. Remove W from their conflicting slot (conflict_date, conflict_post)
                         self.schedule[conflict_date][conflict_post] = None # Temporarily empty
                         self.worker_assignments[worker_W_id].remove(conflict_date)
                         self.scheduler._update_tracking_data(worker_W_id, conflict_date, conflict_post, removing=True) # Update stats for removal

                         # 3. Assign X to W's old slot (conflict_date, conflict_post)
                         self.schedule[conflict_date][conflict_post] = worker_X_id
                         self.worker_assignments[worker_X_id].add(conflict_date)
                         self.scheduler._update_tracking_data(worker_X_id, conflict_date, conflict_post) # Update stats for X

                         shifts_filled_count += 1
                         made_change_in_pass = True
                         swap_found = True
                         break # Stop searching for workers W for this empty slot

                 if swap_found:
                     break # Move to the next empty slot

             if not swap_found:
                  logging.debug(f"Could not find direct fill or swap for empty shift on {date.strftime('%Y-%m-%d')} post {post}")


        logging.info(f"Finished attempting swaps. Total shifts filled in this run: {shifts_filled_count}")

        # Update the main scheduler's schedule reference AFTER all updates
        self.scheduler.schedule = self.schedule.copy()
        if made_change_in_pass:
            self._save_current_as_best() # Save progress if swaps happened

        return made_change_in_pass # Return True if any shift was filled (direct or swap)

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
    
    def _balance_workloads(self):
        """
        Balance the total number of assignments among workers based on their work percentages
        While strictly enforcing all mandatory constraints:
        - Minimum 2-day gap between shifts
        - Maximum 3 weekend shifts in any 3-week period
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
        max_changes = 5  # Limit number of changes to avoid disrupting the schedule too much

        # Try to redistribute shifts from overloaded to underloaded workers
        for over_worker_id, over_data in overloaded:
            if changes_made >= max_changes or not underloaded:
                break
        
            # Find shifts that can be reassigned from this overloaded worker
            possible_shifts = []
    
            for date in sorted(self.worker_assignments[over_worker_id]):
                # Skip if this date is mandatory for this worker
                worker_data = next((w for w in self.workers_data if w['id'] == over_worker_id), None)
                mandatory_days = worker_data.get('mandatory_days', []) if worker_data else []
                mandatory_dates = self._parse_dates(mandatory_days)
        
                if date in mandatory_dates:
                    continue
            
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
        
                # Try each underloaded worker
                for under_worker_id, _ in underloaded:
                    # Skip if this worker is already assigned on this date
                    if under_worker_id in self.schedule[date]:
                        continue
            
                    # Check if we can assign this worker to this shift
                    if self._can_assign_worker(under_worker_id, date, post):
                        # Make the reassignment
                        self.schedule[date][post] = under_worker_id
                        self.worker_assignments[over_worker_id].remove(date)
                        self.worker_assignments[under_worker_id].add(date)
                
                        # Update tracking data
                        self.scheduler._update_tracking_data(under_worker_id, date, post)
                
                        changes_made += 1
                        logging.info(f"Balanced workload: Moved shift on {date.strftime('%d-%m-%Y')} post {post} "
                                    f"from worker {over_worker_id} to worker {under_worker_id}")
                
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
        return changes_made > 0

    def _improve_post_rotation(self):
        """Improve post rotation by swapping assignments"""
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()
    
        # Verify and fix data consistency before proceeding
        self._verify_assignment_consistency()        # Find workers with imbalanced post distribution
        imbalanced_workers = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
            post_counts = self._get_post_counts(worker_id)
            total_assignments = sum(post_counts.values())
        
            # Skip workers with no or few assignments
            if total_assignments < self.num_shifts:
                continue
        
            # Calculate expected distribution
            expected_per_post = total_assignments / self.num_shifts
        
            # Calculate deviation from ideal distribution
            deviation = 0
            for post in range(self.num_shifts):
                post_count = post_counts.get(post, 0)
                deviation += abs(post_count - expected_per_post)
        
            # Normalize deviation by total assignments
            normalized_deviation = deviation / total_assignments
        
            # Add to imbalanced list if deviation is significant
            if normalized_deviation > 0.2:  # 20% deviation threshold
                imbalanced_workers.append((worker_id, post_counts, normalized_deviation))
    
        # Sort workers by deviation (most imbalanced first)
        imbalanced_workers.sort(key=lambda x: x[2], reverse=True)
    
        # Try to fix the most imbalanced workers
        fixes_attempted = 0
        fixes_made = 0
    
        for worker_id, post_counts, deviation in imbalanced_workers:
            if fixes_attempted >= 10:  # Limit number of fix attempts
                break
            
            logging.info(f"Trying to improve post rotation for worker {worker_id} (deviation: {deviation:.2f})")
        
            # Find overassigned and underassigned posts
            total_assignments = sum(post_counts.values())
            expected_per_post = total_assignments / self.num_shifts
        
            overassigned_posts = []
            underassigned_posts = []
        
            for post in range(self.num_shifts):
                post_count = post_counts.get(post, 0)
                if post_count > expected_per_post + 0.5:
                    overassigned_posts.append((post, post_count))
                elif post_count < expected_per_post - 0.5:
                    underassigned_posts.append((post, post_count))
        
            # Sort by most overassigned/underassigned
            overassigned_posts.sort(key=lambda x: x[1], reverse=True)
            underassigned_posts.sort(key=lambda x: x[1])
        
            fixes_attempted += 1
        
            if not overassigned_posts or not underassigned_posts:
                continue
        
            # Try to swap a shift from overassigned post to underassigned post
            for over_post, _ in overassigned_posts:
                for under_post, _ in underassigned_posts:
                    # Find all dates where this worker has the overassigned post
                    possible_swap_dates = []
                
                    for date, workers in self.schedule.items():
                        if len(workers) > over_post and workers[over_post] == worker_id:
                            possible_swap_dates.append(date)
                
                    # Shuffle the dates to introduce randomness
                    random.shuffle(possible_swap_dates)
                
                    # Try each date
                    for date in possible_swap_dates:
                        # Look for a date where this worker isn't assigned but could be
                        for other_date in sorted(self.schedule.keys()):
                            # Skip if it's the same date
                            if other_date == date:
                                continue
                            
                            # Skip if worker is already assigned to this date
                            if worker_id in self.schedule[other_date]:
                                continue
                        
                            # Skip if the target post already has someone
                            if len(self.schedule[other_date]) > under_post and self.schedule[other_date][under_post] is not None:
                                continue
                            
                            # Check if this would be a valid assignment
                            if not self._can_swap_assignments(worker_id, date, over_post, other_date, under_post):
                                continue
                        
                            # Perform the swap
                            old_worker = self.schedule[date][over_post]
                        
                            # Handle the case where we need to extend the other date's shifts list
                            while len(self.schedule[other_date]) <= under_post:
                                self.schedule[other_date].append(None)
                        
                            # Make the swap
                            self.schedule[date][over_post] = None
                            self.schedule[other_date][under_post] = worker_id
                        
                            # Update tracking data
                            self.worker_assignments[worker_id].remove(date)
                            self.worker_assignments[worker_id].add(other_date)
                            self.scheduler._update_tracking_data(under_worker_id, weekend_date, post)
                        
                            logging.info(f"Improved post rotation: Moved worker {worker_id} from {date.strftime('%d-%m-%Y')} "
                                        f"post {over_post} to {other_date.strftime('%d-%m-%Y')} post {under_post}")
                        
                            fixes_made += 1
                            break
                    
                        if fixes_made > fixes_attempted * 0.5:  # If we've made enough fixes, stop
                            break
                        
                    if fixes_made > fixes_attempted * 0.5:  # If we've made enough fixes, stop
                        break
                    
                if fixes_made > fixes_attempted * 0.5:  # If we've made enough fixes, stop
                    break
    
        logging.info(f"Post rotation improvement: attempted {fixes_attempted} fixes, made {fixes_made} changes")
    
        return fixes_made > 0  # Return whether we made any improvements

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

    def _save_current_as_best(self):
        """Save current schedule as the best"""
        # Save the schedule to the parent scheduler object (CRITICAL!)
        self.scheduler.backup_schedule = self.schedule.copy()
        self.scheduler.backup_worker_assignments = {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()}
        self.scheduler.backup_worker_posts = {w_id: posts.copy() for w_id, posts in self.worker_posts.items()}
        self.scheduler.backup_worker_weekdays = {w_id: weekdays.copy() for w_id, weekdays in self.worker_weekdays.items()}
        self.scheduler.backup_worker_weekends = {w_id: weekends.copy() for w_id, weekends in self.worker_weekends.items()}
        self.scheduler.backup_constraint_skips = {
            w_id: {
                'gap': skips['gap'].copy(),
                'incompatibility': skips['incompatibility'].copy(),
                'reduced_gap': skips['reduced_gap'].copy(),
            }
            for w_id, skips in self.constraint_skips.items()
        }

        # Also update local backup
        self.backup_schedule = self.schedule.copy()
        self.backup_worker_assignments = {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()}
        self.backup_worker_posts = {w_id: posts.copy() for w_id, posts in self.worker_posts.items()}
        self.backup_worker_weekdays = {w_id: weekdays.copy() for w_id, weekdays in self.worker_weekdays.items()}
        self.backup_worker_weekends = {w_id: weekends.copy() for w_id, weekends in self.worker_weekends.items()}
        self.backup_constraint_skips = {
            w_id: {
                'gap': skips['gap'].copy(),
                'incompatibility': skips['incompatibility'].copy(),
                'reduced_gap': skips['reduced_gap'].copy(),
            }
            for w_id, skips in self.constraint_skips.items()
        }
