# Imports
from datetime import datetime, timedelta
import logging
from typing import TYPE_CHECKING
from exceptions import SchedulerError
if TYPE_CHECKING:
    from scheduler import Scheduler


class ConstraintChecker:
    "   ""Handles all constraint checking logic for the scheduler"""
    
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
    
        logging.info("ConstraintChecker initialized")
    
    def _are_workers_incompatible(self, worker1_id, worker2_id):
        """
        Check if two workers are incompatible based on incompatibility property or list.
        """
        try:
            if worker1_id == worker2_id:
                return False  # A worker isn't incompatible with themselves
            
            # Get workers' data
            worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
            worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
    
            if not worker1 or not worker2:
                return False
    
            # Case 1: Check 'is_incompatible' property (both must have it for incompatibility)
            has_incompatibility1 = worker1.get('is_incompatible', False)
            has_incompatibility2 = worker2.get('is_incompatible', False)
            if has_incompatibility1 and has_incompatibility2:
                logging.debug(f"Workers {worker1_id} and {worker2_id} are incompatible (both marked incompatible)")
                return True
    
            return False
    
        except Exception as e:
            logging.error(f"Error checking worker incompatibility: {str(e)}")
            return False  # Default to compatible in case of error
        
    def _check_incompatibility(self, worker_id, date):
        """Check if worker is incompatible with already assigned workers"""
        try:
            if date not in self.schedule:
                return True

            # Get the worker's data
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                logging.error(f"Worker {worker_id} not found in workers_data")
                return False
    
            # Check against all workers already assigned to this date
            for assigned_id in self.schedule[date]:
                if assigned_id is None:
                    continue
                
                if self._are_workers_incompatible(worker_id, assigned_id):
                    logging.warning(f"Workers {worker_id} and {assigned_id} are incompatible - cannot assign to {date}")
                    return False

            return True

        except Exception as e:
            logging.error(f"Error checking incompatibility for worker {worker_id}: {str(e)}")
            return False

    def _check_gap_constraint(self, worker_id, date, min_gap):
        """Check minimum gap between assignments"""
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        work_percentage = worker.get('work_percentage', 100) if worker else 100
    
        # Use consistent gap rules
        actual_min_gap = 3 if work_percentage < 100 else 2
    
        assignments = sorted(self.worker_assignments[worker_id])
        if assignments:
            for prev_date in assignments:
                days_between = abs((date - prev_date).days)
            
                # Basic gap check
                if days_between < actual_min_gap:
                    return False
                
                # Special rule for full-time workers: Prevent Friday + Monday assignments
                if work_percentage >= 100:
                    if ((prev_date.weekday() == 4 and date.weekday() == 0) or 
                        (date.weekday() == 4 and prev_date.weekday() == 0)):
                        if days_between == 3:  # The gap between Friday and Monday
                            return False
            
                # Prevent same day of week in consecutive weeks
                if days_between in [7, 14, 21]:
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
                d for d in self.worker_assignments[worker_id] 
                if (d.weekday() >= 4 or d in self.holidays)
            ]
        
            if date not in weekend_assignments:
                weekend_assignments.append(date)
        
            weekend_assignments.sort()  # Sort by date
        
            # CRITICAL: Still maintain the overall limit, but with more flexibility
            # at higher relaxation levels
            max_window_size = 21  # 3 weeks is the default
            max_weekend_count = 3  # Maximum 3 weekend shifts in the window
        
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
        
    def _has_three_consecutive_weekends(self, worker_id, date=None):
        """
        Validation method to check if a worker has three or more consecutive weekends
        Used for schedule validation rather than assignment decisions
        """
        try:
            # Get all weekend dates for this worker
            weekend_dates = sorted([
                d for d in self.worker_assignments[worker_id] 
                if (d.weekday() >= 4 or d in self.holidays or  # Include Friday, Saturday, Sunday and holidays
                    (d + timedelta(days=1)) in self.holidays)  # Include pre-holidays
            ])
    
            if date and (date.weekday() >= 4 or date in self.holidays or 
                        (date + timedelta(days=1)) in self.holidays):
                weekend_dates.append(date)
                weekend_dates.sort()
            
            # Group dates by weekend (assuming Sat/Sun pairs)
            weekends = []
            current_weekend = []
        
            for d in weekend_dates:
                if not current_weekend or (d - current_weekend[-1]).days <= 1:
                    current_weekend.append(d)
                else:
                    weekends.append(min(current_weekend))  # Store start of weekend
                    current_weekend = [d]
                
            if current_weekend:
                weekends.append(min(current_weekend))
            
            # Include potential new date if it's a weekend day
            if date and (date.weekday() >= 4 or date in self.holidays or 
                       (date + timedelta(days=1)) in self.holidays):
                if date not in weekend_dates:
                    weekend_dates.append(date)
                    weekend_dates.sort()
        
            # For consistency, use a 21-day window approach matching _would_exceed_weekend_limit
            for check_date in weekend_dates:
                window_start = check_date - timedelta(days=10)  # 10 days before
                window_end = check_date + timedelta(days=10)    # 10 days after
            
                # Count weekend days in this window
                window_weekend_count = sum(
                    1 for d in weekend_dates
                    if window_start <= d <= window_end
                )
            
                if window_weekend_count > 3:
                    return True  # More than 3 weekend shifts in a 3-week period
                
            return False
        
        except Exception as e:
            logging.error(f"Error checking consecutive weekends: {str(e)}")
            return True  # Fail safe
        
    def _is_worker_unavailable(self, worker_id, date):
        """
        Check if worker is unavailable on a specific date
        """
        try:
            worker = next(w for w in self.workers_data if w['id'] == worker_id)
            
            # Check days off
            if worker.get('days_off'):
                off_periods = self._parse_date_ranges(worker['days_off'])
                if any(start <= date <= end for start, end in off_periods):
                    logging.debug(f"Worker {worker_id} is off on {date}")
                    return True

            # Check work periods
            if worker.get('work_periods'):
                work_periods = self._parse_date_ranges(worker['work_periods'])
                if not any(start <= date <= end for start, end in work_periods):
                    logging.debug(f"Worker {worker_id} is not in work period on {date}")
                    return True

            # Check if worker is already assigned for this date
            if date in self.worker_assignments[worker_id]:
                logging.debug(f"Worker {worker_id} is already assigned on {date}")
                return True

            # NEW CHECK: If this is a weekend day, check if worker already has 3 weekend shifts in any 3-week period
            if self.date_utils.is_weekend_day(date, self.holidays):
                # Only perform this check for weekend days to improve performance
                weekend_dates = sorted([
                    d for d in self.worker_assignments[worker_id] 
                    if (d.weekday() >= 4 or d in self.holidays or 
                        (d + timedelta(days=1)) in self.holidays)
                ])
            
                # For each existing weekend assignment, check if it forms part of 3 consecutive weekends
                for check_date in weekend_dates:
                    window_start = check_date - timedelta(days=10)  # 10 days before
                    window_end = check_date + timedelta(days=10)    # 10 days after
                
                    # If the date we're considering is within this window
                    if window_start <= date <= window_end:
                        # Count weekend days in this window (excluding the new date)
                        window_weekend_count = sum(
                            1 for d in weekend_dates
                            if window_start <= d <= window_end
                        )
                    
                        # If worker already has 3 weekend shifts in this window, they're unavailable
                        if window_weekend_count >= 3:
                            logging.debug(f"Worker {worker_id} already has 3 weekend shifts in 3-week window around {check_date}")
                            return True

            return False

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
            return True
        
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
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                logging.debug(f"- Failed: Max shifts reached ({self.max_shifts_per_worker})")
                return False

            # 3. Check availability
            if self._is_worker_unavailable(worker_id, date):
                logging.debug(f"- Failed: Worker unavailable")
                return False

            # 4. CRITICAL: Check minimum gap - NEVER RELAX THIS
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if assignments:
                for prev_date in assignments:
                    days_since = abs((date - prev_date).days)
                    if days_since < 2:  # STRICT MINIMUM: 2 days
                        logging.debug(f"- Failed: Insufficient gap ({days_since} days)")
                        return False

            # 5. Check monthly targets
            month_key = f"{date.year}-{date.month:02d}"
            if hasattr(self, 'monthly_targets') and month_key in self.monthly_targets.get(worker_id, {}):
                current_month_assignments = sum(1 for d in self.worker_assignments[worker_id] 
                                           if d.strftime("%Y-%m") == date.strftime("%Y-%m"))
                if current_month_assignments >= self.monthly_targets[worker_id][month_key]:
                    logging.debug(f"- Failed: Monthly target reached")
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
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        work_percentage = float(worker.get('work_percentage', 100))

        # Basic availability checks (never skipped)
        if date in self.worker_assignments[worker_id]:
            return False, "already assigned"

        if self._is_worker_unavailable(worker_id, date):
            return False, "unavailable"

        # Gap constraints
        if not skip_constraints:
            min_gap = 3 if try_part_time and work_percentage < 100 else 2
            if not self._check_gap_constraint(worker_id, date, min_gap):
                return False, f"gap constraint ({min_gap} days)"

        # Incompatibility constraints
        if not skip_constraints and not self._check_incompatibility(worker_id, date):
            return False, "incompatibility"

        # Weekend constraints
        if self.date_utils.is_weekend_day(date, self.holidays):
            if self._has_three_consecutive_weekends(worker_id, date):
                return False, "three consecutive weekends"

        return True, "" 
     
    def _check_day_compatibility(self, worker_id, date):
        """Check if worker is compatible with all workers already assigned to this date"""
        if date not in self.schedule:
            return True
        
        for assigned_worker in self.schedule[date]:
            if self._are_workers_incompatible(worker_id, assigned_worker):
                logging.debug(f"Worker {worker_id} is incompatible with assigned worker {assigned_worker}")
                return False
        return True

    def _check_monthly_balance(self, worker_id, date):
        """
        Check if assigning this date would maintain monthly balance based on available days
        """
        try:
            # Get monthly distribution
            distribution = {}
            month_days = self._get_schedule_months()  # This returns a dict, not an int
        
            # Count current assignments
            for assigned_date in self.worker_assignments[worker_id]:
                month_key = f"{assigned_date.year}-{assigned_date.month:02d}"
                distribution[month_key] = distribution.get(month_key, 0) + 1
    
            # Add potential new assignment
            month_key = f"{date.year}-{date.month:02d}"
            new_distribution = distribution.copy()
            new_distribution[month_key] = new_distribution.get(month_key, 0) + 1

            if new_distribution:
                # Calculate ratios of shifts per available day for each month
                ratios = {}
                for m_key, shifts in new_distribution.items():
                    if m_key in month_days:  # Make sure month key exists
                        available_days = month_days[m_key]
                        if available_days > 0:  # Prevent division by zero
                            ratios[m_key] = shifts / available_days
                        else:
                            ratios[m_key] = 0
                    else:
                        logging.warning(f"Month {m_key} not found in month_days for worker {worker_id}")
                        ratios[m_key] = 0

                if ratios:
                    max_ratio = max(ratios.values())
                    min_ratio = min(ratios.values())
            
                    # Allow maximum 20% difference in ratios
                    if (max_ratio - min_ratio) > 0.2:
                        logging.debug(f"Monthly balance violated for worker {worker_id}: {ratios}")
                        return False, max_ratio - min_ratio

            return True, 0.0

        except Exception as e:
            logging.error(f"Error checking monthly balance for worker {worker_id}: {str(e)}", exc_info=True)
            return True, 0.0

    def _check_weekday_balance(self, worker_id, date):
        """
        Check if assigning this date would maintain weekday balance
        
        Returns:
            bool: True if assignment maintains balance, False otherwise
        """
        try:
            # Get current weekday counts including the new date
            weekday = date.weekday()
            weekday_counts = self.worker_weekdays[worker_id].copy()
            weekday_counts[weekday] += 1

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
        
    def _check_post_rotation(self, worker_id, post):
        """
        Check if assigning this post maintains the required distribution.
        Specifically checks that the last post (highest number) is assigned
        approximately 1/num_shifts of the time.
    
        Args:
            worker_id: Worker's ID
            post: Post number being considered
        Returns:
            bool: True if assignment maintains proper distribution
        """
        try:
            # Only check for last post position
            last_post = self.num_shifts - 1
            if post != last_post:
                return True  # Don't restrict other post assignments
        
            # Get current post counts
            post_counts = self._get_post_counts(worker_id)
        
            # Add the potential new assignment
            new_counts = post_counts.copy()
            new_counts[post] = new_counts.get(post, 0) + 1
        
            # Calculate total assignments including the new one
            total_assignments = sum(new_counts.values())
        
            if total_assignments == 0:
                return True
        
            # Calculate target ratio for last post (1/num_shifts)
            target_ratio = 1.0 / self.num_shifts
            actual_ratio = new_counts[last_post] / total_assignments
        
            # Allow ±1 shift deviation from perfect ratio
            allowed_deviation = 1.0 / total_assignments
        
            if abs(actual_ratio - target_ratio) > allowed_deviation:
                logging.debug(
                    f"Post rotation check failed for worker {worker_id}: "
                    f"Last post ratio {actual_ratio:.2f} deviates too much from "
                    f"target {target_ratio:.2f} (allowed deviation: ±{allowed_deviation:.2f})"
                )    
                return False
        
            return True

        except Exception as e:
            logging.error(f"Error checking post rotation for worker {worker_id}: {str(e)}")
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
            weekday_counts = self.worker_weekdays[worker_id].copy()
            weekday_counts[weekday] += 1

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
        
    def _can_swap_assignments(self, worker_id, from_date, from_post, to_date, to_post):
        """
        Check if a worker can be reassigned from one date/post to another

        Checks all relevant constraints:
        - Worker availability (days off)
        - Mandatory assignments
        - Worker incompatibilities
        - Minimum gap requirements
        - Weekend limits
        """
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()
        
        # Check if worker is unavailable on the target date (days off)
        if self._is_worker_unavailable(worker_id, to_date):
            return False

        # Check if from_date was mandatory for this worker
        worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
        mandatory_days = worker_data.get('mandatory_days', []) if worker_data else []
        mandatory_dates = self._parse_dates(mandatory_days)

        # Don't reassign from a mandatory date
        if from_date in mandatory_dates:
            return False

        # Check worker incompatibilities at the target date
        for other_worker_id in self.schedule[to_date]:
            if other_worker_id is not None and self._are_workers_incompatible(worker_id, other_worker_id):
                return False

        # Get all current assignments for this worker
        assignments = list(self.worker_assignments[worker_id])
    
        # Check if from_date is actually in the worker's assignment list
        # This handles cases where the schedule and worker_assignments are out of sync
        if from_date in assignments:
            assignments.remove(from_date)  # Remove the date we're swapping from
    
        # Add the date we're swapping to
        assignments.append(to_date)

        # Get sorted list of assignments
        sorted_assignments = sorted(assignments)

        # Check gaps between consecutive assignments
        for i in range(1, len(sorted_assignments)):
            gap_days = (sorted_assignments[i] - sorted_assignments[i-1]).days
            if gap_days < 2:  # STRICT MINIMUM: 2 days
                return False

        # Check weekend limit
        if self._is_weekend_day(to_date) and not self._is_weekend_day(from_date):
            # We're adding a weekend day, check against the limit
            weekend_days = [d for d in sorted_assignments if self._is_weekend_day(d)]
        
            # If the assignment would result in more than 3 weekend shifts in any 3-week period
            for check_date in weekend_days:
                window_start = check_date - timedelta(days=10)  # 10 days before
                window_end = check_date + timedelta(days=10)    # 10 days after
            
                window_weekend_count = sum(
                    1 for d in weekend_days
                    if window_start <= d <= window_end
                )
            
                if window_weekend_count > 3:  # STRICT MAXIMUM: 3 weekend shifts in 3 weeks
                    return False

        # Check post balance - ensure we're not making another post even more imbalanced
        post_counts = self._get_post_counts(worker_id)

        # Simulate the swap
        if from_post in post_counts:
            post_counts[from_post] -= 1
        if to_post in post_counts:
            post_counts[to_post] += 1
        else:
            post_counts[to_post] = 1

        # Check if this swap would worsen imbalance
        total_assignments = sum(post_counts.values())
        expected_per_post = total_assignments / self.num_shifts

        # Calculate deviation before and after
        current_deviation = 0
        for post in range(self.num_shifts):
            post_count = self._get_post_counts(worker_id).get(post, 0)
            current_deviation += abs(post_count - expected_per_post)

        new_deviation = 0
        for post in range(self.num_shifts):
            post_count = post_counts.get(post, 0)
            new_deviation += abs(post_count - expected_per_post)

        # Don't allow swaps that make post distribution worse
        if new_deviation > current_deviation:
            return False

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
