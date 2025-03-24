from datetime import datetime, timedelta

class WorkerEligibilityTracker:
    """Helper class to track and manage worker eligibility for assignments"""
    
    def __init__(self, workers_data, holidays, gap_between_shifts=1, max_consecutive_weekends=2):
        """
        Initialize the worker eligibility tracker
    
        Args:
            workers_data: List of worker dictionaries
            holidays: List of holiday dates
            gap_between_shifts: Minimum days between shifts (default: 1)
            max_consecutive_weekends: Maximum consecutive weekend/holiday shifts allowed (default: 2)
        """
        self.workers_data = workers_data
        self.holidays = holidays
        self.gap_between_shifts = gap_between_shifts
        self.max_consecutive_weekends = max_consecutive_weekends
        self.last_worked_date = {w['id']: None for w in workers_data}
        self.total_assignments = {w['id']: 0 for w in workers_data}
        self.recent_weekends = {w['id']: [] for w in workers_data}
    
    def update_worker_status(self, worker_id, date):
        """
        Update tracking data when a worker is assigned
    
        Args:
            worker_id: ID of the worker
            date: Date of the assignment
        """
        self.last_worked_date[worker_id] = date
        self.total_assignments[worker_id] += 1
    
        if self._is_weekend_day(date):
            # Only add if not already in the list (prevent duplicates)
            if date not in self.recent_weekends[worker_id]:
                self.recent_weekends[worker_id].append(date)
            
            # Keep only recent weekends (last 21 days)
            cutoff_date = date - timedelta(days=21)
            self.recent_weekends[worker_id] = [
                d for d in self.recent_weekends[worker_id]
                if d > cutoff_date
            ]
        
            # Ensure the list is sorted for consistent window calculations
            self.recent_weekends[worker_id].sort()
    
    def get_eligible_workers(self, date, assigned_workers):
        """
        Get list of workers eligible for assignment on given date
        
        Args:
            date: Date to check eligibility for
            assigned_workers: List of workers already assigned to this date
        Returns:
            list: List of eligible workers
        """
        eligible_workers = []
        
        for worker in self.workers_data:
            worker_id = worker['id']
            
            # Quick checks first (most likely to fail)
            if not self._check_basic_eligibility(worker_id, date, assigned_workers):
                continue
                
            # More expensive checks
            if not self._check_weekend_constraints(worker_id, date):
                continue
                
            eligible_workers.append(worker)
            
        return eligible_workers
    
    def _check_basic_eligibility(self, worker_id, date, assigned_workers):
        # Check if already assigned that day
        if worker_id in assigned_workers:
            return False
    
        # Check minimum gap based on the configurable parameter
        last_worked = self.last_worked_date[worker_id]
        if last_worked:
            days_between = (date - last_worked).days
    
            # Basic minimum gap check - need configured gap days off
            min_days_between = self.gap_between_shifts + 1  # +1 because we need days_between > gap
            if days_between < min_days_between:
                return False
        
            # Special case for Friday-Monday if gap is only 1 day
            if self.gap_between_shifts == 1 and days_between == 3:
                if ((date.weekday() == 0 and last_worked.weekday() == 4) or
                    (date.weekday() == 4 and last_worked.weekday() == 0)):
                    return False
    
        return True
    
    def _check_weekend_constraints(self, worker_id, date):
        """
        Check weekend-related constraints - ensuring max consecutive weekend/holiday shifts
    
        Args:
            worker_id: ID of the worker to check
            date: Date to check
        Returns:
            bool: True if worker can be assigned to this weekend date
        """
        # If not a weekend day, no constraint
        if not self._is_weekend_day(date):
            return True
    
        # Create a temporary list including existing weekend days and the new one
        all_weekend_dates = self.recent_weekends[worker_id].copy()
    
        # Avoid double counting if date is already in the list
        if date not in all_weekend_dates:
            all_weekend_dates.append(date)
    
        # Check for every date in the combined list
        for check_date in all_weekend_dates:
            window_start = check_date - timedelta(days=10)  # 10 days before
            window_end = check_date + timedelta(days=10)    # 10 days after
    
            # Count weekend days in this window
            weekend_count = sum(
                1 for d in all_weekend_dates
                if window_start <= d <= window_end
            )    
    
            if weekend_count > self.max_consecutive_weekends:
                return False  # Exceeds configured limit
    
        return True  # Within limit
    
    def _is_weekend_day(self, date):
        """
        Check if date is a weekend day or holiday
        
        Args:
            date: Date to check
        Returns:
            bool: True if date is a weekend day or holiday
        """
        return (
            date.weekday() >= 4 or  # Friday, Saturday, Sunday
            date in self.holidays or
            date + timedelta(days=1) in self.holidays
        )

    def _update_tracking_data(self, worker_id, date, post):
        """
        Update all tracking data structures after assignment
    
        Args:
            worker_id: ID of the worker being assigned
            date: Date of assignment
            post: Post number being assigned (0-indexed)
        """
        try:
            # Ensure data structures exist for this worker
            if worker_id not in self.scheduler.worker_assignments:
                self.scheduler.worker_assignments[worker_id] = set()
        
            if worker_id not in self.scheduler.worker_posts:
                self.scheduler.worker_posts[worker_id] = set()
            
            if worker_id not in self.scheduler.worker_weekdays:
                self.scheduler.worker_weekdays[worker_id] = {i: 0 for i in range(7)}
            
            if worker_id not in self.scheduler.worker_weekends:
                self.scheduler.worker_weekends[worker_id] = []
        
            # Update worker assignments set
            self.scheduler.worker_assignments[worker_id].add(date)
        
            # Update post tracking
            self.scheduler.worker_posts[worker_id].add(post)
        
            # Update weekday counts
            weekday = date.weekday()
            self.scheduler.worker_weekdays[worker_id][weekday] += 1
        
            # Update weekend tracking if applicable
            is_weekend = self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays)
            if is_weekend:
                # Check if we already have this date in the weekends list
                if date not in self.scheduler.worker_weekends[worker_id]:
                    self.scheduler.worker_weekends[worker_id].append(date)
                
                # Ensure the weekend list is sorted by date
                self.scheduler.worker_weekends[worker_id].sort()
        
            # Update the worker eligibility tracker if it exists
            if hasattr(self.scheduler, 'eligibility_tracker'):
                self.scheduler.eligibility_tracker.update_worker_status(worker_id, date)
            
            # Mark data as needing verification since we've modified it
            self.mark_data_dirty()
        
            # Log the update
            logging.debug(f"Updated tracking data for {worker_id} on {date.strftime('%d-%m-%Y')}, post {post}")
        
        except Exception as e:
            logging.error(f"Error in _update_tracking_data for worker {worker_id}: {str(e)}", exc_info=True)
        
    def remove_worker_assignment(self, worker_id, date):
        """
        Remove tracking data when a worker's assignment is removed
    
        Args:
            worker_id: ID of the worker
            date: Date of the assignment being removed
        """
        # Update last worked date if needed
        if self.last_worked_date[worker_id] == date:
            # Since we don't track all assignments here, we can't determine the next most recent assignment
            # Just set it to None to indicate no recent assignment
            self.last_worked_date[worker_id] = None
    
        # Decrement total assignments
        self.total_assignments[worker_id] = max(0, self.total_assignments[worker_id] - 1)
    
        # Remove from weekend tracking if applicable
        if self._is_weekend_day(date) and date in self.recent_weekends[worker_id]:
            self.recent_weekends[worker_id].remove(date)

    def _remove_tracking_data(self, worker_id, date, post):
        """
        Remove tracking data when a worker is unassigned from a shift
    
        Args:
            worker_id: ID of the worker being unassigned
            date: Date of assignment
            post: Post number being unassigned (0-indexed)
        """
        try:
            # Remove from worker assignments
            if worker_id in self.scheduler.worker_assignments:
                if date in self.scheduler.worker_assignments[worker_id]:
                    self.scheduler.worker_assignments[worker_id].remove(date)
        
            # We cannot directly remove from posts since worker_posts doesn't track which post
            # was assigned on which date. We'll need to recalculate this from the schedule.
        
            # Update weekday counts
            if worker_id in self.scheduler.worker_weekdays:
                weekday = date.weekday()
                if self.scheduler.worker_weekdays[worker_id][weekday] > 0:
                    self.scheduler.worker_weekdays[worker_id][weekday] -= 1
        
            # Update weekend tracking
            is_weekend = self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays)
            if is_weekend and worker_id in self.scheduler.worker_weekends:
                if date in self.scheduler.worker_weekends[worker_id]:
                    self.scheduler.worker_weekends[worker_id].remove(date)
        
            # Update the worker eligibility tracker if it exists
            if hasattr(self.scheduler, 'eligibility_tracker'):
                self.scheduler.eligibility_tracker.remove_worker_assignment(worker_id, date)
            
            # Mark data as needing verification since we've modified it
            self.mark_data_dirty()
        
            # Log the update
            logging.debug(f"Removed tracking data for {worker_id} from {date.strftime('%d-%m-%Y')}, post {post}")
        
        except Exception as e:
            logging.error(f"Error in _remove_tracking_data for worker {worker_id}: {str(e)}", exc_info=True)

    def rebuild_worker_posts(self):
        """
        Rebuild the worker_posts tracking structure from the schedule
        Should be called after making multiple changes to the schedule
        """
        # Initialize empty post sets for all workers
        self.scheduler.worker_posts = {w['id']: set() for w in self.scheduler.workers_data}
    
        # Iterate through the schedule and rebuild post assignments
        for date, workers in self.scheduler.schedule.items():
            for post, worker_id in enumerate(workers):
                if worker_id is not None:
                    self.scheduler.worker_posts[worker_id].add(post)
    
        logging.debug("Rebuilt worker post assignments")
