from datetime import datetime, timedelta

class WorkerEligibilityTracker:
    """Helper class to track and manage worker eligibility for assignments"""
    
    def __init__(self, workers_data, holidays):
        """
        Initialize the worker eligibility tracker
        
        Args:
            workers_data: List of worker dictionaries
            holidays: List of holiday dates
        """
        self.workers_data = workers_data
        self.holidays = holidays
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
        """
        Quick checks for basic eligibility
        
        Args:
            worker_id: ID of the worker to check
            date: Date to check
            assigned_workers: List of workers already assigned to this date
        Returns:
            bool: True if worker passes basic eligibility checks
        """
        # Check if already assigned that day
        if worker_id in assigned_workers:
            return False
            
        # Check minimum gap (3 days)
        last_worked = self.last_worked_date[worker_id]
        if last_worked and (date - last_worked).days < 3:
            return False
            
        return True
    
    def _check_weekend_constraints(self, worker_id, date):
        """
        Check weekend-related constraints - ensuring max 3 weekend days in any 3-week period
    
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
        
            if weekend_count > 3:
                return False  # Exceeds limit
    
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

