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
            self.recent_weekends[worker_id].append(date)
            # Keep only recent weekends (last 21 days)
            cutoff_date = date - timedelta(days=21)
            self.recent_weekends[worker_id] = [
                d for d in self.recent_weekends[worker_id]
                if d > cutoff_date
            ]
    
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
        Check weekend-related constraints
        
        Args:
            worker_id: ID of the worker to check
            date: Date to check
        Returns:
            bool: True if worker can be assigned to this weekend date
        """
        if not self._is_weekend_day(date):
            return True
            
        # Count recent weekend days
        window_start = date - timedelta(days=10)
        window_end = date + timedelta(days=10)
        
        weekend_count = sum(
            1 for d in self.recent_weekends[worker_id]
            if window_start <= d <= window_end
        )
        
        return weekend_count < 3
    
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
