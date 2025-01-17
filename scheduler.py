from datetime import datetime, timedelta
from typing import List, Dict, Set
import calendar

class GuardScheduler:
    def __init__(self, config: dict):
        self.config = config
        self.start_date = config['start_date']
        self.end_date = config['end_date']
        self.num_shifts = config['num_shifts']
        self.workers_data = config['workers_data']
        self.schedule = {}  # Final schedule
        self.worker_assignments = {}  # Track assignments per worker
        self.monthly_assignments = {}  # Track monthly assignments
        self.mandatory_assignments = set()  # Track mandatory assignments

    def process_mandatory_guards(self):
        """First pass: Process all mandatory guards"""
        print("Processing mandatory guards...")
        
        for worker in self.workers_data:
            if not worker.get('mandatory_days'):
                continue
                
            worker_id = worker['id']
            mandatory_days = self.parse_date_ranges(worker['mandatory_days'])
            
            for date, _ in mandatory_days:
                if self.start_date <= date <= self.end_date:
                    # Initialize schedule for this date if needed
                    if date not in self.schedule:
                        self.schedule[date] = []
                    
                    # Check if we can add the mandatory guard
                    if len(self.schedule[date]) < self.num_shifts:
                        if self.is_valid_mandatory_assignment(worker, date):
                            self.schedule[date].append(worker_id)
                            self.register_assignment(worker_id, date)
                            self.mandatory_assignments.add((date, worker_id))
                            print(f"Assigned mandatory guard: {worker_id} on {date.strftime('%d-%m-%Y')}")
                        else:
                            print(f"Warning: Could not assign mandatory guard for {worker_id} on {date.strftime('%d-%m-%Y')}")

    def is_valid_mandatory_assignment(self, worker: dict, date: datetime) -> bool:
        """Check if a mandatory assignment is valid"""
        worker_id = worker['id']

        # Check if worker already has a shift on this date
        if date in self.worker_assignments.get(worker_id, set()):
            return False

        # Check days off
        if worker.get('days_off'):
            off_periods = self.parse_date_ranges(worker['days_off'])
            if any(start <= date <= end for start, end in off_periods):
                return False

        return True

    def generate_schedule(self) -> Dict:
        """Generate the complete guard schedule."""
        print("Starting schedule generation...")
        
        # First pass: Process mandatory guards
        self.process_mandatory_guards()
        
        # Second pass: Fill remaining slots
        current_date = self.start_date
        while current_date <= self.end_date:
            print(f"Processing date: {current_date.strftime('%d-%m-%Y')}")
            
            # Initialize schedule for this date if needed
            if current_date not in self.schedule:
                self.schedule[current_date] = []
            
            # Fill remaining slots for this date
            while len(self.schedule[current_date]) < self.num_shifts:
                best_worker = self.find_best_worker_for_date(current_date)
                if best_worker:
                    self.schedule[current_date].append(best_worker['id'])
                    self.register_assignment(best_worker['id'], current_date)
                    print(f"Assigned regular guard: {best_worker['id']} on {current_date.strftime('%d-%m-%Y')}")
                else:
                    print(f"Warning: Could not find valid worker for {current_date.strftime('%d-%m-%Y')}")
                    break
            
            current_date += timedelta(days=1)

        # Validate the schedule
        self.validate_schedule()
        
        return self.schedule

    def find_best_worker_for_date(self, date: datetime) -> Dict:
        """Find the best worker for a date considering both anterograde and retrograde conditions"""
        candidates = []
        
        for worker in self.workers_data:
            if self.is_valid_assignment(worker, date):
                score = self.calculate_worker_score(worker, date)
                if score is not None:
                    candidates.append((worker, score))
        
        if not candidates:
            return None
            
        # Sort candidates by score (highest first)
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    def calculate_worker_score(self, worker: dict, date: datetime) -> float:
        """Calculate a worker's suitability score for a date"""
        worker_id = worker['id']
        score = 0.0
        
        # Factor 1: Distance from target number of shifts
        target_shifts = self.calculate_target_shifts(worker)
        current_shifts = len(self.worker_assignments.get(worker_id, set()))
        shift_difference = target_shifts - current_shifts
        score += shift_difference * 10
        
        # Factor 2: Distance from last assignment (anterograde)
        last_assignments = sorted(self.worker_assignments.get(worker_id, set()))
        if last_assignments:
            days_since_last = (date - last_assignments[-1]).days
            score += min(days_since_last, 30)  # Cap at 30 days
        
        # Factor 3: Distance to next existing assignment (retrograde)
        future_assignments = [d for d in self.worker_assignments.get(worker_id, set()) if d > date]
        if future_assignments:
            days_to_next = min((d - date).days for d in future_assignments)
            score += min(days_to_next, 30)  # Cap at 30 days
        
        # Factor 4: Monthly balance
        month_key = f"{date.year}-{date.month}"
        monthly_shifts = self.monthly_assignments.get(month_key, {}).get(worker_id, 0)
        score -= monthly_shifts * 5
        
        return score

    def register_assignment(self, worker_id: str, date: datetime):
        """Register a new assignment"""
        if worker_id not in self.worker_assignments:
            self.worker_assignments[worker_id] = set()
        self.worker_assignments[worker_id].add(date)
        
        # Update monthly assignments
        month_key = f"{date.year}-{date.month}"
        if month_key not in self.monthly_assignments:
            self.monthly_assignments[month_key] = {}
        if worker_id not in self.monthly_assignments[month_key]:
            self.monthly_assignments[month_key][worker_id] = 0
        self.monthly_assignments[month_key][worker_id] += 1

    def validate_schedule(self):
        """Validate the generated schedule"""
        print("\nValidating schedule...")
        
        # Check all mandatory guards were assigned
        for worker in self.workers_data:
            if worker.get('mandatory_days'):
                mandatory_days = self.parse_date_ranges(worker['mandatory_days'])
                for date, _ in mandatory_days:
                    if self.start_date <= date <= self.end_date:
                        if date not in self.schedule or worker['id'] not in self.schedule[date]:
                            print(f"Warning: Mandatory guard not assigned - {worker['id']} on {date.strftime('%d-%m-%Y')}")
        
        # Check spacing conditions
        for worker_id, dates in self.worker_assignments.items():
            sorted_dates = sorted(dates)
            for i in range(len(sorted_dates)-1):
                days_between = (sorted_dates[i+1] - sorted_dates[i]).days
                if days_between in [7, 14, 21]:
                    print(f"Warning: Invalid spacing ({days_between} days) for worker {worker_id}")

        print("Validation complete.")
        
    def calculate_minimum_distance(self, worker: dict) -> int:
        """Calculate minimum distance between guards for a worker based on their percentage."""
        work_percentage = worker.get('work_percentage', 100)
        return max(4, int(4 / (work_percentage / 100)))

    def calculate_target_shifts(self, worker: dict) -> float:
        """Calculate target number of shifts for a worker based on their percentage."""
        total_days = (self.end_date - self.start_date).days + 1
        total_shifts = total_days * self.num_shifts
        
        # Calculate total percentage of all workers
        total_percentage = sum(w.get('work_percentage', 100) for w in self.workers_data)
        
        # Calculate worker's share
        worker_percentage = worker.get('work_percentage', 100)
        return (worker_percentage * total_shifts) / total_percentage

    def is_valid_assignment(self, worker: dict, date: datetime) -> bool:
        """Check if a worker can be assigned to a specific date."""
        worker_id = worker['id']

        # Check if worker is available on this date
        if not self.is_worker_available(worker, date):
            return False

        # Check if worker already has a shift on this date
        if date in self.worker_assignments.get(worker_id, set()):
            return False

        # Check prohibited intervals (7, 14, 21 days)
        prohibited_intervals = {7, 14, 21}
        for interval in prohibited_intervals:
            for delta in range(-interval, interval + 1):
                check_date = date + timedelta(days=delta)
                if check_date in self.worker_assignments.get(worker_id, set()):
                    return False

        # Check minimum distance between shifts
        min_distance = self.calculate_minimum_distance(worker)
        recent_assignments = sorted(self.worker_assignments.get(worker_id, set()))
        if recent_assignments:
            days_since_last = abs((date - recent_assignments[-1]).days)
            if days_since_last < min_distance:
                return False

        return True

    def is_worker_available(self, worker: dict, date: datetime) -> bool:
        """Check if worker is available on a specific date."""
        # Check work periods
        if worker.get('work_periods'):
            periods = self.parse_date_ranges(worker['work_periods'])
            if not any(start <= date <= end for start, end in periods):
                return False

        # Check days off
        if worker.get('days_off'):
            off_periods = self.parse_date_ranges(worker['days_off'])
            if any(start <= date <= end for start, end in off_periods):
                return False

        return True

    def parse_date_ranges(self, date_ranges_str: str) -> List[tuple]:
        """Parse date ranges string into list of (start_date, end_date) tuples."""
        ranges = []
        if not date_ranges_str:
            return ranges

        for date_range in date_ranges_str.split(';'):
            date_range = date_range.strip()
            if ' - ' in date_range:
                start_str, end_str = date_range.split(' - ')
                start = datetime.strptime(start_str.strip(), '%d-%m-%Y')
                end = datetime.strptime(end_str.strip(), '%d-%m-%Y')
                ranges.append((start, end))
            else:
                date = datetime.strptime(date_range, '%d-%m-%Y')
                ranges.append((date, date))
        return ranges

    def assign_mandatory_days(self):
        """Assign workers to their mandatory coverage days first."""
        for worker in self.workers_data:
            if worker.get('mandatory_days'):
                mandatory_days = worker['mandatory_days'].split(';')
                for day_str in mandatory_days:
                    date = datetime.strptime(day_str.strip(), '%d-%m-%Y')
                    if self.start_date <= date <= self.end_date:
                        if date not in self.schedule:
                            self.schedule[date] = []
                        if len(self.schedule[date]) < self.num_shifts:
                            self.schedule[date].append(worker['id'])
                            if worker['id'] not in self.worker_assignments:
                                self.worker_assignments[worker['id']] = set()
                            self.worker_assignments[worker['id']].add(date)
                            self.update_monthly_assignments(worker['id'], date)

    def update_monthly_assignments(self, worker_id: str, date: datetime):
        """Update the count of monthly assignments for a worker."""
        month_key = f"{date.year}-{date.month}"
        if month_key not in self.monthly_assignments:
            self.monthly_assignments[month_key] = {}
        if worker_id not in self.monthly_assignments[month_key]:
            self.monthly_assignments[month_key][worker_id] = 0
        self.monthly_assignments[month_key][worker_id] += 1

    def generate_schedule(self) -> Dict:
        """Generate the complete guard schedule."""
        # First, assign mandatory days
        self.assign_mandatory_days()

        current_date = self.start_date
        while current_date <= self.end_date:
            if current_date not in self.schedule:
                self.schedule[current_date] = []

            # Fill remaining slots for this date
            while len(self.schedule[current_date]) < self.num_shifts:
                best_worker = self.find_best_worker_for_date(current_date)
                if best_worker:
                    self.schedule[current_date].append(best_worker['id'])
                    if best_worker['id'] not in self.worker_assignments:
                        self.worker_assignments[best_worker['id']] = set()
                    self.worker_assignments[best_worker['id']].add(current_date)
                    self.update_monthly_assignments(best_worker['id'], current_date)
                else:
                    # Handle case where no valid worker is found
                    break

            current_date += timedelta(days=1)

        return self.schedule

    def find_best_worker_for_date(self, date: datetime) -> Dict:
        """Find the best worker for a specific date based on all conditions."""
        valid_workers = []
        
        for worker in self.workers_data:
            if self.is_valid_assignment(worker, date):
                # Calculate worker's current vs target shifts
                target_shifts = self.calculate_target_shifts(worker)
                current_shifts = len(self.worker_assignments.get(worker['id'], set()))
                
                # Calculate monthly balance
                month_key = f"{date.year}-{date.month}"
                monthly_shifts = self.monthly_assignments.get(month_key, {}).get(worker['id'], 0)
                
                # Score the worker based on various factors
                score = 0
                
                # Factor 1: How far they are from their target shifts
                shift_difference = target_shifts - current_shifts
                score += shift_difference * 10
                
                # Factor 2: Monthly balance
                score -= monthly_shifts * 5
                
                # Factor 3: Days since last assignment
                last_assignment = max(self.worker_assignments.get(worker['id'], {0}), default=0)
                if last_assignment:
                    days_since_last = (date - last_assignment).days
                    score += days_since_last
                
                valid_workers.append((worker, score))

        if valid_workers:
            # Return worker with highest score
            return max(valid_workers, key=lambda x: x[1])[0]
        
        return None
