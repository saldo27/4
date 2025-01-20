from datetime import datetime, timedelta, timezone
import calendar
from zoneinfo import ZoneInfo  # For Python 3.9+

class Scheduler:
    def __init__(self, config):
        self.config = config
        self.start_date = config['start_date']
        self.end_date = config['end_date']
        self.num_shifts = config['num_shifts']
        self.workers_data = config['workers_data']
        self.schedule = {}
        self.worker_assignments = {w['id']: [] for w in self.workers_data}
        
        # Get current UTC time and convert to Spain time (UTC+1)
        self.current_datetime = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Madrid"))
        self.current_user = config.get('current_user', 'saldo27')
        
        print(f"Current time in Spain: {self.current_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self.current_user = 'saldo27'
    
    def generate_schedule(self):
        """Generate the complete guard schedule following all conditions"""
        print(f"\nStarting schedule generation at {self.current_datetime}")
        print(f"Generator: {self.current_user}")
        print("\nWorker Incompatibilities:")
        for worker in self.workers_data:
            if 'incompatible_workers' in worker and worker['incompatible_workers']:
                print(f"Worker {worker['id']} is incompatible with: {worker['incompatible_workers']}")

        try:
            # Reset schedule
            self.schedule = {}
            self.worker_assignments = {w['id']: [] for w in self.workers_data}

            # Step 1: Process mandatory guards first
            self._assign_mandatory_guards()

            # Step 2: Calculate target shifts for each worker
            self._calculate_target_shifts()

            # Step 3: Fill remaining shifts
            current_date = self.start_date
            while current_date <= self.end_date:
                self._assign_day_shifts(current_date)
                current_date += timedelta(days=1)

            # Validate the schedule before returning
            errors, warnings = self.validate_schedule()
            if errors:
                raise ValueError("\n".join(errors))

            return self.schedule

        except Exception as e:
            print(f"Error generating schedule: {str(e)}")
            self.schedule = {}  # Reset schedule on error
            raise ValueError(f"Schedule generation failed: {str(e)}")

    def _assign_mandatory_guards(self):
        """Assign all mandatory guards first"""
        print("Processing mandatory guards...")
    
        # Sort workers by number of mandatory days to handle conflicts
        workers_with_mandatory = [(w, self._parse_dates(w.get('mandatory_days', ''))) 
                                for w in self.workers_data]
        workers_with_mandatory.sort(key=lambda x: len(x[1]), reverse=True)

        for worker, mandatory_dates in workers_with_mandatory:
            if not mandatory_dates:
                continue

            for date in mandatory_dates:
                if self.start_date <= date <= self.end_date:
                    if date not in self.schedule:
                        self.schedule[date] = []
                
                    if worker['id'] not in self.schedule[date] and len(self.schedule[date]) < self.num_shifts:
                        # Force assignment for mandatory days
                        self.schedule[date].append(worker['id'])
                        self.worker_assignments[worker['id']].append(date)
                        print(f"Assigned mandatory guard: {worker['id']} on {date.strftime('%Y-%m-%d')}")

    def _calculate_target_shifts(self):
        """Calculate target number of shifts for each worker based on their percentage"""
        total_days = (self.end_date - self.start_date).days + 1
        total_shifts = total_days * self.num_shifts
        total_percentage = sum(float(w.get('work_percentage', 100)) for w in self.workers_data)

        for worker in self.workers_data:
            percentage = float(worker.get('work_percentage', 100))
            target = (percentage / total_percentage) * total_shifts
            worker['target_shifts'] = round(target)
            print(f"Worker {worker['id']} - Target shifts: {worker['target_shifts']} ({percentage}%)")

    def _fill_remaining_shifts(self):
        """Fill all remaining shifts in the schedule"""
        current_date = self.start_date
        while current_date <= self.end_date:
            self._assign_day_shifts(current_date)
            current_date += timedelta(days=1)

    def _assign_day_shifts(self, date):
        """Assign all shifts for a specific day"""
        if date not in self.schedule:
            self.schedule[date] = []

        while len(self.schedule[date]) < self.num_shifts:
            best_worker = self._find_best_worker(date)
            if best_worker:
                self.schedule[date].append(best_worker['id'])
                self.worker_assignments[best_worker['id']].append(date)
                print(f"Assigned shift: {best_worker['id']} on {date.strftime('%Y-%m-%d')}")
            else:
                print(f"Warning: Could not find suitable worker for {date.strftime('%Y-%m-%d')}")
                break

    def _find_best_worker(self, date):
        """Find the most suitable worker for a given date"""
        candidates = []
        
        for worker in self.workers_data:
            self._print_debug_info(worker['id'], date)  # This line calls the debug method
            if self._can_assign_worker(worker['id'], date):
                score = self._calculate_worker_score(worker, date)
                candidates.append((worker, score))
            else:
                print(f"Worker {worker['id']} cannot be assigned to {date.strftime('%Y-%m-%d')}")

        if not candidates:
            print(f"No suitable candidates found for {date.strftime('%Y-%m-%d')}")
            return None

        best_worker = max(candidates, key=lambda x: x[1])[0]
        print(f"Selected worker {best_worker['id']} for {date.strftime('%Y-%m-%d')}")
        return best_worker

    # Add the debug method here
    def _print_debug_info(self, worker_id, date):
        """Print debug information for worker assignment"""
        print(f"\nDebug info for {date.strftime('%Y-%m-%d')}:")
        print(f"Trying to assign worker: {worker_id}")
        if date in self.schedule:
            print(f"Currently assigned workers: {self.schedule[date]}")
            for w_id in self.schedule[date]:
                worker = next(w for w in self.workers_data if w['id'] == w_id)
                if 'incompatible_workers' in worker and worker['incompatible_workers']:
                    print(f"Worker {w_id} incompatible with: {worker['incompatible_workers']}")

    def _can_assign_worker(self, worker_id, date):
        """Check if a worker can be assigned to a specific date"""
        try:
            worker = next(w for w in self.workers_data if w['id'] == worker_id)
    
            # Check 1: Already assigned that day
            if date in self.worker_assignments[worker_id]:
                print(f"Worker {worker_id} already assigned on {date.strftime('%Y-%m-%d')}")
                return False
    
            # Check 2: Days off (highest priority)
            if worker.get('days_off'):
                off_periods = self._parse_date_ranges(worker['days_off'])
                for start, end in off_periods:
                    if start <= date <= end:
                        print(f"Worker {worker_id} is off on {date.strftime('%Y-%m-%d')}")
                        return False
    
            # Check 3: Worker incompatibility (Combined check)
            if date in self.schedule:
                # Get the workers already assigned to this date
                assigned_workers = self.schedule[date]
                
                # Check incompatibility using both systems
                
                # System 1: is_incompatible flag
                if worker.get('is_incompatible', False):
                    for assigned_worker_id in assigned_workers:
                        assigned_worker = next(w for w in self.workers_data if w['id'] == assigned_worker_id)
                        if assigned_worker.get('is_incompatible', False):
                            print(f"Cannot assign worker {worker_id}: Both workers are marked as incompatible")
                            return False
    
                # System 2: incompatible_workers list
                if 'incompatible_workers' in worker and worker['incompatible_workers']:
                    for assigned_worker_id in assigned_workers:
                        if assigned_worker_id in worker['incompatible_workers']:
                            print(f"Cannot assign worker {worker_id}: Incompatible with {assigned_worker_id}")
                            return False
    
                # Check if any assigned worker has incompatibility with current worker
                for assigned_worker_id in assigned_workers:
                    assigned_worker = next(w for w in self.workers_data if w['id'] == assigned_worker_id)
                    if assigned_worker.get('is_incompatible', False) and worker.get('is_incompatible', False):
                        print(f"Cannot assign worker {worker_id}: Both workers marked as incompatible")
                        return False
                    if 'incompatible_workers' in assigned_worker and assigned_worker['incompatible_workers']:
                        if worker_id in assigned_worker['incompatible_workers']:
                            print(f"Cannot assign worker {worker_id}: Worker {assigned_worker_id} marked them as incompatible")
                            return False
    
            # Rest of the checks remain the same...
            return True

    except Exception as e:
        print(f"Error checking worker {worker_id} availability: {str(e)}")
        return False

    def _calculate_worker_score(self, worker, date):
        """Calculate a score for a worker based on various factors"""
        score = 0.0

        # Factor 1: Mandatory days (highest priority)
        if worker.get('mandatory_days'):
            mandatory_dates = self._parse_dates(worker['mandatory_days'])
            if date in mandatory_dates:
                score += 1000

        # Factor 2: Distance from target shifts
        current_shifts = len(self.worker_assignments[worker['id']])
        shift_difference = worker['target_shifts'] - current_shifts
        score += shift_difference * 25

        # Factor 3: Monthly balance
        month_shifts = sum(1 for d in self.worker_assignments[worker['id']]
                         if d.year == date.year and d.month == date.month)
        target_month_shifts = worker['target_shifts'] / ((self.end_date.year * 12 + self.end_date.month) -
                                                       (self.start_date.year * 12 + self.start_date.month) + 1)
        monthly_balance = target_month_shifts - month_shifts
        score += monthly_balance * 25

        return score

    def _parse_dates(self, date_str):
        """Parse semicolon-separated dates"""
        if not date_str:
            return []

        dates = []
        for date_text in date_str.split(';'):
            date_text = date_text.strip()
            if date_text:
                try:
                    dates.append(datetime.strptime(date_text, '%d-%m-%Y'))
                except ValueError as e:
                    print(f"Warning: Invalid date format '{date_text}' - {str(e)}")
        return dates

    def _parse_date_ranges(self, date_ranges_str):
        """Parse semicolon-separated date ranges"""
        if not date_ranges_str:
            return []

        ranges = []
        for date_range in date_ranges_str.split(';'):
            date_range = date_range.strip()
            try:
                if ' - ' in date_range:
                    start_str, end_str = date_range.split(' - ')
                    start = datetime.strptime(start_str.strip(), '%d-%m-%Y')
                    end = datetime.strptime(end_str.strip(), '%d-%m-%Y')
                    ranges.append((start, end))
                else:
                    date = datetime.strptime(date_range, '%d-%m-%Y')
                    ranges.append((date, date))
            except ValueError as e:
                print(f"Warning: Invalid date range format '{date_range}' - {str(e)}")
        return ranges
                
    def validate_schedule(self):
        """Validate the generated schedule"""
        errors = []
        warnings = []
    
        # Check 1: Every day has guards
        current = self.start_date
        while current <= self.end_date:
            if current not in self.schedule:
                errors.append(f"No guards assigned on {current.strftime('%Y-%m-%d')}")
            elif len(self.schedule[current]) < self.num_shifts:
                warnings.append(f"Insufficient guards on {current.strftime('%Y-%m-%d')}")
            current += timedelta(days=1)
    
        # Check 2: Guard spacing rules
        for worker_id, dates in self.worker_assignments.items():
            sorted_dates = sorted(dates)
            for i in range(len(sorted_dates) - 1):
                days_between = (sorted_dates[i+1] - sorted_dates[i]).days
                if days_between in [7, 14, 21]:
                    errors.append(f"Invalid spacing ({days_between} days) for worker {worker_id}")
    
        # Check 3: Incompatible workers (Combined system check)
        for date, workers in self.schedule.items():
            # Check is_incompatible flag system
            incompatible_workers = []
            for worker_id in workers:
                worker = next(w for w in self.workers_data if w['id'] == worker_id)
                if worker.get('is_incompatible', False):
                    incompatible_workers.append(worker_id)
            
            if len(incompatible_workers) > 1:
                errors.append(f"Multiple incompatible workers {', '.join(map(str, incompatible_workers))} assigned on {date.strftime('%Y-%m-%d')}")
    
            # Check incompatible_workers list system
            for i, worker_id1 in enumerate(workers):
                worker1 = next(w for w in self.workers_data if w['id'] == worker_id1)
                for worker_id2 in workers[i+1:]:
                    if 'incompatible_workers' in worker1 and worker_id2 in worker1.get('incompatible_workers', []):
                        errors.append(f"Incompatible workers {worker_id1} and {worker_id2} assigned on {date.strftime('%Y-%m-%d')}")
    
        # Check 4: Days off violations
        for worker in self.workers_data:
            worker_id = worker['id']
            if worker.get('days_off'):
                off_periods = self._parse_date_ranges(worker['days_off'])
                for date in self.worker_assignments[worker_id]:
                    for start, end in off_periods:
                        if start <= date <= end:
                            errors.append(f"Worker {worker_id} assigned on day off: {date.strftime('%Y-%m-%d')}")
    
        return errors, warnings
