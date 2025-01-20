from datetime import datetime, timedelta
import calendar
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

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
        logging.info(f"\n=== Starting schedule generation at {self.current_datetime} ===")
        logging.info(f"Generator: {self.current_user}")
        logging.info(f"Schedule period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        logging.info(f"Number of shifts per day: {self.num_shifts}")
        logging.info(f"Number of workers: {len(self.workers_data)}")
        
        # Log worker details
        logging.info("\nWorker Details:")
        for worker in self.workers_data:
            logging.info(f"Worker {worker['id']}:")
            logging.info(f"  - Work percentage: {worker.get('work_percentage', 100)}%")
            logging.info(f"  - Is incompatible: {worker.get('is_incompatible', False)}")
            if worker.get('mandatory_days'):
                logging.info(f"  - Mandatory days: {worker['mandatory_days']}")
            if worker.get('days_off'):
                logging.info(f"  - Days off: {worker['days_off']}")
            if worker.get('work_periods'):
                logging.info(f"  - Work periods: {worker['work_periods']}")
    
        try:
            # Reset schedule
            self.schedule = {}
            self.worker_assignments = {w['id']: [] for w in self.workers_data}
    
            # Step 1: Process mandatory guards
            logging.info("\nProcessing mandatory guards...")
            self._assign_mandatory_guards()
    
            # Step 2: Calculate target shifts
            logging.info("\nCalculating target shifts...")
            self._calculate_target_shifts()
    
            # Step 3: Fill remaining shifts
            logging.info("\nFilling remaining shifts...")
            current_date = self.start_date
            while current_date <= self.end_date:
                logging.info(f"\nProcessing date: {current_date.strftime('%Y-%m-%d')}")
                self._assign_day_shifts(current_date)
                current_date += timedelta(days=1)
    
            # Validate schedule
            logging.info("\nValidating schedule...")
            errors, warnings = self.validate_schedule()
            
            if warnings:
                logging.warning("\nSchedule warnings:")
                for warning in warnings:
                    logging.warning(warning)
                    
            if errors:
                raise ValueError("\n".join(errors))
    
            logging.info("\nSchedule generation completed successfully!")
            return self.schedule
    
        except Exception as e:
            logging.error(f"\nError generating schedule: {str(e)}")
            logging.error("Schedule generation failed!")
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
        logging.info(f"\nAssigning shifts for {date.strftime('%Y-%m-%d')}")
        
        if date not in self.schedule:
            self.schedule[date] = []
    
        while len(self.schedule[date]) < self.num_shifts:
            logging.info(f"Finding worker for shift {len(self.schedule[date]) + 1}/{self.num_shifts}")
            best_worker = self._find_best_worker(date)
            if best_worker:
                self.schedule[date].append(best_worker['id'])
                self.worker_assignments[best_worker['id']].append(date)
                logging.info(f"Assigned worker {best_worker['id']} to shift")
            else:
                logging.error(f"Could not find suitable worker for shift {len(self.schedule[date]) + 1}")
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
        logging.debug(f"\nChecking if worker {worker_id} can be assigned to {date.strftime('%Y-%m-%d')}")
        
        try:
            worker = next(w for w in self.workers_data if w['id'] == worker_id)

            # Check 1: Already assigned that day
            if date in self.worker_assignments[worker_id]:
                logging.debug(f"Worker {worker_id} already assigned on this date")
                return False

            # Check 2: Days off
            if worker.get('days_off'):
                off_periods = self._parse_date_ranges(worker['days_off'])
                for start, end in off_periods:
                    if start <= date <= end:
                        logging.debug(f"Worker {worker_id} is off on this date")
                        return False

            # Check 3: Work periods
            if worker.get('work_periods'):
                work_periods = self._parse_date_ranges(worker['work_periods'])
                if not any(start <= date <= end for start, end in work_periods):
                    logging.debug(f"Worker {worker_id} not available on this date (outside work periods)")
                    return False

            # Check 4: Worker incompatibility
            if date in self.schedule:
                assigned_workers = self.schedule[date]
                
                # System 1: is_incompatible flag
                if worker.get('is_incompatible', False):
                    for assigned_worker_id in assigned_workers:
                        assigned_worker = next(w for w in self.workers_data if w['id'] == assigned_worker_id)
                        if assigned_worker.get('is_incompatible', False):
                            logging.debug(f"Cannot assign worker {worker_id}: Both workers marked as incompatible")
                            return False

                # System 2: incompatible_workers list
                if 'incompatible_workers' in worker and worker['incompatible_workers']:
                    for assigned_worker_id in assigned_workers:
                        if assigned_worker_id in worker['incompatible_workers']:
                            logging.debug(f"Cannot assign worker {worker_id}: Incompatible with {assigned_worker_id}")
                            return False

                # Check if any assigned worker has incompatibility with current worker
                for assigned_worker_id in assigned_workers:
                    assigned_worker = next(w for w in self.workers_data if w['id'] == assigned_worker_id)
                    if assigned_worker.get('is_incompatible', False) and worker.get('is_incompatible', False):
                        logging.debug(f"Cannot assign worker {worker_id}: Both workers marked as incompatible")
                        return False
                    if 'incompatible_workers' in assigned_worker and assigned_worker['incompatible_workers']:
                        if worker_id in assigned_worker['incompatible_workers']:
                            logging.debug(f"Cannot assign worker {worker_id}: Worker {assigned_worker_id} marked them as incompatible")
                            return False

            # Check 5: Minimum distance between assignments
            work_percentage = float(worker.get('work_percentage', 100))
            min_distance = max(2, int(4 / (work_percentage / 100)))
            assignments = sorted(self.worker_assignments[worker_id])

            if assignments:
                for prev_date in reversed(assignments):
                    days_between = abs((date - prev_date).days)
                    if days_between < min_distance:
                        logging.debug(f"Worker {worker_id} - too close to previous assignment ({days_between} days)")
                        return False
                    if days_between in [7, 14, 21]:
                        logging.debug(f"Worker {worker_id} - prohibited interval ({days_between} days)")
                        return False

            logging.debug(f"Worker {worker_id} can be assigned to this date")
            return True

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
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
        logging.info("\nValidating schedule...")
        errors = []
        warnings = []
    
        # Validation checks with logging...
        for date, workers in self.schedule.items():
            logging.info(f"\nChecking date: {date.strftime('%Y-%m-%d')}")
            logging.info(f"Assigned workers: {workers}")
            
            # Check incompatibilities
            incompatible_workers = []
            for worker_id in workers:
                worker = next(w for w in self.workers_data if w['id'] == worker_id)
                if worker.get('is_incompatible', False):
                    incompatible_workers.append(worker_id)
            
            if len(incompatible_workers) > 1:
                error_msg = f"Multiple incompatible workers {', '.join(map(str, incompatible_workers))} assigned on {date.strftime('%Y-%m-%d')}"
                logging.error(error_msg)
                errors.append(error_msg)
    
        logging.info(f"\nValidation complete. Found {len(errors)} errors and {len(warnings)} warnings.")
        return errors, warnings
