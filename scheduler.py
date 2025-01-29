from datetime import datetime, timedelta, timezone
import calendar
import logging
import sys
from zoneinfo import ZoneInfo
import requests  

# Configure logging with more detail and ensure file is created
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG level for more detail
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log', mode='w'),  # 'w' mode to create new log each time
        logging.StreamHandler(sys.stdout)
    ]
)

class Scheduler:
    def _get_spain_time(self):
        """Get current time in Spain using a time API"""
        try:
            # Try to get time from WorldTimeAPI
            response = requests.get('http://worldtimeapi.org/api/timezone/Europe/Madrid')
            if response.status_code == 200:
                time_data = response.json()
                return datetime.fromisoformat(time_data['datetime']).replace(tzinfo=None)
        except Exception as e:
            logging.warning(f"Could not fetch time from API: {str(e)}")
    
        # Fallback: Use system time converted to Spain time
        return datetime.now(ZoneInfo('Europe/Madrid')).replace(tzinfo=None)
    
    def __init__(self, config):
        try:
            self.config = config
            self.start_date = config['start_date']
            self.end_date = config['end_date']
            self.num_shifts = config['num_shifts']
            self.workers_data = config['workers_data']
            self.schedule = {}
            self.worker_assignments = {w['id']: [] for w in self.workers_data}
    
            # New tracking dictionaries
            self.worker_posts = {w['id']: set() for w in self.workers_data}
            self.worker_weekdays = {w['id']: {0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data}
    
            # Get current time in Spain
            self.current_datetime = self._get_spain_time()
            self.current_user = 'saldo27'
    
            logging.info(f"Scheduler initialized with:")
            logging.info(f"Start date: {self.start_date}")
            logging.info(f"End date: {self.end_date}")
            logging.info(f"Number of shifts: {self.num_shifts}")
            logging.info(f"Number of workers: {len(self.workers_data)}")
            logging.info(f"Current datetime (Spain): {self.current_datetime}")
            logging.info(f"Current user: {self.current_user}")
    
        except Exception as e:
            logging.error(f"Error initializing scheduler: {str(e)}")
            raise
     
    def generate_schedule(self):
        """Generate the complete guard schedule following all conditions"""
        logging.info("=== Starting schedule generation ===")
        try:
            # Log initial configuration
            logging.info("\nConfiguration:")
            logging.info(f"Start Date: {self.start_date.strftime('%Y-%m-%d')}")
            logging.info(f"End Date: {self.end_date.strftime('%Y-%m-%d')}")
            logging.info(f"Shifts per day: {self.num_shifts}")
            logging.info(f"Total workers: {len(self.workers_data)}")

            # Log worker details
            logging.info("\nWorker Details:")
            for worker in self.workers_data:
                logging.info(f"\nWorker {worker['id']}:")
                for key, value in worker.items():
                    if key != 'id':
                        logging.info(f"  - {key}: {value}")

            # Reset schedule
            self.schedule = {}
            self.worker_assignments = {w['id']: [] for w in self.workers_data}
            
            # Step 1: Process mandatory guards
            logging.info("\nStep 1: Processing mandatory guards...")
            self._assign_mandatory_guards()
            logging.info("Mandatory guards processed")

            # Step 2: Calculate target shifts
            logging.info("\nStep 2: Calculating target shifts...")
            self._calculate_target_shifts()
            logging.info("Target shifts calculated")

            # Step 3: Fill remaining shifts
            logging.info("\nStep 3: Filling remaining shifts...")
            current_date = self.start_date
            while current_date <= self.end_date:
                logging.info(f"\nProcessing date: {current_date.strftime('%Y-%m-%d')}")
                self._assign_day_shifts(current_date)
                if current_date in self.schedule:
                    logging.info(f"Assigned workers for {current_date.strftime('%Y-%m-%d')}: {self.schedule[current_date]}")
                else:
                    logging.warning(f"No assignments made for {current_date.strftime('%Y-%m-%d')}")
                current_date += timedelta(days=1)

            # Validate final schedule
            logging.info("\nValidating final schedule...")
            errors, warnings = self.validate_schedule()
            
            if warnings:
                logging.warning("\nSchedule warnings:")
                for warning in warnings:
                    logging.warning(warning)
                    
            if errors:
                logging.error("\nSchedule errors:")
                for error in errors:
                    logging.error(error)
                raise ValueError("\n".join(errors))

            logging.info("\nSchedule generation completed successfully!")
            return self.schedule

        except Exception as e:
            logging.error(f"Error generating schedule: {str(e)}")
            logging.error("Stack trace:", exc_info=True)
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
    
        for post in range(self.num_shifts):
            logging.info(f"Finding worker for shift {post + 1}/{self.num_shifts}")
            best_worker = self._find_best_worker(date, post)
            if best_worker:
                self.schedule[date].append(best_worker['id'])
                self.worker_assignments[best_worker['id']].append(date)
            
                # Update tracking data
                self.worker_posts[best_worker['id']].add(post)
                self.worker_weekdays[best_worker['id']][date.weekday()] += 1
                if self._is_weekend_day(date):
                    weekend_friday = date - timedelta(days=date.weekday() - 4)
                    if weekend_friday not in self.worker_weekends[best_worker['id']]:
                        self.worker_weekends[best_worker['id']].append(weekend_friday)
            
                logging.info(f"Assigned worker {best_worker['id']} to shift {post}")
            else:
                logging.error(f"Could not find suitable worker for shift {post + 1}")
                break

    def _is_weekend_day(self, date):
        """Check if the date is a weekend day (Friday, Saturday, or Sunday)"""
        return date.weekday() in [4, 5, 6]  # 4=Friday, 5=Saturday, 6=Sunday

    def _has_three_consecutive_weekends(self, worker_id, date):
        """Check if worker has worked three consecutive weekends"""
        if not self._is_weekend_day(date):
            return False
        
        weekends = self.worker_weekends[worker_id]
        # Add current weekend if not already included
        current_weekend = date - timedelta(days=date.weekday() - 5)  # Get Friday of current week
        if current_weekend not in weekends:
            weekends.append(current_weekend)
    
        # Sort weekends and check for three consecutive
        weekends.sort()
        if len(weekends) >= 3:
            for i in range(len(weekends) - 2):
                if (weekends[i + 2] - weekends[i]).days <= 14:  # Two weeks difference or less
                    return True
        return False

    def _get_least_used_weekday(self, worker_id):
        """Get the weekday that the worker has worked least"""
        weekdays = self.worker_weekdays[worker_id]
        return min(weekdays.items(), key=lambda x: x[1])[0]

    def _is_balanced_post_rotation(self, worker_id, post_number):
        """Check if assigning this post maintains balanced rotation"""
        posts = self.worker_posts[worker_id]
        if len(posts) < self.num_shifts:  # If worker hasn't worked all posts yet
            return post_number not in posts
        return True
    
    def _find_best_worker(self, date, post):
        """Find the most suitable worker for a given date and post"""
        logging.info(f"\nFinding best worker for {date.strftime('%Y-%m-%d')} post {post}")
        candidates = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
            logging.debug(f"Checking worker {worker_id}")
        
            if self._can_assign_worker(worker_id, date):
                score = self._calculate_worker_score(worker, date, post)  # Add post parameter
                candidates.append((worker, score))
                logging.debug(f"Worker {worker_id} is candidate with score {score}")
            else:
                logging.debug(f"Worker {worker_id} cannot be assigned to this date")

        if not candidates:
            logging.warning(f"No suitable candidates found for {date.strftime('%Y-%m-%d')} post {post}")
            return None

        best_worker = max(candidates, key=lambda x: x[1])[0]
        logging.info(f"Selected worker {best_worker['id']} with score {max(candidates, key=lambda x: x[1])[1]}")
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
             # New checks:
        
            # Check 6 weekend rule
            if self._has_three_consecutive_weekends(worker_id, date):
                logging.debug(f"Worker {worker_id} would exceed three consecutive weekends")
                return False
        
            # Check 7 weekday balance 
            weekday = date.weekday()
            min_weekday = self._get_least_used_weekday(worker_id)
            if len(self.worker_assignments[worker_id]) > 7 and weekday != min_weekday:
                logging.debug(f"Worker {worker_id} should work on {min_weekday} first")
                return False
            logging.debug(f"Worker {worker_id} can be assigned to this date")
            return True

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
            return False 

    def _calculate_worker_score(self, worker, date, post):
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

        # Factor 4: Post rotation balance
        if self._is_balanced_post_rotation(worker['id'], post):
            score += 15

        # Factor 5: Weekday balance
        if date.weekday() == self._get_least_used_weekday(worker['id']):
            score += 20

        # Factor 6: Weekend distribution
        if self._is_weekend_day(date):
            weekend_count = len(self.worker_weekends[worker['id']])
            score -= weekend_count * 5  # Penalize workers with more weekend assignments

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
