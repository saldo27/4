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
        """Get current time in Spain using a time API with improved error handling and timeout"""
        try:
            # Try to get time from WorldTimeAPI with a 5 second timeout
            response = requests.get('http://worldtimeapi.org/api/timezone/Europe/Madrid', 
                                  timeout=5,  # Add 5 second timeout
                                  verify=True)  # Ensure SSL verification
        
            if response.status_code == 200:
                time_data = response.json()
                return datetime.fromisoformat(time_data['datetime']).replace(tzinfo=None)
            else:
                logging.warning(f"WorldTimeAPI returned status code: {response.status_code}")
            
        except requests.Timeout:
            logging.warning("WorldTimeAPI request timed out after 5 seconds")
        except requests.RequestException as e:
            logging.warning(f"Error connecting to WorldTimeAPI: {str(e)}")
        except Exception as e:
            logging.warning(f"Unexpected error while fetching time: {str(e)}")
    
        # Improved fallback: Use system time converted to Spain time
        try:
            spain_tz = ZoneInfo('Europe/Madrid')
            current_time = datetime.now(spain_tz)
            logging.info(f"Using system time (converted to Spain timezone): {current_time}")
            return current_time.replace(tzinfo=None)
        except Exception as e:
            logging.error(f"Error in fallback time calculation: {str(e)}")
            # Ultimate fallback: just use UTC time
            return datetime.utcnow()
        
    def __init__(self, config):
        try:
            self.config = config
            self.start_date = config['start_date']
            self.end_date = config['end_date']
            self.num_shifts = config['num_shifts']
            self.workers_data = config['workers_data']
            self.holidays = config.get('holidays', [])  # Add this line
            self.schedule = {}
            self.worker_assignments = {w['id']: [] for w in self.workers_data}
    
            # New tracking dictionaries
            self.worker_posts = {w['id']: set() for w in self.workers_data}
            self.worker_weekdays = {w['id']: {0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data}

            # Add tracking for skipped constraints
            self.skipped_constraints = {
                'incompatibility': set(),  # Will store (date, (worker1, worker2)) tuples
                'gap': set()              # Will store (date, worker_id) tuples
            }
            
            # Get current time in Spain
            self.current_datetime = self._get_spain_time()
            self.current_user = 'saldo27'
    
            logging.info(f"Scheduler initialized with:")
            logging.info(f"Start date: {self.start_date}")
            logging.info(f"End date: {self.end_date}")
            logging.info(f"Number of shifts: {self.num_shifts}")
            logging.info(f"Number of workers: {len(self.workers_data)}")
            logging.info(f"Holidays: {[h.strftime('%Y-%m-%d') for h in self.holidays]}")
            logging.info(f"Current datetime (Spain): {self.current_datetime}")
            logging.info(f"Current user: {self.current_user}")
    
        except Exception as e:
            logging.error(f"Error initializing scheduler: {str(e)}")
            raise
        
    def _is_holiday(self, date):
        """Check if a date is a holiday"""
        return date in self.holidays

    def _is_pre_holiday(self, date):
        """Check if a date is the day before a holiday"""
        next_day = date + timedelta(days=1)
        return next_day in self.holidays

    def _get_effective_weekday(self, date):
        """Get the effective weekday, treating holidays as Sundays and pre-holidays as Fridays"""
        if self._is_holiday(date):
            return 6  # Sunday
        if self._is_pre_holiday(date):
            return 4  # Friday
        return date.weekday()
        
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

            # Clean up any over-assigned days
            self._cleanup_schedule()
        
            # Validate final schedule
            logging.info("\nValidating final schedule...")
            errors, warnings = self._validate_schedule()  # Changed from validate_schedule to _validate_schedule
        
            if warnings:
                logging.warning("Schedule warnings:")
                for warning in warnings:
                    logging.warning(warning)
                
            if errors:
                raise ValueError("\n".join(errors))
            
            return self.schedule
        
        except Exception as e:
            logging.error(f"Error generating schedule: {str(e)}")
            logging.error("Stack trace:", exc_info=True)
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
        elif len(self.schedule[date]) >= self.num_shifts:
            logging.warning(f"Day {date.strftime('%Y-%m-%d')} already has {len(self.schedule[date])} shifts assigned (max: {self.num_shifts})")
            return
    
        # Calculate how many shifts still need to be assigned
        remaining_shifts = self.num_shifts - len(self.schedule[date])
    
        for post in range(remaining_shifts):
            logging.info(f"Finding worker for shift {post + 1}/{self.num_shifts}")
            best_worker = self._find_best_worker(date, post)
            if best_worker:
                if len(self.schedule[date]) < self.num_shifts:  # Double-check before adding
                    self.schedule[date].append(best_worker['id'])
                    self.worker_assignments[best_worker['id']].append(date)
                
                    # Update tracking data
                    self.worker_posts[best_worker['id']].add(post)
                    effective_weekday = self._get_effective_weekday(date)
                    self.worker_weekdays[best_worker['id']][effective_weekday] += 1
                
                    if self._is_weekend_day(date):
                        # Get the appropriate weekend start date
                        if self._is_pre_holiday(date):
                            weekend_start = date
                        elif self._is_holiday(date):
                            weekend_start = date - timedelta(days=1)
                        else:
                            weekend_start = date - timedelta(days=date.weekday() - 4)
                        
                        if weekend_start not in self.worker_weekends[best_worker['id']]:
                            self.worker_weekends[best_worker['id']].append(weekend_start)
                
                    logging.info(f"Assigned worker {best_worker['id']} to shift {post + 1}")
                else:
                    logging.warning(f"Maximum shifts ({self.num_shifts}) reached for {date.strftime('%Y-%m-%d')}")
                    break
            else:
                logging.error(f"Could not find suitable worker for shift {post + 1}")
                break

    def _is_weekend_day(self, date):
        """Check if the date is a weekend day (Friday, Saturday, Sunday) or a holiday/pre-holiday"""
        if self._is_holiday(date):
            return True  # Holidays are treated as Sundays
        if self._is_pre_holiday(date):
            return True  # Days before holidays are treated as Fridays
        return date.weekday() in [4, 5, 6]  # 4=Friday, 5=Saturday, 6=Sunday

    def _has_three_consecutive_weekends(self, worker_id, date):
        """
        Check if assigning this date would result in more than 3 consecutive weekends.
        Returns True if the worker would exceed 3 consecutive weekends, False otherwise.
        """
        if not self._is_weekend_day(date):
            return False

        # Get the current weekend's Friday date (or pre-holiday date)
        if self._is_pre_holiday(date):
            current_weekend = date
        elif self._is_holiday(date):
            current_weekend = date - timedelta(days=1)
        else:
            current_weekend = date - timedelta(days=date.weekday() - 4)  # Get to Friday

        # Get all weekend dates for this worker and add current one if not present
        weekends = sorted(self.worker_weekends[worker_id])
        if current_weekend not in weekends:
            weekends = sorted(weekends + [current_weekend])

        # Count consecutive weekends ending with current_weekend
        consecutive_count = 1
        for i in range(len(weekends) - 1, -1, -1):  # Start from the end
            if i > 0:  # Check if there's a previous weekend to compare
                days_diff = abs((weekends[i] - weekends[i-1]).days)
                if days_diff == 7:  # Check if weekends are consecutive
                    consecutive_count += 1
                    if consecutive_count > 3:  # If we would exceed 3 consecutive weekends
                        return True
                else:
                    break  # Break the count if weekends are not consecutive
    
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
    def _ask_permission(self, message):
        """Simulate asking the user for permission to skip a constraint.
        Replace this with actual user input handling in your application."""
        print(message)
        response = input("Type 'yes' to allow, 'no' to deny: ").strip().lower()
        return response == 'yes'
        
    def _find_best_worker(self, date, post):
        """Find the most suitable worker for a given date and post"""
        logging.info(f"\nFinding best worker for {date.strftime('%Y-%m-%d')} post {post}")
        candidates = []

        # First try: Normal assignment with all constraints
        for worker in self.workers_data:
            worker_id = worker['id']
            if not self._can_assign_worker(worker_id, date):
                continue
            score = self._calculate_worker_score(worker, date, post)
            candidates.append((worker, score))

        if not candidates:
            # Get worker with least constraint skips
            min_skips = float('inf')
            eligible_workers = []
        
            for worker in self.workers_data:
                worker_id = worker['id']
                total_skips = (self.constraint_skips[worker_id]['incompatibility'] + 
                             self.constraint_skips[worker_id]['gap'])
                if total_skips < min_skips:
                    min_skips = total_skips
                    eligible_workers = [worker]
                elif total_skips == min_skips:
                    eligible_workers.append(worker)

            # Try incompatibility skip
            for worker in eligible_workers:
                worker_id = worker['id']
                if (self._can_assign_worker_except_incompatibility(worker_id, date) and 
                    not self._check_incompatibility(worker_id, date)):
                
                    # Check the number of incompatible workers
                    incompatible_workers = []
                    if date in self.schedule:
                        for assigned_id in self.schedule[date]:
                            assigned_worker = next(w for w in self.workers_data if w['id'] == assigned_id)
                            if assigned_worker.get('is_incompatible', False):
                                incompatible_workers.append(assigned_id)
                
                    if len(incompatible_workers) >= 2:
                        continue
                
                    date_str = date.strftime('%Y-%m-%d')
                    if self._ask_permission(
                        f"Can I skip the incompatibility constraint for worker {worker_id} "
                        f"on {date_str}? "
                        f"(Current skips: {min_skips})"
                    ):
                        # Record the skipped constraint
                        for assigned_id in self.schedule.get(date, []):
                            if next(w for w in self.workers_data if w['id'] == assigned_id).get('is_incompatible', False):
                                worker_pair = tuple(sorted([worker_id, assigned_id]))
                                self.skipped_constraints['incompatibility'].add((date_str, worker_pair))
                    
                        return worker

            # Try gap constraint skip
            for worker in eligible_workers:
                worker_id = worker['id']
                if (self._can_assign_worker_except_gap(worker_id, date) and 
                    not self._check_gap_constraint(worker_id, date)):
                    if self._ask_permission(
                        f"Can I skip the gap constraint for worker {worker_id} "
                        f"on {date.strftime('%Y-%m-%d')}? "
                        f"(Current skips: {self.constraint_skips[worker_id]['gap']})"
                    ):
                        self.constraint_skips[worker_id]['gap'] += 1
                        return worker

            return None

        return max(candidates, key=lambda x: x[1])[0]

    def _was_constraint_skipped(self, date_str, worker_pair):
        """Check if a constraint was skipped for these workers on this date"""
        return (date_str, worker_pair) in self.skipped_constraints['incompatibility']
       
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
        """Check if a worker can be assigned to a date.
        Only includes basic availability, weekend rule, and weekday balance checks."""
        try:
            # First check basic availability
            if not self._check_basic_availability(worker_id, date):
                return False

            worker = next(w for w in self.workers_data if w['id'] == worker_id)

            # Check 6: Weekend rule
            if self._has_three_consecutive_weekends(worker_id, date):
                logging.debug(f"Worker {worker_id} would exceed three consecutive weekends")
                return False

            # Check 7: Weekday balance
            weekday = date.weekday()
            min_weekday = self._get_least_used_weekday(worker_id)
            if len(self.worker_assignments[worker_id]) > 7 and weekday != min_weekday:
                logging.debug(f"Worker {worker_id} should work on {min_weekday} first")
                return False

            return True

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
            return False
        
    def _can_assign_worker_except_incompatibility(self, worker_id, date):
        """Check if worker can be assigned ignoring incompatibility constraint"""
        try:
            if not self._check_basic_availability(worker_id, date):
                return False
            if not self._check_gap_constraint(worker_id, date):
                return False
            if not self._check_weekend_constraint(worker_id, date):
                return False
            return True
        except Exception as e:
            logging.error(f"Error checking worker availability: {str(e)}")
            return False

    def _can_assign_worker_except_gap(self, worker_id, date):
        """Check if worker can be assigned ignoring gap constraint"""
        try:
            if not self._check_basic_availability(worker_id, date):
                return False
            if not self._check_incompatibility(worker_id, date):
                return False
            if not self._check_weekend_constraint(worker_id, date):
                return False
            return True
        except Exception as e:
            logging.error(f"Error checking worker availability: {str(e)}")
            return False

    def _check_basic_availability(self, worker_id, date):
        """Check basic availability constraints that should never be skipped"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
    
        # Already assigned that day
        if date in self.worker_assignments[worker_id]:
            return False
        
        # Days off
        if worker.get('days_off'):
            off_periods = self._parse_date_ranges(worker['days_off'])
            if any(start <= date <= end for start, end in off_periods):
                return False
            
        # Work periods
        if worker.get('work_periods'):
            work_periods = self._parse_date_ranges(worker['work_periods'])
            if not any(start <= date <= end for start, end in work_periods):
                return False
            
        return True

    def _check_incompatibility(self, worker_id, date):
        """Check only incompatibility constraints"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)

        if date in self.schedule:
            assigned_workers = self.schedule[date]
        
            # Count incompatible workers already assigned
            incompatible_count = sum(
                1 for w_id in assigned_workers 
                if next(w for w in self.workers_data if w['id'] == w_id).get('is_incompatible', False)
            )
        
            # Don't allow more than 2 incompatible workers
            if incompatible_count >= 2:
                return False
            
            if worker.get('is_incompatible', False):
                if incompatible_count >= 2:
                    return False
                
            if 'incompatible_workers' in worker and worker['incompatible_workers']:
                if any(w_id in worker['incompatible_workers'] for w_id in assigned_workers):
                    return False
                
        return True

    def _check_gap_constraint(self, worker_id, date):
        """Check only the 21-day gap constraint"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        work_percentage = float(worker.get('work_percentage', 100))
        min_distance = max(2, int(4 / (work_percentage / 100)))
        assignments = sorted(self.worker_assignments[worker_id])
    
        if assignments:
            for prev_date in reversed(assignments):
                days_between = abs((date - prev_date).days)
                if days_between < min_distance or days_between in [7, 14, 21]:
                    return False
                
        return True
    
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
                
    def _validate_schedule(self):
        """Validate the generated schedule"""
        errors = []
        warnings = []

        for date in sorted(self.schedule.keys()):
            assigned_workers = self.schedule[date]
            logging.info(f"Checking date {date}")
            logging.info(f"Assigned workers {assigned_workers}")

            # Check number of workers
            if len(assigned_workers) < self.num_shifts:
                warnings.append(f"Too few workers ({len(assigned_workers)}) assigned on {date}. Expected {self.num_shifts}")
    
            # Check for incompatible workers
            incompatible_groups = []
            for i, worker_id in enumerate(assigned_workers):
                worker = next(w for w in self.workers_data if w['id'] == worker_id)

                incompatible_group = [worker_id]
                for other_id in assigned_workers[i+1:]:
                    if (worker.get('is_incompatible', False) and 
                        next(w for w in self.workers_data if w['id'] == other_id).get('is_incompatible', False)):
                        incompatible_group.append(other_id)
                    elif ('incompatible_workers' in worker and 
                          other_id in worker['incompatible_workers']):
                        incompatible_group.append(other_id)
            
                if len(incompatible_group) > 1:
                    # Add to errors if we didn't get permission to skip constraint
                    date_str = date.strftime('%Y-%m-%d')
                    worker_pair = tuple(sorted(incompatible_group))
                    if not self._was_constraint_skipped(date_str, worker_pair):
                        errors.append(f"Multiple incompatible workers {', '.join(map(str, incompatible_group))} assigned on {date}")

        return errors, warningss

    def _cleanup_schedule(self):
        """Clean up any days that have too many assignments"""
        for date in list(self.schedule.keys()):
            workers = self.schedule[date]
            if len(workers) > self.num_shifts:
                logging.warning(f"Cleaning up over-assigned day {date.strftime('%Y-%m-%d')}")
                # Keep only the first num_shifts workers
                removed_workers = workers[self.num_shifts:]
                self.schedule[date] = workers[:self.num_shifts]
                
                # Update worker assignments
                for worker_id in removed_workers:
                    if date in self.worker_assignments[worker_id]:
                        self.worker_assignments[worker_id].remove(date)
                    logging.info(f"Removed excess assignment of worker {worker_id}")
