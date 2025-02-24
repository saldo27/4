from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import calendar
import logging
import sys
import requests

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

class SchedulerError(Exception):
    """Custom exception for Scheduler errors"""
    pass

class Scheduler:
    """
    Scheduler class for managing guard duty assignments
    Methods are organized in logical groups:
    1. Core/Initialization
    2. Schedule Generation
    3. Constraint Checking
    4. Balance and Distribution
    5. Date/Time Helpers
    6. Worker Statistics
    7. Data Management
    8. Cleanup and Validation
    9. Output/Export
    """

    # ------------------------
    # 1. Core/Initialization Methods
    # ------------------------
    
    def __init__(self, config):
        """
        Initialize the scheduler with configuration
        Args:
            config: Dictionary containing schedule configuration
        """
        try:
            # First validate the configuration
            self._validate_config(config)

            self.config = config
            self.start_date = config['start_date']
            self.end_date = config['end_date']
            self.num_shifts = config['num_shifts']
            self.workers_data = config['workers_data']
            self.holidays = config.get('holidays', [])
            
            # Initialize tracking dictionaries
            self.schedule = {}
            self.worker_assignments = {w['id']: set() for w in self.workers_data}  # Changed to set()
            self.worker_posts = {w['id']: set() for w in self.workers_data}
            self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data}
            
            # Track constraint skips
            self.constraint_skips = {
                w['id']: {
                    'gap': [],
                    'incompatibility': [],
                    'reduced_gap': []  # For part-time workers
                } for w in self.workers_data
            }

            # Set current time and user
            self.current_datetime = self._get_spain_time()
            self.current_user = 'saldo27'

            self._log_initialization()

            # Add max_shifts_per_worker calculation
            total_days = (self.end_date - self.start_date).days + 1
            total_shifts = total_days * self.num_shifts
            num_workers = len(self.workers_data)
            self.max_shifts_per_worker = (total_shifts // num_workers) + 2  # Add some flexibility

        except Exception as e:
            logging.error(f"Initialization error: {str(e)}")
            raise SchedulerError(f"Failed to initialize scheduler: {str(e)}")

    def _validate_config(self, config):
        """
        Validate configuration parameters
        
        Args:
            config: Dictionary containing schedule configuration
            
        Raises:
            SchedulerError: If configuration is invalid
        """
        # Check required fields
        required_fields = ['start_date', 'end_date', 'num_shifts', 'workers_data']
        for field in required_fields:
            if field not in config:
                raise SchedulerError(f"Missing required configuration field: {field}")

        # Validate date range
        if not isinstance(config['start_date'], datetime) or not isinstance(config['end_date'], datetime):
            raise SchedulerError("Start date and end date must be datetime objects")
            
        if config['start_date'] > config['end_date']:
            raise SchedulerError("Start date must be before end date")

        # Validate shifts
        if not isinstance(config['num_shifts'], int) or config['num_shifts'] < 1:
            raise SchedulerError("Number of shifts must be a positive integer")

        # Validate workers data
        if not config['workers_data'] or not isinstance(config['workers_data'], list):
            raise SchedulerError("workers_data must be a non-empty list")

        # Validate each worker's data
        for worker in config['workers_data']:
            if not isinstance(worker, dict):
                raise SchedulerError("Each worker must be a dictionary")
            
            if 'id' not in worker:
                raise SchedulerError("Each worker must have an 'id' field")
            
            # Validate work percentage if present
            if 'work_percentage' in worker:
                try:
                    work_percentage = float(str(worker['work_percentage']).strip())
                    if work_percentage <= 0 or work_percentage > 100:
                        raise SchedulerError(f"Invalid work percentage for worker {worker['id']}: {work_percentage}")
                except ValueError:
                    raise SchedulerError(f"Invalid work percentage format for worker {worker['id']}")

            # Validate date formats in mandatory_days if present
            if 'mandatory_days' in worker:
                try:
                    self._parse_dates(worker['mandatory_days'])
                except ValueError as e:
                    raise SchedulerError(f"Invalid mandatory_days format for worker {worker['id']}: {str(e)}")

            # Validate date formats in days_off if present
            if 'days_off' in worker:
                try:
                    self._parse_date_ranges(worker['days_off'])
                except ValueError as e:
                    raise SchedulerError(f"Invalid days_off format for worker {worker['id']}: {str(e)}")

        # Validate holidays if present
        if 'holidays' in config:
            if not isinstance(config['holidays'], list):
                raise SchedulerError("holidays must be a list")
            
            for holiday in config['holidays']:
                if not isinstance(holiday, datetime):
                    raise SchedulerError("Each holiday must be a datetime object")

    def _log_initialization(self):
        """Log initialization parameters"""
        logging.info("Scheduler initialized with:")
        logging.info(f"Start date: {self.start_date}")
        logging.info(f"End date: {self.end_date}")
        logging.info(f"Number of shifts: {self.num_shifts}")
        logging.info(f"Number of workers: {len(self.workers_data)}")
        logging.info(f"Holidays: {[h.strftime('%Y-%m-%d') for h in self.holidays]}")
        logging.info(f"Current datetime (Spain): {self.current_datetime}")
        logging.info(f"Current user: {self.current_user}")

    def _reset_schedule(self):
        """Reset all schedule data"""
        self.schedule = {}
        self.worker_assignments = {w['id']: set() for w in self.workers_data}
        self.worker_posts = {w['id']: set() for w in self.workers_data}
        self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
        self.worker_weekends = {w['id']: [] for w in self.workers_data}
        self.constraint_skips = {
            w['id']: {'gap': [], 'incompatibility': [], 'reduced_gap': []}
            for w in self.workers_data
        }

    def _get_spain_time(self):
        """Get current time in Spain timezone with fallback options"""
        try:
            response = requests.get(
                'http://worldtimeapi.org/api/timezone/Europe/Madrid',
                timeout=5,
                verify=True
            )
            
            if response.status_code == 200:
                time_data = response.json()
                return datetime.fromisoformat(time_data['datetime']).replace(tzinfo=None)
                
        except (requests.RequestException, ValueError) as e:
            logging.warning(f"Error getting time from API: {str(e)}")

        try:
            spain_tz = ZoneInfo('Europe/Madrid')
            return datetime.now(spain_tz).replace(tzinfo=None)
        except Exception as e:
            logging.error(f"Fallback time error: {str(e)}")
            return datetime.utcnow()

    # ------------------------
    # 2. Schedule Generation Methods
    # ------------------------

    def generate_schedule(self):
        """Generate the complete schedule"""
        logging.info("=== Starting schedule generation ===")
        try:
            self._reset_schedule()
        
            # Step 1: Calculate target shifts
            logging.info("Step 1: Calculating target shifts...")
            self._calculate_target_shifts()
        
            # Step 2: Assign mandatory days
            logging.info("Step 2: Assigning mandatory days...")
            self._assign_mandatory_guards()
        
            # Step 3: Proceed with regular shift assignments
            logging.info("Step 3: Proceeding with regular shift assignments...")
            current_date = self.start_date
            while current_date <= self.end_date:
                self._assign_day_shifts(current_date)  # This method handles post assignment
                current_date += timedelta(days=1)

            self._cleanup_schedule()
            self._validate_final_schedule()
        
            return self.schedule

        except Exception as e:
            logging.error("Schedule generation failed", exc_info=True)
            raise SchedulerError(f"Failed to generate schedule: {str(e)}")

    def _calculate_target_shifts(self):
        """Calculate target number of shifts for each worker based on their percentage"""
        total_days = (self.end_date - self.start_date).days + 1
        total_shifts = total_days * self.num_shifts

        # Convert work_percentage to float when summing
        total_percentage = sum(float(str(w.get('work_percentage', 100)).strip()) for w in self.workers_data)

        for worker in self.workers_data:
            try:
                # Ensure work_percentage is properly converted to float
                percentage = float(str(worker.get('work_percentage', 100)).strip())
                target = (percentage / total_percentage) * total_shifts
                worker['target_shifts'] = round(target)
                logging.info(f"Worker {worker['id']} - Target shifts: {worker['target_shifts']} ({percentage}%)")
            except (ValueError, TypeError) as e:
                logging.error(f"Error processing work percentage for worker {worker.get('id')}: {str(e)}")
                raise SchedulerError(f"Invalid work percentage for worker {worker.get('id')}")

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
                        self._update_tracking_data(worker_id, date, post)

    def _assign_day_shifts(self, date):
        """
        Modified assignment method that keeps partial assignments
        """
        logging.info(f"\nAssigning shifts for {date.strftime('%Y-%m-%d')}")

        if date not in self.schedule:
            self.schedule[date] = []

        remaining_shifts = self.num_shifts - len(self.schedule[date])

        for post in range(remaining_shifts):
            best_worker = None
            best_score = float('-inf')

            for worker in self.workers_data:
                if self._can_assign_worker(worker['id'], date, post):
                    score = self._calculate_worker_score(worker, date, post)
                    if score is not None and score > best_score:
                        best_worker = worker
                        best_score = score
    
            if best_worker:
                worker_id = best_worker['id']
                self.schedule[date].append(worker_id)
                self.worker_assignments[worker_id].add(date)
                self._update_tracking_data(worker_id, date, post)
            else:
                logging.error(f"Could not find suitable worker for shift {post + 1}")

    def _get_candidates(self, date, post, skip_constraints=False, try_part_time=False):
        """Get suitable candidates with their scores"""
        candidates = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
            work_percentage = float(worker.get('work_percentage', 100))

            # Skip if max shifts reached
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                continue

            # For part-time assignment, only consider part-time workers
            if try_part_time and work_percentage >= 100:
                continue

            # Check constraints
            passed, reason = self._check_constraints(
                worker_id, 
                date,
                skip_constraints=skip_constraints,
                try_part_time=try_part_time
            )
        
            if not passed:
                logging.debug(f"Worker {worker_id} skipped: {reason}")
                continue

            score = self._calculate_worker_score(worker, date, post)
            # Only add candidate if score is not None or -inf
            if score is not None and score != float('-inf'):
                candidates.append((worker, score))
                logging.debug(f"Worker {worker_id} added as candidate with score {score}")

        return candidates

    def _calculate_worker_score(self, worker, date, post):
        """
        Calculate a score for assigning a worker to a shift
        Higher score means better fit
        """
        try:
            worker_id = worker['id']
            score = 0
        
            # --- Hard Constraints ---
            if (self._is_worker_unavailable(worker_id, date) or
                date in self.schedule and worker_id in self.schedule[date]):
                return float('-inf')
        
            current_shifts = len(self.worker_assignments[worker_id])
            target_shifts = worker.get('target_shifts', 0)
        
            # --- Target Progress Score ---
            shift_difference = target_shifts - current_shifts
        
            if shift_difference <= 0:
                return float('-inf')  # Never exceed target
        
            # Calculate progress percentage
            progress = (current_shifts / target_shifts) * 100 if target_shifts > 0 else 0
        
            # More granular progress scoring
            if progress < 60:
                score += 25000  # High priority for workers below 60%
            elif progress < 80:
                score += 20000
            elif progress < 90:
                score += 15000
            elif progress < 100:
                score += 10000
            
           # Monthly balance score
            month_key = f"{date.year}-{date.month:02d}"
            month_counts = {}
            for d in self.worker_assignments[worker_id]:
                mk = f"{d.year}-{d.month:02d}"
                month_counts[mk] = month_counts.get(mk, 0) + 1
            current_month_shifts = month_counts.get(month_key, 0)
            other_months_max = max(month_counts.values()) if month_counts else 0
            
            if current_month_shifts <= other_months_max:
                score += 5000  # Strongly prefer under-assigned months

            # Weekday balance score
            weekday = date.weekday()
            current_weekday_count = self.worker_weekdays[worker_id][weekday]
            other_weekdays_max = max(count for day, count in self.worker_weekdays[worker_id].items() if day != weekday)
            
            if current_weekday_count <= other_weekdays_max:
                score += 3000  # Prefer under-assigned weekdays

            # Post rotation score
            post_counts = self._get_post_counts(worker_id)
            current_post_count = post_counts.get(post, 0)
            other_posts_max = max(post_counts.values()) if post_counts else 0
            
            if current_post_count <= other_posts_max:
                score += 2000  # Prefer under-assigned posts
                                    
            # --- Gap Analysis ---
            recent_assignments = sorted(list(self.worker_assignments[worker_id]))
            if recent_assignments:
                days_since_last = (date - recent_assignments[-1]).days
                if days_since_last < 2:
                    return float('-inf')  # Maintain minimum 2-day gap
                elif days_since_last <= 3:
                    score += 2000
                elif days_since_last <= 5:
                    score += 4000
                else:
                    score += 6000
                
            return score
        
        except Exception as e:
            logging.error(f"Error calculating score for worker {worker['id']}: {str(e)}")
            return None

    # ------------------------
    # 3. Constraint Checking Methods
    # ------------------------

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
            min_gap = 5 if try_part_time and work_percentage < 100 else max(2, int(4 / (work_percentage / 100)))
            if not self._check_gap_constraint(worker_id, date, min_gap):
                return False, f"gap constraint ({min_gap} days)"

        # Incompatibility constraints
        if not skip_constraints and not self._check_incompatibility(worker_id, date):
            return False, "incompatibility"

        # Weekend constraints
        if self._is_weekend_day(date):
            if self._has_three_consecutive_weekends(worker_id, date):
                return False, "three consecutive weekends"

        return True, ""

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

            return False

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
            return True

    def _check_incompatibility(self, worker_id, date):
        """
        Check worker compatibility with already assigned workers for a given date
        """
        try:
            if date not in self.schedule:
                return True

            worker = next(w for w in self.workers_data if w['id'] == worker_id)
            
            for assigned_id in self.schedule[date]:
                assigned_worker = next(w for w in self.workers_data if w['id'] == assigned_id)
                
                # Check general incompatibility flag
                if worker.get('is_incompatible', False) and assigned_worker.get('is_incompatible', False):
                    date_str = date.strftime('%Y-%m-%d')
                    skip_pairs = [pair for d, pair in self.constraint_skips[worker_id]['incompatibility'] if d == date_str]
                    
                    if not any((worker_id, assigned_id) in pairs or (assigned_id, worker_id) in pairs for pairs in skip_pairs):
                        logging.debug(f"Workers {worker_id} and {assigned_id} are incompatible on {date}")
                        return False
                
                # Check specific incompatibilities list
                if ('incompatible_workers' in worker and 
                    assigned_id in worker.get('incompatible_workers', [])):
                    logging.debug(f"Worker {worker_id} is specifically incompatible with {assigned_id}")
                    return False
                    
                if ('incompatible_workers' in assigned_worker and 
                    worker_id in assigned_worker.get('incompatible_workers', [])):
                    logging.debug(f"Worker {assigned_id} is specifically incompatible with {worker_id}")
                    return False

            return True

        except Exception as e:
            logging.error(f"Error checking incompatibility for worker {worker_id}: {str(e)}")
            return False

    def _check_gap_constraint(self, worker_id, date, min_gap):
        """Check minimum gap between assignments"""
        assignments = sorted(self.worker_assignments[worker_id])
        if assignments:
            for prev_date in assignments:
                days_between = abs((date - prev_date).days)
                if days_between < min_gap or days_between in [7, 14, 21]:
                    return False
        return True

    def _has_three_consecutive_weekends(self, worker_id, date):
        """
        Check if assigning this date would result in more than 3 consecutive weekends
        """
        if not self._is_weekend_day(date):
            return False

        def get_weekend_start(d):
            if self._is_pre_holiday(d):
                return d
            elif self._is_holiday(d):
                return d - timedelta(days=1)
            else:
                return d - timedelta(days=d.weekday() - 4)

        current_weekend = get_weekend_start(date)
        weekends = sorted(self.worker_weekends[worker_id])
        
        if current_weekend not in weekends:
            weekends = sorted(weekends + [current_weekend])

        consecutive_count = 1
        for i in range(len(weekends) - 1, -1, -1):
            if i > 0:
                days_diff = abs((weekends[i] - weekends[i-1]).days)
                if days_diff == 7:
                    consecutive_count += 1
                    if consecutive_count > 3:
                        logging.debug(f"Worker {worker_id} would exceed 3 consecutive weekends")
                        return True
                else:
                    break

        return False

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
                    logging.warning(f"Invalid date format '{date_text}' - {str(e)}")
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
                logging.warning(f"Invalid date range format '{date_range}' - {str(e)}")
        return ranges

    # ------------------------
    # 4. Balance and Distribution Methods
    # ------------------------

    def _get_worker_monthly_shifts(self, worker_id, include_date=None, include_month=None):
        """
        Get the number of shifts per month for a worker
        
        Args:
            worker_id: The worker's ID
            include_date: Optional date to include in the count (for calculating potential assignments)
            include_month: The month key (YYYY-MM) of include_date
        
        Returns:
            dict: Monthly shift counts {YYYY-MM: count}
        """
        monthly_shifts = {}
        
        # Count existing assignments
        for date in self.worker_assignments[worker_id]:
            month_key = f"{date.year}-{date.month:02d}"
            monthly_shifts[month_key] = monthly_shifts.get(month_key, 0) + 1
        
        # Include potential new assignment if date provided
        if include_date and include_month:
            monthly_shifts[include_month] = monthly_shifts.get(include_month, 0) + 1
        
        return monthly_shifts

    def _can_assign_worker(self, worker_id, date, post):
        """
        Checks if a worker can be assigned to a shift by validating all constraints
        """
        try:
            # Check basic constraints first
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                logging.debug(f"Worker {worker_id} has reached maximum shifts")
                return False

            if self._is_worker_unavailable(worker_id, date):
                logging.debug(f"Worker {worker_id} is unavailable on {date}")
                return False

            # Check minimum gap between shifts
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if assignments:
                days_since_last = (date - assignments[-1]).days
                if days_since_last < 2:
                    logging.debug(f"Worker {worker_id} has insufficient gap ({days_since_last} days)")
                    return False

            # Check balance constraints
            if not self._check_monthly_balance(worker_id, date):
                logging.debug(f"Monthly balance check failed for worker {worker_id}")
                return False

            if not self._check_weekday_balance(worker_id, date):
                logging.debug(f"Weekday balance check failed for worker {worker_id}")
                return False

            if not self._check_post_rotation(worker_id, post):
                logging.debug(f"Post rotation check failed for worker {worker_id}")
                return False

            # Check incompatibility
            if not self._check_incompatibility(worker_id, date):
                logging.debug(f"Worker {worker_id} has incompatibility issue")
                return False

            # Check consecutive weekends
            if self._is_weekend_day(date):
                if self._has_three_consecutive_weekends(worker_id, date):
                    logging.debug(f"Worker {worker_id} would exceed consecutive weekends limit")
                    return False

            return True

        except Exception as e:
            logging.error(f"Error checking if worker {worker_id} can be assigned: {str(e)}")
            return False

    def _is_worker_unavailable(self, worker_id, date):
        """
        Check if worker is unavailable on a specific date
        
        Args:
            worker_id: The worker's ID
            date: Date to check availability
            
        Returns:
            bool: True if worker is unavailable, False otherwise
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
                # If work periods are specified, worker is unavailable if date is not in any work period
                if not any(start <= date <= end for start, end in work_periods):
                    logging.debug(f"Worker {worker_id} is not in work period on {date}")
                    return True

            # Check if worker is already assigned for this date
            if date in self.worker_assignments[worker_id]:
                logging.debug(f"Worker {worker_id} is already assigned on {date}")
                return True

            return False

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
            return True  # Assume unavailable in case of error

    def _calculate_weekday_imbalance(self, worker_id, date):
        """Calculate how much this assignment would affect weekday balance"""
        weekday = date.weekday()
        counts = self.worker_weekdays[worker_id].copy()
        counts[weekday] += 1
        return max(counts.values()) - min(counts.values())

    def _calculate_post_imbalance(self, worker_id, post):
        """Calculate how much this assignment would affect post balance"""
        post_counts = {i: 0 for i in range(self.num_shifts)}
        for assigned_date in self.worker_assignments[worker_id]:
            if assigned_date in self.schedule:
                assigned_post = self.schedule[assigned_date].index(worker_id)
                post_counts[assigned_post] += 1
        post_counts[post] += 1
        return max(post_counts.values()) - min(post_counts.values())

    def _try_balance_assignment(self, date, post):
        """
        Try to find a worker that would improve balance
        Returns the worker that would create the least imbalance
        """
        candidates = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
        
            # Skip if worker is unavailable
            if not self._check_constraints(worker_id, date, skip_constraints=False)[0]:
                continue
            
            # Calculate imbalance scores
            weekday_imbalance = self._calculate_weekday_imbalance(worker_id, date)
            post_imbalance = self._calculate_post_imbalance(worker_id, post)
            
            # Calculate monthly imbalance
            month_key = f"{date.year}-{date.month:02d}"
            _, monthly_imbalance = self._check_monthly_balance(worker_id, date)
        
            # Lower score is better
            total_imbalance = weekday_imbalance + post_imbalance + (monthly_imbalance * 2)
            candidates.append((worker, total_imbalance))
    
        if candidates:
            # Return worker with lowest imbalance
            return min(candidates, key=lambda x: x[1])[0]
        return None

    # ------------------------
    # 4. Balance and Distribution Methods
    # ------------------------

    def _get_worker_monthly_shifts(self, worker_id, include_date=None, include_month=None):
        """
        Get the number of shifts per month for a worker
        
        Args:
            worker_id: The worker's ID
            include_date: Optional date to include in the count (for calculating potential assignments)
            include_month: The month key (YYYY-MM) of include_date
        
        Returns:
            dict: Monthly shift counts {YYYY-MM: count}
        """
        monthly_shifts = {}
        
        # Count existing assignments
        for date in self.worker_assignments[worker_id]:
            month_key = f"{date.year}-{date.month:02d}"
            monthly_shifts[month_key] = monthly_shifts.get(month_key, 0) + 1
        
        # Include potential new assignment if date provided
        if include_date and include_month:
            monthly_shifts[include_month] = monthly_shifts.get(include_month, 0) + 1
        
        return monthly_shifts

    def _calculate_weekday_imbalance(self, worker_id, date):
        """Calculate how much this assignment would affect weekday balance"""
        weekday = date.weekday()
        counts = self.worker_weekdays[worker_id].copy()
        counts[weekday] += 1
        return max(counts.values()) - min(counts.values())

    def _calculate_post_imbalance(self, worker_id, post):
        """Calculate how much this assignment would affect post balance"""
        post_counts = {i: 0 for i in range(self.num_shifts)}
        for assigned_date in self.worker_assignments[worker_id]:
            if assigned_date in self.schedule:
                assigned_post = self.schedule[assigned_date].index(worker_id)
                post_counts[assigned_post] += 1
        post_counts[post] += 1
        return max(post_counts.values()) - min(post_counts.values())

    def _try_balance_assignment(self, date, post):
        """Try to find a worker that would improve balance"""
        candidates = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
        
            # Skip if worker is unavailable
            if not self._check_constraints(worker_id, date, skip_constraints=False)[0]:
                continue
            
            # Calculate imbalance scores
            weekday_imbalance = self._calculate_weekday_imbalance(worker_id, date)
            post_imbalance = self._calculate_post_imbalance(worker_id, post)
            monthly_imbalance = self._check_monthly_balance(worker_id, date)[1]
        
            # Lower score is better
            total_imbalance = weekday_imbalance + post_imbalance + monthly_imbalance
            candidates.append((worker, total_imbalance))
    
        if candidates:
            # Return worker with lowest imbalance
            return min(candidates, key=lambda x: x[1])[0]
        return None

    def _check_monthly_balance(self, worker_id, date):
       """
       Check if assigning this date would maintain monthly balance
        
       Returns:
           bool: True if assignment maintains balance, False otherwise
       """
       try:
            # Get current monthly distribution
               distribution = {}
               for assigned_date in self.worker_assignments[worker_id]:
                month_key = f"{assigned_date.year}-{assigned_date.month:02d}"
                distribution[month_key] = distribution.get(month_key, 0) + 1

            # Add the new date to distribution
            month_key = f"{date.year}-{date.month:02d}"
            new_distribution = distribution.copy()
            new_distribution[month_key] = new_distribution.get(month_key, 0) + 1

            # Check balance
            if new_distribution:
                max_shifts = max(new_distribution.values())
                min_shifts = min(new_distribution.values())
                if max_shifts - min_shifts > 2:  # Allow maximum 2 shifts difference between months
                    logging.debug(f"Monthly balance violated for worker {worker_id}: {new_distribution}")
                    return False

            return True

        except Exception as e:
            logging.error(f"Error checking monthly balance for worker {worker_id}: {str(e)}")
            return True  # Allow assignment in case of error

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
            
            # Allow maximum 1 shift difference between weekdays
            if max_count - min_count > 1:
                logging.debug(f"Weekday balance violated for worker {worker_id}: {weekday_counts}")
                return False

            return True

        except Exception as e:
            logging.error(f"Error checking weekday balance for worker {worker_id}: {str(e)}")
            return True

    def _check_post_rotation(self, worker_id, post):
    """
    Check if assigning this post would maintain proper rotation
    
    Returns:
        bool: True if assignment maintains balance, False otherwise
    """
    try:
        # Get current post counts
        post_counts = self._get_post_counts(worker_id)
        post_counts[post] = post_counts.get(post, 0) + 1

        # Calculate maximum difference
        if post_counts:
            max_posts = max(post_counts.values())
            min_posts = min(post_counts.values())
            if max_posts - min_posts > 1:  # Allow maximum 1 post difference
                logging.debug(f"Post rotation violated for worker {worker_id}: {post_counts}")
                return False

        return True

    except Exception as e:
        logging.error(f"Error checking post rotation for worker {worker_id}: {str(e)}")
        return True

    # ------------------------
    # 5. Date/Time Helper Methods
    # ------------------------

    def _get_effective_weekday(self, date):
        """
        Get the effective weekday, treating holidays as Sundays and pre-holidays as Fridays
        
        Args:
            date: datetime object
        Returns:
            int: 0-6 representing Monday-Sunday, with holidays as 6 and pre-holidays as 4
        """
        if self._is_holiday(date):
            return 6  # Sunday
        if self._is_pre_holiday(date):
            return 4  # Friday
        return date.weekday()

    def _is_weekend_day(self, date):
        """
        Check if date is a weekend day, holiday, or pre-holiday
        
        Args:
            date: datetime object
        Returns:
            bool: True if weekend/holiday/pre-holiday, False otherwise
        """
        if self._is_holiday(date):
            return True
        if self._is_pre_holiday(date):
            return True
        return date.weekday() in [4, 5, 6]  # Friday = 4, Saturday = 5, Sunday = 6

    def _is_holiday(self, date):
        """
        Check if a date is a holiday
        
        Args:
            date: datetime object
        Returns:
            bool: True if holiday, False otherwise
        """
        return date in self.holidays

    def _is_pre_holiday(self, date):
        """
        Check if a date is the day before a holiday
        
        Args:
            date: datetime object
        Returns:
            bool: True if pre-holiday, False otherwise
        """
        next_day = date + timedelta(days=1)
        return next_day in self.holidays

    def _get_weekend_start(self, date):
        """
        Get the start date (Friday) of the weekend containing this date
        
        Args:
            date: datetime object
        Returns:
            datetime: Friday date of the weekend (or holiday start)
        """
        if self._is_pre_holiday(date):
            return date
        elif self._is_holiday(date):
            return date - timedelta(days=1)
        else:
            # Regular weekend - get to Friday
            return date - timedelta(days=date.weekday() - 4)

    def _get_schedule_months(self):
        """
        Calculate number of months in schedule period
        
        Returns:
            int: Number of months between start_date and end_date
        """
        return ((self.end_date.year * 12 + self.end_date.month) -
                (self.start_date.year * 12 + self.start_date.month) + 1)

    def _get_month_key(self, date):
        """
        Get standardized month key for a date
        
        Args:
            date: datetime object
        Returns:
            str: Month key in format 'YYYY-MM'
        """
        return f"{date.year}-{date.month:02d}"

    def _days_between(self, date1, date2):
        """
        Calculate the number of days between two dates
        
        Args:
            date1: datetime object
            date2: datetime object
        Returns:
            int: Absolute number of days between dates
        """
        return abs((date2 - date1).days)

    def _is_same_month(self, date1, date2):
        """
        Check if two dates are in the same month
        
        Args:
            date1: datetime object
            date2: datetime object
        Returns:
            bool: True if same year and month, False otherwise
        """
        return date1.year == date2.year and date1.month == date2.month

    def _get_month_dates(self, year, month):
        """
        Get all dates in a specific month
        
        Args:
            year: int
            month: int
        Returns:
            list: List of datetime objects for each day in the month
        """
        num_days = calendar.monthrange(year, month)[1]
        return [
            datetime(year, month, day)
            for day in range(1, num_days + 1)
        ]

    def _get_month_workdays(self, year, month):
        """
        Get all workdays (non-holidays, non-weekends) in a specific month
        
        Args:
            year: int
            month: int
        Returns:
            list: List of datetime objects for workdays in the month
        """
        return [
            date for date in self._get_month_dates(year, month)
            if not self._is_weekend_day(date) and not self._is_holiday(date)
        ]

    # ------------------------
    # 6. Worker Statistics Methods
    # ------------------------

    def _get_post_counts(self, worker_id):
        """
        Get count of assignments per post for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            dict: Count of assignments for each post number
        """
        post_counts = {i: 0 for i in range(self.num_shifts)}
        for assigned_date in self.worker_assignments[worker_id]:
            if assigned_date in self.schedule:
                post = self.schedule[assigned_date].index(worker_id)
                post_counts[post] += 1
        return post_counts

    def _get_monthly_distribution(self, worker_id):
        """
        Get monthly shift distribution for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            dict: Monthly shift counts {YYYY-MM: count}
        """
        distribution = {}
        for date in sorted(list(self.worker_assignments[worker_id])):
            month_key = f"{date.year}-{date.month:02d}"
            distribution[month_key] = distribution.get(month_key, 0) + 1
        return distribution

    def _analyze_gaps(self, worker_id):
        """
        Analyze gaps between shifts for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            dict: Statistics about gaps between assignments
        """
        assignments = sorted(list(self.worker_assignments[worker_id]))
        if len(assignments) <= 1:
            return {'min_gap': None, 'max_gap': None, 'avg_gap': None}

        gaps = [(assignments[i+1] - assignments[i]).days 
                for i in range(len(assignments)-1)]
        
        return {
            'min_gap': min(gaps),
            'max_gap': max(gaps),
            'avg_gap': sum(gaps) / len(gaps)
        }

    def _get_least_used_weekday(self, worker_id):
        """
        Get the weekday that has been used least often for this worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            int: Weekday number (0-6 for Monday-Sunday)
        """
        weekdays = self.worker_weekdays[worker_id]
        min_count = min(weekdays.values())
        # If there are multiple weekdays with the same minimum count,
        # prefer the earliest one in the week
        for weekday in range(7):  # 0-6 for Monday-Sunday
            if weekdays[weekday] == min_count:
                return weekday
        return 0  # Fallback to Monday if something goes wrong

    def gather_statistics(self):
        """
        Gather comprehensive schedule statistics
        
        Returns:
            dict: Detailed statistics about the schedule and worker assignments
        """
        stats = {
            'general': {
                'total_days': (self.end_date - self.start_date).days + 1,
                'total_shifts': sum(len(shifts) for shifts in self.schedule.values()),
                'constraint_skips': {
                    'gap': sum(len(skips['gap']) for skips in self.constraint_skips.values()),
                    'incompatibility': sum(len(skips['incompatibility']) for skips in self.constraint_skips.values()),
                    'reduced_gap': sum(len(skips['reduced_gap']) for skips in self.constraint_skips.values())
                }
            },
            'workers': {}
        }

        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = self.worker_assignments[worker_id]
            
            monthly_dist = self._get_monthly_distribution(worker_id)
            monthly_stats = {
                'distribution': monthly_dist,
                'min_monthly': min(monthly_dist.values()) if monthly_dist else 0,
                'max_monthly': max(monthly_dist.values()) if monthly_dist else 0,
                'monthly_imbalance': max(monthly_dist.values()) - min(monthly_dist.values()) if monthly_dist else 0
            }
            
            stats['workers'][worker_id] = {
                'total_shifts': len(assignments),
                'target_shifts': worker.get('target_shifts', 0),
                'work_percentage': worker.get('work_percentage', 100),
                'weekend_shifts': len(self.worker_weekends[worker_id]),
                'weekday_distribution': self.worker_weekdays[worker_id],
                'post_distribution': self._get_post_counts(worker_id),
                'constraint_skips': self.constraint_skips[worker_id],
                'monthly_stats': monthly_stats,
                'gaps_analysis': self._analyze_gaps(worker_id)
            }

        # Add monthly balance analysis
        stats['monthly_balance'] = self._analyze_monthly_balance()
        
        return stats

    def _analyze_monthly_balance(self):
        """
        Analyze monthly balance across all workers
        
        Returns:
            dict: Statistics about monthly distribution balance
        """
        monthly_stats = {}
        
        # Get all months in schedule period
        all_months = set()
        for worker_id in self.worker_assignments:
            dist = self._get_monthly_distribution(worker_id)
            all_months.update(dist.keys())
        
        for month in sorted(all_months):
            worker_counts = []
            for worker_id in self.worker_assignments:
                dist = self._get_monthly_distribution(worker_id)
                worker_counts.append(dist.get(month, 0))
            
            if worker_counts:
                monthly_stats[month] = {
                    'min_shifts': min(worker_counts),
                    'max_shifts': max(worker_counts),
                    'avg_shifts': sum(worker_counts) / len(worker_counts),
                    'imbalance': max(worker_counts) - min(worker_counts)
                }
        
        return monthly_stats

    def _get_worker_shift_ratio(self, worker_id):
        """
        Calculate the ratio of assigned shifts to target shifts for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            float: Ratio of assigned/target shifts (1.0 = perfect match)
        """
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        target = worker.get('target_shifts', 0)
        if target == 0:
            return 0
        return len(self.worker_assignments[worker_id]) / target

    # ------------------------
    # 7. Data Management Methods
    # ------------------------

    def _update_tracking_data(self, worker_id, date, post):
        """
        Update all tracking data for a new assignment
        
        Args:
            worker_id: The worker's ID
            date: Assignment date
            post: Post number
        """
        # Update post tracking
        self.worker_posts[worker_id].add(post)
        
        # Update weekday tracking
        effective_weekday = self._get_effective_weekday(date)
        self.worker_weekdays[worker_id][effective_weekday] += 1
    
        # Update weekend tracking if applicable
        if self._is_weekend_day(date):
            weekend_start = self._get_weekend_start(date)
            if weekend_start not in self.worker_weekends[worker_id]:
                self.worker_weekends[worker_id].append(weekend_start)

    def _update_worker_stats(self, worker_id, date, removing=False):
        """
        Update worker statistics for assignment or removal
        
        Args:
            worker_id: The worker's ID
            date: Date of assignment
            removing: Boolean indicating if this is a removal operation
        """
        effective_weekday = self._get_effective_weekday(date)
    
        if removing:
            # Decrease weekday count
            self.worker_weekdays[worker_id][effective_weekday] = max(
                0, self.worker_weekdays[worker_id][effective_weekday] - 1
            )
        
            # Remove weekend if applicable
            if self._is_weekend_day(date):
                weekend_start = self._get_weekend_start(date)
                if weekend_start in self.worker_weekends[worker_id]:
                    self.worker_weekends[worker_id].remove(weekend_start)
        else:
            # Increase weekday count
            self.worker_weekdays[worker_id][effective_weekday] += 1
        
            # Add weekend if applicable
            if self._is_weekend_day(date):
                weekend_start = self._get_weekend_start(date)
                if weekend_start not in self.worker_weekends[worker_id]:
                    self.worker_weekends[worker_id].append(weekend_start)

    def _record_constraint_skip(self, worker_id, date, constraint_type, other_worker_id=None):
        """
        Record when a constraint is skipped for tracking purposes
        
        Args:
            worker_id: The worker's ID
            date: Date of the constraint skip
            constraint_type: Type of constraint ('gap', 'incompatibility', 'reduced_gap')
            other_worker_id: Optional ID of other worker involved (for incompatibility)
        """
        date_str = date.strftime('%Y-%m-%d')
        if constraint_type == 'incompatibility' and other_worker_id:
            self.constraint_skips[worker_id][constraint_type].append(
                (date_str, (worker_id, other_worker_id))
            )
        else:
            self.constraint_skips[worker_id][constraint_type].append(date_str)
        
        logging.warning(
            f"Constraint skip recorded - Type: {constraint_type}, "
            f"Worker: {worker_id}, Date: {date_str}"
        )

    def _find_incomplete_days(self):
        """
        Find days with incomplete shift assignments
        
        Returns:
            list: Dates where not all shifts are assigned
        """
        return [
            date for date in self.schedule.keys()
            if len(self.schedule[date]) < self.num_shifts
        ]

    def _remove_day_assignments(self, date):
        """
        Remove all assignments for a specific day and update statistics
        
        Args:
            date: Date to remove assignments from
        """
        if date not in self.schedule:
            return
    
        for worker_id in self.schedule[date]:
            # Remove from worker assignments
            self.worker_assignments[worker_id].discard(date)
        
            # Update tracking data
            self._update_worker_stats(worker_id, date, removing=True)
        
        # Remove the day from schedule
        del self.schedule[date]

    # ------------------------
    # 8. Cleanup and Validation Methods
    # ------------------------

    def _cleanup_schedule(self):
        """
        Clean up the schedule by removing incomplete assignments
        Logs cleanup operations and updates all relevant tracking data
        """
        logging.info("Starting schedule cleanup...")
        dates_to_remove = self._find_incomplete_days()
    
        for date in dates_to_remove:
            logging.info(f"Removing incomplete assignments for {date.strftime('%Y-%m-%d')}")
            self._remove_day_assignments(date)
    
        logging.info(f"Schedule cleanup complete. Removed {len(dates_to_remove)} incomplete days.")

    def _validate_final_schedule(self):
        """
        Comprehensive schedule validation including all balance requirements
        Raises SchedulerError if critical issues are found
        """
        errors = []
        warnings = []

        logging.info("Starting final schedule validation...")

        # Check each date in schedule
        for date in sorted(self.schedule.keys()):
            self._validate_daily_assignments(date, errors, warnings)

        # Check worker-specific constraints
        for worker in self.workers_data:
            worker_id = worker['id']
            self._validate_worker_constraints(worker_id, errors, warnings)

        # Handle validation results
        if errors:
            error_msg = "Schedule validation failed:\n" + "\n".join(errors)
            logging.error(error_msg)
            raise SchedulerError(error_msg)
        
        if warnings:
            for warning in warnings:
                logging.warning(warning)

        logging.info("Schedule validation completed successfully")

    def _validate_daily_assignments(self, date, errors, warnings):
        """
        Validate assignments for a specific date
        
        Args:
            date: Date to validate
            errors: List to collect critical errors
            warnings: List to collect non-critical issues
        """
        assigned_workers = self.schedule[date]
        
        # Check staffing levels
        if len(assigned_workers) < self.num_shifts:
            warnings.append(
                f"Understaffed on {date.strftime('%Y-%m-%d')}: "
                f"{len(assigned_workers)} of {self.num_shifts} shifts filled"
            )
        
        # Check worker incompatibilities
        for i, worker_id in enumerate(assigned_workers):
            worker = next(w for w in self.workers_data if w['id'] == worker_id)
            
            for other_id in assigned_workers[i+1:]:
                other_worker = next(w for w in self.workers_data if w['id'] == other_id)
                
                if (worker.get('is_incompatible', False) and 
                    other_worker.get('is_incompatible', False)):
                    if not self._is_authorized_incompatibility(date, worker_id, other_id):
                        errors.append(
                            f"Unauthorized incompatible workers {worker_id} and {other_id} "
                            f"on {date.strftime('%Y-%m-%d')}"
                        )

    def _validate_worker_constraints(self, worker_id, errors, warnings):
        """
        Validate all constraints for a specific worker
        
        Args:
            worker_id: Worker's ID to validate
            errors: List to collect critical errors
            warnings: List to collect non-critical issues
        """
        # Validate post rotation
        self._validate_post_rotation(worker_id, warnings)
        
        # Validate monthly distribution
        self._validate_monthly_distribution(worker_id, warnings)
        
        # Validate weekday distribution
        self._validate_weekday_distribution(worker_id, warnings)
        
        # Validate consecutive weekends
        self._validate_consecutive_weekends(worker_id, errors)
        
        # Validate shift targets
        self._validate_shift_targets(worker_id, warnings)

    def _validate_post_rotation(self, worker_id, warnings):
        """Validate post rotation balance for a worker"""
        post_counts = self._get_post_counts(worker_id)
        max_diff = max(post_counts.values()) - min(post_counts.values())
        if max_diff > 2:
            warnings.append(
                f"Worker {worker_id} post rotation imbalance: {post_counts}"
            )

    def _validate_monthly_distribution(self, worker_id, warnings):
        """Validate monthly shift distribution for a worker"""
        monthly_shifts = self._get_monthly_distribution(worker_id)
        
        if monthly_shifts:
            max_monthly = max(monthly_shifts.values())
            min_monthly = min(monthly_shifts.values())
        
            if max_monthly - min_monthly > 1:
                warnings.append(
                    f"Worker {worker_id} monthly shift imbalance: "
                    f"Max={max_monthly}, Min={min_monthly}\n"
                    f"Distribution: {monthly_shifts}"
                )

    def _validate_weekday_distribution(self, worker_id, warnings):
        """Validate weekday distribution for a worker"""
        weekday_counts = self.worker_weekdays[worker_id]
        max_weekday_diff = max(weekday_counts.values()) - min(weekday_counts.values())
        if max_weekday_diff > 2:
            warnings.append(
                f"Worker {worker_id} weekday imbalance: {weekday_counts}"
            )

    def _validate_consecutive_weekends(self, worker_id, errors):
        """Validate consecutive weekend assignments for a worker"""
        weekends = sorted(self.worker_weekends[worker_id])
        consecutive_count = 1
        max_consecutive = 1
        
        for i in range(1, len(weekends)):
            if (weekends[i] - weekends[i-1]).days == 7:
                consecutive_count += 1
                max_consecutive = max(max_consecutive, consecutive_count)
            else:
                consecutive_count = 1
        
        if max_consecutive > 3:
            errors.append(
                f"Worker {worker_id} has {max_consecutive} consecutive weekends"
            )

    def _validate_shift_targets(self, worker_id, warnings):
        """Validate if worker has met their shift targets"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        assignments = self.worker_assignments[worker_id]
        
        shift_difference = abs(len(assignments) - worker['target_shifts'])
        if shift_difference > 2:  # Allow small deviation
            warnings.append(
                f"Worker {worker_id} has {len(assignments)} shifts "
                f"(target: {worker['target_shifts']})"
            )

    def _is_authorized_incompatibility(self, date, worker1_id, worker2_id):
        """
        Check if an incompatibility was authorized via constraint skip
        
        Args:
            date: Date of the assignment
            worker1_id: First worker's ID
            worker2_id: Second worker's ID
        Returns:
            bool: True if incompatibility was authorized
        """
        date_str = date.strftime('%Y-%m-%d')
        return (date_str, (worker1_id, worker2_id)) in self.constraint_skips[worker1_id]['incompatibility'] or \
               (date_str, (worker2_id, worker1_id)) in self.constraint_skips[worker2_id]['incompatibility']

    # ------------------------
    # 9. Output/Export Methods
    # ------------------------

    def export_schedule(self, format='txt'):
        """
        Export the schedule in the specified format
        
        Args:
            format: Output format ('txt' currently supported)
        Returns:
            str: Name of the generated file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'schedule_{timestamp}.{format}'
        
        if format == 'txt':
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(self._generate_schedule_header())
                f.write(self._generate_schedule_body())
                f.write(self._generate_schedule_summary())
        
        logging.info(f"Schedule exported to {filename}")
        return filename

    def _generate_schedule_header(self):
        """Generate the header section of the schedule output"""
        return (
            "=== Guard Schedule ===\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Generated by: {self.current_user}\n"
            f"Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}\n"
            f"Total Workers: {len(self.workers_data)}\n"
            f"Shifts per Day: {self.num_shifts}\n"
            "=" * 40 + "\n\n"
        )

    def _generate_schedule_body(self):
        """Generate the main body of the schedule output"""
        output = ""
        for date in sorted(self.schedule.keys()):
            # Date header
            output += f"\n{date.strftime('%Y-%m-%d')} ({date.strftime('%A')})"
            if self._is_holiday(date):
                output += " [HOLIDAY]"
            elif self._is_pre_holiday(date):
                output += " [PRE-HOLIDAY]"
            output += "\n"
            
            # Shift assignments
            for i, worker_id in enumerate(self.schedule[date], 1):
                worker = next(w for w in self.workers_data if w['id'] == worker_id)
                output += f"  Shift {i}: Worker {worker_id}"
                
                # Add worker details
                work_percentage = worker.get('work_percentage', 100)
                if float(work_percentage) < 100:
                    output += f" (Part-time: {work_percentage}%)"
                
                # Add post rotation info
                post_counts = self._get_post_counts(worker_id)
                output += f" [Post {i} count: {post_counts.get(i-1, 0)}]"
                
                output += "\n"
            output += "-" * 40 + "\n"
        
        return output

    def _generate_schedule_summary(self):
        """Generate summary statistics for the schedule output"""
        stats = self.gather_statistics()
        
        summary = "\n=== Schedule Summary ===\n"
        summary += f"Total Days: {stats['general']['total_days']}\n"
        summary += f"Total Shifts: {stats['general']['total_shifts']}\n"
        
        # Constraint skip summary
        summary += "\nConstraint Skips:\n"
        for skip_type, count in stats['general']['constraint_skips'].items():
            summary += f"  {skip_type.title()}: {count}\n"
        
        # Worker summary
        summary += "\nWorker Statistics:\n"
        for worker_id, worker_stats in stats['workers'].items():
            summary += f"\nWorker {worker_id}:\n"
            summary += f"  Assigned/Target: {worker_stats['total_shifts']}/{worker_stats['target_shifts']}\n"
            summary += f"  Weekend Shifts: {worker_stats['weekend_shifts']}\n"
            
            # Monthly distribution
            monthly_stats = worker_stats['monthly_stats']
            summary += "  Monthly Distribution:\n"
            for month, count in monthly_stats['distribution'].items():
                summary += f"    {month}: {count}\n"
        
        summary += "\n" + "=" * 40 + "\n"
        return summary

    def get_worker_schedule(self, worker_id):
        """
        Get detailed schedule for a specific worker
        
        Args:
            worker_id: The worker's ID to get schedule for
        Returns:
            dict: Detailed schedule information for the worker
        """
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        assignments = sorted(list(self.worker_assignments[worker_id]))
        
        schedule_info = {
            'worker_id': worker_id,
            'work_percentage': worker.get('work_percentage', 100),
            'total_shifts': len(assignments),
            'target_shifts': worker.get('target_shifts', 0),
            'assignments': [
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'weekday': date.strftime('%A'),
                    'post': self.schedule[date].index(worker_id) + 1,
                    'is_weekend': self._is_weekend_day(date),
                    'is_holiday': self._is_holiday(date),
                    'is_pre_holiday': self._is_pre_holiday(date)
                }
                for date in assignments
            ],
            'distribution': {
                'monthly': self._get_monthly_distribution(worker_id),
                'weekday': self.worker_weekdays[worker_id],
                'posts': self._get_post_counts(worker_id)
            },
            'constraint_skips': self.constraint_skips[worker_id],
            'gaps_analysis': self._analyze_gaps(worker_id)
        }
        
        return schedule_info

    def generate_worker_report(self, worker_id):
        """
        Generate a detailed report for a specific worker
        
        Args:
            worker_id: The worker's ID to generate report for
        Returns:
            str: Formatted report text
        """

    def get_schedule_metrics(self):
        """
        Calculate schedule performance metrics
        
        Returns:
            dict: Dictionary containing various schedule performance metrics
        """
        metrics = {
            'coverage': self._calculate_coverage(),
            'balance_score': self._calculate_balance_score(),
            'constraint_violations': self._count_constraint_violations(),
            'worker_satisfaction': self._calculate_worker_satisfaction()
        }
        
        logging.info("Generated schedule metrics")
        return metrics

    def _calculate_coverage(self):
        """Calculate schedule coverage percentage"""
        total_required_shifts = (
            (self.end_date - self.start_date).days + 1
        ) * self.num_shifts
        
        actual_shifts = sum(len(shifts) for shifts in self.schedule.values())
        return (actual_shifts / total_required_shifts) * 100

    def _calculate_balance_score(self):
        """Calculate overall balance score based on various factors"""
        scores = []
        
        # Post rotation balance
        for worker_id in self.worker_assignments:
            post_counts = self._get_post_counts(worker_id)
            if post_counts.values():
                post_imbalance = max(post_counts.values()) - min(post_counts.values())
                scores.append(max(0, 100 - (post_imbalance * 20)))
        
        # Weekday distribution balance
        for worker_id, weekdays in self.worker_weekdays.items():
            if weekdays.values():
                weekday_imbalance = max(weekdays.values()) - min(weekdays.values())
                scores.append(max(0, 100 - (weekday_imbalance * 20)))
        
        return sum(scores) / len(scores) if scores else 0

    def _count_constraint_violations(self):
        """Count total constraint violations"""
        return {
            'gap_violations': sum(
                len(skips['gap']) for skips in self.constraint_skips.values()
            ),
            'incompatibility_violations': sum(
                len(skips['incompatibility']) for skips in self.constraint_skips.values()
            ),
            'reduced_gap_violations': sum(
                len(skips['reduced_gap']) for skips in self.constraint_skips.values()
            )
        }

    def _calculate_worker_satisfaction(self):
        """Calculate worker satisfaction score based on preferences and constraints"""
        satisfaction_scores = []
        
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = len(self.worker_assignments[worker_id])
            target = worker.get('target_shifts', 0)
            
            # Calculate basic satisfaction score
            if target > 0:
                target_satisfaction = 100 - (abs(assignments - target) / target * 100)
                satisfaction_scores.append(target_satisfaction)
            
            # Deduct points for constraint violations
            violations = sum(len(v) for v in self.constraint_skips[worker_id].values())
            if violations > 0:
                violation_penalty = min(violations * 10, 50)  # Cap penalty at 50%
                satisfaction_scores.append(100 - violation_penalty)
        
        return sum(satisfaction_scores) / len(satisfaction_scores) if satisfaction_scores else 0

    def verify_schedule_integrity(self):
        """
        Verify schedule integrity and constraints
        
        Returns:
            tuple: (bool, dict) - (is_valid, results)
                is_valid: True if schedule passes all validations
                results: Dictionary containing validation results and metrics
        """
        try:
            # Run comprehensive validation
            self._validate_final_schedule()
            
            # Gather statistics and metrics
            stats = self.gather_statistics()
            metrics = self.get_schedule_metrics()
            
            # Calculate coverage
            coverage = self._calculate_coverage()
            if coverage < 95:  # Less than 95% coverage is considered problematic
                logging.warning(f"Low schedule coverage: {coverage:.1f}%")
            
            # Check worker assignment balance
            for worker_id, worker_stats in stats['workers'].items():
                if abs(worker_stats['total_shifts'] - worker_stats['target_shifts']) > 2:
                    logging.warning(
                        f"Worker {worker_id} has significant deviation from target shifts: "
                        f"Actual={worker_stats['total_shifts']}, "
                        f"Target={worker_stats['target_shifts']}"
                    )
            
            return True, {
                'stats': stats,
                'metrics': metrics,
                'coverage': coverage,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'validator': self.current_user
            }
            
        except SchedulerError as e:
            logging.error(f"Schedule validation failed: {str(e)}")
            return False, str(e)

    


    
