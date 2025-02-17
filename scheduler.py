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
    def __init__(self, config):
        """
        Initialize the scheduler with configuration
        Args:
            config: Dictionary containing schedule configuration
        """
        try:
            self.config = config
            self.start_date = config['start_date']
            self.end_date = config['end_date']
            self.num_shifts = config['num_shifts']
            self.workers_data = config['workers_data']
            self.holidays = config.get('holidays', [])
            
            # Initialize tracking dictionaries
            self.schedule = {}
            self.worker_assignments = {w['id']: [] for w in self.workers_data}
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

    def _reset_schedule(self):
        """Reset all schedule data"""
        self.schedule = {}
        self.worker_assignments = {w['id']: [] for w in self.workers_data}
        self.worker_posts = {w['id']: set() for w in self.workers_data}
        self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
        self.worker_weekends = {w['id']: [] for w in self.workers_data}
        self.constraint_skips = {
            w['id']: {'gap': [], 'incompatibility': [], 'reduced_gap': []}
            for w in self.workers_data
        }

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
    
    def _assign_mandatory_guards(self):
        """
        Assign all mandatory guards first
        Mandatory guards take precedence over all other constraints
        """
        logging.info("Processing mandatory guards...")

        # Sort workers by number of mandatory days to handle conflicts
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
                        # Force assignment for mandatory days
                        self.schedule[date].append(worker_id)
                        self.worker_assignments[worker_id].append(date)
                    
                        # Update tracking data
                        post = len(self.schedule[date]) - 1
                        self.worker_posts[worker_id].add(post)
                    
                        # Update weekday tracking
                        effective_weekday = self._get_effective_weekday(date)
                        self.worker_weekdays[worker_id][effective_weekday] += 1
                    
                        # Update weekend tracking if applicable
                        if self._is_weekend_day(date):
                            weekend_start = self._get_weekend_start(date)
                            if weekend_start not in self.worker_weekends[worker_id]:
                                self.worker_weekends[worker_id].append(weekend_start)
                    
                        logging.info(
                            f"Assigned mandatory guard: Worker {worker_id} on "
                            f"{date.strftime('%Y-%m-%d')} (post {post})"
                        )
                    else:
                        logging.warning(
                            f"Could not assign mandatory guard for Worker {worker_id} on "
                            f"{date.strftime('%Y-%m-%d')} - slot unavailable"
                        )

    def _get_weekend_start(self, date):
        """Get the start date (Friday) of the weekend containing this date"""
        if self._is_pre_holiday(date):
            return date
        elif self._is_holiday(date):
            return date - timedelta(days=1)
        else:
            # Regular weekend - get to Friday
            return date - timedelta(days=date.weekday() - 4)

    def _get_effective_weekday(self, date):
        """Get the effective weekday, treating holidays as Sundays and pre-holidays as Fridays"""
        if self._is_holiday(date):
            return 6  # Sunday
        if self._is_pre_holiday(date):
            return 4  # Friday
        return date.weekday()

    def _is_worker_unavailable(self, worker_id, date):
        """Check if worker is unavailable on date"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        
        # Check days off
        if worker.get('days_off'):
            off_periods = self._parse_date_ranges(worker['days_off'])
            if any(start <= date <= end for start, end in off_periods):
                return True

        # Check work periods
        if worker.get('work_periods'):
            work_periods = self._parse_date_ranges(worker['work_periods'])
            if not any(start <= date <= end for start, end in work_periods):
                return True

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

    def _check_incompatibility(self, worker_id, date):
        """Check worker compatibility with already assigned workers"""
        if date not in self.schedule:
            return True

        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        
        for assigned_id in self.schedule[date]:
            assigned_worker = next(w for w in self.workers_data if w['id'] == assigned_id)
            
            # Check general incompatibility
            if worker.get('is_incompatible', False) and assigned_worker.get('is_incompatible', False):
                return False
                
            # Check specific incompatibilities
            if ('incompatible_workers' in worker and assigned_id in worker['incompatible_workers']):
                return False
            if ('incompatible_workers' in assigned_worker and worker_id in assigned_worker['incompatible_workers']):
                return False

        return True

    def _check_weekday_balance(self, worker_id, date):
        """
        Check if assigning this date maintains weekday balance
        More lenient version that allows some imbalance
        """
        weekday = date.weekday()
        weekdays = self.worker_weekdays[worker_id]
    
        # Get the last 7 assignments to check for a complete cycle
        recent_assignments = sorted(self.worker_assignments[worker_id])[-7:]
        recent_days = {d.weekday() for d in recent_assignments}
    
        if len(recent_days) < 7:
            # Still completing first cycle, be more lenient
            max_same_weekday = 2  # Allow up to 2 shifts on same weekday during first cycle
            current_count = sum(1 for d in recent_assignments if d.weekday() == weekday)
            return current_count < max_same_weekday
        else:
            # After first cycle, allow more variation
            min_count = min(weekdays.values())
            max_count = max(weekdays.values())
            current_count = weekdays[weekday]
        
            # Allow up to 2 shifts difference between min and max
            return (current_count - min_count) <= 2

    def _is_balanced_post_rotation(self, worker_id, post_number):
        """
        More lenient version of post rotation balance
        Allow some imbalance while maintaining basic rotation
        """
        posts = self.worker_posts[worker_id]
    
        if len(posts) < self.num_shifts:
            # Worker hasn't worked all posts yet
            # Allow repeating a post if no other option
            return True
        else:
            # Worker has worked all posts, check distribution
            post_counts = {i: 0 for i in range(self.num_shifts)}
            for assigned_date in self.worker_assignments[worker_id]:
                if assigned_date in self.schedule:
                    post = self.schedule[assigned_date].index(worker_id)
                    post_counts[post] += 1
        
            # Allow up to 2 shifts difference between posts
            current_count = post_counts.get(post_number, 0)
            min_count = min(post_counts.values())
            return (current_count - min_count) <= 2

    def _has_three_consecutive_weekends(self, worker_id, date):
        """
        Check if assigning this date would result in more than 3 consecutive weekends.
        Includes holidays and pre-holiday days in the weekend check.
        """
        if not self._is_weekend_day(date):
            return False

        def get_weekend_start(d):
            """Get the start date of the weekend containing this date"""
            if self._is_pre_holiday(d):
                return d
            elif self._is_holiday(d):
                return d - timedelta(days=1)
            else:
                # Regular weekend - get to Friday
                return d - timedelta(days=d.weekday() - 4)

        # Get current weekend's start date
        current_weekend = get_weekend_start(date)
    
        # Get all weekend dates for this worker
        weekends = sorted(self.worker_weekends[worker_id])
        if current_weekend not in weekends:
            weekends = sorted(weekends + [current_weekend])

        # Count consecutive weekends
        consecutive_count = 1
        for i in range(len(weekends) - 1, -1, -1):
            if i > 0:
                days_diff = abs((weekends[i] - weekends[i-1]).days)
                if days_diff == 7:  # Consecutive weekends
                    consecutive_count += 1
                    if consecutive_count > 3:
                        logging.debug(f"Worker {worker_id} would exceed 3 consecutive weekends")
                        return True
                else:
                    break  # Break if weekends not consecutive

        return False

    def _is_weekend_day(self, date):
        """
        Check if date is a weekend day, holiday, or pre-holiday
        """
        if self._is_holiday(date):
            return True
        if self._is_pre_holiday(date):
            return True
        return date.weekday() in [4, 5, 6]  # Friday = 4, Saturday = 5, Sunday = 6

    def _is_holiday(self, date):
        """Check if a date is a holiday"""
        return date in self.holidays

    def _is_pre_holiday(self, date):
        """Check if a date is the day before a holiday"""
        next_day = date + timedelta(days=1)
        return next_day in self.holidays

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

    def _assign_day_shifts(self, date):
        """Assign shifts using established constraint checking methods"""
        logging.info(f"\nAssigning shifts for {date.strftime('%Y-%m-%d')}")
    
        if date not in self.schedule:
            self.schedule[date] = []
    
        remaining_shifts = self.num_shifts - len(self.schedule[date])
    
        for post in range(remaining_shifts):
            best_worker = None
            best_score = float('-inf')
        
            for worker in self.workers_data:
                worker_id = worker['id']
            
                # Check basic constraints first
                if any([
                    date in self.schedule and worker_id in self.schedule[date],
                    self._is_worker_unavailable(worker_id, date),
                    not self._check_gap_constraint(worker_id, date, 3),
                    not self._check_incompatibility(worker_id, date)
                ]):
                    continue
                    
                score = self._calculate_worker_score(worker, date, post)
                if score is not None and score > best_score:
                    best_worker = worker
                    best_score = score
        
            if best_worker:
                worker_id = best_worker['id']
                self.schedule[date].append(worker_id)
                self.worker_assignments[worker_id].append(date)
                self._update_tracking_data(worker_id, date, post)
            else:
                logging.error(f"Could not find suitable worker for shift {post + 1}")
                break

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
    
    def _get_least_used_weekday(self, worker_id):
        """
        Get the weekday that has been used least often for this worker
        Returns the weekday number (0-6 for Monday-Sunday)
        """
        weekdays = self.worker_weekdays[worker_id]
        min_count = min(weekdays.values())
        # If there are multiple weekdays with the same minimum count,
        # prefer the earliest one in the week
        for weekday in range(7):  # 0-6 for Monday-Sunday
            if weekdays[weekday] == min_count:
                return weekday
        return 0  # Fallback to Monday if something goes wrong

    def _calculate_worker_score(self, worker, date, post):
        """Enhanced score calculation with strict balance enforcement"""
        try:
            score = 0
            worker_id = worker['id']

            # PREVENT SEVERE IMBALANCES - Check before proceeding
            weekday = date.weekday()
            weekdays = self.worker_weekdays[worker_id]
            post_counts = self._get_post_counts(worker_id)
        
            # Calculate imbalance metrics
            weekday_spread = max(weekdays.values()) - min(weekdays.values())
            post_spread = max(post_counts.values()) - min(post_counts.values())
        
            # ABSOLUTE PREVENTIONS
            if any([
                # Prevent assignments that would worsen weekday imbalance
                weekdays[weekday] > min(weekdays.values()) and weekday_spread >= 2,
                # Prevent assignments that would worsen post rotation imbalance
                post_counts.get(post, 0) > min(post_counts.values()) and post_spread >= 2,
                # Prevent exceeding target + 1
                len(self.worker_assignments[worker_id]) >= worker['target_shifts'] + 1
            ]):
                return float('-inf')

            # TARGET SHIFTS ENFORCEMENT
            current_shifts = len(self.worker_assignments[worker_id])
            target_shifts = worker['target_shifts']
            shift_difference = target_shifts - current_shifts

           # Prioritize target shifts more strongly
            current_shifts = len(self.worker_assignments[worker_id])
            target_shifts = worker['target_shifts']
            shift_difference = target_shifts - current_shifts
        
            if shift_difference > 5:  # Increased threshold
                score += 10000
            elif shift_difference > 0:
                score += 5000
            elif shift_difference == 0:
                score += 1000
            elif shift_difference > -3:  # Allow slight over-assignment
                score += 500
            else:
                return float('-inf')
    
            # WEEKDAY BALANCE
                min_weekday_count = min(weekdays.values())
                if weekdays[weekday] == min_weekday_count:
                    # Strongly encourage assignments on least-used weekdays
                    score += 3000
                else:
                    # Heavily penalize assignments that increase imbalance
                    score -= (weekdays[weekday] - min_weekday_count) * 1000

            # POST ROTATION BALANCE
            min_post_count = min(post_counts.values())
            current_post_count = post_counts.get(post, 0)
            if current_post_count == min_post_count:
                # Strongly encourage assignments to least-used posts
                score += 3000
            else:
                # Heavily penalize assignments that increase imbalance
                score -= (current_post_count - min_post_count) * 1000

            # Gap considerations with stricter penalties
            assignments = sorted(self.worker_assignments[worker_id])
            if assignments:
                days_since_last = abs((date - assignments[-1]).days)
                if days_since_last < 3:
                    return float('-inf')
                if days_since_last not in [6, 7, 8, 13, 14, 15, 20, 21, 22]:
                    score += days_since_last * 10  # Increased weight

            # Weekend handling with stronger restrictions
            if self._is_weekend_day(date):
                weekend_count = len(self.worker_weekends[worker_id])
                score -= weekend_count * 500  # Much stronger weekend penalty
            
                if self._has_three_consecutive_weekends(worker_id, date):
                    return float('-inf')

            return score

        except Exception as e:
            logging.error(f"Error calculating score for worker {worker['id']}: {str(e)}")
            return None
        
    def _get_post_counts(self, worker_id):
        """Helper method to get post counts for a worker"""
        post_counts = {i: 0 for i in range(self.num_shifts)}
        for assigned_date in self.worker_assignments[worker_id]:
            if assigned_date in self.schedule:
                post = self.schedule[assigned_date].index(worker_id)
                post_counts[post] += 1
        return post_counts

    def _validate_assignment(self, worker_id, date, post):
        """Validate if an assignment would create or worsen imbalances"""
        weekdays = self.worker_weekdays[worker_id].copy()
        weekdays[date.weekday()] += 1
    
        post_counts = self._get_post_counts(worker_id)
        post_counts[post] = post_counts.get(post, 0) + 1
    
        # Check weekday spread
        weekday_spread = max(weekdays.values()) - min(weekdays.values())
        if weekday_spread > 1:
            logging.warning(f"Assignment would create weekday imbalance for Worker {worker_id}")
            return False
        
        # Check post rotation spread
        post_spread = max(post_counts.values()) - min(post_counts.values())
        if post_spread > 1:
            logging.warning(f"Assignment would create post rotation imbalance for Worker {worker_id}")
            return False
        
        return True
        
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
        
            # Lower score is better
            total_imbalance = weekday_imbalance + post_imbalance
            candidates.append((worker, total_imbalance))
    
        if candidates:
            # Return worker with lowest imbalance
            return min(candidates, key=lambda x: x[1])[0]
        return None

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
                
    def _assign_day_shifts(self, date):
        """Assign all shifts for a specific day with strict balance validation"""
        logging.info(f"\nAssigning shifts for {date.strftime('%Y-%m-%d')}")

        if date not in self.schedule:
            self.schedule[date] = []

        remaining_shifts = self.num_shifts - len(self.schedule[date])

        for post in range(remaining_shifts):
            best_worker = None
            best_score = float('-inf')
    
            for worker in self.workers_data:
                worker_id = worker['id']
            
                # Check if worker can work using existing methods
                if (date in self.schedule and worker_id in self.schedule[date] or
                    self._is_worker_unavailable(worker_id, date) or
                    not self._check_gap_constraint(worker_id, date, 3) or
                    not self._check_incompatibility(worker_id, date)):
                    continue
                
                # Check if assignment would create imbalance
                if not self._validate_assignment(worker_id, date, post):
                    continue
                
                score = self._calculate_worker_score(worker, date, post)
                if score is not None and score > best_score:
                    best_worker = worker
                    best_score = score
    
            if best_worker:
                worker_id = best_worker['id']
                self.schedule[date].append(worker_id)
                self.worker_assignments[worker_id].append(date)
                self._update_tracking_data(worker_id, date, post)
            else:
                logging.error(f"Could not find suitable worker for shift {post + 1}")
                break

    def _update_tracking_data(self, worker_id, date, post):
        """Update all tracking data for a new assignment"""
        self.worker_posts[worker_id].add(post)
        effective_weekday = self._get_effective_weekday(date)
        self.worker_weekdays[worker_id][effective_weekday] += 1
    
        if self._is_weekend_day(date):
            weekend_start = self._get_weekend_start(date)
            if weekend_start not in self.worker_weekends[worker_id]:
                self.worker_weekends[worker_id].append(weekend_start)
                
    def _get_schedule_months(self):
        """Calculate number of months in schedule period"""
        return ((self.end_date.year * 12 + self.end_date.month) -
                (self.start_date.year * 12 + self.start_date.month) + 1)

    def gather_statistics(self):
        """Gather comprehensive schedule statistics"""
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
            
            stats['workers'][worker_id] = {
                'total_shifts': len(assignments),
                'target_shifts': worker.get('target_shifts', 0),
                'work_percentage': worker.get('work_percentage', 100),
                'weekend_shifts': len(self.worker_weekends[worker_id]),
                'weekday_distribution': self.worker_weekdays[worker_id],
                'constraint_skips': self.constraint_skips[worker_id],
                'monthly_distribution': self._get_monthly_distribution(worker_id),
                'gaps_analysis': self._analyze_gaps(worker_id)
            }

        return stats

    def _get_monthly_distribution(self, worker_id):
        """Get monthly shift distribution for a worker"""
        distribution = {}
        for date in sorted(self.worker_assignments[worker_id]):
            month_key = f"{date.year}-{date.month:02d}"
            distribution[month_key] = distribution.get(month_key, 0) + 1
        return distribution

    def _analyze_gaps(self, worker_id):
        """Analyze gaps between shifts for a worker"""
        assignments = sorted(self.worker_assignments[worker_id])
        if len(assignments) <= 1:
            return {'min_gap': None, 'max_gap': None, 'avg_gap': None}

        gaps = [(assignments[i+1] - assignments[i]).days 
                for i in range(len(assignments)-1)]
        
        return {
            'min_gap': min(gaps),
            'max_gap': max(gaps),
            'avg_gap': sum(gaps) / len(gaps)
        }
    def _cleanup_schedule(self):
        """Clean up the schedule by removing incomplete or invalid assignments"""
        logging.info("Cleaning up schedule...")
        dates_to_remove = []

        for date, assignments in self.schedule.items():
            # Check if the day has all required shifts filled
            if len(assignments) < self.num_shifts:
                logging.warning(f"Incomplete assignments for {date.strftime('%Y-%m-%d')}")
                dates_to_remove.append(date)
            
                # Remove these assignments from worker records
                for worker_id in assignments:
                    # Remove from worker assignments
                    if date in self.worker_assignments[worker_id]:
                        self.worker_assignments[worker_id].remove(date)
                
                    # Update weekday counts
                    effective_weekday = self._get_effective_weekday(date)
                    self.worker_weekdays[worker_id][effective_weekday] = max(
                        0, self.worker_weekdays[worker_id][effective_weekday] - 1
                    )
                
                    # Update weekend tracking if applicable
                    if self._is_weekend_day(date):
                        weekend_start = self._get_weekend_start(date)
                        if weekend_start in self.worker_weekends[worker_id]:
                            # Only remove if there are no other assignments on this weekend
                            other_weekend_assignments = [
                                d for d in self.worker_assignments[worker_id]
                                if (self._is_weekend_day(d) and 
                                    self._get_weekend_start(d) == weekend_start)
                            ]
                            if not other_weekend_assignments:
                                self.worker_weekends[worker_id].remove(weekend_start)

        # Remove incomplete days from schedule
        for date in dates_to_remove:
            del self.schedule[date]
            logging.info(f"Removed incomplete schedule for {date.strftime('%Y-%m-%D')}")

        # Recalculate posts for each worker
        for worker_id in self.worker_posts.keys():
            self.worker_posts[worker_id] = set()
            for date in self.worker_assignments[worker_id]:
                if date in self.schedule:
                    post = self.schedule[date].index(worker_id)
                    self.worker_posts[worker_id].add(post)

        logging.info(f"Schedule cleanup complete. Removed {len(dates_to_remove)} incomplete days.")

    def _validate_final_schedule(self):
        """Enhanced validation including all constraints"""
        errors = []
        warnings = []

        # Check each date in schedule
        for date in sorted(self.schedule.keys()):
            assigned_workers = self.schedule[date]
            
            # Check number of workers
            if len(assigned_workers) < self.num_shifts:
                warnings.append(
                    f"Understaffed on {date.strftime('%Y-%m-%d')}: "
                    f"{len(assigned_workers)} of {self.num_shifts} shifts filled"
                )
            
            # Check for incompatible workers
            for i, worker_id in enumerate(assigned_workers):
                worker = next(w for w in self.workers_data if w['id'] == worker_id)
                
                for other_id in assigned_workers[i+1:]:
                    other_worker = next(w for w in self.workers_data if w['id'] == other_id)
                    
                    if (worker.get('is_incompatible', False) and 
                        other_worker.get('is_incompatible', False)):
                        if (date.strftime('%Y-%m-%d'), (worker_id, other_id)) not in [
                            (d, pair) for d, pair in self.constraint_skips[worker_id]['incompatibility']
                        ]:
                            errors.append(
                                f"Unauthorized incompatible workers {worker_id} and {other_id} "
                                f"on {date.strftime('%Y-%m-%d')}"
                            )
        # Check rotation constraints for each worker
        for worker in self.workers_data:
            worker_id = worker['id']
        
            # Check post rotation
            post_counts = {i: 0 for i in range(self.num_shifts)}
            for date in self.worker_assignments[worker_id]:
                if date in self.schedule:
                    post = self.schedule[date].index(worker_id)
                    post_counts[post] += 1
        
            max_diff = max(post_counts.values()) - min(post_counts.values())
            if max_diff > 2:
                warnings.append(f"Worker {worker_id} post rotation imbalance: {post_counts}")

            # Check weekday distribution
            weekday_counts = self.worker_weekdays[worker_id]
            max_weekday_diff = max(weekday_counts.values()) - min(weekday_counts.values())
            if max_weekday_diff > 2:
                warnings.append(f"Worker {worker_id} weekday imbalance: {weekday_counts}")

            # Check consecutive weekends
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
                errors.append(f"Worker {worker_id} has {max_consecutive} consecutive weekends")
                
        # Check worker assignments
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = self.worker_assignments[worker_id]
            
            # Check total shifts against target
            shift_difference = abs(len(assignments) - worker['target_shifts'])
            if shift_difference > 2:  # Allow small deviation
                warnings.append(
                    f"Worker {worker_id} has {len(assignments)} shifts "
                    f"(target: {worker['target_shifts']})"
                )

        if errors:
            raise SchedulerError("Schedule validation failed:\n" + "\n".join(errors))
        
        if warnings:
            for warning in warnings:
                logging.warning(warning)

    def export_schedule(self, format='txt'):
        """Export the schedule in the specified format"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'schedule_{timestamp}.{format}'
        
        if format == 'txt':
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("=== Guard Schedule ===\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}\n\n")
                
                for date in sorted(self.schedule.keys()):
                    f.write(f"\n{date.strftime('%Y-%m-%d')} ({date.strftime('%A')})")
                    if self._is_holiday(date):
                        f.write(" [HOLIDAY]")
                    f.write("\n")
                    
                    for i, worker_id in enumerate(self.schedule[date], 1):
                        worker = next(w for w in self.workers_data if w['id'] == worker_id)
                        f.write(f"  Shift {i}: Worker {worker_id}")
                        if float(worker.get('work_percentage', 100)) < 100:
                            f.write(f" (Part-time: {worker['work_percentage']}%)")
                        f.write("\n")
                    f.write("-" * 40 + "\n")

        return filename

    def get_worker_schedule(self, worker_id):
        """Get detailed schedule for a specific worker"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        assignments = sorted(self.worker_assignments[worker_id])
        
        return {
            'worker_id': worker_id,
            'work_percentage': worker.get('work_percentage', 100),
            'total_shifts': len(assignments),
            'target_shifts': worker.get('target_shifts', 0),
            'assignments': [
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'weekday': date.strftime('%A'),
                    'is_weekend': self._is_weekend_day(date),
                    'is_holiday': self._is_holiday(date)
                }
                for date in assignments
            ],
            'constraint_skips': self.constraint_skips[worker_id],
            'stats': self._analyze_gaps(worker_id)
        }
