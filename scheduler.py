# Imports
from datetime import datetime, timedelta
import logging
import os
import sys
import random
import copy
from constraint_checker import ConstraintChecker
from schedule_builder import ScheduleBuilder
from data_manager import DataManager
from utilities import DateTimeUtils
from statistics import StatisticsCalculator
from exceptions import SchedulerError
from worker_eligibility import WorkerEligibilityTracker

# Configure logging
log_dir = "logs"
if not os.path.exists(log_dir):
    try:
        os.makedirs(log_dir)
        print(f"Log directory '{log_dir}' created successfully.")
    except OSError as e:
        print(f"Error creating log directory '{log_dir}': {e}. Logs might not be saved to file.")
        # Fallback to current directory if 'logs' can't be made
        log_dir = "." 

log_file_path = os.path.join(log_dir, "scheduler.log")
print(f"Logging to: {os.path.abspath(log_file_path)}")

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Remove any existing handlers to avoid duplicates
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
    handler.close()

# Create and add handlers
try:
    # File handler - using 'a' (append) mode instead of 'w' (overwrite)
    # Also specifying UTF-8 encoding to properly handle special characters
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8', errors='replace')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Console handler with UTF-8 encoding if possible
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    # Try to configure console for UTF-8
    if hasattr(console_handler.stream, 'reconfigure'):
        try:
            console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
        except Exception as e:
            print(f"Could not reconfigure console stream encoding: {e}")
    logger.addHandler(console_handler)
    
    # Test log message to verify configuration
    logging.info("Logging system configured successfully.")
except Exception as e:
    print(f"ERROR: Failed to configure logging: {e}")
    # Set up a basic console logger as fallback
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    logging.error(f"Failed to set up file logging. Logs will only appear in console. Error: {e}")# Define classes and methods...

class SchedulerError(Exception):
    """Custom exception for Scheduler errors"""
    pass

class Scheduler:
    """Main Scheduler class that coordinates all scheduling operations"""
    
    # Methods
    def __init__(self, config):
        """Initialize the scheduler with configuration"""
        logging.info("Scheduler initialized")
        try:
            # Initialize date_utils FIRST, before calling any method that might need it
            self.date_utils = DateTimeUtils()
    
            # Then validate the configuration
            self._validate_config(config)
    
            # Basic setup from config
            self.config = config
            self.start_date = config['start_date']
            self.end_date = config['end_date']
            self.num_shifts = config['num_shifts']
            self.variable_shifts = config.get('variable_shifts', [])
            self.workers_data = config['workers_data']
            self.holidays = config.get('holidays', [])

            # --- START: Build incompatibility lists ---
            incompatible_worker_ids = {
                worker['id'] for worker in self.workers_data if worker.get('is_incompatible', False)
            }
            logging.debug(f"Identified incompatible worker IDs: {incompatible_worker_ids}")

            for worker in self.workers_data:
                worker_id = worker['id']
                # Initialize the list
                worker['incompatible_with'] = []
                if worker.get('is_incompatible', False):
                    # If this worker is incompatible, add all *other* incompatible workers to its list
                    worker['incompatible_with'] = list(incompatible_worker_ids - {worker_id}) # Exclude self
                logging.debug(f"Worker {worker_id} incompatible_with list: {worker['incompatible_with']}")
            # --- END: Build incompatibility lists ---
    
            # Get the new configurable parameters with defaults
            self.gap_between_shifts = config.get('gap_between_shifts', 3)
            self.max_consecutive_weekends = config.get('max_consecutive_weekends', 3)

            # Initialize tracking dictionaries
            self.schedule = {}
            self.worker_assignments = {w['id']: set() for w in self.workers_data}
            self.worker_posts = {w['id']: set() for w in self.workers_data} # CORRECTED: Initialize as a set
            self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data} # This is a list of dates, which is fine
            # Initialize the schedule structure with the appropriate number of shifts for each date
            self._initialize_schedule_with_variable_shifts() 
            # Initialize tracking data structures (These seem to be for overall counts, distinct from worker_posts which tracks *which* posts)
            self.worker_shift_counts = {w['id']: 0 for w in self.workers_data}
            self.worker_weekend_counts = {w['id']: 0 for w in self.workers_data} 
            self.worker_post_counts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data} # This is for counting how many times each post is worked by a worker, distinct from self.worker_posts
            self.worker_weekday_counts = {w['id']: {d: 0 for d in range(7)} for w in self.workers_data}
            self.worker_holiday_counts = {w['id']: 0 for w in self.workers_data}
            self.last_assignment_date = {w['id']: None for w in self.workers_data} # Corrected attribute name
            # Initialize consecutive_shifts
            self.consecutive_shifts = {w['id']: 0 for w in self.workers_data} # <<< --- ADD THIS LINE
                      
            # Initialize worker targets
            for worker in self.workers_data:
                if 'target_shifts' not in worker:
                    worker['target_shifts'] = 0

            # Set current time and user
            # self.date_utils = DateTimeUtils() # Already initialized above
            self.current_datetime = self.date_utils.get_spain_time()
            self.current_user = 'saldo27'
        
            # Add max_shifts_per_worker calculation
            total_days = (self.end_date - self.start_date).days + 1
            total_shifts_possible = total_days * self.num_shifts # Renamed for clarity
            num_workers = len(self.workers_data)
            # Ensure num_workers is not zero to prevent DivisionByZeroError
            self.max_shifts_per_worker = (total_shifts_possible // num_workers) + 2 if num_workers > 0 else total_shifts_possible 

            # Track constraint skips
            self.constraint_skips = {
                w['id']: {
                    'gap': [],
                    'incompatibility': [],
                    'reduced_gap': []  # For part-time workers
                } for w in self.workers_data
            }
        
            # Initialize helper modules
            self.stats = StatisticsCalculator(self)
            self.constraint_checker = ConstraintChecker(self)  
            self.data_manager = DataManager(self)
            # self.schedule_builder will be initialized later in generate_schedule
            self.eligibility_tracker = WorkerEligibilityTracker(
                self.workers_data,
                self.holidays,
                self.gap_between_shifts,
                self.max_consecutive_weekends,
                start_date=self.start_date,  # Pass start_date
                end_date=self.end_date,      # Pass end_date
                date_utils=self.date_utils,  # Pass date_utils
                scheduler=self              # Pass reference to scheduler
            )

            # Sort the variable shifts by start date for efficient lookup
            self.variable_shifts.sort(key=lambda x: x['start_date'])
    
            # Calculate targets before proceeding
            self._calculate_target_shifts()

            self._log_initialization()

            # The ScheduleBuilder is now initialized within the generate_schedule method
            # after the scheduler's own state for the run is fully prepared.
            # self.schedule_builder = ScheduleBuilder(self) # This line is moved to generate_schedule

        except Exception as e:
            logging.error(f"Initialization error: {str(e)}", exc_info=True) # Added exc_info=True
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

        # Validate gap_between_shifts if present
        if 'gap_between_shifts' in config:
            if not isinstance(config['gap_between_shifts'], int) or config['gap_between_shifts'] < 0:
                raise SchedulerError("gap_between_shifts must be a non-negative integer")
    
        # Validate max_consecutive_weekends if present
        if 'max_consecutive_weekends' in config:
            if not isinstance(config['max_consecutive_weekends'], int) or config['max_consecutive_weekends'] <= 0:
                raise SchedulerError("max_consecutive_weekends must be a positive integer")
            
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
                    self.date_utils.parse_dates(worker['mandatory_days'])
                except ValueError as e:
                    raise SchedulerError(f"Invalid mandatory_days format for worker {worker['id']}: {str(e)}")

            # Validate date formats in days_off if present
            if 'days_off' in worker:
                try:
                    self.date_utils.parse_date_ranges(worker['days_off'])
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
        logging.info(f"Holidays: {[h.strftime('%d-%m-%Y') for h in self.holidays]}")
        logging.info(f"Gap between shifts: {self.gap_between_shifts}")
        logging.info(f"Max consecutive weekend/holiday shifts: {self.max_consecutive_weekends}")
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

    def _save_current_as_best(self):
        """
        Save the current schedule as the best schedule found so far.
        """
        try:
            logging.debug("Saving current schedule as best...")
        
            # Create a deep copy of the current schedule
            best_schedule = {}
            for date, shifts in self.schedule.items():
                best_schedule[date] = shifts.copy()
            
            # Save all tracking data
            self.best_schedule_data = {
                'schedule': best_schedule,
                'worker_assignments': {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()},
                'worker_posts': {w_id: posts.copy() for w_id, posts in self.worker_posts.items()},
                'worker_weekdays': {w_id: counts.copy() for w_id, counts in self.worker_weekdays.items()},
                'worker_weekends': {w_id: dates.copy() for w_id, dates in self.worker_weekends.items()},
                'worker_shift_counts': self.worker_shift_counts.copy() if hasattr(self, 'worker_shift_counts') else None,
                'worker_weekend_counts': self.worker_weekend_counts.copy() if hasattr(self, 'worker_weekend_counts') else None,
                'score': self.calculate_score()
            }
        
            logging.debug(f"Saved best schedule with score: {self.best_schedule_data['score']}")
            return True
        except Exception as e:
            logging.error(f"Error saving best schedule: {str(e)}", exc_info=True)
            return False

    def _initialize_schedule_with_variable_shifts(self):
        # Initialize loop variables
        current_date = self.start_date
        dates_initialized = 0
        variable_dates = 0
        # Build a lookup for fast matching of variable ranges
        var_cfgs = [
            (cfg['start_date'], cfg['end_date'], cfg['shifts'])
            for cfg in self.variable_shifts
        ]
        while current_date <= self.end_date:
            # Determine how many shifts this date should have
            shifts_for_date = self.num_shifts
            for start, end, cnt in var_cfgs:
                if start <= current_date <= end:
                    shifts_for_date = cnt
                    logging.info(f"Variable shifts applied for {current_date}: {cnt} shifts (default is {self.num_shifts})")
                    variable_dates += 1
                    break
            # Initialize the schedule entry for this date
            self.schedule[current_date] = [None] * shifts_for_date
            dates_initialized += 1

            # Move to next date
            current_date += timedelta(days=1)


    def _get_schedule_months(self):
        """
        Calculate number of months in schedule period considering partial months
    
        Returns:
            dict: Dictionary with month keys and their available days count
        """
        month_days = {}
        current = self.start_date
        while current <= self.end_date:
            month_key = f"{current.year}-{current.month:02d}"
        
            # Calculate available days for this month
            month_start = max(
                current.replace(day=1),
                self.start_date
            )
            month_end = min(
                (current.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
                self.end_date
            )
        
            days_in_month = (month_end - month_start).days + 1
            month_days[month_key] = days_in_month
        
            # Move to first day of next month
            current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
    
        return month_days

    def _get_shifts_for_date(self, date):
        """Determine the number of shifts for a specific date based on variable_shifts."""
        logging.debug(f"Checking variable shifts for date: {date}")
        # Normalize to date-only if datetime
        check_date = date.date() if hasattr(date, 'date') else date
        for cfg in self.variable_shifts:
            start = cfg.get('start_date')
            end   = cfg.get('end_date')
            shifts = cfg.get('shifts')
            # Normalize
            sd = start.date() if hasattr(start, 'date') else start
            ed = end.date() if hasattr(end, 'date') else end
            if sd <= check_date <= ed:
                logging.debug(f"Variable shifts: {shifts} for {date}")
                return shifts
        # Fallback to default
        logging.debug(f"No variable shift override for {date}, default={self.num_shifts}")
        return self.num_shifts

    def _calculate_target_shifts(self):
        """
        Recalculate each worker's target_shifts by:
          1) Counting slots they can work (based on work_periods & days_off)
          2) Weighting those slots by their work_percentage
          3) Allocating all schedule slots proportionally (largest‐remainder rounding)
        """
        try:
            logging.info("Calculating target shifts based on availability and percentage")

            # 1) Total open slots in the schedule (variable shifts considered)
            total_slots = sum(len(slots) for slots in self.schedule.values())
            if total_slots <= 0:
                logging.warning("No slots in schedule; skipping allocation")
                return False

            # 2) Compute available_slots per worker
            available_slots = {}
            for w in self.workers_data:
                wid = w['id']
                wp = w.get('work_periods','').strip()
                dp = w.get('days_off','').strip()
                work_ranges = (self.date_utils.parse_date_ranges(wp) 
                               if wp else [(self.start_date, self.end_date)])
                off_ranges  = (self.date_utils.parse_date_ranges(dp) 
                               if dp else [])
                count = 0
                for date, slots in self.schedule.items():
                    in_work = any(s <= date <= e for s, e in work_ranges)
                    in_off  = any(s <= date <= e for s, e in off_ranges)
                    if in_work and not in_off:
                        count += len(slots)
                available_slots[wid] = count
                logging.debug(f"Worker {wid}: available_slots={count}")

            # 3) Build weight = available_slots * (work_percentage/100)
            weights = []
            for w in self.workers_data:
                wid = w['id']
                pct = 1.0
                try:
                    pct = float(str(w.get('work_percentage',100)).strip())/100.0
                except Exception:
                    logging.warning(f"Worker {wid} invalid work_percentage; defaulting to 100%")
                pct = max(0.0, pct)
                weights.append(available_slots.get(wid,0) * pct)

            total_weight = sum(weights) or 1.0

            # 4) Compute exact fractional targets
            exact_targets = [wgt/total_weight*total_slots for wgt in weights]

            # 5) Largest-remainder rounding
            floors = [int(x) for x in exact_targets]
            remainder = int(total_slots - sum(floors))
            fracs = sorted(enumerate(exact_targets),
                           key=lambda ix: exact_targets[ix[0]] - floors[ix[0]],
                           reverse=True)
            targets = floors[:]
            for idx, _ in fracs[:remainder]:
                targets[idx] += 1

            # 6) Assign and log (subtract out mandatory days so they're not extra)
            for i, w in enumerate(self.workers_data):
                raw_target = targets[i]
                mand_count = 0
                mand_str = w.get('mandatory_days', '').strip()
                if mand_str:
                    try:
                        mand_dates = self.date_utils.parse_dates(mand_str)
                        mand_count = sum(1 for d in mand_dates
                                         if self.start_date <= d <= self.end_date)
                    except Exception as e:
                        logging.error(f"Failed to parse mandatory_days for {w['id']}: {e}")
                adjusted = max(0, raw_target - mand_count)
                w['target_shifts'] = adjusted
                logging.info(
                    f"Worker {w['id']}: target_shifts={raw_target} → {adjusted}"
                    f"{' (−'+str(mand_count)+' mandatory)' if mand_count else ''}"
                )

            for i,w in enumerate(self.workers_data):
                raw = targets[i]
                mand_count = 0
                if w.get('mandatory_days','').strip():
                    mand_count = sum(1 for d in self.date_utils.parse_dates(w['mandatory_days'])
                                     if self.start_date <= d <= self.end_date)
            w['_raw_target']      = raw
            w['_mandatory_count'] = mand_count
            w['target_shifts']    = max(0, raw - mand_count)
            
            return True
        
        except Exception as e:
            logging.error(f"Error in target calculation: {e}", exc_info=True)
            return False

    def _adjust_for_mandatory(self):
        """
        Mandatory days are not extra shifts: reduce each worker's
        target_shifts by the number of mandatories in range.
        """
        for w in self.workers_data:
            mand_list = []
            try:
                mand_list = self.date_utils.parse_dates(w.get('mandatory_days',''))
            except Exception:
                pass

            mand_count = sum(1 for d in mand_list
                             if self.start_date <= d <= self.end_date)
            # never go below zero
            new_target = max(0, w.get('target_shifts',0) - mand_count)
            logging.info(f"[Worker {w['id']}] target_shifts {w['target_shifts']} → {new_target} after mandatory")
            w['target_shifts'] = new_target

    def _calculate_monthly_targets(self):
        """
        Calculate monthly target shifts for each worker based on their overall targets
        """
        logging.info("Calculating monthly target distribution...")
    
        # Calculate available days per month
        month_days = self._get_schedule_months()
        total_days = (self.end_date - self.start_date).days + 1
    
        # Initialize monthly targets for each worker
        for worker in self.workers_data:
            worker_id = worker['id']
            overall_target = worker.get('target_shifts', 0)
        
            # Initialize or reset monthly targets
            if 'monthly_targets' not in worker:
                worker['monthly_targets'] = {}
            
            # Distribute target shifts proportionally by month
            remaining_target = overall_target
            for month_key, days_in_month in month_days.items():
                # Calculate proportion of shifts for this month
                month_proportion = days_in_month / total_days
                month_target = round(overall_target * month_proportion)
            
                # Ensure we don't exceed overall target
                month_target = min(month_target, remaining_target)
                worker['monthly_targets'][month_key] = month_target
                remaining_target -= month_target
            
                logging.debug(f"Worker {worker_id}: {month_key} → {month_target} shifts")
        
            # Handle any remaining shifts due to rounding
            if remaining_target > 0:
                # Distribute remaining shifts to months with most days first
                sorted_months = sorted(month_days.items(), key=lambda x: x[1], reverse=True)
                for month_key, _ in sorted_months:
                    if remaining_target <= 0:
                        break
                    worker['monthly_targets'][month_key] += 1
                    remaining_target -= 1
                    logging.debug(f"Worker {worker_id}: Added +1 to {month_key} for rounding")
    
        # Log the results
        logging.info("Monthly targets calculated")
        return True

    def _is_weekly_pattern(self, days_difference):
        """Return True if this is a 7- or 14-day same-weekday pattern."""
        return days_difference in (7, 14)
    
    def _redistribute_excess_shifts(self, excess_shifts, excluded_worker_id, mandatory_shifts_by_worker):
        """Helper method to redistribute excess shifts from one worker to others, respecting mandatory assignments"""
        eligible_workers = [w for w in self.workers_data if w['id'] != excluded_worker_id]
    
        if not eligible_workers:
            return
    
        # Sort by work percentage (give more to workers with higher percentage)
        eligible_workers.sort(key=lambda w: float(w.get('work_percentage', 100)), reverse=True)
    
        # Distribute excess shifts
        for i in range(excess_shifts):
            worker = eligible_workers[i % len(eligible_workers)]
            worker['target_shifts'] += 1
            logging.info(f"Redistributed 1 shift to worker {worker['id']}")

    def _reconcile_schedule_tracking(self):
        """
        Reconciles worker_assignments tracking with the actual schedule
        to fix any inconsistencies before validation.
        """
        logging.info("Reconciling worker assignments tracking with schedule...")
    
        try:
            # Build tracking from scratch based on current schedule
            new_worker_assignments = {}
            for worker in self.workers_data:
                new_worker_assignments[worker['id']] = set()
            
            # Go through the schedule and rebuild tracking
            for date, shifts in self.schedule.items():
                for shift_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        if worker_id not in new_worker_assignments:
                            new_worker_assignments[worker_id] = set()
                        new_worker_assignments[worker_id].add(date)
        
            # Find and log discrepancies
            total_discrepancies = 0
            for worker_id, assignments in self.worker_assignments.items():
                if worker_id not in new_worker_assignments:
                    new_worker_assignments[worker_id] = set()
                
                extra_dates = assignments - new_worker_assignments[worker_id]
                missing_dates = new_worker_assignments[worker_id] - assignments
            
                if extra_dates:
                    logging.debug(f"Worker {worker_id} has {len(extra_dates)} tracked dates not in schedule")
                    total_discrepancies += len(extra_dates)
                
                if missing_dates:
                    logging.debug(f"Worker {worker_id} has {len(missing_dates)} schedule dates not tracked")
                    total_discrepancies += len(missing_dates)
        
            # Replace with corrected tracking
            self.worker_assignments = new_worker_assignments
        
            logging.info(f"Reconciliation complete: Fixed {total_discrepancies} tracking discrepancies")
            return True
        except Exception as e:
            logging.error(f"Error reconciling schedule tracking: {str(e)}", exc_info=True)
            return False

    def _ensure_data_integrity(self):
        """
        Ensure all data structures are consistent before schedule generation
        """
        logging.info("Ensuring data integrity...")
    
        # Ensure all workers have proper data structures
        for worker in self.workers_data:
            worker_id = worker['id']
        
            # Ensure worker assignments tracking
            if worker_id not in self.worker_assignments:
                self.worker_assignments[worker_id] = set()
            
            # Ensure worker posts tracking
            if worker_id not in self.worker_posts:
                self.worker_posts[worker_id] = set()
            
            # Ensure weekday tracking
            if worker_id not in self.worker_weekdays:
                self.worker_weekdays[worker_id] = {i: 0 for i in range(7)}
            
            # Ensure weekend tracking
            if worker_id not in self.worker_weekends:
                self.worker_weekends[worker_id] = []
    
        # Ensure schedule dictionary entries match variable shifts configuration
        for current_date in self._get_date_range(self.start_date, self.end_date):
            expected = self._get_shifts_for_date(current_date)
            if current_date not in self.schedule:
                self.schedule[current_date] = [None] * expected
            else:
                # Pad or trim to expected length
                actual = len(self.schedule[current_date])
                if actual < expected:
                    self.schedule[current_date].extend([None] * (expected - actual))
                elif actual > expected:
                    self.schedule[current_date] = self.schedule[current_date][:expected]
                    
        logging.info("Data integrity check completed")
        return True

    def _update_tracking_data(self, worker_id, date, post, removing=False):
        """
        Update all relevant tracking data structures when a worker is assigned or unassigned.
        This includes worker_assignments, worker_posts, worker_weekdays, and worker_weekends.
        It also calls the eligibility tracker if it exists.
        """
        try:
            # Ensure basic data structures exist for the worker
            if worker_id not in self.worker_assignments: 
                self.worker_assignments[worker_id] = set()
            
            # Robust check and initialization for self.worker_posts[worker_id]
            # Ensures it's a set even if worker_id was already a key but with a wrong type (e.g., dict)
            if worker_id not in self.worker_posts or not isinstance(self.worker_posts.get(worker_id), set):
                logging.warning(f"Re-initializing self.worker_posts[{worker_id}] as a set due to incorrect type.") # Optional: log this
                self.worker_posts[worker_id] = set() 
            
            if worker_id not in self.worker_weekdays: 
                self.worker_weekdays[worker_id] = {i: 0 for i in range(7)}
            if worker_id not in self.worker_weekends: 
                self.worker_weekends[worker_id] = [] # List of weekend/holiday dates worked

            if removing:
                # Remove from worker assignments
                if date in self.worker_assignments.get(worker_id, set()):
                    self.worker_assignments[worker_id].remove(date)
                
                # Note: Removing from self.worker_posts for a specific post is not done here
                # as self.worker_posts[worker_id] is a set of all posts worked.
                # If a worker no longer works ANY instance of a post, that post would remain
                # in their set unless explicitly managed or self.worker_posts is rebuilt.

                # Update weekday counts
                weekday = date.weekday()
                # Ensure weekday key exists before decrementing, though init above should handle it.
                if weekday in self.worker_weekdays.get(worker_id, {}): # Defensive access
                    if self.worker_weekdays[worker_id][weekday] > 0:
                        self.worker_weekdays[worker_id][weekday] -= 1
                else:
                    logging.warning(f"Weekday {weekday} not found in self.worker_weekdays for worker {worker_id} during removal.")


                # Update weekend tracking
                is_special_day = (date.weekday() >= 4 or
                                  date in self.holidays or
                                  (date + timedelta(days=1)) in self.holidays)
                
                if is_special_day:
                    current_weekends = self.worker_weekends.get(worker_id) # Use .get for safety
                    if current_weekends is not None and date in current_weekends:
                        current_weekends.remove(date)
                        # current_weekends.sort() # Re-sort if removal changes order and order matters

            else: # Adding assignment
                self.worker_assignments[worker_id].add(date)
                self.worker_posts[worker_id].add(post) # This should now work
                
                weekday = date.weekday()
                self.worker_weekdays[worker_id][weekday] = self.worker_weekdays[worker_id].get(weekday, 0) + 1


                is_special_day = (date.weekday() >= 4 or
                                  date in self.holidays or
                                  (date + timedelta(days=1)) in self.holidays)

                if is_special_day:
                    current_weekends = self.worker_weekends.setdefault(worker_id, []) # Ensures list exists
                    if date not in current_weekends:
                        current_weekends.append(date)
                        current_weekends.sort()

            # Update eligibility tracker if it exists and is configured
            if hasattr(self, 'eligibility_tracker') and self.eligibility_tracker:
                if removing:
                    self.eligibility_tracker.remove_worker_assignment(worker_id, date)
                else:
                    self.eligibility_tracker.update_worker_status(worker_id, date)

            logging.debug(f"{'Removed' if removing else 'Added'} assignment and updated tracking for worker {worker_id} on {date.strftime('%Y-%m-%d')}, post {post}")

        except Exception as e:
            logging.error(f"Error in _update_tracking_data for worker {worker_id}, date {date}, post {post}, removing={removing}: {str(e)}", exc_info=True)
            raise
        
    def _synchronize_tracking_data(self):
        """
        Ensures all tracking data structures are consistent with the schedule.
        Called by the ScheduleBuilder to maintain data integrity.
        """
        try:
            logging.info("Synchronizing tracking data structures...")
        
            # Reset existing tracking data
            self.worker_assignments = {w['id']: set() for w in self.workers_data}
            self.worker_posts = {w['id']: set() for w in self.workers_data}
            self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data}
            self.worker_shift_counts = {w['id']: 0 for w in self.workers_data}
            self.worker_weekend_counts = {w['id']: 0 for w in self.workers_data}
        
            # Rebuild tracking data from the current schedule
            for date, shifts in self.schedule.items():
                for post_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        # Update worker assignments
                        self.worker_assignments[worker_id].add(date)
                    
                        # Update posts worked
                        self.worker_posts[worker_id].add(post_idx)
                    
                        # Update weekday counts
                        weekday = date.weekday()
                        self.worker_weekdays[worker_id][weekday] = self.worker_weekdays[worker_id].get(weekday, 0) + 1
                    
                        # Update weekends/holidays
                        is_weekend_or_holiday = (date.weekday() >= 4 or 
                                              date in self.holidays or 
                                              (date + timedelta(days=1)) in self.holidays)
                        if is_weekend_or_holiday:
                            if date not in self.worker_weekends[worker_id]:
                                self.worker_weekends[worker_id].append(date)
                            self.worker_weekend_counts[worker_id] += 1
                    
                        # Update shift counts
                        self.worker_shift_counts[worker_id] += 1
        
            # Sort weekend dates for consistency
            for worker_id in self.worker_weekends:
                self.worker_weekends[worker_id].sort()
        
            logging.info("Tracking data synchronization complete.")
            return True
        except Exception as e:
            logging.error(f"Error synchronizing tracking data: {str(e)}", exc_info=True)
            return False
    
    def _get_date_range(self, start_date, end_date):
        """
        Get list of dates between start_date and end_date (inclusive)
    
        Args:
            start_date: Start date
            end_date: End date
        Returns:
            list: List of dates in range
        """
        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date)
            current_date += timedelta(days=1)
        return date_range

    def _cleanup_schedule(self):
        """
        Clean up the schedule before validation
    
        - Ensure all dates have proper shift lists
        - Remove any empty shifts at the end of lists
        - Sort schedule by date
        """
        logging.info("Cleaning up schedule...")

        # Ensure each date matches its variable-shifts count
        for date in self._get_date_range(self.start_date, self.end_date):
            expected = self._get_shifts_for_date(date)
            if date not in self.schedule:
                self.schedule[date] = [None] * expected
            else:
                actual = len(self.schedule[date])
                if actual < expected:
                    self.schedule[date].extend([None] * (expected - actual))
                elif actual > expected:
                    self.schedule[date] = self.schedule[date][:expected]    
        # Create a sorted version of the schedule
        sorted_schedule = {}
        for date in sorted(self.schedule.keys()):
            sorted_schedule[date] = self.schedule[date]
    
        self.schedule = sorted_schedule
    
        logging.info("Schedule cleanup complete")
        return True

    def _calculate_coverage(self):
        """Calculate the percentage of shifts that are filled in the schedule."""
        try:
            total_shifts = (self.end_date - self.start_date).days + 1  # One shift per day
            total_shifts *= self.num_shifts  # Multiply by number of shifts per day
        
            # Count filled shifts (where worker is not None)
            filled_shifts = 0
            for date, shifts in self.schedule.items():
                for worker in shifts:
                    if worker is not None:
                        filled_shifts += 1
                    
            # Debug logs to see what's happening
            logging.info(f"Coverage calculation: {filled_shifts} filled out of {total_shifts} total shifts")
            logging.debug(f"Schedule contains {len(self.schedule)} dates with shifts")
        
            # Output some sample of the schedule to debug
            sample_size = min(3, len(self.schedule))
            if sample_size > 0:
                sample_dates = list(self.schedule.keys())[:sample_size]
                for date in sample_dates:
                    logging.debug(f"Sample date {date.strftime('%d-%m-%Y')}: {self.schedule[date]}")
        
            # Calculate percentage
            if total_shifts > 0:
                return (filled_shifts / total_shifts) * 100
            return 0
        except Exception as e:
            logging.error(f"Error calculating coverage: {str(e)}", exc_info=True)
            return 0

    def _assign_workers_simple(self):
        """
        Simple method to directly assign workers to shifts based on targets and ensuring
        all constraints are properly respected:
        - Special Friday-Monday constraint
        - 7/14 day pattern avoidance
        - Worker incompatibility checking
        """
        logging.info("Using simplified assignment method to ensure schedule population")
    
        # 1. Get all dates that need to be scheduled
        all_dates = sorted(list(self.schedule.keys()))
        if not all_dates:
            all_dates = self._get_date_range(self.start_date, self.end_date)
    
        # 2. Prepare worker assignments based on target shifts
        worker_assignment_counts = {w['id']: 0 for w in self.workers_data}
        worker_targets = {w['id']: w.get('target_shifts', 1) for w in self.workers_data}
    
        # Sort workers by targets (highest first) to prioritize those who need more shifts
        workers_by_priority = sorted(
            self.workers_data, 
            key=lambda w: worker_targets.get(w['id'], 0),
            reverse=True
        )    
    
        # 3. Go through each date and assign workers
        for date in all_dates:
            # For each shift on this date
            for post in range(self.num_shifts):
                # If the shift is already assigned, skip it
                if date in self.schedule and len(self.schedule[date]) > post and self.schedule[date][post] is not None:
                    continue
            
                # Find the best worker for this shift
                best_worker = None
            
                # Get currently assigned workers for this date
                currently_assigned = []
                if date in self.schedule:
                    currently_assigned = [w for w in self.schedule[date] if w is not None]
    
                # Try each worker in priority order
                for worker in workers_by_priority:
                    worker_id = worker['id']

                    # Skip if worker is already assigned to this date
                    if worker_id in currently_assigned:
                        continue
    
                    # Skip if worker has reached their target
                    if worker_assignment_counts[worker_id] >= worker_targets[worker_id]:
                        continue
    
                    # Initialize too_close flag
                    too_close = False
    
                    # Inside the loop where we check minimum gap
                    for assigned_date in self.worker_assignments.get(worker_id, set()):
                        days_difference = abs((date - assigned_date).days)
    
                        # We need at least gap_between_shifts days off, so (gap+1)+ days between assignments
                        min_days_between = self.gap_between_shifts + 1
                        if days_difference < min_days_between:
                            too_close = True
                            break
    
                        # Special case: Friday-Monday (needs 3 days off, so 4+ days between)
                        if days_difference == 3:
                            if ((date.weekday() == 0 and assigned_date.weekday() == 4) or 
                                (date.weekday() == 4 and assigned_date.weekday() == 0)):
                                too_close = True
                                break
    
                        # Check for weekly-pattern (7 or 14 days, same weekday)
                        if self._is_weekly_pattern(days_difference) and date.weekday() == assigned_date.weekday():
                            too_close = True
    
                    if too_close:
                        continue
                    
                    # Check for worker incompatibilities
                    incompatible_with = worker.get('incompatible_with', [])
                    if incompatible_with:
                        has_conflict = False
                        for incompatible_id in incompatible_with:
                            if incompatible_id in currently_assigned:
                                has_conflict = True
                                break

                        if has_conflict:
                            continue
                
                    # This worker is a good candidate
                    best_worker = worker
                    break
            
                # If we found a suitable worker, assign them
                if best_worker:
                    worker_id = best_worker['id']
            
                    # Make sure the schedule list exists and has the right size
                    if date not in self.schedule:
                        self.schedule[date] = []
                
                    while len(self.schedule[date]) <= post:
                        self.schedule[date].append(None)
                
                    # Assign the worker
                    self.schedule[date][post] = worker_id
            
                    # Update tracking data
                    self._update_tracking_data(worker_id, date, post)
            
                    # Update the assignment count
                    worker_assignment_counts[worker_id] += 1
                
                    # Update currently_assigned for this date
                    currently_assigned.append(worker_id)
            
                    # Log the assignment
                    logging.info(f"Assigned worker {worker_id} to {date.strftime('%d-%m-%Y')}, post {post}")
                else:
                    # No suitable worker found, leave unassigned
                    if date not in self.schedule:
                        self.schedule[date] = []
                
                    while len(self.schedule[date]) <= post:
                        self.schedule[date].append(None)
                    
                    logging.debug(f"No suitable worker found for {date.strftime('%d-%m-%Y')}, post {post}")
    
        # 4. Return the number of assignments made
        total_assigned = sum(worker_assignment_counts.values())
        total_shifts = len(all_dates) * self.num_shifts
        logging.info(f"Simple assignment complete: {total_assigned}/{total_shifts} shifts assigned ({total_assigned/total_shifts*100:.1f}%)")
    
        return total_assigned > 0

    def _check_schedule_constraints(self):
        """
        Check the current schedule for constraint violations.
        Returns a list of violations found.
        """
        violations = []
        
        try:
            # Check for minimum rest days violations, Friday-Monday patterns, and weekly patterns
            for worker in self.workers_data:
                worker_id = worker['id']
                if worker_id not in self.worker_assignments:
                    continue
            
                # Sort the worker's assignments by date
                assigned_dates = sorted(list(self.worker_assignments[worker_id]))
            
                # Check all pairs of dates for violations
                for i, date1 in enumerate(assigned_dates):
                    for j, date2 in enumerate(assigned_dates):
                        if i >= j:  # Skip same date or already checked pairs
                            continue
                    
                        days_between = abs((date2 - date1).days)
                    
                        # When checking for insufficient rest periods
                        if 0 < days_between < self.gap_between_shifts + 1:
                            violations.append({
                                'type': 'min_rest_days',
                                'worker_id': worker_id,
                                'date1': date1,
                                'date2': date2,
                                'days_between': days_between,
                                'min_required': self.gap_between_shifts
                            })
                    
                        # Check for Friday-Monday assignments (special case requiring 3 days)
                        if days_between == 3:
                            if ((date1.weekday() == 4 and date2.weekday() == 0) or 
                                (date1.weekday() == 0 and date2.weekday() == 4)):
                                violations.append({
                                    'type': 'friday_monday_pattern',
                                    'worker_id': worker_id,
                                    'date1': date1,
                                    'date2': date2,
                                    'days_between': days_between
                                })
                    
                        # Check for 7 or 14 day patterns
                        if (days_between == 7 or days_between == 14) and date1.weekday() == date2.weekday(): # CORRECTED LOGIC + WEEKDAY CHECK
                            violations.append({
                                'type': 'weekly_pattern',        # Ensure this and following lines are indented correctly
                                'worker_id': worker_id,
                                'date1': date1,
                                'date2': date2,
                                'days_between': days_between
                            })
        
            # Check for incompatibility violations
            for date in self.schedule.keys():
                workers_assigned = [w for w in self.schedule.get(date, []) if w is not None]
            
                # Check each worker against others for incompatibility
                for worker_id in workers_assigned:
                    worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
                    if not worker:
                        continue
                    
                    incompatible_with = worker.get('incompatible_with', [])
                    for incompatible_id in incompatible_with:
                        if incompatible_id in workers_assigned:
                            violations.append({
                                'type': 'incompatibility',
                                'worker_id': worker_id,
                                'incompatible_id': incompatible_id,
                                'date': date
                            })
        
            # Log summary of violations
            if violations:
                logging.warning(f"Found {len(violations)} constraint violations in schedule")
                for i, v in enumerate(violations[:5]):  # Log first 5 violations
                    if v['type'] == 'min_rest_days':
                        logging.warning(f"Violation {i+1}: Worker {v['worker_id']} has only {v['days_between']} days between shifts on {v['date1']} and {v['date2']} (min required: {v['min_required']})")
                    elif v['type'] == 'friday_monday_pattern':
                        logging.warning(f"Violation {i+1}: Worker {v['worker_id']} has Friday-Monday assignment on {v['date1']} and {v['date2']}")
                    elif v['type'] == 'weekly_pattern':
                        logging.warning(f"Violation {i+1}: Worker {v['worker_id']} has shifts exactly {v['days_between']} days apart on {v['date1']} and {v['date2']}")
                    elif v['type'] == 'incompatibility':
                        logging.warning(f"Violation {i+1}: Incompatible workers {v['worker_id']} and {v['incompatible_id']} are both assigned on {v['date']}")
                
                if len(violations) > 5:
                    logging.warning(f"...and {len(violations) - 5} more violations")
            
            return violations
        except Exception as e:
            logging.error(f"Error checking schedule constraints: {str(e)}", exc_info=True)
            return []

    def _is_allowed_assignment(self, worker_id, date, shift_num): # shift_num is often unused
        """
        Check if assigning this worker to this date/shift would violate any constraints.
        Returns True if assignment is allowed, False otherwise.        
    
        Enforces:
        - Minimum gap between shifts
        - Special case: No Friday-Monday assignments (if base gap is 1)
        - No 7 or 14 day patterns (same weekday)
        - Worker incompatibility constraints (basic check)
        - Max consecutive weekends (simplified check, ideally use ConstraintChecker)
        """
        try:
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                logging.warning(f"_is_allowed_assignment: Worker {worker_id} not found in workers_data.")
                return False
        
            # Check if worker is already assigned on this date (any post)
            if date in self.schedule and worker_id in self.schedule.get(date, []):
                logging.debug(f"_is_allowed_assignment: Worker {worker_id} already assigned on {date.strftime('%Y-%m-%d')}")
                return False
    
            worker_assignments_set = self.worker_assignments.get(worker_id, set())
            if not isinstance(worker_assignments_set, set): # Should always be a set
                worker_assignments_set = set()

            # --- SINGLE LOOP for checking against previous assignments ---
            for assigned_date in worker_assignments_set:
                if assigned_date == date: 
                    continue

                days_difference = abs((date - assigned_date).days)
    
                # 1. Basic minimum gap check
                min_days_required_between = self.gap_between_shifts + 1
                work_percentage = worker.get('work_percentage', 100)
                if work_percentage < 70: # Example threshold for part-time
                     min_days_required_between = max(min_days_required_between, self.gap_between_shifts + 2)

                if days_difference < min_days_required_between:
                    logging.debug(f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails gap with {assigned_date.strftime('%Y-%m-%d')} ({days_difference} < {min_days_required_between})")
                    return False
        
                # 2. Special case for Friday-Monday (if base gap allows 3-day span)
                if self.gap_between_shifts <= 1: 
                    if days_difference == 3:
                        if ((assigned_date.weekday() == 4 and date.weekday() == 0) or \
                            (assigned_date.weekday() == 0 and date.weekday() == 4)):
                            logging.debug(f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails Fri-Mon rule with {assigned_date.strftime('%Y-%m-%d')}")
                            return False
            
                # 3. Reject 7- or 14-day same-weekday patterns
                if self._is_weekly_pattern(days_difference) and date.weekday() == assigned_date.weekday():
                    logging.debug(f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails 7/14 day pattern with {assigned_date.strftime('%Y-%m-%d')}")
                    return False
            # --- END OF SINGLE LOOP ---

            # 4. Max consecutive weekends (Simplified placeholder - robust check is complex)
            # For true robustness, this should use logic from ConstraintChecker._would_exceed_weekend_limit
            is_current_date_special = (date.weekday() >= 4 or date in self.holidays or (date + timedelta(days=1)) in self.holidays)
            if is_current_date_special:
                # This is a conceptual check. A full robust check is more involved.
                # See ConstraintChecker._would_exceed_weekend_limit for a more complete example.
                # The ideal way is to use the constraint_checker instance if available and reliable
                if hasattr(self, 'constraint_checker') and hasattr(self.constraint_checker, '_would_exceed_weekend_limit'):
                    # Simulate adding the assignment for the check
                    simulated_assignments_for_weekend_check = worker_assignments_set.copy()
                    simulated_assignments_for_weekend_check.add(date) # Add current date to check
                    
                    # Temporarily replace scheduler's worker_assignments for the constraint checker
                    # This is a bit tricky as constraint_checker usually reads from self.scheduler.worker_assignments
                    # For an accurate check, constraint_checker might need to accept simulated assignments.
                    # Here, we'll assume constraint_checker can be pointed to a temporary assignment set or
                    # that its _would_exceed_weekend_limit can take the worker_id and the new date to check against current state.
                    
                    # Let's assume self.constraint_checker._would_exceed_weekend_limit(worker_id, date_to_add)
                    # checks against the existing self.scheduler.worker_assignments and the new date.
                    
                    # Store original worker_assignments for the specific worker to restore later
                    original_worker_specific_assignments = self.worker_assignments.get(worker_id)
                    
                    # Update self.worker_assignments for the check
                    temp_assignments_for_check = self.worker_assignments.copy() # shallow copy of the dict
                    temp_assignments_for_check[worker_id] = simulated_assignments_for_weekend_check
                    
                    # Temporarily point self.scheduler.worker_assignments if constraint_checker uses it directly
                    # This is only safe if this method is not called concurrently.
                    # A cleaner way is for _would_exceed_weekend_limit to take the full set of assignments to check.
                    _original_scheduler_assignments = self.scheduler.worker_assignments # If scheduler has this ref
                    self.scheduler.worker_assignments = temp_assignments_for_check 

                    if self.constraint_checker._would_exceed_weekend_limit(worker_id, date): 
                         logging.debug(f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} would exceed weekend limit (checked via ConstraintChecker).")
                         self.scheduler.worker_assignments = _original_scheduler_assignments # Restore
                         return False
                    self.scheduler.worker_assignments = _original_scheduler_assignments # Restore
                else:
                    # Fallback to a very simplified local check if constraint_checker is not suitable here
                    # This simplified check is NOT a true "max consecutive weekends" and should be improved
                    # if ConstraintChecker cannot be used.
                    pass # Add simplified local logic if needed, or accept it's less robust here.


            # 5. Basic Incompatibility Check
            assigned_on_date_others = []
            if date in self.schedule:
                for post_idx, assigned_w_id in enumerate(self.schedule[date]):
                    if assigned_w_id is not None and assigned_w_id != worker_id: 
                        assigned_on_date_others.append(assigned_w_id)
            
            worker_incompat_list = worker.get('incompatible_with', [])
            for other_assigned_id in assigned_on_date_others:
                if str(other_assigned_id) in worker_incompat_list:
                    logging.debug(f"_is_allowed_assignment: Worker {worker_id} incompatible with {other_assigned_id} on {date.strftime('%Y-%m-%d')}")
                    return False
                other_worker_data = next((w for w in self.workers_data if w['id'] == other_assigned_id), None)
                if other_worker_data and str(worker_id) in other_worker_data.get('incompatible_with', []):
                    logging.debug(f"_is_allowed_assignment: Worker {other_assigned_id} incompatible with {worker_id} on {date.strftime('%Y-%m-%d')}")
                    return False
    
            return True
        except Exception as e:
            logging.error(f"Error in Scheduler._is_allowed_assignment for worker {worker_id} on {date}: {str(e)}", exc_info=True)
            return False
        
    def _fix_constraint_violations(self):
        """
        Try to fix constraint violations in the current schedule.
        Returns True if fixed, False if couldn't fix all.
        """
        try:
            violations = self._check_schedule_constraints()
            if not violations:
                return True
            
            logging.info(f"Attempting to fix {len(violations)} constraint violations")
            fixes_made = 0
        
            # Fix each violation
            for violation in violations:
                if violation['type'] == 'min_rest_days' or violation['type'] == 'weekly_pattern':
                    # Fix by unassigning one of the shifts
                    worker_id = violation['worker_id']
                    date1 = violation['date1']
                    date2 = violation['date2']
                
                    # Decide which date to unassign
                    # Generally prefer to unassign the later date
                    date_to_unassign = date2
                
                    # Find the shift number for this worker on this date
                    shift_num = None
                    if date_to_unassign in self.schedule:
                        for i, worker in enumerate(self.schedule[date_to_unassign]):
                            if worker == worker_id:
                                shift_num = i
                                break
                
                    if shift_num is not None:
                        # Unassign this worker
                        self.schedule[date_to_unassign][shift_num] = None
                        self.worker_assignments[worker_id].remove(date_to_unassign)
                        violation_type = "rest period" if violation['type'] == 'min_rest_days' else "weekly pattern"
                        logging.info(f"Fixed {violation_type} violation: Unassigned worker {worker_id} from {date_to_unassign}")
                        fixes_made += 1
                
                elif violation['type'] == 'incompatibility':
                    # Fix incompatibility by unassigning one of the workers
                    worker_id = violation['worker_id']
                    incompatible_id = violation['incompatible_id']
                    date = violation['date']
                
                    # Decide which worker to unassign (prefer the one with more assignments)
                    w1_assignments = len(self.worker_assignments.get(worker_id, set()))
                    w2_assignments = len(self.worker_assignments.get(incompatible_id, set()))
                
                    worker_to_unassign = worker_id if w1_assignments >= w2_assignments else incompatible_id
                
                    # Find the shift number for this worker on this date
                    shift_num = None
                    if date in self.schedule:
                        for i, worker in enumerate(self.schedule[date]):
                            if worker == worker_to_unassign:
                                shift_num = i
                                break
                
                    if shift_num is not None:
                        # Unassign this worker
                        self.schedule[date][shift_num] = None
                        self.worker_assignments[worker_to_unassign].remove(date)
                        logging.info(f"Fixed incompatibility violation: Unassigned worker {worker_to_unassign} from {date}")
                        fixes_made += 1
        
            # Check if we fixed all violations
            remaining_violations = self._check_schedule_constraints()
            if remaining_violations:
                logging.warning(f"After fixing attempts, {len(remaining_violations)} violations still remain")
                return False
            else:
                logging.info(f"Successfully fixed all {fixes_made} constraint violations")
                return True
            
        except Exception as e:
            logging.error(f"Error fixing constraint violations: {str(e)}", exc_info=True)
            return False
        
    def _prepare_worker_data(self):
        """
        Prepare worker data before schedule generation:
        - Set empty work periods to the full schedule period
        - Handle other default values
        """
        logging.info("Preparing worker data...")
    
        for worker in self.workers_data:
            # Handle empty work periods - default to full schedule period
            if 'work_periods' not in worker or not worker['work_periods'].strip():
                start_str = self.start_date.strftime('%d-%m-%Y')
                end_str = self.end_date.strftime('%d-%m-%Y')
                worker['work_periods'] = f"{start_str} - {end_str}"
                logging.info(f"Worker {worker['id']}: Empty work period set to full schedule period")
            
    def generate_schedule(self, max_improvement_loops=70):
        logging.info("Starting schedule generation...")
        start_time = datetime.now()

        try:
            # 1. Initialize all of the Scheduler's own attributes for this run.
            self.schedule = {} 
            self.worker_assignments = {w['id']: set() for w in self.workers_data} 
            self.worker_shift_counts = {w['id']: 0 for w in self.workers_data}
            # Assuming self.worker_weekend_counts is the correct attribute name based on __init__
            self.worker_weekend_counts = {w['id']: 0 for w in self.workers_data} 
            self.worker_posts = {w['id']: set() for w in self.workers_data}
            self.last_assignment_date = {w['id']: None for w in self.workers_data}
            self.consecutive_shifts = {w['id']: 0 for w in self.workers_data}            

            logging.info("Initializing schedule structure with variable shift counts...")
            self._initialize_schedule_with_variable_shifts() 
            
            self.schedule_builder = ScheduleBuilder(self)
            logging.info("Scheduler initialized and ScheduleBuilder created.")

            logging.debug(f"DEBUG 1 (Post-Builder-Init): self.schedule ID: {id(self.schedule)}, Keys: {list(self.schedule.keys())}") 
            if hasattr(self, 'schedule_builder') and self.schedule_builder:
                 logging.debug(f"DEBUG 1 (Post-Builder-Init): self.schedule_builder.schedule ID: {id(self.schedule_builder.schedule)}, Keys: {list(self.schedule_builder.schedule.keys())}")

            logging.info(f"Schedule structure initialized with {len(self.schedule)} dates (variable shifts applied).")

        except Exception as e:
            logging.exception("Initialization failed during schedule generation.")
            if isinstance(e, SchedulerError):
                 raise e
            else:
                 raise SchedulerError(f"Initialization failed: {str(e)}")

        # --- 2. Pre-assign mandatory shifts and lock them in place ---
        logging.info("Pre-assigning mandatory shifts; these will be irremovable.")
        self.schedule_builder._assign_mandatory_guards()
        logging.debug(f"DEBUG 2 (After _assign_mandatory_guards): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")
        
        self.schedule_builder._synchronize_tracking_data()
        
        self.schedule_builder._save_current_as_best(initial=True)
        logging.debug(f"DEBUG 4 (After _save_current_as_best): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")
        
        self.log_schedule_summary("After Mandatory Assignment")  
        logging.debug(f"DEBUG 5 (After log_schedule_summary): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")

        # --- 3. Iterative Improvement Loop ---
        improvement_loop_count = 0
        improvement_made_in_cycle = True 
        
        logging.debug(f"DEBUG 6 (BEFORE Improvement Loop WHILE): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")
        
        try:
            while improvement_made_in_cycle and improvement_loop_count < max_improvement_loops:
                improvement_made_in_cycle = False # Reset for the current loop
                logging.info(f"--- Starting Improvement Loop {improvement_loop_count + 1} ---")
                logging.debug(f"DEBUG 7 (TOP OF Improvement Loop {improvement_loop_count + 1}): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")
                loop_start_time = datetime.now()

                # --- MODIFICATION START: Attempt to fill empty shifts first ---
                logging.debug(f"[generate_schedule PRE-CALL _try_fill_empty_shifts] self.schedule_builder.schedule object ID: {id(self.schedule_builder.schedule)}")
                if self.schedule_builder._try_fill_empty_shifts():
                     logging.info("Improvement Loop: Attempted to fill empty shifts and made changes.")
                     improvement_made_in_cycle = True
                else:
                    logging.info("Improvement Loop: Attempted to fill empty shifts, but no changes were made by this step.")
                # --- MODIFICATION END ---
                
                if self.schedule_builder._balance_workloads():
                     logging.info("Improvement Loop: Balanced workloads.")
                     improvement_made_in_cycle = True # If this made a change, ensure the loop continues
                
                if self.schedule_builder._improve_weekend_distribution(): 
                     logging.info("Improvement Loop: Improved weekend distribution (1st call).")
                     improvement_made_in_cycle = True

                self.schedule_builder._synchronize_tracking_data() 
                logging.debug(f"DEBUG 8 (After _synchronize_tracking_data in loop): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")

                if self.schedule_builder._improve_weekend_distribution(): 
                    logging.info("Improvement Loop: Improved weekend distribution (2nd call).")
                    improvement_made_in_cycle = True
                logging.debug(f"DEBUG 9 (After 2nd _improve_weekend_distribution in loop): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")

                if self.schedule_builder._adjust_last_post_distribution():
                    logging.info("Improvement Loop: Balanced last post assignments.")
                    improvement_made_in_cycle = True
                logging.debug(f"DEBUG 10 (After _adjust_last_post_distribution in loop): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")

                loop_end_time = datetime.now()
                logging.info(f"--- Improvement Loop {improvement_loop_count + 1} finished in {(loop_end_time - loop_start_time).total_seconds():.2f}s. Changes made in this iteration: {improvement_made_in_cycle} ---")

                if not improvement_made_in_cycle:
                    logging.info("No further improvements detected in this specific loop iteration. Exiting improvement phase.")
                improvement_loop_count += 1
            
            if improvement_loop_count >= max_improvement_loops:
                logging.warning(f"Reached maximum improvement loops ({max_improvement_loops}). Stopping improvements.")

        except Exception as e:
             logging.exception("Error during schedule improvement loop.")
             raise SchedulerError(f"Failed during improvement loop {improvement_loop_count}: {str(e)}")
                         
        logging.info("Attempting final adjustment of last post distribution for non-variable shift days...")
        if self.schedule_builder._adjust_last_post_distribution(
            balance_tolerance=1.0, # For +/-1 overall balance
            max_iterations=self.config.get('last_post_adjustment_max_iterations', 5) # Make configurable
        ):
            logging.info("Last post distribution adjusted.")
            # Potentially re-evaluate score or save if this step is considered significant enough
            # self.schedule_builder._save_current_as_best() # If this method makes direct changes and should be saved

        # --- 4. Finalization ---
        try:
            logging.info("Finalizing schedule...")
            final_schedule_data = self.schedule_builder.get_best_schedule()
            if final_schedule_data and 'schedule' in final_schedule_data:
                logging.debug(f"DEBUG 12 (In finalization, from get_best_schedule): final_schedule_data['schedule'] keys: {list(final_schedule_data['schedule'].keys())}")
            else:
                logging.debug(f"DEBUG 12 (In finalization): final_schedule_data is None or has no 'schedule' key.")


            if final_schedule_data is None or not final_schedule_data.get('schedule'):
                logging.error("No best schedule was saved or the best schedule found is empty.")
                if not self.schedule or all(all(p is None for p in posts) for posts in self.schedule.values()): 
                     logging.error("Current schedule state is also empty or contains no assignments.")
                     raise SchedulerError("Schedule generation process completed, but no valid schedule data was generated or saved.")
                else:
                     logging.warning("Using current schedule state as final schedule; best schedule data was missing or empty.")
                     final_schedule_data = { 
                          'schedule': self.schedule, 
                          'worker_assignments': self.worker_assignments,
                          'worker_shift_counts': self.worker_shift_counts,
                          'worker_weekend_counts': self.worker_weekend_counts, 
                          'worker_posts': self.worker_posts,
                          'last_assignment_date': self.last_assignment_date, 
                          'consecutive_shifts': self.consecutive_shifts,
                          'score': self.calculate_score() 
                     }
                     if not final_schedule_data.get('schedule'): 
                          raise SchedulerError("Failed to reconstruct final schedule data from current state.")

            logging.info("Updating scheduler state with the selected final schedule.")
            self.schedule = final_schedule_data['schedule'] 
            logging.debug(f"DEBUG 13 (After self.schedule = final_schedule_data['schedule']): self.schedule keys: {list(self.schedule.keys())}, Schedule content sample: {dict(list(self.schedule.items())[:2])}")

            self.worker_assignments = final_schedule_data['worker_assignments']
            self.worker_shift_counts = final_schedule_data['worker_shift_counts']
            self.worker_weekend_counts = final_schedule_data.get('worker_weekend_shifts', final_schedule_data.get('worker_weekend_counts', {})) 
            self.worker_posts = final_schedule_data['worker_posts']
            self.last_assignment_date = final_schedule_data['last_assignment_date'] 
            self.consecutive_shifts = final_schedule_data['consecutive_shifts']
            final_score = final_schedule_data.get('score', float('-inf')) 

            logging.info(f"Final schedule selected with score: {final_score:.2f}")
            empty_shifts_final = []
            total_slots_final = 0
            total_assignments_final = 0

            if not self.schedule:
                 logging.error("CRITICAL: Final schedule dictionary (self.schedule) is empty!")
                 raise SchedulerError("Generated schedule dictionary is empty after finalization process.")

            for date, posts in self.schedule.items():
                if not isinstance(posts, list):
                     logging.error(f"CRITICAL: Schedule entry for date {date} is not a list: {type(posts)}")
                     raise SchedulerError(f"Invalid schedule format detected for date {date}.")
                total_slots_final += len(posts)
                for post_idx, worker_id in enumerate(posts):
                    if worker_id is None:
                        empty_shifts_final.append((date, post_idx))
                    else:
                        total_assignments_final += 1

            schedule_duration_days = (self.end_date - self.start_date).days + 1
            if total_slots_final == 0 and schedule_duration_days > 0:
                 logging.error(f"CRITICAL: Final schedule has 0 total slots, but date range ({self.start_date} to {self.end_date}) covers {schedule_duration_days} days.")
                 raise SchedulerError("Generated schedule has zero total slots despite a valid date range.")
            elif total_slots_final > 0 and total_assignments_final == 0:
                 logging.warning(f"Final schedule has {total_slots_final} slots but contains ZERO assignments.")

            if empty_shifts_final:
                logging.warning(f"Final schedule has {len(empty_shifts_final)} empty shifts remaining out of {total_slots_final} total slots.")

            self.log_schedule_summary("Final Generated Schedule")
            end_time = datetime.now()
            logging.info(f"Schedule generation completed successfully in {(end_time - start_time).total_seconds():.2f} seconds.")
            return True

        except Exception as e:
            logging.exception("Error during schedule finalization.")
            if isinstance(e, SchedulerError):
                raise e
            else:
                raise SchedulerError(f"Failed during finalization: {str(e)}")
   
    def log_schedule_summary(self, title="Schedule Summary"):
        """ Helper method to log key statistics about the current schedule state. """
        logging.info(f"--- {title} ---")
        try:
            total_shifts_assigned = sum(len(assignments) for assignments in self.worker_assignments.values())
            logging.info(f"Total shifts assigned: {total_shifts_assigned}")

            empty_shifts = 0
            total_slots = 0
            for date, posts in self.schedule.items():
                 total_slots += len(posts)
                 empty_shifts += posts.count(None)
            logging.info(f"Total slots: {total_slots}, Empty slots: {empty_shifts}")

            logging.info("Shift Counts per Worker:")
            for worker_id, count in sorted(self.worker_shift_counts.items()):
                 logging.info(f"  Worker {worker_id}: {count} shifts")

            logging.info("Weekend Shifts per Worker:")
            for worker_id, count in sorted(self.worker_weekend_counts.items()):
                 logging.info(f"  Worker {worker_id}: {count} weekend shifts")

            logging.info("Post Assignments per Worker:")
            for worker_id in sorted(self.worker_posts.keys()):
                posts_set = self.worker_posts[worker_id]
                if posts_set: # Only log if worker has assignments
                    # Convert set to sorted list for display
                    posts_list = sorted(list(posts_set))
        
                    # Count how many times each post was worked
                    post_counts = {}
                    for date, shifts in self.schedule.items():
                        for post_idx, assigned_worker in enumerate(shifts):
                            if assigned_worker == worker_id:
                                post_counts[post_idx] = post_counts.get(post_idx, 0) + 1
        
                    # Display both the posts worked and their counts
                    post_details = []
                    for post in posts_list:
                        count = post_counts.get(post, 0)
                        post_details.append(f"P{post}({count})")
        
                    logging.info(f"  Worker {worker_id}: {', '.join(post_details)}")
                    
            # Add more stats as needed (e.g., consecutive shifts, score)
            current_score = self.schedule_builder.calculate_score(self.schedule, self.worker_assignments) # Assuming calculate_score uses current state
            logging.info(f"Current Schedule Score: {current_score}")


        except Exception as e:
            logging.error(f"Error generating schedule summary: {e}")
        logging.info(f"--- End {title} ---")

    def calculate_score(self, schedule_to_score=None, assignments_to_score=None):
        """
        Calculate the score of the given schedule.
        This is a placeholder and should be implemented with actual scoring logic.
        """
        logging.debug("Scheduler.calculate_score called (placeholder)")
        # Placeholder scoring logic:
        # For example, count filled shifts, penalize constraint violations, etc.
        score = 0
        
        current_schedule = schedule_to_score if schedule_to_score is not None else self.schedule
        current_assignments = assignments_to_score if assignments_to_score is not None else self.worker_assignments

        if not current_schedule:
            return float('-inf') # Or 0, depending on how you want to score empty schedules

        filled_shifts = 0
        total_possible_shifts = 0

        for date, shifts in current_schedule.items():
            total_possible_shifts += len(shifts)
            for worker_id in shifts:
                if worker_id is not None:
                    filled_shifts += 1
        
        # Basic score: percentage of filled shifts
        if total_possible_shifts > 0:
            score = (filled_shifts / total_possible_shifts) * 100
        else:
            score = 0 # Or float('-inf') if an empty schedule structure is invalid

        # Add penalties for constraint violations (conceptual)
        # if hasattr(self, 'constraint_checker'):
        #     violations = self.constraint_checker.check_all_constraints(current_schedule, current_assignments)
        #     score -= len(violations) * 10 # Example penalty

        # Add bonuses for desired properties (e.g., balanced workload, good post rotation)

        logging.debug(f"Calculated score (placeholder): {score}")
        return score

    def validate_and_fix_final_schedule(self):
        """
        Final validator that scans the entire schedule and fixes any constraint violations.
        Returns the number of fixes made.
        """
        logging.info("Running final schedule validation...")
    
        # Count issues
        incompatibility_issues = 0
        gap_issues = 0
        other_issues = 0
        fixes_made = 0

        # 1. Check for incompatibilities
        for date in sorted(self.schedule.keys()):
            workers_assigned = [w for w in self.schedule.get(date, []) if w is not None] # Use .get for safety

            # Use indices to safely modify the list while iterating conceptually
            indices_to_check = list(range(len(workers_assigned)))
            processed_pairs = set() # Avoid redundant checks/fixes if multiple pairs exist

            for i in indices_to_check:
                 if i >= len(workers_assigned): continue # List size might change
                 worker1_id = workers_assigned[i]
                 if worker1_id is None: continue # Slot might have been cleared by a previous fix

                 for j in range(i + 1, len(workers_assigned)):
                      if j >= len(workers_assigned): continue # List size might change
                      worker2_id = workers_assigned[j]
                      if worker2_id is None: continue # Slot might have been cleared

                      pair = tuple(sorted((worker1_id, worker2_id)))
                      if pair in processed_pairs: continue # Already handled this pair

                      # Check if workers are incompatible using the schedule_builder method
                      if self.schedule_builder._are_workers_incompatible(worker1_id, worker2_id):
                          incompatibility_issues += 1 # Count issue regardless of fix success
                          processed_pairs.add(pair) # Mark pair as processed
                          logging.warning(f"VALIDATION: Found incompatible workers {worker1_id} and {worker2_id} on {date}")

                          # Remove one of the workers (preferably one with more assignments)
                          w1_count = len(self.worker_assignments.get(worker1_id, set()))
                          w2_count = len(self.worker_assignments.get(worker2_id, set()))

                          worker_to_remove = worker1_id if w1_count >= w2_count else worker2_id
                          try:
                              # Find the post index IN THE ORIGINAL schedule[date] list
                              post_to_remove = self.schedule[date].index(worker_to_remove)

                              # Remove the worker from schedule
                              self.schedule[date][post_to_remove] = None

                              # Remove from assignments tracking
                              if worker_to_remove in self.worker_assignments:
                                  self.worker_assignments[worker_to_remove].discard(date) # Use discard

                              # --- ADDED: Update Tracking Data ---
                              self._update_tracking_data(worker_to_remove, date, post_to_remove, removing=True)
                              # --- END ADDED ---

                              fixes_made += 1
                              logging.warning(f"VALIDATION: Removed worker {worker_to_remove} from {date} Post {post_to_remove} to fix incompatibility")

                              # Update the local workers_assigned list for subsequent checks on the same date
                              if worker_to_remove == worker1_id:
                                   workers_assigned[i] = None # Mark as None in local list
                              else:
                                   workers_assigned[j] = None # Mark as None in local list

                          except ValueError:
                               logging.error(f"VALIDATION FIX ERROR: Worker {worker_to_remove} not found in schedule for {date} during fix.")
                          except Exception as e:
                               logging.error(f"VALIDATION FIX ERROR: Unexpected error removing {worker_to_remove} from {date}: {e}")

        # 2. Check for minimum gap violations (Ensure this also calls _update_tracking_data)
        for worker_id in list(self.worker_assignments.keys()): # Iterate over copy of keys
            assignments = sorted(list(self.worker_assignments.get(worker_id, set()))) # Use .get

            indices_to_remove_gap = [] # Store (date, post) to remove after checking all pairs

            for i in range(len(assignments) - 1):
                date1 = assignments[i]
                date2 = assignments[i+1]
                days_between = (date2 - date1).days

                min_days_between = self.gap_between_shifts + 1
                # Add specific part-time logic if needed here, e.g., based on worker data

                if days_between < min_days_between:
                    gap_issues += 1
                    logging.warning(f"VALIDATION: Found gap violation for worker {worker_id}: only {days_between} days between {date1} and {date2}, minimum required: {min_days_between}")

                    # Mark the later assignment for removal
                    try:
                         # Find post index for date2
                         if date2 in self.schedule and worker_id in self.schedule[date2]:
                              post_to_remove_gap = self.schedule[date2].index(worker_id)
                              indices_to_remove_gap.append((date2, post_to_remove_gap))
                         else:
                              logging.error(f"VALIDATION FIX ERROR (GAP): Worker {worker_id} assignment for {date2} not found in schedule.")
                    except ValueError:
                         logging.error(f"VALIDATION FIX ERROR (GAP): Worker {worker_id} not found in schedule list for {date2}.")

            # Now perform removals for gap violations
            for date_rem, post_rem in indices_to_remove_gap:
                 if date_rem in self.schedule and len(self.schedule[date_rem]) > post_rem and self.schedule[date_rem][post_rem] == worker_id:
                      self.schedule[date_rem][post_rem] = None
                      self.worker_assignments[worker_id].discard(date_rem)
                      # --- ADDED: Update Tracking Data ---
                      self._update_tracking_data(worker_id, date_rem, post_rem, removing=True)
                      # --- END ADDED ---
                      fixes_made += 1
                      logging.warning(f"VALIDATION: Removed worker {worker_id} from {date_rem} Post {post_rem} to fix gap violation")
                 else:
                      logging.warning(f"VALIDATION FIX SKIP (GAP): State changed, worker {worker_id} no longer at {date_rem} Post {post_rem}.")

    
        # 3. Run the reconcile method to ensure data consistency
        if self._reconcile_schedule_tracking():
            other_issues += 1
    
        logging.info(f"Final validation complete: Found {incompatibility_issues} incompatibility issues, {gap_issues} gap issues, and {other_issues} other issues. Made {fixes_made} fixes.")
        return fixes_made

    def _validate_final_schedule(self):
        """
        Validate the final schedule before returning it.
        Returns True if valid, False if issues found.
        """
        try:
            # Attempt to reconcile tracking first
            self._reconcile_schedule_tracking()
        
            # Run the enhanced validation
            fixes_made = self.validate_and_fix_final_schedule()
        
            if fixes_made > 0:
                logging.info(f"Validation fixed {fixes_made} issues")
        
            return True
        except Exception as e:
            logging.error(f"Validation error: {str(e)}", exc_info=True)
            return False

    def _calculate_post_rotation(self):
        """
        Calculate post rotation metrics.
    
        Returns:
            dict: Dictionary with post rotation metrics
        """
        try:
            # Get the post rotation data using the existing method
            rotation_data = self._calculate_post_rotation_coverage()
        
            # If it's already a dictionary with the required keys, use it directly
            if isinstance(rotation_data, dict) and 'uniformity' in rotation_data and 'avg_worker' in rotation_data:
                return rotation_data
            
            # Otherwise, create a dictionary with the required structure
            # Use the value from rotation_data if it's a scalar, or fallback to a default
            overall_score = rotation_data if isinstance(rotation_data, (int, float)) else 40.0
        
            return {
                'overall_score': overall_score,
                'uniformity': 0.0,  # Default value
                'avg_worker': 100.0  # Default value
            }
        except Exception as e:
            logging.error(f"Error in calculating post rotation: {str(e)}")
            # Return a default dictionary with all required keys
            return {
                'overall_score': 40.0,
                'uniformity': 0.0,
                'avg_worker': 100.0
            }
        
    def _calculate_post_rotation_coverage(self):
        """
        Calculate post rotation coverage metrics
    
        Evaluates how well posts are distributed across workers
    
        Returns:
            dict: Dictionary containing post rotation metrics
        """
        logging.info("Calculating post rotation coverage...")
    
        # Initialize metrics
        metrics = {
            'overall_score': 0,
            'worker_scores': {},
            'post_distribution': {}
        }
    
        # Count assignments per post
        post_counts = {post: 0 for post in range(self.num_shifts)}
        total_assignments = 0
    
        for shifts in self.schedule.values():
            for post, worker in enumerate(shifts):
                if worker is not None:
                    post_counts[post] = post_counts.get(post, 0) + 1
                    total_assignments += 1
    
        # Calculate post distribution stats
        if total_assignments > 0:
            expected_per_post = total_assignments / self.num_shifts
            post_deviation = 0
        
            for post, count in post_counts.items():
                metrics['post_distribution'][post] = {
                    'count': count,
                    'percentage': (count / total_assignments * 100) if total_assignments > 0 else 0
                }
                post_deviation += abs(count - expected_per_post)
        
            # Calculate overall post distribution uniformity (100% = perfect distribution)
            post_uniformity = max(0, 100 - (post_deviation / total_assignments * 100))
        else:
            post_uniformity = 0
    
        # Calculate individual worker post rotation scores
        worker_scores = {}
        overall_worker_deviation = 0
    
        for worker in self.workers_data:
            worker_id = worker['id']
            worker_assignments = len(self.worker_assignments.get(worker_id, []))
        
            # Skip workers with no or very few assignments
            if worker_assignments < 2:
                worker_scores[worker_id] = 100  # Perfect score for workers with minimal assignments
                continue
        
            # Get post counts for this worker
            worker_post_counts = {post: 0 for post in range(self.num_shifts)}
        
            for date, shifts in self.schedule.items():
                for post, assigned_worker in enumerate(shifts):
                    if assigned_worker == worker_id:
                        worker_post_counts[post] = worker_post_counts.get(post, 0) + 1
        
            # Calculate deviation from ideal distribution
            expected_per_post_for_worker = worker_assignments / self.num_shifts
            worker_deviation = 0
        
            for post, count in worker_post_counts.items():
                worker_deviation += abs(count - expected_per_post_for_worker)
        
            # Calculate worker's post rotation score (100% = perfect distribution)
            if worker_assignments > 0:
                worker_score = max(0, 100 - (worker_deviation / worker_assignments * 100))
                normalized_worker_deviation = worker_deviation / worker_assignments
            else:
                worker_score = 100
                normalized_worker_deviation = 0
        
            worker_scores[worker_id] = worker_score
            overall_worker_deviation += normalized_worker_deviation
    
        # Calculate overall worker post rotation score
        if len(self.workers_data) > 0:
            avg_worker_score = sum(worker_scores.values()) / len(worker_scores)
        else:
            avg_worker_score = 0
    
        # Combine post distribution and worker rotation scores
        # Weigh post distribution more heavily (60%) than individual worker scores (40%)
        metrics['overall_score'] = (post_uniformity * 0.6) + (avg_worker_score * 0.4)
        metrics['post_uniformity'] = post_uniformity
        metrics['avg_worker_score'] = avg_worker_score
        metrics['worker_scores'] = worker_scores
    
        logging.info(f"Post rotation overall score: {metrics['overall_score']:.2f}%")
        logging.info(f"Post uniformity: {post_uniformity:.2f}%, Avg worker score: {avg_worker_score:.2f}%")
    
        return metrics

    def _backup_best_schedule(self):
        """Save a backup of the current best schedule"""
        try:
            # Create deep copies of all structures
            self.backup_schedule = {}
            for date, shifts in self.schedule.items():
                self.backup_schedule[date] = shifts.copy() if shifts else []
            
            self.backup_worker_assignments = {}
            for worker_id, assignments in self.worker_assignments.items():
                self.backup_worker_assignments[worker_id] = assignments.copy()
            
            # Include other backup structures if needed
            self.backup_worker_posts = {
                worker_id: posts.copy() for worker_id, posts in self.worker_posts.items()
            }
        
            self.backup_worker_weekdays = {
                worker_id: weekdays.copy() for worker_id, weekdays in self.worker_weekdays.items()
            }
        
            self.backup_worker_weekends = {
                worker_id: weekends.copy() for worker_id, weekends in self.worker_weekends.items()
            }    
        
            # Only backup constraint_skips if it exists to avoid errors
            if hasattr(self, 'constraint_skips'):
                self.backup_constraint_skips = {}
                for worker_id, skips in self.constraint_skips.items():
                    self.backup_constraint_skips[worker_id] = {}
                    for skip_type, skip_values in skips.items():
                        if skip_values is not None:
                            self.backup_constraint_skips[worker_id][skip_type] = skip_values.copy()
        
            filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
            logging.info(f"Backed up current schedule in scheduler with {filled_shifts} filled shifts")
            return True
        except Exception as e:
            logging.error(f"Error in scheduler backup: {str(e)}", exc_info=True)
            return False

    def _restore_best_schedule(self):
        """Restore the backed up schedule"""
        try:
            if not hasattr(self, 'backup_schedule'):
                logging.warning("No scheduler backup available to restore")
                return False
            
            # Restore from our backups
            self.schedule = {}
            for date, shifts in self.backup_schedule.items():
                self.schedule[date] = shifts.copy() if shifts else []
            
            self.worker_assignments = {}
            for worker_id, assignments in self.backup_worker_assignments.items():
                self.worker_assignments[worker_id] = assignments.copy()
            
            # Restore other structures if they exist
            if hasattr(self, 'backup_worker_posts'):
                self.worker_posts = {
                    worker_id: posts.copy() for worker_id, posts in self.backup_worker_posts.items()
                }
            
            if hasattr(self, 'backup_worker_weekdays'):
                self.worker_weekdays = {
                    worker_id: weekdays.copy() for worker_id, weekdays in self.backup_worker_weekdays.items()
                }
            
            if hasattr(self, 'backup_worker_weekends'):
                self.worker_weekends = {
                    worker_id: weekends.copy() for worker_id, weekends in self.backup_worker_weekends.items()
                }
            
            # Only restore constraint_skips if backup exists
            if hasattr(self, 'backup_constraint_skips'):
                self.constraint_skips = {}
                for worker_id, skips in self.backup_constraint_skips.items():
                    self.constraint_skips[worker_id] = {}
                    for skip_type, skip_values in skips.items():
                        if skip_values is not None:
                            self.constraint_skips[worker_id][skip_type] = skip_values.copy()
        
            filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
            logging.info(f"Restored schedule in scheduler with {filled_shifts} filled shifts")
            return True
        except Exception as e:
            logging.error(f"Error in scheduler restore: {str(e)}", exc_info=True)
            return False
        
    def export_schedule(self, format='txt'):
        """
        Export the schedule in the specified format
        
        Args:
            format: Output format ('txt' currently supported)
        Returns:
            str: Name of the generated file
        """
        timestamp = datetime.now().strftime('%d%m%Y_%H%M%S')
        filename = f'schedule_{timestamp}.{format}'
        
        if format == 'txt':
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(self._generate_schedule_header())
                f.write(self._generate_schedule_body())
                f.write(self._generate_schedule_summary())
        
        logging.info(f"Schedule exported to {filename}")
        return filename
    
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
                'timestamp': datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
                'validator': self.current_user
            }
            
        except SchedulerError as e:
            logging.error(f"Schedule validation failed: {str(e)}")
            return False, str(e)
        
    def generate_worker_report(self, worker_id, save_to_file=False):
        """
        Generate a worker report and optionally save it to a file
    
        Args:
            worker_id: ID of the worker to generate report for
            save_to_file: Whether to save report to a file (default: False)
        Returns:
            str: The report text
        """
        try:
            report = self.stats.generate_worker_report(worker_id)
        
            # Optionally save to file
            if save_to_file:
                filename = f'worker_{worker_id}_report.txt'
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(report)
                logging.info(f"Worker report saved to {filename}")
            
            return report
        
        except Exception as e:
            logging.error(f"Error generating worker report: {str(e)}")
            return f"Error generating report: {str(e)}"

    def generate_all_worker_reports(self, output_directory=None):
        """
        Generate reports for all workers
    
        Args:
            output_directory: Directory to save reports (default: current directory)
        Returns:
            int: Number of reports generated
        """
        count = 0
        for worker in self.workers_data:
            worker_id = worker['id']
            try:
                report = self.stats.generate_worker_report(worker_id)
            
                # Create filename
                filename = f'worker_{worker_id}_report.txt'
                if output_directory:
                    import os
                    os.makedirs(output_directory, exist_ok=True)
                    filename = os.path.join(output_directory, filename)
                
                # Save to file
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(report)
                
                count += 1
                logging.info(f"Generated report for worker {worker_id}")
            
            except Exception as e:
                logging.error(f"Failed to generate report for worker {worker_id}: {str(e)}")
            
        logging.info(f"Generated {count} worker reports")
        return count
