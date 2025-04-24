# Imports
from datetime import datetime, timedelta
import logging
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

# Class definition
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
            self.gap_between_shifts = config.get('gap_between_shifts', 1)
            self.max_consecutive_weekends = config.get('max_consecutive_weekends', 2)
    
            # Initialize tracking dictionaries
            self.schedule = {}
            self.worker_assignments = {w['id']: set() for w in self.workers_data}
            self.worker_posts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data}
            self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data}

            # Initialize tracking data structures
            self.worker_shift_counts = {w['id']: 0 for w in self.workers_data}
            self.worker_weekend_counts = {w['id']: 0 for w in self.workers_data}
            self.worker_post_counts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data}
            self.worker_weekday_counts = {w['id']: {d: 0 for d in range(7)} for w in self.workers_data}
            self.worker_holiday_counts = {w['id']: 0 for w in self.workers_data}
            # Store last assignment date for gap checks
            self.last_assignment_date = {w['id']: None for w in self.workers_data}
                      
            # Initialize worker targets
            for worker in self.workers_data:
                if 'target_shifts' not in worker:
                    worker['target_shifts'] = 0

            # Set current time and user
            self.date_utils = DateTimeUtils()
            self.current_datetime = self.date_utils.get_spain_time()
            self.current_user = 'saldo27'
        
            # Add max_shifts_per_worker calculation
            total_days = (self.end_date - self.start_date).days + 1
            total_shifts = total_days * self.num_shifts
            num_workers = len(self.workers_data)
            self.max_shifts_per_worker = (total_shifts // num_workers) + 2  # Add some flexibility

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
            self.schedule_builder = ScheduleBuilder(self)
            self.eligibility_tracker = WorkerEligibilityTracker(
                self.workers_data,
                self.holidays,
                self.gap_between_shifts,
                self.max_consecutive_weekends
            )

            # Calculate targets before proceeding
            self._calculate_target_shifts()

            self._log_initialization()

            # Ensure ScheduleBuilder receives the updated self.workers_data
            self.builder = ScheduleBuilder(self)
    

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
        self.worker_posts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data}
        self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
        self.worker_weekends = {w['id']: [] for w in self.workers_data}
        self.constraint_skips = {
            w['id']: {'gap': [], 'incompatibility': [], 'reduced_gap': []}
            for w in self.workers_data
        }
        
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

    def _calculate_target_shifts(self):
        """
        Calculate target number of shifts for each worker based on their work percentage,
        ensuring optimal distribution and fairness while respecting mandatory shifts.
        """
        try:
            logging.info("Calculating target shifts distribution...")
            total_days = (self.end_date - self.start_date).days + 1
            total_shifts = total_days * self.num_shifts
        
            # First, account for mandatory shifts which cannot be redistributed
            mandatory_shifts_by_worker = {}
            total_mandatory_shifts = 0
        
            for worker in self.workers_data:
                worker_id = worker['id']
                mandatory_days = worker.get('mandatory_days', [])
                mandatory_dates = self.date_utils.parse_dates(mandatory_days)
            
                # Count only mandatory days within schedule period
                valid_mandatory_dates = [d for d in mandatory_dates 
                                        if self.start_date <= d <= self.end_date]
            
                mandatory_count = len(valid_mandatory_dates)
                mandatory_shifts_by_worker[worker_id] = mandatory_count
                total_mandatory_shifts += mandatory_count
            
                logging.debug(f"Worker {worker_id} has {mandatory_count} mandatory shifts")
        
            # Remaining shifts to distribute
            remaining_shifts = total_shifts - total_mandatory_shifts
            logging.info(f"Total shifts: {total_shifts}, Mandatory shifts: {total_mandatory_shifts}, Remaining: {remaining_shifts}")
        
            if remaining_shifts < 0:
                logging.error("More mandatory shifts than total available shifts!")
                remaining_shifts = 0
        
            # Get and validate all worker percentages
            percentages = []
            for worker in self.workers_data:
                try:
                    percentage = float(str(worker.get('work_percentage', 100)).strip())
                    if percentage <= 0:
                        logging.warning(f"Worker {worker['id']} has invalid percentage ({percentage}), using 100%")
                        percentage = 100
                    percentages.append(percentage)
                except (ValueError, TypeError) as e:
                    logging.warning(f"Invalid percentage for worker {worker['id']}, using 100%: {str(e)}")
                    percentages.append(100)
        
            # Calculate the total percentage points available across all workers
            total_percentage = sum(percentages)
        
            # First pass: Calculate exact targets based on percentages for remaining shifts
            exact_targets = []
            for percentage in percentages:
                target = (percentage / total_percentage) * remaining_shifts
                exact_targets.append(target)
        
            # Second pass: Round targets while minimizing allocation error
            rounded_targets = []
            leftover = 0.0
        
            for target in exact_targets:
                # Add leftover from previous rounding
                adjusted_target = target + leftover
            
                # Round to nearest integer
                rounded = round(adjusted_target)
            
                # Calculate new leftover
                leftover = adjusted_target - rounded
            
                # Ensure minimum of 0 non-mandatory shifts (since we add mandatory later)
                rounded = max(0, rounded)
            
                rounded_targets.append(rounded)
        
            # Final adjustment to ensure total equals required remaining shifts
            sum_targets = sum(rounded_targets)
            difference = remaining_shifts - sum_targets
        
            if difference != 0:
                logging.info(f"Adjusting allocation by {difference} shifts")
            
                # Create sorted indices based on fractional part distance from rounding threshold
                frac_distances = [abs((t + leftover) - round(t + leftover)) for t in exact_targets]
                sorted_indices = sorted(range(len(frac_distances)), key=lambda i: frac_distances[i], reverse=(difference > 0))
            
                # Add or subtract from workers with smallest rounding error first
                for i in range(abs(difference)):
                    adj_index = sorted_indices[i % len(sorted_indices)]
                    rounded_targets[adj_index] += 1 if difference > 0 else -1
                
                    # Ensure minimums
                    if rounded_targets[adj_index] < 0:
                        rounded_targets[adj_index] = 0
        
            # Add mandatory shifts to calculated targets
            for i, worker in enumerate(self.workers_data):
                worker_id = worker['id']
                # Total target = non-mandatory target + mandatory shifts
                worker['target_shifts'] = rounded_targets[i] + mandatory_shifts_by_worker[worker_id]
            
                # Apply additional constraints based on work percentage
                work_percentage = float(str(worker.get('work_percentage', 100)).strip())
            
                # Calculate reasonable maximum based on work percentage (excluding mandatory shifts)
                max_reasonable = total_days * (work_percentage / 100) * 0.8
            
                # For target exceeding reasonable maximum, adjust non-mandatory part only
                non_mandatory_target = worker['target_shifts'] - mandatory_shifts_by_worker[worker_id]
                if non_mandatory_target > max_reasonable and max_reasonable >= 0:
                    logging.warning(f"Worker {worker['id']} non-mandatory target {non_mandatory_target} exceeds reasonable maximum {max_reasonable}")
                
                    # Reduce target and redistribute, but preserve mandatory shifts
                    excess = non_mandatory_target - int(max_reasonable)
                    if excess > 0:
                        worker['target_shifts'] = int(max_reasonable) + mandatory_shifts_by_worker[worker_id]
                        self._redistribute_excess_shifts(excess, worker['id'], mandatory_shifts_by_worker)
            
                logging.info(f"Worker {worker['id']}: {work_percentage}% → {worker['target_shifts']} shifts "
                             f"({mandatory_shifts_by_worker[worker_id]} mandatory, "
                             f"{worker['target_shifts'] - mandatory_shifts_by_worker[worker_id]} calculated)")
        
            # Final verification - ensure at least 1 total shift per worker
            for worker in self.workers_data:
                if 'target_shifts' not in worker or worker['target_shifts'] <= 0:
                    worker['target_shifts'] = 1
                    logging.warning(f"Assigned minimum 1 shift to worker {worker['id']}")
        
            return True
    
        except Exception as e:
            logging.error(f"Error in target calculation: {str(e)}", exc_info=True)
        
            # Emergency fallback - equal distribution plus mandatory shifts
            default_target = max(1, round(remaining_shifts / len(self.workers_data)))
            for worker in self.workers_data:
                worker_id = worker['id']
                worker['target_shifts'] = default_target + mandatory_shifts_by_worker.get(worker_id, 0)
        
            logging.warning(f"Using fallback distribution: {default_target} non-mandatory shifts per worker plus mandatory shifts")
            return False

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
    
        # Ensure schedule dictionary is initialized
        for current_date in self._get_date_range(self.start_date, self.end_date):
            if current_date not in self.schedule:
                self.schedule[current_date] = [None] * self.num_shifts
    
        logging.info("Data integrity check completed")
        return True

    def _update_tracking_data(self, worker_id, date, post, removing=False):
        """
        Update all tracking dictionaries when a worker is assigned OR removed.
        Handles worker_posts, worker_weekdays, and worker_weekends.
        Note: worker_assignments modification is handled directly in the calling
        functions (_try_fill_empty_shifts, _balance_workloads, etc.) for atomicity.

        Args:
            worker_id: ID of the worker. Can be None if removing a placeholder.
            date: The datetime.date object of the assignment/removal.
            post: The post number (integer) of the assignment/removal.
            removing (bool): If True, decrement stats/remove entries; otherwise, increment/add.
                             Defaults to False.
        """
        # Safety check if a None assignment is somehow passed during removal
        if worker_id is None:
            logging.debug(f"Skipping tracking update for None worker on {date.strftime('%Y-%m-%d')}, post {post}, removing={removing}")
            return

        logging.debug(f"Updating tracking data for worker {worker_id} on {date.strftime('%Y-%m-%d')}, post {post}, removing={removing}")

        # Determine the increment/decrement value for counts
        change = -1 if removing else 1

        # --- Update worker_posts ---
        # Assumes self.worker_posts is {worker_id: {post_index: count}}
        if worker_id in self.worker_posts:
            current_post_count = self.worker_posts[worker_id].get(post, 0)
            new_post_count = max(0, current_post_count + change) # Ensure count doesn't go below 0
            self.worker_posts[worker_id][post] = new_post_count
            logging.debug(f"Worker {worker_id} post {post} count updated to {new_post_count}")
        elif not removing: # Only initialize if adding and worker dict doesn't exist
            # Initialize post counts for this worker if they are being added for the first time
            self.worker_posts[worker_id] = {p: 0 for p in range(self.num_shifts)}
            self.worker_posts[worker_id][post] = 1 # Set initial count to 1
            logging.debug(f"Initialized post counts for worker {worker_id}; post {post} set to 1")
        else:
             # Worker not found during removal, log a warning
             logging.warning(f"Tried to update post counts for non-existent worker {worker_id} during removal.")


        # --- Update worker_weekdays ---
        # Assumes self.worker_weekdays is {worker_id: {weekday_index: count}}
        weekday = date.weekday() # Monday is 0 and Sunday is 6
        if worker_id in self.worker_weekdays:
            current_weekday_count = self.worker_weekdays[worker_id].get(weekday, 0)
            new_weekday_count = max(0, current_weekday_count + change) # Ensure count doesn't go below 0
            self.worker_weekdays[worker_id][weekday] = new_weekday_count
            logging.debug(f"Worker {worker_id} weekday {weekday} count updated to {new_weekday_count}")
        elif not removing: # Only initialize if adding and worker dict doesn't exist
            # Initialize weekday counts for this worker
            self.worker_weekdays[worker_id] = {wd: 0 for wd in range(7)}
            self.worker_weekdays[worker_id][weekday] = 1 # Set initial count to 1
            logging.debug(f"Initialized weekday counts for worker {worker_id}; weekday {weekday} set to 1")
        else:
            # Worker not found during removal, log a warning
            logging.warning(f"Tried to update weekday counts for non-existent worker {worker_id} during removal.")


        # --- Update worker_weekends ---
        # Assumes self.worker_weekends is {worker_id: [date1, date2, ...]}
        # Check if the date is a weekend (Sat/Sun) or a defined holiday
        # Ensure self.holidays is a list or set of datetime.date objects
        # Ensure self.date_utils exists and has is_holiday method if using holidays
        is_weekend_day = date.weekday() >= 5 # Saturday (5) or Sunday (6)
        is_holiday_day = hasattr(self, 'date_utils') and hasattr(self.date_utils, 'is_holiday') and self.date_utils.is_holiday(date)
        # Alternatively, if date_utils not available/reliable:
        # is_holiday_day = date in self.holidays

        is_tracked_as_weekend = is_weekend_day or is_holiday_day

        if is_tracked_as_weekend:
            if worker_id in self.worker_weekends:
                if removing:
                    # Try to remove the date if it exists in the list
                    if date in self.worker_weekends[worker_id]:
                        try:
                            self.worker_weekends[worker_id].remove(date)
                            logging.debug(f"Removed weekend/holiday date {date.strftime('%Y-%m-%d')} for worker {worker_id}")
                        except ValueError:
                            # Should not happen due to 'if date in ...' check, but belt-and-suspenders
                            logging.error(f"Internal error: Date {date.strftime('%Y-%m-%d')} was in worker {worker_id}'s weekend list but remove failed.")
                    else:
                        logging.warning(f"Tried to remove weekend/holiday date {date.strftime('%Y-%m-%d')} for worker {worker_id}, but it was not in their list.")
                else: # Adding
                    # Add the date only if it's not already present
                    if date not in self.worker_weekends[worker_id]:
                        self.worker_weekends[worker_id].append(date)
                        self.worker_weekends[worker_id].sort() # Keep the list sorted
                        logging.debug(f"Added weekend/holiday date {date.strftime('%Y-%m-%d')} for worker {worker_id}")
                    else:
                        # Date already exists, likely due to multiple shifts on the same weekend day or re-processing
                        logging.debug(f"Weekend/holiday date {date.strftime('%Y-%m-%d')} already present for worker {worker_id}, not adding again.")

            elif not removing: # Initialize if adding and worker dict doesn't exist
                # Initialize weekend list for this worker
                self.worker_weekends[worker_id] = [date]
                logging.debug(f"Initialized weekend/holiday list for worker {worker_id} with date {date.strftime('%Y-%m-%d')}")
            else:
                 # Worker not found during removal, log a warning
                 logging.warning(f"Tried to update weekend/holiday list for non-existent worker {worker_id} during removal.")

        # --- Update Constraint Skips (Optional) ---
        # If removing an assignment could potentially resolve a previously logged constraint skip,
        # you might add logic here to remove entries from self.constraint_skips.
        # However, this is complex and often not necessary unless you explicitly need to track
        # the *current* number of active violations. Usually, skips are logged historically.
        # Example (conceptual):
        # if removing and worker_id in self.constraint_skips:
        #     # Check if removing 'date' resolves a 'gap' skip involving 'date'
        #     # Check if removing 'date' resolves an 'incompatibility' skip involving 'date'
        #     pass # Add specific logic if needed

        logging.debug(f"Finished updating tracking data for worker {worker_id}")

    
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

        # Ensure all dates in the period are in the schedule
        for date in self._get_date_range(self.start_date, self.end_date):
            if date not in self.schedule:
                self.schedule[date] = [None] * self.num_shifts
            elif len(self.schedule[date]) < self.num_shifts:
                # Fill missing shifts with None
                self.schedule[date].extend([None] * (self.num_shifts - len(self.schedule[date])))
            elif len(self.schedule[date]) > self.num_shifts:
                # Trim excess shifts (shouldn't happen, but just in case)
                self.schedule[date] = self.schedule[date][:self.num_shifts]
    
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
    
                        # Check for 7 or 14 day patterns (same day of week)
                        if days_difference == 7 or days_difference == 14:
                            too_close = True
                            break
    
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

    def _assign_mixed_strategy(self):
        """
        Try multiple assignment strategies and choose the best result.
        """
        logging.info("Using mixed strategy approach to generate optimal schedule")
    
        try:
            # Strategy 1: Simple assignment
            self._backup_best_schedule()  # Save current state
            success1 = self._assign_workers_simple()
        
            # Ensure tracking is consistent
            self._reconcile_schedule_tracking()
        
            coverage1 = self._calculate_coverage() if success1 else 0
            post_rotation1 = self._calculate_post_rotation()['overall_score'] if success1 else 0
        
            # Create deep copies of the simple assignment result
            simple_schedule = {}
            for date, shifts in self.schedule.items():
                simple_schedule[date] = shifts.copy() if shifts else []
            
            simple_assignments = {}
            for worker_id, assignments in self.worker_assignments.items():
                simple_assignments[worker_id] = set(assignments)
        
            logging.info(f"Simple assignment strategy: {coverage1:.1f}% coverage, {post_rotation1:.1f}% rotation")
        
            # Strategy 2: Cadence-based assignment
            self._restore_best_schedule()  # Restore to original state
            try:
                success2 = self._assign_workers_cadence()
            
                # Ensure tracking is consistent
                self._reconcile_schedule_tracking()
            
                coverage2 = self._calculate_coverage() if success2 else 0
                post_rotation2 = self._calculate_post_rotation()['overall_score'] if success2 else 0
            
                # Create deep copies of the cadence result
                cadence_schedule = {}
                for date, shifts in self.schedule.items():
                    cadence_schedule[date] = shifts.copy() if shifts else []
                
                cadence_assignments = {}
                for worker_id, assignments in self.worker_assignments.items():
                    cadence_assignments[worker_id] = set(assignments)
                
                logging.info(f"Cadence assignment strategy: {coverage2:.1f}% coverage, {post_rotation2:.1f}% rotation")
            except Exception as e:
                logging.error(f"Error in cadence assignment: {str(e)}", exc_info=True)
                # Default to simple assignment if cadence fails
                success2 = False
                coverage2 = 0
                post_rotation2 = 0
        
            # Compare results
            logging.info(f"Strategy comparison: Simple ({coverage1:.1f}% coverage, {post_rotation1:.1f}% rotation) vs "
                        f"Cadence ({coverage2:.1f}% coverage, {post_rotation2:.1f}% rotation)")
        
            # Choose the better strategy based on combined score (coverage is more important)
            score1 = coverage1 * 0.7 + post_rotation1 * 0.3
            score2 = coverage2 * 0.7 + post_rotation2 * 0.3
        
            if score1 >= score2 or not success2:
                # Use simple assignment results
                self.schedule = simple_schedule
                self.worker_assignments = simple_assignments
                logging.info(f"Selected simple assignment strategy (score: {score1:.1f})")
            else:
                # Use cadence assignment results
                self.schedule = cadence_schedule
                self.worker_assignments = cadence_assignments
                logging.info(f"Selected cadence assignment strategy (score: {score2:.1f})")
        
            # Final reconciliation to ensure consistency
            self._reconcile_schedule_tracking()
        
            # Final coverage calculation
            final_coverage = self._calculate_coverage()
            logging.info(f"Final mixed strategy coverage: {final_coverage:.1f}%")
        
            return final_coverage > 0
        
        except Exception as e:
            logging.error(f"Error in mixed strategy assignment: {str(e)}", exc_info=True)
            # Fall back to simple assignment if mixed strategy fails
            return self._assign_workers_simple()

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
                        if days_between == 7 or days_between == 14:
                            violations.append({
                                'type': 'weekly_pattern',
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

    def _is_allowed_assignment(self, worker_id, date, shift_num):
        """
        Check if assigning this worker to this date/shift would violate any constraints.
        Returns True if assignment is allowed, False otherwise.        
    
        Enforces:
        - Special case: No Friday-Monday assignments (require 3 days gap)
        - No 7 or 14 day patterns
        - Worker incompatibility constraints
        """
        try:
            # Check if worker is available on this date
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                return False
    
            # Check if worker is already assigned on this date
            if worker_id in self.worker_assignments and date in self.worker_assignments[worker_id]:
                return False
    
            # Check past assignments for minimum gap and patterns
            for assigned_date in self.worker_assignments.get(worker_id, set()):
                days_difference = abs((date - assigned_date).days)
        
                # Check past assignments for minimum gap and patterns
                for assigned_date in self.worker_assignments.get(worker_id, set()):
                    days_difference = abs((date - assigned_date).days)
    
                    # Basic minimum gap check based on configurable parameter
                    min_days_between = self.gap_between_shifts + 1  # +1 because we need days_difference > gap
                    if days_difference < min_days_between:
                        logging.debug(f"Worker {worker_id} cannot be assigned on {date} due to insufficient rest (needs at least {min_days_between} days)")
                        return False
        
                    # Special case for Friday-Monday if gap is only 1 day
                    if self.gap_between_shifts == 1 and days_difference == 3:
                        # Check if one date is Friday (weekday 4) and the other is Monday (weekday 0)
                        if ((assigned_date.weekday() == 4 and date.weekday() == 0) or 
                            (assigned_date.weekday() == 0 and date.weekday() == 4)):
                            logging.debug(f"Worker {worker_id} cannot be assigned Friday-Monday (needs at least {self.gap_between_shifts + 2} days gap)")
                            return False
            
                # Check 7 or 14 day patterns (to avoid same day of week assignments)
                if days_difference == 7 or days_difference == 14:
                    logging.debug(f"Worker {worker_id} cannot be assigned on {date} as it would create a 7 or 14 day pattern")
                    return False
        
            # Check incompatibility constraints using worker data
            incompatible_with = worker.get('incompatible_with', [])
            if incompatible_with:
                # Check if any incompatible worker is already assigned to this date
                for incompatible_id in incompatible_with:
                    if date in self.schedule and incompatible_id in self.schedule[date]:
                        logging.debug(f"Worker {worker_id} cannot work with incompatible worker {incompatible_id} on {date}")
                        return False
        
            # Use the schedule_builder's incompatibility check method
            if not self.schedule_builder._check_incompatibility(worker_id, date):
                return False
    
            # All checks passed
            return True
        except Exception as e:
            logging.error(f"Error checking assignment constraints: {str(e)}")
            # Default to not allowing on error
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
            
    def generate_schedule(self, max_improvement_loops=30):
        """
        Generates the duty schedule.

        Orchestrates the schedule generation process, including initial assignments
        and iterative improvements.

        Args:
            max_improvement_loops (int): Maximum number of times to loop through
                                         improvement steps if changes are still being made.

        Returns:
            bool: True if a valid schedule was successfully generated and finalized,
                  False otherwise.

        Raises:
            SchedulerError: If a fatal error occurs during generation that prevents
                            a schedule from being created.
        """
        logging.info("Starting schedule generation...")
        start_time = datetime.now()

        # --- 1. Initialization ---
        try:
            # Clear previous state
            self.schedule = {} # Initialize as empty dict first
            self.worker_assignments = {w['id']: set() for w in self.workers_data} # Ensure keys exist
            self.worker_shift_counts = {w['id']: 0 for w in self.workers_data}
            self.worker_weekend_shifts = {w['id']: 0 for w in self.workers_data}
            self.worker_posts = {w['id']: {} for w in self.workers_data}
            self.last_assigned_date = {w['id']: None for w in self.workers_data}
            self.consecutive_shifts = {w['id']: 0 for w in self.workers_data}            # Initialize any other tracking data structures

            # Create ScheduleBuilder instance
            self.schedule_builder = ScheduleBuilder(self)
            logging.info("Scheduler initialized and ScheduleBuilder created.")

            # --- ADDED: Initialize Schedule Structure ---
            logging.info(f"Initializing schedule structure from {self.start_date} to {self.end_date} with {self.num_shifts} posts per day.")
            if not self.start_date or not self.end_date or self.num_shifts is None:
                 raise SchedulerError("Start date, end date, or num_shifts not properly initialized in Scheduler.")
            if self.num_shifts <= 0:
                 raise SchedulerError(f"Number of shifts per day (num_shifts) must be positive, got {self.num_shifts}.")

            current_date = self.start_date
            while current_date <= self.end_date:
                self.schedule[current_date] = [None] * self.num_shifts
                current_date += timedelta(days=1)
            logging.info(f"Schedule structure initialized with {len(self.schedule)} dates.")
            # --- END ADDED ---

        except Exception as e:
            logging.exception("Initialization failed during schedule generation.")
            # Wrap non-SchedulerErrors
            if isinstance(e, SchedulerError):
                 raise e
            else:
                 raise SchedulerError(f"Initialization failed: {str(e)}")


        # --- 2. Assign Mandatory Shifts ---
        try:
            # Now self.schedule has the correct structure to place mandatory shifts
            logging.info("Assigning mandatory shifts...")
            self.schedule_builder._assign_mandatory_guards()
            # Save this initial state (might have mandatory shifts now)
            self.schedule_builder._save_current_as_best(initial=True)
            logging.info("Mandatory shifts assigned.")
            self.log_schedule_summary("After Mandatory Assignment") # This should now show total slots

        except Exception as e:
            # ... (error handling) ...
            logging.exception("Error assigning mandatory guards.")
            raise SchedulerError(f"Failed during mandatory assignment: {str(e)}")
        
        # --- 3. Iterative Improvement Loop ---
        improvement_loop_count = 0
        improvement_made_in_cycle = True # Start as True to enter the loop

        try:
            # The loop structure remains the same
            while improvement_made_in_cycle and improvement_loop_count < max_improvement_loops:
                improvement_made_in_cycle = False
                logging.info(f"--- Starting Improvement Loop {improvement_loop_count + 1} ---")
                loop_start_time = datetime.now()

                # --- Run Improvement Steps ---
                # !!! IMPORTANT: Call _try_fill_empty_shifts FIRST !!!
                if self.schedule_builder._try_fill_empty_shifts():
                    logging.info("Improvement Loop: Filled empty shifts.")
                    improvement_made_in_cycle = True

                # Now run other balancing/improvement steps
                if self.schedule_builder._balance_workloads():
                     logging.info("Improvement Loop: Balanced workloads.")
                     improvement_made_in_cycle = True

                if self.schedule_builder._improve_post_rotation():
                     logging.info("Improvement Loop: Improved general post rotation.")
                     improvement_made_in_cycle = True

                if self.schedule_builder._balance_last_post():
                     logging.info("Improvement Loop: Balanced last post assignments.")
                     improvement_made_in_cycle = True

                if self.schedule_builder._improve_weekend_distribution():
                     logging.info("Improvement Loop: Improved weekend distribution.")
                     improvement_made_in_cycle = True

                # Add other steps like _fix_incompatibility_violations if needed
                # if self.schedule_builder._fix_incompatibility_violations():
                #      logging.info("Improvement Loop: Fixed incompatibilities.")
                #      improvement_made_in_cycle = True

                loop_end_time = datetime.now()
                logging.info(f"--- Improvement Loop {improvement_loop_count + 1} finished in {(loop_end_time - loop_start_time).total_seconds():.2f}s. Changes made: {improvement_made_in_cycle} ---")

                if not improvement_made_in_cycle:
                    logging.info("No further improvements detected in this loop. Exiting improvement phase.")
                improvement_loop_count += 1

                # Optional: Log schedule summary after each full loop
                # self.log_schedule_summary(f"After Improvement Loop {improvement_loop_count}")


            if improvement_loop_count >= max_improvement_loops:
                logging.warning(f"Reached maximum improvement loops ({max_improvement_loops}). Stopping improvements.")

        except Exception as e:
             logging.exception("Error during schedule improvement loop.")
             # Depending on severity, either raise or try to use the best schedule found so far
             # Let's raise for now, as an error here indicates a problem in the logic
             raise SchedulerError(f"Failed during improvement loop {improvement_loop_count}: {str(e)}")


        # --- 4. Finalization ---
        try:
            logging.info("Finalizing schedule...")
            # Retrieve the best schedule data dictionary found by the builder
            final_schedule_data = self.schedule_builder.get_best_schedule()

            # Check if a best schedule was actually saved and if it contains schedule data
            if final_schedule_data is None or not final_schedule_data.get('schedule'):
                logging.error("No best schedule was saved or the best schedule found is empty.")
                # Fallback: Check if the current state has a schedule (maybe mandatory shifts were assigned but no improvements saved)
                if not self.schedule or all(all(p is None for p in posts) for posts in self.schedule.values()):
                     # If the current schedule is also empty or contains only None
                     logging.error("Current schedule state is also empty or contains no assignments.")
                     raise SchedulerError("Schedule generation process completed, but no valid schedule data was generated or saved.")
                else:
                     # Use the current state as a last resort if the builder didn't save a 'best'
                     logging.warning("Using current schedule state as final schedule; best schedule data was missing or empty.")
                     final_schedule_data = { # Reconstruct from current state
                          'schedule': self.schedule,
                          'worker_assignments': self.worker_assignments,
                          'worker_shift_counts': self.worker_shift_counts,
                          'worker_weekend_shifts': self.worker_weekend_shifts,
                          'worker_posts': self.worker_posts,
                          'last_assigned_date': self.last_assigned_date,
                          'consecutive_shifts': self.consecutive_shifts,
                          'score': self.schedule_builder.calculate_score() # Recalculate score based on current state
                     }
                     # Ensure the reconstructed data is not empty
                     if not final_schedule_data.get('schedule'):
                          raise SchedulerError("Failed to reconstruct final schedule data from current state.")


            # Update the main scheduler's state with the data from the best found schedule
            logging.info("Updating scheduler state with the selected final schedule.")
            self.schedule = final_schedule_data['schedule']
            self.worker_assignments = final_schedule_data['worker_assignments']
            self.worker_shift_counts = final_schedule_data['worker_shift_counts']
            self.worker_weekend_shifts = final_schedule_data['worker_weekend_shifts']
            self.worker_posts = final_schedule_data['worker_posts']
            self.last_assigned_date = final_schedule_data['last_assigned_date']
            self.consecutive_shifts = final_schedule_data['consecutive_shifts']
            final_score = final_schedule_data.get('score', float('-inf')) # Use .get for safety

            logging.info(f"Final schedule selected with score: {final_score:.2f}")

            # --- Final Validation ---
            empty_shifts_final = []
            total_slots_final = 0
            total_assignments_final = 0

            # Check if the final schedule dictionary itself is empty
            if not self.schedule:
                 logging.error("CRITICAL: Final schedule dictionary (self.schedule) is empty!")
                 raise SchedulerError("Generated schedule dictionary is empty after finalization process.")

            # Iterate through the final schedule to count slots and assignments
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

            # Sanity check: If the date range is valid, we expect > 0 total slots
            schedule_duration_days = (self.end_date - self.start_date).days + 1
            if total_slots_final == 0 and schedule_duration_days > 0:
                 logging.error(f"CRITICAL: Final schedule has 0 total slots, but date range ({self.start_date} to {self.end_date}) covers {schedule_duration_days} days.")
                 # This indicates a fundamental failure, likely during schedule structure initialization or retrieval
                 raise SchedulerError("Generated schedule has zero total slots despite a valid date range.")
            elif total_slots_final > 0 and total_assignments_final == 0:
                 logging.warning(f"Final schedule has {total_slots_final} slots but contains ZERO assignments.")
                 # This might be acceptable if constraints are impossible, but it's suspicious.

            # Report remaining empty shifts
            if empty_shifts_final:
                logging.warning(f"Final schedule has {len(empty_shifts_final)} empty shifts remaining out of {total_slots_final} total slots.")
                # Depending on requirements, you might return False or raise an error here.
                # For now, we proceed but log the warning.
                # Example: if len(empty_shifts_final) > allowed_empty_threshold: return False

            # Log summary of the final schedule state
            self.log_schedule_summary("Final Generated Schedule")

            end_time = datetime.now()
            logging.info(f"Schedule generation completed successfully in {(end_time - start_time).total_seconds():.2f} seconds.")
            # Return True indicating the process finished, even if the schedule has empty slots
            return True

        except Exception as e:
            # Catch any exception during finalization
            logging.exception("Error during schedule finalization.")
            # Re-raise SchedulerError directly, wrap others
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
            for worker_id, count in sorted(self.worker_weekend_shifts.items()):
                 logging.info(f"  Worker {worker_id}: {count} weekend shifts")

            logging.info("Post Assignments per Worker:")
            for worker_id, posts_dict in sorted(self.worker_posts.items()):
                 if posts_dict: # Only log if worker has assignments
                      logging.info(f"  Worker {worker_id}: {dict(sorted(posts_dict.items()))}")

            # Add more stats as needed (e.g., consecutive shifts, score)
            current_score = self.schedule_builder.calculate_score(self.schedule, self.worker_assignments) # Assuming calculate_score uses current state
            logging.info(f"Current Schedule Score: {current_score}")


        except Exception as e:
            logging.error(f"Error generating schedule summary: {e}")
        logging.info(f"--- End {title} ---")


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
