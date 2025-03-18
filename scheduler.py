# Imports
from datetime import datetime, timedelta
import logging
import sys
import random
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
        
            # Initialize tracking dictionaries
            self.schedule = {}
            self.worker_assignments = {w['id']: set() for w in self.workers_data}
            self.worker_posts = {w['id']: set() for w in self.workers_data}
            self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data}

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
                self.holidays
            )

            # Calculate targets before proceeding
            self._calculate_target_shifts()

            self._log_initialization()

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

    def _update_tracking_data(self, worker_id, date, shift_idx):
        """
        Update tracking data structures when a worker is assigned to a shift

        Args:
            worker_id: ID of the worker being assigned
            date: Date of assignment
            shift_idx: Index of the shift being assigned (0-indexed)
        """
        try:
            # Update worker assignments
            if worker_id not in self.worker_assignments:
                self.worker_assignments[worker_id] = set()
            self.worker_assignments[worker_id].add(date)
    
            # Update post tracking
            if worker_id not in self.worker_posts:
                self.worker_posts[worker_id] = set()
            self.worker_posts[worker_id].add(shift_idx)
    
            # Update weekday counts
            if worker_id not in self.worker_weekdays:
                self.worker_weekdays[worker_id] = {i: 0 for i in range(7)}
            weekday = date.weekday()
            self.worker_weekdays[worker_id][weekday] += 1
    
            # Update weekend tracking
            if worker_id not in self.worker_weekends:
                self.worker_weekends[worker_id] = []
            is_weekend = date.weekday() >= 4 or date in self.holidays  # Friday, Saturday, Sunday or holiday
            if is_weekend:
                if date not in self.worker_weekends[worker_id]:
                    self.worker_weekends[worker_id].append(date)
                self.worker_weekends[worker_id].sort()  # Keep sorted
    
            # Update the worker eligibility tracker if it exists
            if hasattr(self, 'eligibility_tracker'):
                self.eligibility_tracker.update_worker_status(worker_id, date)
            
            # Update the main schedule
            if date not in self.schedule:
                self.schedule[date] = [None] * self.num_shifts
            
            # Fill any gaps in the schedule list
            while len(self.schedule[date]) <= shift_idx:
                self.schedule[date].append(None)
            
            # Set the worker at the specific shift
            self.schedule[date][shift_idx] = worker_id
        
            logging.debug(f"Updated tracking data for worker {worker_id} on {date.strftime('%Y-%m-%d')}, shift {shift_idx}")
        
        except Exception as e:
            logging.error(f"Error updating tracking data for worker {worker_id}: {str(e)}", exc_info=True)
    
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
                    logging.debug(f"Sample date {date.strftime('%Y-%m-%d')}: {self.schedule[date]}")
        
            # Calculate percentage
            if total_shifts > 0:
                return (filled_shifts / total_shifts) * 100
            return 0
        except Exception as e:
            logging.error(f"Error calculating coverage: {str(e)}", exc_info=True)
            return 0

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
        
    def generate_schedule(self, num_attempts=60, allow_feedback_improvement=True, improvement_attempts=20):
        """
        Generate the complete schedule using a multi-phase approach to maximize shift coverage
    
        Args:
            num_attempts: Number of initial attempts to generate a schedule
            allow_feedback_improvement: Whether to allow feedback-based improvement
            improvement_attempts: Number of attempts to improve the best schedule
        """
        logging.info("=== Starting schedule generation with multi-phase approach ===")
        
        try:
            # Ensure data structures are consistent before we start
            self._ensure_data_integrity()
        
            # Prepare worker data - set defaults for empty fields
            self._prepare_worker_data()
            
            # PHASE 1: Generate multiple initial schedules and select the best one
            best_schedule = None
            best_coverage = 0
            best_post_rotation = 0
            best_worker_assignments = None
            best_worker_posts = None
            best_worker_weekdays = None
            best_worker_weekends = None
            best_constraint_skips = None
            coverage_stats = []
            
            # Initialize relax_level variable for use in coverage stats
            relax_level = 0

            # Start with fewer attempts, but make each one more effective
            for attempt in range(num_attempts):
                # Set a seed based on the attempt number to ensure different runs
                random.seed(attempt + 1)
        
                logging.info(f"Attempt {attempt + 1} of {num_attempts}")
                self._reset_schedule()
            
                # Calculate target shifts for workers
                self._calculate_target_shifts()
                self._calculate_monthly_targets()
        
                # Choose direction: Forward-only for odd attempts, backward-only for even attempts
                forward = attempt % 2 == 0
                logging.info(f"Direction: {'forward' if forward else 'backward'}")
        
                # STEP 1: Process mandatory assignments first (always!)
                logging.info("Processing mandatory guards...")
                self.schedule_builder._assign_mandatory_guards()
        
                # STEP 2: Process weekend and holiday assignments next (they're harder to fill)
                self.schedule_builder._assign_priority_days(forward)
            
                # STEP 3: Process the remaining days
                dates_to_process = self.schedule_builder._get_remaining_dates_to_process(forward)
                for date in dates_to_process:
                    # Use strict constraints for first half of attempts, then progressively relax
                    relax_level = min(2, attempt // (num_attempts // 3))
                    self.schedule_builder._assign_day_shifts_with_relaxation(date, attempt, relax_level)
        
                # Clean up and validate
                self._cleanup_schedule()
                try:
                    self._validate_final_schedule()
                except SchedulerError as e:
                    logging.warning(f"Schedule validation found issues: {str(e)}")
        
                # Calculate coverage
                total_shifts = (self.end_date - self.start_date + timedelta(days=1)).days * self.num_shifts
                filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
                coverage = (filled_shifts / total_shifts * 100) if total_shifts > 0 else 0
        
                # Calculate post rotation scores
                post_rotation_stats = self._calculate_post_rotation()
        
                # Log detailed coverage information
                logging.info(f"Attempt {attempt + 1} coverage: {coverage:.2f}%")
                logging.info(f"Post rotation score: {post_rotation_stats['overall_score']:.2f}%")
        
                # Store coverage stats for summary
                coverage_stats.append({
                    'attempt': attempt + 1,
                    'coverage': coverage,
                    'post_rotation_score': post_rotation_stats['overall_score'],
                    'unfilled_shifts': total_shifts - filled_shifts,
                    'direction': 'forward' if forward else 'backward',
                    'relax_level': relax_level
                })

                # Check if this is the best schedule so far
                # Prioritize coverage first, then post rotation
                if coverage > best_coverage or (coverage == best_coverage and 
                                              post_rotation_stats['overall_score'] > best_post_rotation):
                    best_coverage = coverage
                    best_post_rotation = post_rotation_stats['overall_score']
                    best_schedule = self.schedule.copy()
                    best_worker_assignments = {w_id: assignments.copy() 
                                             for w_id, assignments in self.worker_assignments.items()}
                    best_worker_posts = {w_id: posts.copy() 
                                       for w_id, posts in self.worker_posts.items()}
                    best_worker_weekdays = {w_id: weekdays.copy() 
                                          for w_id, weekdays in self.worker_weekdays.items()}
                    best_worker_weekends = {w_id: weekends.copy() 
                                          for w_id, weekends in self.worker_weekends.items()}
                    best_constraint_skips = {
                        w_id: {
                            'gap': skips['gap'].copy(),
                            'incompatibility': skips['incompatibility'].copy(),
                            'reduced_gap': skips['reduced_gap'].copy(),
                        }
                        for w_id, skips in self.constraint_skips.items()
                    }
                
                    # Early stopping if we get 100% coverage
                    if coverage >= 99.9:
                        logging.info("Found a perfect schedule! Stopping early.")
                        break

            # Use the best schedule found
            if best_schedule:
                self.schedule = best_schedule
                self.worker_assignments = best_worker_assignments
                self.worker_posts = best_worker_posts
                self.worker_weekdays = best_worker_weekdays
                self.worker_weekends = best_worker_weekends
                self.constraint_skips = best_constraint_skips

                else:
                    # If we couldn't generate any schedule, try the simple approach
                    logging.warning("Standard scheduling methods failed, falling back to simple assignment")
                    success = self.schedule_builder._assign_workers_simple()
                    if success:
                        logging.info("Simple assignment created a basic schedule")
                    else:
                        logging.error("Failed to generate any valid schedule")
                        raise SchedulerError("Failed to generate a valid schedule")
    
                # Log summary statistics
                logging.info("=== Initial Coverage Statistics Summary ===")
                for stats in coverage_stats:
                    logging.info(f"[Attempt {stats['attempt']:2d}] Coverage={stats['coverage']:.2f}%, "
                                f"Post Rotation={stats['post_rotation_score']:.2f}%, "
                                f"Unfilled={stats['unfilled_shifts']}, "
                                f"Direction={stats['direction']}, "
                                f"RelaxLevel={stats['relax_level']}")
    
                # PHASE 2: Targeted improvement for the best schedule
                if allow_feedback_improvement and best_coverage < 99.9:
                    logging.info("\n=== Starting targeted improvement phase ===")
            
                    # Save the current best metrics
                    initial_best_coverage = best_coverage
                    initial_best_post_rotation = best_post_rotation
                
                    # Initialize improvement tracker
                    improvements_made = 0
            
                    # Try to improve the schedule through targeted modifications
                    for i in range(improvement_attempts):
                        logging.info(f"Improvement attempt {i+1}/{improvement_attempts}")
                
                        # Create a copy of the best schedule to work with
                        self.schedule_builder._backup_best_schedule()
                
                        # Try different improvement strategies based on attempt number
                        if i % 5 == 0:
                            logging.info("Strategy: Fix incompatibility violations")
                            self.schedule_builder._fix_incompatibility_violations()
                        elif i % 5 == 1:
                            logging.info("Strategy: Fill empty shifts")
                            self.schedule_builder._try_fill_empty_shifts()
                        elif i % 5 == 2:
                            logging.info("Strategy: Balance workloads")
                            self.schedule_builder._balance_workloads()
                        elif i % 5 == 3:
                            logging.info("Strategy: Improve post rotation")
                            self.schedule_builder._improve_post_rotation()
                        elif i % 5 == 4:
                            logging.info("Strategy: Improve weekend distribution")
                            self.schedule_builder._improve_weekend_distribution()
                
                         # Validate the schedule
                        logging.info("Validating final schedule...")
                        if not self._validate_final_schedule():
                            logging.warning("Schedule validation failed after improvement attempt")
                            # If validation fails, restore previous best
                            self.schedule_builder._restore_best_schedule()
                            continue
                
                        # Calculate new metrics
                        post_rotation_stats = self._calculate_post_rotation()
                        coverage = self._calculate_coverage()
                
                        logging.info(f"[Post rotation overall score] {post_rotation_stats['overall_score']:.2f}%")
                        logging.info(f"[Post uniformity] {post_rotation_stats['uniformity']:.2f}%, Avg worker score: {post_rotation_stats['avg_worker']:.2f}%")
                        logging.info(f"[Improved coverage] {coverage:.2f}%, Post Rotation: {post_rotation_stats['overall_score']:.2f}%")
                
                        # If this attempt improved either coverage or post rotation, keep it
                        if coverage > best_coverage + 0.5 or (coverage > best_coverage and post_rotation_stats['overall_score'] >= best_post_rotation - 1):
                            best_coverage = coverage
                            best_post_rotation = post_rotation_stats['overall_score']
                            self.schedule_builder._save_current_as_best()
                            improvements_made += 1
                            logging.info(f"Improvement accepted! New best coverage: {best_coverage:.2f}%")
                        else:
                            # Restore the previous best schedule
                            self.schedule_builder._restore_best_schedule()
                
                    # Final report on improvements
                    if best_coverage > initial_best_coverage or best_post_rotation > initial_best_post_rotation:
                        improvement = f"Coverage improved by {best_coverage - initial_best_coverage:.2f}%, " \
                                    f"Post Rotation improved by {best_post_rotation - initial_best_post_rotation:.2f}%"
                        logging.info(f"=== Schedule successfully improved! ===")
                        logging.info(improvement)
                    else:
                        logging.info("=== No improvements found over initial schedule ===")
                else:
                    logging.error("Failed to generate any valid schedule")
                    raise SchedulerError("Failed to generate a valid schedule")

                # Ensure we use the best schedule found during improvements
                if best_coverage > 0 and hasattr(self, 'backup_schedule'):
                    self.schedule = self.backup_schedule.copy()
                    self.worker_assignments = {w_id: assignments.copy() 
                                          for w_id, assignments in self.backup_worker_assignments.items()}
                    self.worker_posts = {w_id: posts.copy() 
                                      for w_id, posts in self.backup_worker_posts.items()}
                    self.worker_weekdays = {w_id: weekdays.copy() 
                                         for w_id, weekdays in self.backup_worker_weekdays.items()}
                    self.worker_weekends = {w_id: weekends.copy() 
                                         for w_id, weekends in self.backup_worker_weekends.items()}
                    self.constraint_skips = {
                        w_id: {
                            'gap': skips['gap'].copy(),
                            'incompatibility': skips['incompatibility'].copy(),
                            'reduced_gap': skips['reduced_gap'].copy(),
                        }
                        for w_id, skips in self.backup_constraint_skips.items()
                    }
                    logging.info("Restored best schedule found during improvements")

                # Before calculating final stats, make sure we're using the most up-to-date schedule
                if hasattr(self, 'backup_schedule') and self.backup_schedule:
                    self.schedule = self.backup_schedule.copy()
                    logging.info("Final update of schedule from backup before stats")
                else:
                    logging.warning("No backup schedule found for final update")

                # Extra check to see if schedule is actually filled
                filled_count = 0
                for date, shifts in self.schedule.items():
                    for worker in shifts:
                        if worker is not None:
                            filled_count += 1
                logging.info(f"Final schedule check: {filled_count} filled shifts before final calculation")

                # Final stats
                total_shifts = sum(len(shifts) for shifts in self.schedule.values())
                filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
                logging.info(f"Final schedule coverage: {(filled_shifts / total_shifts * 100 if total_shifts > 0 else 0):.2f}% "
                            f"({filled_shifts}/{total_shifts} shifts filled)")

                return True
        
        except Exception as e:
            logging.error(f"Failed to generate schedule: {str(e)}", exc_info=True)
            raise SchedulerError(f"Failed to generate schedule: {str(e)}")

    def _validate_final_schedule(self):
        """
        Validate the final schedule to ensure all constraints are met
    
        Raises:
            SchedulerError: If validation fails
        """
        logging.info("Validating final schedule...")
    
        # Track validation issues
        validation_issues = []
    
        # Check 1: Ensure all dates have the correct number of shifts
        for date in self._get_date_range(self.start_date, self.end_date):
            if date not in self.schedule:
                validation_issues.append(f"Date {date.strftime('%Y-%m-%d')} is missing from the schedule")
            elif len(self.schedule[date]) != self.num_shifts:
                validation_issues.append(
                    f"Date {date.strftime('%Y-%m-%d')} has {len(self.schedule[date])} shifts, expected {self.num_shifts}"
                )
    
        # Check 2: Ensure no worker is assigned to multiple shifts on the same day
        for date, shifts in self.schedule.items():
            worker_counts = {}
            for worker_id in shifts:
                if worker_id is not None:
                    worker_counts[worker_id] = worker_counts.get(worker_id, 0) + 1
                    if worker_counts[worker_id] > 1:
                        validation_issues.append(
                            f"Worker {worker_id} is assigned to multiple shifts on {date.strftime('%Y-%m-%d')}"
                        )
    
        # Check 3: Ensure workers have at least minimum gap between shifts
        for worker in self.workers_data:
            worker_id = worker['id']
            dates = sorted(list(self.worker_assignments.get(worker_id, [])))
        
            for i in range(len(dates) - 1):
                gap = (dates[i+1] - dates[i]).days
                if gap < 2:  # Minimum gap is 2 days
                    validation_issues.append(
                        f"Worker {worker_id} has insufficient gap ({gap} days) between shifts on "
                        f"{dates[i].strftime('%Y-%m-%d')} and {dates[i+1].strftime('%Y-%m-%d')}"
                    )
    
        # Check 4: Ensure all mandatory shifts are assigned
        for worker in self.workers_data:
            worker_id = worker['id']
            mandatory_days = worker.get('mandatory_days', '')
            if mandatory_days:
                mandatory_dates = self.date_utils.parse_dates(mandatory_days)
                relevant_mandatory_dates = [
                    d for d in mandatory_dates if self.start_date <= d <= self.end_date
                ]
            
                for date in relevant_mandatory_dates:
                    if date not in self.worker_assignments.get(worker_id, []):
                        validation_issues.append(
                            f"Worker {worker_id} is not assigned to mandatory date {date.strftime('%Y-%m-%d')}"
                        )
    
        # Check 5: Ensure no worker is assigned on their days off
        for worker in self.workers_data:
            worker_id = worker['id']
            days_off = worker.get('days_off', '')
            if days_off:
                day_ranges = self.date_utils.parse_date_ranges(days_off)
                for start_date, end_date in day_ranges:
                    current = start_date
                    while current <= end_date:
                        if current in self.worker_assignments.get(worker_id, []):
                            validation_issues.append(
                                f"Worker {worker_id} is assigned on day off {current.strftime('%Y-%m-%d')}"
                            )
                        current += timedelta(days=1)
    
        # Check 6: Ensure tracking data is consistent with schedule
        for worker_id, assignment_dates in self.worker_assignments.items():
            for date in assignment_dates:
                if date not in self.schedule or worker_id not in self.schedule[date]:
                    validation_issues.append(
                        f"Tracking inconsistency: Worker {worker_id} is tracked for {date.strftime('%Y-%m-%d')} "
                        f"but not in schedule"
                    )
    
        # Check for reverse consistency
        for date, shifts in self.schedule.items():
            for worker_id in [w for w in shifts if w is not None]:
                if date not in self.worker_assignments.get(worker_id, []):
                    validation_issues.append(
                        f"Tracking inconsistency: Worker {worker_id} is in schedule for {date.strftime('%Y-%m-%d')} "
                        f"but not in tracking"
                    )

        # Direct update of schedule from backup after improvements
        if hasattr(self, 'backup_schedule') and self.backup_schedule:
            self.schedule = self.backup_schedule.copy()
            logging.info("Explicitly updated schedule from backup after improvements")
    
        # Report validation issues
        if validation_issues:
            # Log at most 10 issues to keep logs manageable
            for issue in validation_issues[:10]:
                logging.error(f"Validation issue: {issue}")
        
            if len(validation_issues) > 10:
                logging.error(f"... and {len(validation_issues) - 10} more issues")
        
            raise SchedulerError(f"Schedule validation failed with {len(validation_issues)} issues")
    
        logging.info("Schedule validation successful!")
        return True

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

    def _restore_best_schedule(self):
        """Restore from backup of the best schedule"""
        if not hasattr(self, 'backup_schedule'):
            logging.warning("No backup schedule to restore")
            return False
        
        self.schedule = self.backup_schedule.copy()
        self.worker_assignments = {w_id: assignments.copy() for w_id, assignments in self.backup_worker_assignments.items()}
        self.worker_posts = {w_id: posts.copy() for w_id, posts in self.backup_worker_posts.items()}
        self.worker_weekdays = {w_id: weekdays.copy() for w_id, weekdays in self.backup_worker_weekdays.items()}
        self.worker_weekends = {w_id: weekends.copy() for w_id, weekends in self.backup_worker_weekends.items()}
        self.constraint_skips = {
            w_id: {
                'gap': skips['gap'].copy(),
                'incompatibility': skips['incompatibility'].copy(),
                'reduced_gap': skips['reduced_gap'].copy(),
            }
            for w_id, skips in self.backup_constraint_skips.items()
        }
        return True
        
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
