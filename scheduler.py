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

    def _assign_workers_simple(self):
        """
        Simple method to directly assign workers to shifts based on targets and ensuring
        all constraints are properly respected:
        - Minimum 2 days off between shifts (3+ days between assignments)
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
                
                    # IMPORTANT: Check for minimum gap of 2 days off (3+ days between assignments)
                    too_close = False
                    for assigned_date in self.worker_assignments.get(worker_id, set()):
                        days_difference = abs((date - assigned_date).days)
                    
                        # We need at least 2 days off, so 3+ days between assignments
                        if days_difference < 3:  # THIS IS THE KEY CHANGE
                            too_close = True
                            break
                    
                        # Special case: Friday-Monday (needs 3 days off, so 4+ days between)
                        # This is handled by the general case above (< 3), but keeping for clarity
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
                    logging.info(f"Assigned worker {worker_id} to {date.strftime('%Y-%m-%d')}, post {post}")
                else:
                    # No suitable worker found, leave unassigned
                    if date not in self.schedule:
                        self.schedule[date] = []
                
                    while len(self.schedule[date]) <= post:
                        self.schedule[date].append(None)
                    
                    logging.debug(f"No suitable worker found for {date.strftime('%Y-%m-%d')}, post {post}")
    
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
                    
                        # Check for insufficient rest periods (less than 2 days)
                        if 0 < days_between < 2:
                            violations.append({
                                'type': 'min_rest_days',
                                'worker_id': worker_id,
                                'date1': date1,
                                'date2': date2,
                                'days_between': days_between,
                                'min_required': 2
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
        - Minimum 2 days between shifts in general
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
            
                # Basic minimum gap check (2 days)
                if days_difference < 2:
                    logging.debug(f"Worker {worker_id} cannot be assigned on {date} due to insufficient rest (needs at least 2 days)")
                    return False
                
                # Special case: Friday-Monday check (requires 3 days gap)
                # Check if either date is Friday and the other is Monday
                if days_difference == 3:
                    # Check if one date is Friday (weekday 4) and the other is Monday (weekday 0)
                    if ((assigned_date.weekday() == 4 and date.weekday() == 0) or 
                        (assigned_date.weekday() == 0 and date.weekday() == 4)):
                        logging.debug(f"Worker {worker_id} cannot be assigned Friday-Monday (needs at least 3 days gap)")
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
            
    def generate_schedule(self, num_attempts=60, allow_feedback_improvement=True, improvement_attempts=30):
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
    
                    # Try mixed strategy first
                    if best_coverage == 0:
                        logging.warning("Zero coverage detected, using mixed assignment strategies")
                        success = self._assign_mixed_strategy()
                        if success:
                            # Recalculate coverage
                            coverage = self._calculate_coverage() 
                            post_rotation_stats = self._calculate_post_rotation()
                            best_coverage = coverage
                            best_post_rotation = post_rotation_stats['overall_score']
                            logging.info(f"Mixed strategy assignment resulted in coverage: {coverage:.2f}%")
                
                            # Create a backup of this successful assignment
                            self._backup_best_schedule()
                            best_schedule = self.schedule.copy()
                            best_worker_assignments = {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()}
                            best_worker_posts = {w_id: posts.copy() for w_id, posts in self.worker_posts.items()}    
                            best_worker_weekdays = {w_id: weekdays.copy() for w_id, weekdays in self.worker_weekdays.items()}
                            best_worker_weekends = {w_id: weekends.copy() for w_id, weekends in self.worker_weekends.items()}
                            best_constraint_skips = {
                                w_id: {
                                    'gap': skips['gap'].copy() if 'gap' in skips else [],
                                    'incompatibility': skips['incompatibility'].copy() if 'incompatibility' in skips else [],
                                    'reduced_gap': skips['reduced_gap'].copy() if 'reduced_gap' in skips else []
                                }
                                for w_id, skips in self.constraint_skips.items()
                            }
            
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

                    # Save the current best metrics
                    initial_best_coverage = best_coverage
                    initial_best_post_rotation = best_post_rotation

                    # Final report on improvements
                    if best_coverage > initial_best_coverage or best_post_rotation > initial_best_post_rotation:
                        improvement = f"Coverage improved by {best_coverage - initial_best_coverage:.2f}%, " \
                                    f"Post Rotation improved by {best_post_rotation - initial_best_post_rotation:.2f}%"
                        logging.info(f"=== Schedule successfully improved! ===")
                        logging.info(improvement)
                    else:
                        logging.info("=== No improvements found over initial schedule ===")
                else:
                    # If no valid schedule was found, try a simple direct assignment approach
                    logging.warning("Standard scheduling failed. Trying simple direct assignment.")
                    self._assign_workers_simple()
                    coverage = self._calculate_coverage()
                    if coverage > 0:
                        logging.info(f"Simple assignment created a basic schedule with {coverage:.2f}% coverage.")

                # Ensure we use the best schedule found during improvements
                if best_coverage > 0:
                    try:
                        # First ensure all required backup attributes exist
                        if not hasattr(self, 'backup_schedule'):
                            self.backup_schedule = {}
                            for date, shifts in self.schedule.items():
                                self.backup_schedule[date] = shifts.copy() if shifts else []
        
                        if not hasattr(self, 'backup_worker_assignments'):
                            self.backup_worker_assignments = {
                                w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()
                            }
        
                        if not hasattr(self, 'backup_worker_posts'):
                            self.backup_worker_posts = {
                                w_id: set() if w_id not in self.worker_posts else self.worker_posts[w_id].copy() 
                                for w_id in self.worker_assignments
                            }
        
                        if not hasattr(self, 'backup_worker_weekdays'):
                            self.backup_worker_weekdays = {
                                w_id: {} if w_id not in self.worker_weekdays else self.worker_weekdays[w_id].copy()
                                for w_id in self.worker_assignments
                            }
        
                        if not hasattr(self, 'backup_worker_weekends'):
                            self.backup_worker_weekends = {
                                w_id: [] if w_id not in self.worker_weekends else self.worker_weekends[w_id].copy()
                                for w_id in self.worker_assignments
                            }
        
                        # Now do the actual assignment from backups
                        self.schedule = {date: shifts.copy() for date, shifts in self.backup_schedule.items()}
                        self.worker_assignments = {
                            w_id: assignments.copy() for w_id, assignments in self.backup_worker_assignments.items()
                        }
                        self.worker_posts = {
                            w_id: posts.copy() for w_id, posts in self.backup_worker_posts.items()
                        }
                        self.worker_weekdays = {
                            w_id: weekdays.copy() for w_id, weekdays in self.backup_worker_weekdays.items()
                        }
                        self.worker_weekends = {
                            w_id: weekends.copy() for w_id, weekends in self.backup_worker_weekends.items()
                        }
        
                        logging.info("Restored best schedule found during improvements")
                    except Exception as e:
                        logging.error(f"Error restoring from backup: {str(e)}")
                        # Fallback: just use simple assignment if restoration fails
                        self._assign_workers_simple()

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

                # Before final stats, make sure we have assignments
                filled_count = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
                logging.info(f"Final schedule check: {filled_count} filled shifts before final calculation")

                # If we lost our assignments but have a backup with assignments, use it
                if filled_count == 0 and hasattr(self, 'backup_schedule'):
                    backup_filled = sum(1 for shifts in self.backup_schedule.values() for worker in shifts if worker is not None)
                    if backup_filled > 0:
                        logging.info(f"Using backup schedule which has {backup_filled} filled shifts")
                        self.schedule = {date: shifts.copy() for date, shifts in self.backup_schedule.items()}
                        # Update filled_count
                        filled_count = backup_filled

                # If we still don't have assignments, retry with simple assignment
                if filled_count == 0:
                    logging.warning("Schedule is empty. Running simple assignment as last resort.")
                    self._assign_workers_simple()

                # Ensure we have a valid schedule with assignments
                try:
                    # Check if our schedule got wiped out
                    filled_count = sum(1 for date in self.schedule for shift in self.schedule[date] if shift is not None)
                    logging.info(f"Final check: Schedule has {filled_count} filled shifts")
    
                    if filled_count == 0:
                        logging.warning("Schedule is empty at end of processing, running simple assignment")
                        self._assign_workers_simple()
    
                    # Final coverage stats
                    total_shifts = sum(len(shifts) for shifts in self.schedule.values())
                    filled_shifts = sum(1 for date in self.schedule for shift in self.schedule[date] if shift is not None)
                    coverage = (filled_shifts / total_shifts * 100) if total_shifts > 0 else 0
                    logging.info(f"Final schedule coverage: {coverage:.2f}% ({filled_shifts}/{total_shifts} shifts filled)")
                    
                    # Final incompatibility verification check
                    if filled_count > 0:
                        self.schedule_builder._verify_no_incompatibilities()
    
                    return True
                except Exception as e:
                    logging.error(f"Error in final schedule check: {str(e)}", exc_info=True)
                    # Run simple assignment as a last resort
                    try:
                        logging.info("Attempting emergency simple assignment")
                        self._assign_workers_simple()
                        return True
                    except Exception as e2:
                        logging.error(f"Emergency simple assignment failed: {str(e2)}", exc_info=True)
                        return False

                # Final report on improvements
                if best_coverage > initial_best_coverage or best_post_rotation > initial_best_post_rotation:
                    improvement = f"Coverage improved by {best_coverage - initial_best_coverage:.2f}%, " \
                                f"Post Rotation improved by {best_post_rotation - initial_best_post_rotation:.2f}%"
                    logging.info(f"=== Schedule successfully improved! ===")
                    logging.info(improvement)
                else:
                    logging.info("=== No improvements found over initial schedule ===")
                        
                # If we encountered an error but had a working schedule, restore it
                if best_coverage > 0:
                    self.schedule = best_schedule
                    self.worker_assignments = best_worker_assignments
                    self.worker_posts = best_worker_posts
                    self.worker_weekdays = best_worker_weekdays
                    self.worker_weekends = best_worker_weekends
                    # Don't try to restore constraint_skips here since it might be the source of the error

            # Final validation to ensure all constraints are met
            self.validate_and_fix_final_schedule()
        
        except Exception as e:
            logging.error(f"Failed to generate schedule: {str(e)}", exc_info=True)
            raise SchedulerError(f"Failed to generate schedule: {str(e)}")

    def validate_and_fix_final_schedule(self):
        """
        Final validator that scans the entire schedule and fixes any constraint violations.
        Returns the number of fixes made.
        """
        logging.info("Running final schedule validation...")
    
        # Count issues
        incompatibility_issues = 0
        gap_issues = 0
        fixes_made = 0
    
        # 1. Check for incompatibilities
        for date in sorted(self.schedule.keys()):
            # Get non-None workers assigned to this date
            workers_assigned = [w for w in self.schedule[date] if w is not None]
        
            # Check each pair of workers for incompatibility
            for i, worker1_id in enumerate(workers_assigned):
                for worker2_id in workers_assigned[i+1:]:
                    # Get worker data
                    worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
                    worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
                
                    if not worker1 or not worker2:
                        continue
                
                    # Check incompatibility lists from both sides
                    is_incompatible = False
                    if 'incompatible_with' in worker1 and worker2_id in worker1['incompatible_with']:
                        is_incompatible = True
                    if 'incompatible_with' in worker2 and worker1_id in worker2['incompatible_with']:
                        is_incompatible = True
                
                    if is_incompatible:
                        incompatibility_issues += 1
                        logging.warning(f"VALIDATION: Found incompatible workers {worker1_id} and {worker2_id} on {date}")
                    
                        # Remove one of the workers (preferably one with more assignments)
                        w1_count = len(self.worker_assignments.get(worker1_id, set()))
                        w2_count = len(self.worker_assignments.get(worker2_id, set()))
                    
                        worker_to_remove = worker1_id if w1_count >= w2_count else worker2_id
                        post = self.schedule[date].index(worker_to_remove)
                    
                        # Remove the worker
                        self.schedule[date][post] = None
                        if worker_to_remove in self.worker_assignments:
                            self.worker_assignments[worker_to_remove].remove(date)
                    
                        fixes_made += 1
                        logging.warning(f"VALIDATION: Removed worker {worker_to_remove} from {date} to fix incompatibility")
    
        # 2. Check for minimum gap violations
        for worker_id in self.worker_assignments:
            # Get all dates this worker is assigned to
            assignments = sorted(list(self.worker_assignments[worker_id]))
        
            # Check pairs of dates for gap violations
            for i in range(len(assignments) - 1):
                for j in range(i + 1, len(assignments)):
                    date1 = assignments[i]
                    date2 = assignments[j]
                    days_between = abs((date2 - date1).days)
                
                    # Check for minimum 3-day gap (2 days off between shifts)
                    if days_between < 3:
                        gap_issues += 1
                        logging.warning(f"VALIDATION: Found gap violation for worker {worker_id}: only {days_between} days between {date1} and {date2}")
                    
                        # Remove the later assignment
                        post = self.schedule[date2].index(worker_id) if worker_id in self.schedule[date2] else -1
                        if post >= 0:
                            self.schedule[date2][post] = None
                            self.worker_assignments[worker_id].remove(date2)
                        
                            fixes_made += 1
                            logging.warning(f"VALIDATION: Removed worker {worker_id} from {date2} to fix gap violation")
    
        logging.info(f"Final validation complete: Found {incompatibility_issues} incompatibility issues and {gap_issues} gap issues. Made {fixes_made} fixes.")
        return fixes_made

    def _validate_final_schedule(self):
        """
        Validate the final schedule before returning it.
        Returns True if valid, False if issues found.
        """
        try:
            validation_issues = []
        
            # Attempt to reconcile tracking first
            self._reconcile_schedule_tracking()
        
            # Now validate the reconciled schedule
            # Check for tracking consistency
            for worker_id, assignments in self.worker_assignments.items():
                for date in assignments:
                    # Check if this date exists in schedule
                    if date not in self.schedule:
                        validation_issues.append(f"Tracking inconsistency: Worker {worker_id} is tracked for {date} but date not in schedule")
                        continue
                    
                    # Check if worker is actually assigned on this date
                    worker_found = False
                    for shift_idx, shift_worker in enumerate(self.schedule[date]):
                        if shift_worker == worker_id:
                            worker_found = True
                            break
                        
                    if not worker_found:
                        validation_issues.append(f"Tracking inconsistency: Worker {worker_id} is tracked for {date} but not in schedule")
        
            # Check for untracked assignments
            for date, shifts in self.schedule.items():
                for shift_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        # Ensure worker tracking exists
                        if worker_id not in self.worker_assignments:
                            validation_issues.append(f"Missing tracking: Worker {worker_id} is in schedule on {date} but not tracked")
                        # Check if this date is in the worker's tracking
                        elif date not in self.worker_assignments[worker_id]:
                            validation_issues.append(f"Missing tracking: Worker {worker_id} is scheduled on {date} but date not tracked")
        
            # Report validation issues
            if validation_issues:
                # Log first 10 issues in detail
                for i, issue in enumerate(validation_issues[:10]):
                    logging.error(f"[Validation issue] {issue}")
                
                if len(validation_issues) > 10:
                    logging.error(f"... and {len(validation_issues) - 10} more issues")
                
                # Instead of failing, fix the issues automatically
                logging.warning(f"Found {len(validation_issues)} validation issues - attempting automatic fix")
                if self._reconcile_schedule_tracking():
                    logging.info("Validation issues fixed automatically")
                    return True
                else:
                    # Only fail if we couldn't fix the issues
                    raise SchedulerError(f"Schedule validation failed with {len(validation_issues)} issues that couldn't be fixed")
                
                # Add call to the new validation method
                fixes_made = self.validate_and_fix_final_schedule()
        
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
