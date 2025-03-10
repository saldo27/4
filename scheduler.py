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
                self._assign_mandatory_guards()
        
                # STEP 2: Process weekend and holiday assignments next (they're harder to fill)
                self._assign_priority_days(forward)
            
                # STEP 3: Process the remaining days
                dates_to_process = self._get_remaining_dates_to_process(forward)
                for date in dates_to_process:
                    # Use strict constraints for first half of attempts, then progressively relax
                    relax_level = min(2, attempt // (num_attempts // 3))
                    self._assign_day_shifts_with_relaxation(date, attempt, relax_level)
        
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
                post_rotation_stats = self._calculate_post_rotation_coverage()
        
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
            
                    # Save the current best metrics
                    initial_best_coverage = best_coverage
                    initial_best_post_rotation = best_post_rotation
                
                    # Initialize improvement tracker
                    improvements_made = 0
            
                    # Try to improve the schedule through targeted modifications
                    for i in range(improvement_attempts):
                        logging.info(f"Improvement attempt {i+1}/{improvement_attempts}")
                
                        # Create a copy of the best schedule to work with
                        self._backup_best_schedule()
                
                        # Try different improvement strategies based on attempt number
                        if i % 5 == 0:
                            logging.info("Strategy: Fix incompatibility violations")
                            self._fix_incompatibility_violations()
                        elif i % 5 == 1:
                            logging.info("Strategy: Fill empty shifts")
                            self._try_fill_empty_shifts()
                        elif i % 5 == 2:
                            logging.info("Strategy: Balance workloads")
                            self._balance_workloads()
                        elif i % 5 == 3:
                            logging.info("Strategy: Improve post rotation")
                            self._improve_post_rotation()
                        elif i % 5 == 4:
                            logging.info("Strategy: Improve weekend distribution")
                            self._improve_weekend_distribution()
                
                        # Validate the new schedule
                        try:
                            self._validate_final_schedule()
                        except SchedulerError as e:
                            logging.warning(f"Improvement validation found issues: {str(e)}")
                            self._restore_best_schedule()
                            continue
                
                        # Calculate coverage
                        total_shifts = (self.end_date - self.start_date + timedelta(days=1)).days * self.num_shifts
                        filled_shifts = sum(1 for shifts in self.schedule.values() 
                                         for worker in shifts if worker is not None)
                        coverage = (filled_shifts / total_shifts * 100) if total_shifts > 0 else 0
                
                        # Calculate post rotation scores
                        post_rotation_stats = self._calculate_post_rotation_coverage()
                
                        logging.info(f"Improved coverage: {coverage:.2f}%, "
                                    f"Post Rotation: {post_rotation_stats['overall_score']:.2f}%")
                
                        # Check if this improvement is better than the best so far
                        # Prioritize coverage improvements first, then post rotation
                        if coverage > best_coverage + 0.5 or coverage > best_coverage and post_rotation_stats['overall_score'] >= best_post_rotation - 1:
                            best_coverage = coverage
                            best_post_rotation = post_rotation_stats['overall_score']
                            self._save_current_as_best()
                            improvements_made += 1
                            logging.info(f"Improvement accepted! New best coverage: {best_coverage:.2f}%")
                        else:
                            # Restore the previous best schedule
                            self._restore_best_schedule()
            
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

            # Final stats
            total_shifts = sum(len(shifts) for shifts in self.schedule.values())
            filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
            logging.info(f"Final schedule coverage: {(filled_shifts / total_shifts * 100 if total_shifts > 0 else 0):.2f}% "
                        f"({filled_shifts}/{total_shifts} shifts filled)")

            return self.schedule
    
        except Exception as e:
            logging.error(f"Schedule generation error: {str(e)}", exc_info=True)
            raise SchedulerError(f"Failed to generate schedule: {str(e)}")
        
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
