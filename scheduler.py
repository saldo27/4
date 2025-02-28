from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import calendar
import logging
import sys
import requests
import random
from worker_eligibility import WorkerEligibilityTracker

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
            self.worker_assignments = {w['id']: set() for w in self.workers_data}
            self.worker_posts = {w['id']: set() for w in self.workers_data}
            self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data}

            # Initialize worker targets
            for worker in self.workers_data:
                if 'target_shifts' not in worker:
                    worker['target_shifts'] = 0

            # Calculate targets before proceeding
            self._calculate_target_shifts()
        
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

            # Add max_shifts_per_worker calculation
            total_days = (self.end_date - self.start_date).days + 1
            total_shifts = total_days * self.num_shifts
            num_workers = len(self.workers_data)
            self.max_shifts_per_worker = (total_shifts // num_workers) + 2  # Add some flexibility

            # Add eligibility tracker
            self.eligibility_tracker = WorkerEligibilityTracker(
                self.workers_data,
                self.holidays
            )

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

    def generate_schedule(self, num_attempts=40, allow_feedback_improvement=True, improvement_attempts=20):
        """
        Generate the complete schedule with possibility of unfilled shifts
    
        Args:
            num_attempts: Number of initial attempts to generate a schedule
            allow_feedback_improvement: Whether to allow feedback-based improvement
            improvement_attempts: Number of attempts to improve the best schedule
        """
        logging.info("=== Starting initial schedule generation ===")
        try:
            best_schedule = None
            best_coverage = 0
            best_post_rotation = 0
            best_worker_assignments = None
            best_worker_posts = None
            best_worker_weekdays = None
            best_worker_weekends = None
            best_constraint_skips = None
            coverage_stats = []

            # Initial attempts to generate schedules
            for attempt in range(num_attempts):
                # Set a seed based on the attempt number to ensure different runs
                random.seed(attempt + 1)
            
                logging.info(f"Attempt {attempt + 1} of {num_attempts}")
                self._reset_schedule()
                self._calculate_monthly_targets()
            
                # Choose direction for this attempt: forward or backward
                forward = random.choice([True, False])
                logging.info(f"Direction: {'forward' if forward else 'backward'}")
            
                # Process mandatory assignments first
                logging.info("Processing mandatory guards...")
                self._assign_mandatory_guards()
            
                # Process remaining days - either forward or backward
                dates_to_process = []
                if forward:
                    current_date = self.start_date
                    while current_date <= self.end_date:
                        dates_to_process.append(current_date)
                        current_date += timedelta(days=1)
                else:
                    current_date = self.end_date
                    while current_date >= self.start_date:
                        dates_to_process.append(current_date)
                        current_date -= timedelta(days=1)
                    
                # Process the days
                for date in dates_to_process:
                    self._assign_day_shifts(date, attempt)
            
                self._cleanup_schedule()
                self._validate_final_schedule()
            
                # Calculate coverage
                total_shifts = sum(len(shifts) for shifts in self.schedule.values())
                filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
                coverage = (filled_shifts / total_shifts * 100) if total_shifts > 0 else 0
            
                # Calculate post rotation scores
                post_rotation_stats = self._calculate_post_rotation_coverage()
            
                # Log detailed coverage information
                logging.info(f"Attempt {attempt + 1} coverage: {coverage:.2f}%")
                logging.info(f"Post rotation coverage: {post_rotation_stats['overall_score']:.2f}%")
            
                # Store coverage stats for summary
                coverage_stats.append({
                    'attempt': attempt + 1,
                    'coverage': coverage,
                    'post_rotation_score': post_rotation_stats['overall_score'],
                    'unfilled_shifts': total_shifts - filled_shifts,
                    'direction': 'forward' if forward else 'backward'
                })

                # Check if this is the best schedule so far
                if coverage > best_coverage or (coverage == best_coverage and post_rotation_stats['overall_score'] > best_post_rotation):
                    best_coverage = coverage
                    best_post_rotation = post_rotation_stats['overall_score']
                    best_schedule = self.schedule.copy()
                    best_worker_assignments = {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()}
                    best_worker_posts = {w_id: posts.copy() for w_id, posts in self.worker_posts.items()}
                    best_worker_weekdays = {w_id: weekdays.copy() for w_id, weekdays in self.worker_weekdays.items()}
                    best_worker_weekends = {w_id: weekends.copy() for w_id, weekends in self.worker_weekends.items()}
                    best_constraint_skips = {
                        w_id: {
                            'gap': skips['gap'].copy(),
                            'incompatibility': skips['incompatibility'].copy(),
                            'reduced_gap': skips['reduced_gap'].copy(),
                        }
                        for w_id, skips in self.constraint_skips.items()
                    }

            # Use the best schedule found
            self.schedule = best_schedule
            self.worker_assignments = best_worker_assignments
            self.worker_posts = best_worker_posts
            self.worker_weekdays = best_worker_weekdays
            self.worker_weekends = best_worker_weekends
            self.constraint_skips = best_constraint_skips
        
            logging.info("=== Initial Coverage Statistics Summary ===")
            for stats in coverage_stats:
                logging.info(f"[Attempt {stats['attempt']:2d}   ] Coverage={stats['coverage']:.2f}%, "
                            f"Post Rotation Score={stats['post_rotation_score']:.2f}%, "
                            f"Unfilled Shifts={stats['unfilled_shifts']}, "
                            f"Direction={stats['direction']}")
        
            # Calculate averages
            avg_coverage = sum(stats['coverage'] for stats in coverage_stats) / len(coverage_stats)
            avg_post_rotation = sum(stats['post_rotation_score'] for stats in coverage_stats) / len(coverage_stats)
            avg_unfilled = sum(stats['unfilled_shifts'] for stats in coverage_stats) / len(coverage_stats)
        
            logging.info(f"[Average Coverage] {avg_coverage:.2f}%")
            logging.info(f"[Average Post Rotation Score] {avg_post_rotation:.2f}%")
            logging.info(f"[Average Unfilled Shifts] {avg_unfilled:.2f}")
            logging.info(f"[Best Result] Coverage={best_coverage:.2f}%, Post Rotation Score={best_post_rotation:.2f}%")

            # Ask for permission to try improvement if allowed
            if allow_feedback_improvement:
                logging.info("\n=== Best schedule found. Starting targeted improvement phase... ===")
            
                # Save the current best metrics
                initial_best_coverage = best_coverage
                initial_best_post_rotation = best_post_rotation
            
                # Try to improve the schedule through targeted modifications
                for i in range(improvement_attempts):
                    logging.info(f"Improvement attempt {i+1}/{improvement_attempts}")
                
                    # Create a copy of the best schedule to work with
                    self._backup_best_schedule()
                
                    # Apply targeted improvements
                    self._apply_targeted_improvements(i)
                
                    # Evaluate the new schedule
                    self._validate_final_schedule()
                
                    # Calculate coverage
                    total_shifts = sum(len(shifts) for shifts in self.schedule.values())
                    filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
                    coverage = (filled_shifts / total_shifts * 100) if total_shifts > 0 else 0
                
                    # Calculate post rotation scores
                    post_rotation_stats = self._calculate_post_rotation_coverage()
                
                    logging.info(f"Improved coverage: {coverage:.2f}%, Post Rotation: {post_rotation_stats['overall_score']:.2f}%")
                
                    # Check if this improvement is better than the best so far
                    if coverage > best_coverage or (coverage == best_coverage and post_rotation_stats['overall_score'] > best_post_rotation):
                        best_coverage = coverage
                        best_post_rotation = post_rotation_stats['overall_score']
                        self._save_current_as_best()
                    else:
                        # Restore the previous best schedule
                        self._restore_best_schedule()
            
                if best_coverage > initial_best_coverage or best_post_rotation > initial_best_post_rotation:
                    improvement = f"Coverage improved by {best_coverage - initial_best_coverage:.2f}%, " \
                                 f"Post Rotation improved by {best_post_rotation - initial_best_post_rotation:.2f}%"
                    logging.info(f"=== Schedule successfully improved! ===")
                    logging.info(improvement)
                else:
                    logging.info("=== No improvements found over initial schedule ===")

            if self.schedule is None:
                raise SchedulerError("Failed to generate a valid schedule")

            return self.schedule
        
        except Exception as e:
            logging.error(f"Schedule generation error: {str(e)}")
            raise SchedulerError(f"Failed to generate schedule: {str(e)}")

    def _backup_best_schedule(self):
        """Save a backup of the current best schedule"""
        self.backup_schedule = self.schedule.copy()
        self.backup_worker_assignments = {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()}
        self.backup_worker_posts = {w_id: posts.copy() for w_id, posts in self.worker_posts.items()}
        self.backup_worker_weekdays = {w_id: weekdays.copy() for w_id, weekdays in self.worker_weekdays.items()}
        self.backup_worker_weekends = {w_id: weekends.copy() for w_id, weekends in self.worker_weekends.items()}
        self.backup_constraint_skips = {
            w_id: {
                'gap': skips['gap'].copy(),
                'incompatibility': skips['incompatibility'].copy(),
                'reduced_gap': skips['reduced_gap'].copy(),
            }
            for w_id, skips in self.constraint_skips.items()
        }    

    def _restore_best_schedule(self):
        """Restore from backup of the best schedule"""
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

    def _save_current_as_best(self):
        """Save current schedule as the best"""
        self.backup_schedule = self.schedule.copy()
        self.backup_worker_assignments = {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()}
        self.backup_worker_posts = {w_id: posts.copy() for w_id, posts in self.worker_posts.items()}
        self.backup_worker_weekdays = {w_id: weekdays.copy() for w_id, weekdays in self.worker_weekdays.items()}
        self.backup_worker_weekends = {w_id: weekends.copy() for w_id, weekends in self.worker_weekends.items()}
        self.backup_constraint_skips = {
            w_id: {
                'gap': skips['gap'].copy(),
                'incompatibility': skips['incompatibility'].copy(),
                'reduced_gap': skips['reduced_gap'].copy(),
            }
            for w_id, skips in self.constraint_skips.items()
        }

    def _apply_targeted_improvements(self, attempt_number):
        """
        Apply targeted improvements to the schedule
    
        This method looks for specific issues in the current best schedule
        and tries to fix them through strategic reassignments
        """
        # Set a seed for this improvement attempt
        random.seed(1000 + attempt_number)
    
        # 1. Try to fill empty shifts by relaxing some constraints
        self._try_fill_empty_shifts()
    
        # 2. Try to improve post rotation by swapping assignments
        self._improve_post_rotation()
    
        # 3. Try to improve weekend distribution
        self._improve_weekend_distribution()
    
        # 4. Try to balance workload distribution
        self._balance_workloads()

    def _try_fill_empty_shifts(self):
        """Try to fill empty shifts by relaxing certain constraints"""
        empty_shifts = []
    
        # Find all empty shifts
        for date, workers in self.schedule.items():
            for post, worker in enumerate(workers):
                if worker is None:
                    empty_shifts.append((date, post))
    
        if not empty_shifts:
            logging.info("No empty shifts to fill")
            return
    
        logging.info(f"Attempting to fill {len(empty_shifts)} empty shifts")
    
        # Shuffle the empty shifts to introduce randomness
        random.shuffle(empty_shifts)
    
        # Try to fill each empty shift
        for date, post in empty_shifts:
            filled = False
            candidates = []
        
            # Find possible workers for this shift with relaxed constraints
            for worker in self.workers_data:
                worker_id = worker['id']
            
                # Skip if already assigned to this date
                if worker_id in self.schedule[date]:
                    continue
            
                # Check base eligibility (ignoring some constraints)
                if not self._is_worker_unavailable(worker_id, date):
                    # Allow smaller gaps between assignments
                    assignments = sorted(list(self.worker_assignments[worker_id]))
                    min_gap = 2  # Reduced from 3
                
                    if not assignments or all((date - d).days >= min_gap and (d - date).days >= min_gap for d in assignments):
                        # Calculate a score for this worker
                        score = self._calculate_improvement_score(worker, date, post)
                        if score > float('-inf'):
                            candidates.append((worker, score))
        
            if candidates:
                # Group candidates by score
                candidates_by_score = {}
                for worker, score in candidates:
                    if score not in candidates_by_score:
                        candidates_by_score[score] = []
                    candidates_by_score[score].append(worker)
            
                # Sort scores in descending order
                sorted_scores = sorted(candidates_by_score.keys(), reverse=True)
            
                # Get workers with the highest score
                best_score = sorted_scores[0]
                best_workers = candidates_by_score[best_score]
            
                # Shuffle the best workers
                random.shuffle(best_workers)
                best_worker = best_workers[0]
                worker_id = best_worker['id']
            
                # Assign the worker to this shift
                self.schedule[date][post] = worker_id
                self.worker_assignments[worker_id].add(date)
                self._update_tracking_data(worker_id, date, post)
                filled = True
                logging.info(f"Filled empty shift on {date.strftime('%Y-%m-%d')}, post {post} with worker {worker_id}")
        
            if not filled:
                logging.info(f"Could not fill empty shift on {date.strftime('%Y-%m-%d')}, post {post}")

    def _improve_post_rotation(self):
        """Improve post rotation by swapping assignments"""
        # Find workers with imbalanced post distribution
        imbalanced_workers = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
            post_counts = self._get_post_counts(worker_id)
            total_assignments = sum(post_counts.values())
        
            # Skip workers with no or few assignments
            if total_assignments < self.num_shifts:
                continue
        
            # Calculate expected distribution
            expected_per_post = total_assignments / self.num_shifts
        
            # Calculate deviation from ideal distribution
            deviation = 0
            for post in range(self.num_shifts):
                post_count = post_counts.get(post, 0)
                deviation += abs(post_count - expected_per_post)
        
            # Normalize deviation by total assignments
            normalized_deviation = deviation / total_assignments
        
            # Add to imbalanced list if deviation is significant
            if normalized_deviation > 0.2:  # 20% deviation threshold
                imbalanced_workers.append((worker_id, post_counts, normalized_deviation))
    
        # Sort workers by deviation (most imbalanced first)
        imbalanced_workers.sort(key=lambda x: x[2], reverse=True)
    
        # Try to fix the most imbalanced workers
        fixes_attempted = 0
        fixes_made = 0
    
        for worker_id, post_counts, deviation in imbalanced_workers:
            if fixes_attempted >= 10:  # Limit number of fix attempts
                break
            
            logging.info(f"Trying to improve post rotation for worker {worker_id} (deviation: {deviation:.2f})")
        
            # Find overassigned and underassigned posts
            total_assignments = sum(post_counts.values())
            expected_per_post = total_assignments / self.num_shifts
        
            overassigned_posts = []
            underassigned_posts = []
        
            for post in range(self.num_shifts):
                post_count = post_counts.get(post, 0)
                if post_count > expected_per_post + 0.5:
                    overassigned_posts.append((post, post_count))
                elif post_count < expected_per_post - 0.5:
                    underassigned_posts.append((post, post_count))
        
            # Sort by most overassigned/underassigned
            overassigned_posts.sort(key=lambda x: x[1], reverse=True)
            underassigned_posts.sort(key=lambda x: x[1])
        
            fixes_attempted += 1
        
            if not overassigned_posts or not underassigned_posts:
                continue
        
            # Try to swap a shift from overassigned post to underassigned post
            for over_post, _ in overassigned_posts:
                for under_post, _ in underassigned_posts:
                    # Find all dates where this worker has the overassigned post
                    possible_swap_dates = []
                
                    for date, workers in self.schedule.items():
                        if len(workers) > over_post and workers[over_post] == worker_id:
                            possible_swap_dates.append(date)
                
                    # Shuffle the dates to introduce randomness
                    random.shuffle(possible_swap_dates)
                
                    # Try each date
                    for date in possible_swap_dates:
                        # Look for a date where this worker isn't assigned but could be
                        for other_date in sorted(self.schedule.keys()):
                            # Skip if it's the same date
                            if other_date == date:
                                continue
                            
                            # Skip if worker is already assigned to this date
                            if worker_id in self.schedule[other_date]:
                                continue
                        
                            # Skip if the target post already has someone
                            if len(self.schedule[other_date]) > under_post and self.schedule[other_date][under_post] is not None:
                                continue
                            
                            # Check if this would be a valid assignment
                            if not self._can_swap_assignments(worker_id, date, over_post, other_date, under_post):
                                continue
                        
                            # Perform the swap
                            old_worker = self.schedule[date][over_post]
                        
                            # Handle the case where we need to extend the other date's shifts list
                            while len(self.schedule[other_date]) <= under_post:
                                self.schedule[other_date].append(None)
                        
                            # Make the swap
                            self.schedule[date][over_post] = None
                            self.schedule[other_date][under_post] = worker_id
                        
                            # Update tracking data
                            self.worker_assignments[worker_id].remove(date)
                            self.worker_assignments[worker_id].add(other_date)
                            self._update_tracking_data(worker_id, other_date, under_post)
                        
                            logging.info(f"Improved post rotation: Moved worker {worker_id} from {date.strftime('%Y-%m-%d')} "
                                        f"post {over_post} to {other_date.strftime('%Y-%m-%d')} post {under_post}")
                        
                            fixes_made += 1
                            break
                    
                        if fixes_made > fixes_attempted * 0.5:  # If we've made enough fixes, stop
                            break
                        
                    if fixes_made > fixes_attempted * 0.5:  # If we've made enough fixes, stop
                        break
                    
                if fixes_made > fixes_attempted * 0.5:  # If we've made enough fixes, stop
                    break
    
        logging.info(f"Post rotation improvement: attempted {fixes_attempted} fixes, made {fixes_made} changes")
    
        return fixes_made > 0  # Return whether we made any improvements

    def _can_swap_assignments(self, worker_id, from_date, from_post, to_date, to_post):
        """
        Check if a worker can be reassigned from one date/post to another
    
        Checks all relevant constraints:
        - Worker availability (days off)
        - Mandatory assignments
        - Worker incompatibilities
        - Minimum gap requirements
        - Weekend limits
        """
        # Check if worker is unavailable on the target date (days off)
        if self._is_worker_unavailable(worker_id, to_date):
            return False
    
        # Check if from_date was mandatory for this worker
        worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
        mandatory_days = worker_data.get('mandatory_days', []) if worker_data else []
        mandatory_dates = self._parse_dates(mandatory_days)
    
        # Don't reassign from a mandatory date
        if from_date in mandatory_dates:
            return False
    
        # Check worker incompatibilities at the target date
        for other_worker_id in self.schedule[to_date]:
            if other_worker_id is not None and self._are_workers_incompatible(worker_id, other_worker_id):
                return False
    
        # Check minimum gap requirement
        assignments = list(self.worker_assignments[worker_id])
        assignments.remove(from_date)  # Remove the date we're swapping from
        assignments.append(to_date)  # Add the date we're swapping to
    
        # Get sorted list of assignments
        sorted_assignments = sorted(assignments)
    
        # Check gaps between consecutive assignments
        for i in range(1, len(sorted_assignments)):
            gap_days = (sorted_assignments[i] - sorted_assignments[i-1]).days
        
            # Skip reduced gap check on holidays if not part-time worker
            if (worker_data and worker_data.get('work_percentage', 100) >= 100 and
                (self._is_holiday(sorted_assignments[i]) or self._is_holiday(sorted_assignments[i-1]))):
                continue
            
            # Determine minimum gap based on worker status
            min_gap = 2 if worker_data and worker_data.get('work_percentage', 100) < 100 else 3
        
            if gap_days < min_gap:
                return False
    
        # Check weekend limit
        if self._is_weekend_day(to_date) and not self._is_weekend_day(from_date):
            # We're adding a weekend, check if it would exceed limits
            current_month = to_date.month
            month_start = datetime(to_date.year, current_month, 1)
        
            if current_month == 12:
                month_end = datetime(to_date.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = datetime(to_date.year, current_month + 1, 1) - timedelta(days=1)
        
            # Count weekend days for this worker in the month
            weekend_count = sum(
                1 for d in self.worker_assignments[worker_id]
                if month_start <= d <= month_end and self._is_weekend_day(d) and d != from_date
            )
        
            # Add the new weekend assignment
            weekend_count += 1
        
            # Check against limit
            max_weekends = 3  # Default limit
            if worker_data:
                # Adjust limit based on work percentage
                work_percentage = worker_data.get('work_percentage', 100)
                if work_percentage < 100:
                    max_weekends = max(1, int(max_weekends * work_percentage / 100))
        
            if weekend_count > max_weekends:
                return False
    
        # Check post balance - ensure we're not making another post even more imbalanced
        post_counts = self._get_post_counts(worker_id)
    
        # Simulate the swap
        if from_post in post_counts:
            post_counts[from_post] -= 1
        if to_post in post_counts:
            post_counts[to_post] += 1
        else:
            post_counts[to_post] = 1
    
        # Check if this swap would worsen imbalance
        total_assignments = sum(post_counts.values())
        expected_per_post = total_assignments / self.num_shifts
    
        # Calculate deviation before and after
        current_deviation = 0
        for post in range(self.num_shifts):
            post_count = self._get_post_counts(worker_id).get(post, 0)
            current_deviation += abs(post_count - expected_per_post)
    
        new_deviation = 0
        for post in range(self.num_shifts):
            post_count = post_counts.get(post, 0)
            new_deviation += abs(post_count - expected_per_post)
    
        # Don't allow swaps that make post distribution worse
        if new_deviation > current_deviation:
            return False
    
        return True

    def _improve_weekend_distribution(self):
        """
        Improve weekend distribution by balancing weekend shifts more evenly among workers
        and attempting to resolve weekend overloads
        """
        logging.info("Attempting to improve weekend distribution")
    
        # Count weekend assignments for each worker by month
        weekend_counts_by_month = {}
    
        # Group dates by month
        months = {}
        current_date = self.start_date
        while current_date <= self.end_date:
            month_key = (current_date.year, current_date.month)
            if month_key not in months:
                months[month_key] = []
            months[month_key].append(current_date)
            current_date += timedelta(days=1)
    
        # Count weekend assignments by month for each worker
        for month_key, dates in months.items():
            weekend_counts = {}
            for worker in self.workers_data:
                worker_id = worker['id']
                weekend_count = sum(1 for date in dates if date in self.worker_assignments[worker_id] and self._is_weekend_day(date))
                weekend_counts[worker_id] = weekend_count
            weekend_counts_by_month[month_key] = weekend_counts
    
        changes_made = 0
    
        # Identify months with overloaded workers
        for month_key, weekend_counts in weekend_counts_by_month.items():
            overloaded_workers = []
            underloaded_workers = []
        
            for worker in self.workers_data:
                worker_id = worker['id']
                work_percentage = worker.get('work_percentage', 100)
            
                # Calculate weekend limit based on work percentage
                max_weekends = 3  # Default for full-time
                if work_percentage < 100:
                    max_weekends = max(1, int(3 * work_percentage / 100))
            
                weekend_count = weekend_counts.get(worker_id, 0)
            
                if weekend_count > max_weekends:
                    overloaded_workers.append((worker_id, weekend_count, max_weekends))
                elif weekend_count < max_weekends:
                    available_slots = max_weekends - weekend_count
                    underloaded_workers.append((worker_id, weekend_count, available_slots))
        
            # Sort by most overloaded and most available
            overloaded_workers.sort(key=lambda x: x[1] - x[2], reverse=True)
            underloaded_workers.sort(key=lambda x: x[2], reverse=True)
        
            # Get dates in this month
            month_dates = months[month_key]
            weekend_dates = [date for date in month_dates if self._is_weekend_day(date)]
        
            # Try to redistribute weekend shifts
            for over_worker_id, over_count, over_limit in overloaded_workers:
                if not underloaded_workers:
                    break
                
                for weekend_date in weekend_dates:
                    # Skip if this worker isn't assigned on this date
                    if over_worker_id not in self.schedule[weekend_date]:
                        continue
                
                    # Find the post this worker is assigned to
                    post = self.schedule[weekend_date].index(over_worker_id)
                
                    # Try to find a suitable replacement
                    for under_worker_id, _, _ in underloaded_workers:
                        # Skip if this worker is already assigned on this date
                        if under_worker_id in self.schedule[weekend_date]:
                            continue
                    
                        # Check if we can assign this worker to this shift
                        if self._can_assign_worker(under_worker_id, weekend_date, post):
                            # Make the swap
                            self.schedule[weekend_date][post] = under_worker_id
                            self.worker_assignments[over_worker_id].remove(weekend_date)
                            self.worker_assignments[under_worker_id].add(weekend_date)
                        
                            # Remove the weekend tracking for the over-loaded worker
                            self._update_worker_stats(over_worker_id, weekend_date, removing=True)

                            # Update tracking data for the under-loaded worker
                            self._update_tracking_data(under_worker_id, weekend_date, post)
                        
                            # Update counts
                            weekend_counts[over_worker_id] -= 1
                            weekend_counts[under_worker_id] += 1
                        
                            changes_made += 1
                            logging.info(f"Improved weekend distribution: Moved weekend shift on {weekend_date.strftime('%Y-%m-%d')} "
                                        f"from worker {over_worker_id} to worker {under_worker_id}")
                        
                            # Update worker lists
                            if weekend_counts[over_worker_id] <= over_limit:
                                # This worker is no longer overloaded
                                overloaded_workers = [(w, c, l) for w, c, l in overloaded_workers if w != over_worker_id]
                        
                            # Check if under worker is now fully loaded
                            for i, (w_id, count, slots) in enumerate(underloaded_workers):
                                if w_id == under_worker_id:
                                    if weekend_counts[w_id] >= count + slots:
                                        # Remove from underloaded
                                        underloaded_workers.pop(i)
                                    break
                        
                            # Break to try next overloaded worker
                            break
    
        logging.info(f"Weekend distribution improvement: made {changes_made} changes")
        return changes_made > 0

    def _balance_workloads(self):
        """
        Balance the total number of assignments among workers based on their work percentages
        """
        logging.info("Attempting to balance worker workloads")
    
        # Count total assignments for each worker
        assignment_counts = {}
        for worker in self.workers_data:
            worker_id = worker['id']
            work_percentage = worker.get('work_percentage', 100)
        
            # Count assignments
            count = len(self.worker_assignments[worker_id])
        
            # Normalize by work percentage
            normalized_count = count * 100 / work_percentage if work_percentage > 0 else 0
        
            assignment_counts[worker_id] = {
                'worker_id': worker_id,
                'count': count,
                'work_percentage': work_percentage,
                'normalized_count': normalized_count
            }    
    
        # Calculate average normalized count
        total_normalized = sum(data['normalized_count'] for data in assignment_counts.values())
        avg_normalized = total_normalized / len(assignment_counts) if assignment_counts else 0
    
        # Identify overloaded and underloaded workers
        overloaded = []
        underloaded = []
    
        for worker_id, data in assignment_counts.items():
            # Allow 10% deviation from average
            if data['normalized_count'] > avg_normalized * 1.1:
                overloaded.append((worker_id, data))
            elif data['normalized_count'] < avg_normalized * 0.9:
                underloaded.append((worker_id, data))
    
        # Sort by most overloaded/underloaded
        overloaded.sort(key=lambda x: x[1]['normalized_count'], reverse=True)
        underloaded.sort(key=lambda x: x[1]['normalized_count'])
    
        changes_made = 0
        max_changes = 5  # Limit number of changes to avoid disrupting the schedule too much
    
        # Try to redistribute shifts from overloaded to underloaded workers
        for over_worker_id, over_data in overloaded:
            if changes_made >= max_changes or not underloaded:
                break
            
            # Find shifts that can be reassigned from this overloaded worker
            possible_shifts = []
        
            for date in sorted(self.worker_assignments[over_worker_id]):
                # Skip if today's date is mandatory for this worker
                worker_data = next((w for w in self.workers_data if w['id'] == over_worker_id), None)
                mandatory_days = worker_data.get('mandatory_days', []) if worker_data else []
                mandatory_dates = self._parse_dates(mandatory_days)
            
                if date in mandatory_dates:
                    continue
                
                # Find the post this worker is assigned to
                post = self.schedule[date].index(over_worker_id)
            
                possible_shifts.append((date, post))
        
            # Shuffle to introduce randomness
            random.shuffle(possible_shifts)
        
            # Try each shift
            for date, post in possible_shifts:
                reassigned = False
            
                # Try each underloaded worker
                for under_worker_id, _ in underloaded:
                    # Skip if this worker is already assigned on this date
                    if under_worker_id in self.schedule[date]:
                        continue
                
                    # Check if we can assign this worker to this shift
                    if self._can_assign_worker(under_worker_id, date, post):
                        # Make the reassignment
                        self.schedule[date][post] = under_worker_id
                        self.worker_assignments[over_worker_id].remove(date)
                        self.worker_assignments[under_worker_id].add(date)
                    
                        # Update tracking data
                        self._update_tracking_data(under_worker_id, date, post)
                    
                        changes_made += 1
                        logging.info(f"Balanced workload: Moved shift on {date.strftime('%Y-%m-%d')} post {post} "
                                    f"from worker {over_worker_id} to worker {under_worker_id}")
                    
                        # Update counts
                        assignment_counts[over_worker_id]['count'] -= 1
                        assignment_counts[over_worker_id]['normalized_count'] = (
                            assignment_counts[over_worker_id]['count'] * 100 / 
                            assignment_counts[over_worker_id]['work_percentage']
                        )
                    
                        assignment_counts[under_worker_id]['count'] += 1
                        assignment_counts[under_worker_id]['normalized_count'] = (
                            assignment_counts[under_worker_id]['count'] * 100 / 
                            assignment_counts[under_worker_id]['work_percentage']
                        )
                    
                        reassigned = True
                    
                        # Check if workers are still overloaded/underloaded
                        if assignment_counts[over_worker_id]['normalized_count'] <= avg_normalized * 1.1:
                            # No longer overloaded
                            overloaded = [(w, d) for w, d in overloaded if w != over_worker_id]
                    
                        if assignment_counts[under_worker_id]['normalized_count'] >= avg_normalized * 0.9:
                            # No longer underloaded
                            underloaded = [(w, d) for w, d in underloaded if w != under_worker_id]
                    
                        break
            
                if reassigned:
                    break
                
                if changes_made >= max_changes:
                    break
    
        logging.info(f"Workload balancing: made {changes_made} changes")
        return changes_made > 0

    def _calculate_improvement_score(self, worker, date, post):
        """
        Calculate a score for a worker assignment during the improvement phase.
    
        This uses a more lenient scoring approach to encourage filling empty shifts.
        """
        worker_id = worker['id']
    
        # Base score from standard calculation
        base_score = self._calculate_worker_score(worker, date, post)
    
        # If base score is negative infinity, the assignment is invalid
        if base_score == float('-inf'):
            return float('-inf')
    
        # Bonus for balancing post rotation
        post_counts = self._get_post_counts(worker_id)
        total_assignments = sum(post_counts.values())
    
        # Skip post balance check for workers with few assignments
        if total_assignments >= self.num_shifts:
            expected_per_post = total_assignments / self.num_shifts
            current_count = post_counts.get(post, 0)
        
            # Give bonus if this post is underrepresented for this worker
            if current_count < expected_per_post:
                base_score += 10 * (expected_per_post - current_count)
    
        # Bonus for balancing workload
        work_percentage = worker.get('work_percentage', 100)
        current_assignments = len(self.worker_assignments[worker_id])
    
        # Calculate average assignments per worker, adjusted for work percentage
        total_assignments_all = sum(len(self.worker_assignments[w['id']]) for w in self.workers_data)
        total_work_percentage = sum(w.get('work_percentage', 100) for w in self.workers_data)
    
        # Expected assignments based on work percentage
        expected_assignments = (total_assignments_all / (total_work_percentage / 100)) * (work_percentage / 100)
    
        # Bonus for underloaded workers
        if current_assignments < expected_assignments:
            base_score += 5 * (expected_assignments - current_assignments)
    
        return base_score
               
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
        """Calculate target number of shifts for each worker based on their percentage"""
        total_days = (self.end_date - self.start_date).days + 1
        total_shifts = total_days * self.num_shifts
    
        logging.info(f"Calculating targets for {total_days} days with {self.num_shifts} shifts per day")
        logging.info(f"Total shifts to distribute: {total_shifts}")

        # Initialize target_shifts to 0 for all workers
        for worker in self.workers_data:
            worker['target_shifts'] = 0

        try:
            # Convert work_percentage to float when summing
            percentages = []
            for worker in self.workers_data:
                try:
                    percentage = float(str(worker.get('work_percentage', 100)).strip())
                    if percentage <= 0:
                        logging.warning(f"Worker {worker['id']} has 0 or negative percentage: {percentage}")
                        percentage = 100  # Default to 100% if invalid
                    percentages.append(percentage)
                except (ValueError, TypeError) as e:
                    logging.warning(f"Invalid percentage for worker {worker['id']}, using 100%: {str(e)}")
                    percentages.append(100)
        
            total_percentage = sum(percentages)
            if total_percentage <= 0:
                logging.error("Total percentage is 0 or negative")
                raise SchedulerError("Invalid total percentage")

            # Calculate and assign targets
            for worker, percentage in zip(self.workers_data, percentages):
                target = (percentage / total_percentage) * total_shifts
                worker['target_shifts'] = max(1, round(target))  # Ensure at least 1 shift
                logging.info(f"Worker {worker['id']} - Percentage: {percentage}%, "
                            f"Target shifts: {worker['target_shifts']}")

        except Exception as e:
            logging.error(f"Error in target calculation: {str(e)}", exc_info=True)
            # Set default targets if calculation fails
            default_target = max(1, round(total_shifts / len(self.workers_data)))
            for worker in self.workers_data:
                worker['target_shifts'] = default_target
                logging.warning(f"Using default target for worker {worker['id']}: {default_target}")

        # Verify all workers have targets
        for worker in self.workers_data:
            if 'target_shifts' not in worker or worker['target_shifts'] <= 0:
                logging.error(f"Worker {worker['id']} has invalid target_shifts: {worker.get('target_shifts')}")
                worker['target_shifts'] = 1
                
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

    def _assign_day_shifts(self, date, attempt_number=0):
        """Modified assignment method that allows for unfilled shifts with variability"""
        logging.info(f"\nAssigning shifts for {date.strftime('%Y-%m-%d')}")

        if date not in self.schedule:
            self.schedule[date] = []

        remaining_shifts = self.num_shifts - len(self.schedule[date])

        for post in range(remaining_shifts):
            assigned = False
            candidates = []
    
            for worker in self.workers_data:
                worker_id = worker['id']
            
                # Skip if already assigned to this date
                if worker_id in self.schedule[date]:
                    continue
                
                # Skip if would exceed weekend limit
                if self._would_exceed_weekend_limit(worker_id, date):
                    continue
            
                # Check other constraints
                if self._can_assign_worker(worker_id, date, post):
                    score = self._calculate_worker_score(worker, date, post)
                    if score > float('-inf'):
                        candidates.append((worker, score))

            if candidates:
                # Group candidates by score
                candidates_by_score = {}
                for worker, score in candidates:
                    if score not in candidates_by_score:
                        candidates_by_score[score] = []
                    candidates_by_score[score].append(worker)
            
                # Sort scores in descending order
                sorted_scores = sorted(candidates_by_score.keys(), reverse=True)
            
                # Get workers with the highest score
                best_score = sorted_scores[0]
                best_workers = candidates_by_score[best_score]
            
                # Introduce variability: shuffle the best workers based on the attempt number
                shuffled_workers = best_workers.copy()
                random.Random(attempt_number + date.toordinal()).shuffle(shuffled_workers)
            
                # Select the first worker from the shuffled list
                best_worker = shuffled_workers[0]
            
                worker_id = best_worker['id']
                self.schedule[date].append(worker_id)
                self.worker_assignments[worker_id].add(date)
                self._update_tracking_data(worker_id, date, post)
                assigned = True
                logging.info(f"Assigned worker {worker_id} to {date}, post {post}")
        
            if not assigned:
                # Leave the shift empty if no suitable worker found
                self.schedule[date].append(None)
                logging.warning(f"No suitable worker found for {date}, post {post} - leaving shift unfilled")

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

            score = self._ore(worker, date, post)
            # Only add candidate if score is not None or -inf
            if score is not None and score != float('-inf'):
                candidates.append((worker, score))
                logging.debug(f"Worker {worker_id} added as candidate with score {score}")

        return candidates

    def _calculate_worker_score(self, worker, date, post):
        """Calculate a score for assigning a worker to a shift"""
        try:
            worker_id = worker['id']
            score = 0

            # First check incompatibility - reject immediately if incompatible
            if not self._check_incompatibility(worker_id, date):
                return float('-inf')

            # Check minimum gap requirement first
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if assignments:
                days_since_last = (date - assignments[-1]).days
                if days_since_last < 3:
                    return float('-inf')
                    
            # Check weekend limit - reject if would exceed
            if self._would_exceed_weekend_limit(worker_id, date):
                return float('-inf')
            
            # --- Weekend Scoring Component ---
            if date.weekday() >= 4:  # Friday, Saturday, Sunday
                weekend_assignments = sum(
                    1 for d in self.worker_assignments[worker_id]
                    if d.weekday() >= 4
                )
                # Lower score for workers with more weekend assignments
                score -= weekend_assignments * 300

            # Weekday Balance Check - STRICT
            weekday = date.weekday()
            weekday_counts = self.worker_weekdays[worker_id].copy()
            weekday_counts[weekday] += 1  # Simulate adding this assignment
        
            max_weekday = max(weekday_counts.values())
            min_weekday = min(weekday_counts.values())
        
            # If this assignment would create more than 1 day difference, reject it
            if (max_weekday - min_weekday) > 1:
                return float('-inf')

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
            
            # Higher priority for workers further from their target
            score += shift_difference * 1000
        
            # Post rotation score - focus on last post distribution
            last_post = self.num_shifts - 1
            if post == last_post:
                post_counts = self._get_post_counts(worker_id)
                total_assignments = sum(post_counts.values()) + 1
                target_last_post = total_assignments * (1 / self.num_shifts)
                current_last_post = post_counts.get(last_post, 0)
                
                # Encourage assignments when below target
                if current_last_post < target_last_post - 1:
                    score += 1000
                # Discourage assignments when above target
                elif current_last_post > target_last_post + 1:
                    score -= 1000

            # Add penalty for weekend assignments
            if date.weekday() in [4, 5, 6]:
                score -= 1000

            # Log the score calculation
            logging.debug(f"Score for worker {worker_id}: {score} "
                         f"(current: {current_shifts}, target: {target_shifts}")

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
        """Check if worker is incompatible with already assigned workers"""
        try:
            if date not in self.schedule:
                return True

            # Get the worker's data
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                logging.error(f"Worker {worker_id} not found in workers_data")
                return False
    
            # Check against all workers already assigned to this date
            for assigned_id in self.schedule[date]:
                if assigned_id is None:
                    continue
                
                if self._are_workers_incompatible(worker_id, assigned_id):
                    logging.debug(f"Workers {worker_id} and {assigned_id} are incompatible")
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
                if days_between < 3 or days_between in [7, 14, 21]:
                    return False
        return True

    def _would_exceed_weekend_limit(self, worker_id, date):
        """
        Check if assigning this date would exceed the weekend limit
        Maximum 3 weekend days in any 3-week period
        Weekend days include: Friday, Saturday, Sunday, holidays, and pre-holidays
        """
        try:
            # If it's not a weekend day or holiday, no need to check
            if not (date.weekday() >= 4 or date in self.holidays or 
                (date + timedelta(days=1)) in self.holidays):
                return False
        
            # Get all weekend assignments INCLUDING the new date
            all_weekend_assignments = [
                d for d in self.worker_assignments[worker_id] 
                if (d.weekday() >= 4 or d in self.holidays or 
                    (d + timedelta(days=1)) in self.holidays)
            ]    
        
            # Add the new date if it's not already in the list
            if date not in all_weekend_assignments:
                all_weekend_assignments.append(date)
        
            # For each weekend date, check if there are more than 3 weekend days 
            # in the 21-day window centered on that date
            for check_date in all_weekend_assignments:
                # Define a 21-day window centered on this date
                window_start = check_date - timedelta(days=10)  # 10 days before
                window_end = check_date + timedelta(days=10)    # 10 days after
            
                # Count weekend days in this window
                window_weekend_count = sum(
                    1 for d in all_weekend_assignments
                    if window_start <= d <= window_end
                )
            
                if window_weekend_count > 3:
                    logging.debug(f"Worker {worker_id} would exceed weekend limit: "
                                f"{window_weekend_count} weekend days in 3-week window around {check_date}")
                    return True
        
            return False
        
        except Exception as e:
            logging.error(f"Error checking weekend limit: {str(e)}")
            return True  # Fail safe

    def _has_three_consecutive_weekends(self, worker_id, date=None):
        """
        Validation method to check if a worker has three or more consecutive weekends
        Used for schedule validation rather than assignment decisions
        """
        try:
            # Get all weekend dates for this worker
            weekend_dates = sorted([
                d for d in self.worker_assignments[worker_id] 
                if d.weekday() >= 5  # Only Saturday(5) and Sunday(6)
            ])
        
            if date and date.weekday() >= 5:
                weekend_dates.append(date)
                weekend_dates.sort()
            
            # Group dates by weekend (assuming Sat/Sun pairs)
            weekends = []
            current_weekend = []
        
            for d in weekend_dates:
                if not current_weekend or (d - current_weekend[-1]).days <= 1:
                    current_weekend.append(d)
                else:
                    weekends.append(min(current_weekend))  # Store start of weekend
                    current_weekend = [d]
                
            if current_weekend:
                weekends.append(min(current_weekend))
            
            # Check for consecutive weekends
            for i in range(len(weekends)-2):
                days_between_first_last = (weekends[i+2] - weekends[i]).days
                if days_between_first_last <= 14:  # 14 days = 2 weeks
                    return True
                
            return False
        
        except Exception as e:
            logging.error(f"Error checking consecutive weekends: {str(e)}")
            return True  # Fail safe
        
    def _is_holiday_or_before(self, date):
        """
        Check if a date is a holiday or the day before a holiday
        Returns True for holidays and days before holidays
        """
        try:
            # You'll need to provide the holiday list in the format you prefer
            holidays = set()  # Add your holidays here
        
            # Check if date is a holiday
            if date in holidays:
                return True
            
            # Check if next day is a holiday
            next_day = date + timedelta(days=1)
            if next_day in holidays:
                return True
            
            return False
        
        except Exception as e:
            logging.error(f"Error checking holiday: {str(e)}")
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
        Check if a worker can be assigned to a shift
        """
        try:
            # Log all constraint checks
            logging.debug(f"\nChecking worker {worker_id} for {date}, post {post}")

            # 1. First check - Incompatibility
            if not self._check_incompatibility(worker_id, date):
                logging.debug(f"- Failed: Worker {worker_id} is incompatible with assigned workers")
                return False

            # 2. Check max shifts
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                logging.debug(f"- Failed: Max shifts reached ({self.max_shifts_per_worker})")
                return False

            # 3. Check availability
            if self._is_worker_unavailable(worker_id, date):
                logging.debug(f"- Failed: Worker unavailable")
                return False

            # 4. Check minimum gap
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if assignments:
                days_since_last = (date - assignments[-1]).days
                if days_since_last < 3:
                    logging.debug(f"- Failed: Insufficient gap ({days_since_last} days)")
                    return False

            # 5. Check monthly targets
            month_key = f"{date.year}-{date.month:02d}"
            if hasattr(self, 'monthly_targets') and month_key in self.monthly_targets.get(worker_id, {}):
                current_month_assignments = sum(1 for d in self.worker_assignments[worker_id] 
                                           if d.strftime("%Y-%m") == date.strftime("%Y-%m"))
                if current_month_assignments >= self.monthly_targets[worker_id][month_key]:
                    logging.debug(f"- Failed: Monthly target reached")
                    return False

            # 6. Check weekend limit
            if self._would_exceed_weekend_limit(worker_id, date):
                logging.debug(f"- Failed: Would exceed weekend limit")
                return False
            
            return True

        except Exception as e:
            logging.error(f"Error in _can_assign_worker for worker {worker_id}: {str(e)}", exc_info=True)
            return False
            
    def _are_workers_incompatible(self, worker1_id, worker2_id):
        """
        Check if two workers are incompatible based on incompatibility property or list.
        """
        try:
            # Get workers' data
            worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
            worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
        
            if not worker1 or not worker2:
                return False
    
            # Case 1: Check 'is_incompatible' property (both must have it for incompatibility)
            has_incompatibility1 = worker1.get('is_incompatible', False)
            has_incompatibility2 = worker2.get('is_incompatible', False)
            if has_incompatibility1 and has_incompatibility2:
                logging.debug(f"Workers {worker1_id} and {worker2_id} are incompatible")
                return True
    
            # Case 2: Check if either worker lists the other in 'incompatible_workers'
            incompatible_list1 = worker1.get('incompatible_workers', [])
            incompatible_list2 = worker2.get('incompatible_workers', [])
        
            if worker2_id in incompatible_list1 or worker1_id in incompatible_list2:
                logging.debug(f"Workers {worker1_id} and {worker2_id} are incompatible")
                return True
    
            return False
    
        except Exception as e:
            logging.error(f"Error checking worker incompatibility: {str(e)}")
            return False  # Default to compatible in case of error
            
    def _check_day_compatibility(self, worker_id, date):
        """Check if worker is compatible with all workers already assigned to this date"""
        if date not in self.schedule:
            return True
        
        for assigned_worker in self.schedule[date]:
            if self._are_workers_incompatible(worker_id, assigned_worker):
                logging.debug(f"Worker {worker_id} is incompatible with assigned worker {assigned_worker}")
                return False
        return True

    def get_assigned_workers(self, date):
        """
        Return a list of worker IDs that are scheduled on a given date.
        """
        return self.schedule.get(date, [])
        
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
            
        # Add potential new assignment
        post_counts[post] += 1
    
        # Calculate target per post
        total = sum(post_counts.values())
        target_per_post = total / self.num_shifts
    
        # Return maximum deviation from target
        max_deviation = max(abs(count - target_per_post) for count in post_counts.values())
        return max_deviation
        
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
        Check if assigning this date would maintain monthly balance based on available days
        """
        try:
            # Get monthly distribution
            distribution = {}
            month_days = self._get_schedule_months()  # This returns a dict, not an int
        
            # Count current assignments
            for assigned_date in self.worker_assignments[worker_id]:
                month_key = f"{assigned_date.year}-{assigned_date.month:02d}"
                distribution[month_key] = distribution.get(month_key, 0) + 1
    
            # Add potential new assignment
            month_key = f"{date.year}-{date.month:02d}"
            new_distribution = distribution.copy()
            new_distribution[month_key] = new_distribution.get(month_key, 0) + 1

            if new_distribution:
                # Calculate ratios of shifts per available day for each month
                ratios = {}
                for m_key, shifts in new_distribution.items():
                    if m_key in month_days:  # Make sure month key exists
                        available_days = month_days[m_key]
                        if available_days > 0:  # Prevent division by zero
                            ratios[m_key] = shifts / available_days
                        else:
                            ratios[m_key] = 0
                    else:
                        logging.warning(f"Month {m_key} not found in month_days for worker {worker_id}")
                        ratios[m_key] = 0

                if ratios:
                    max_ratio = max(ratios.values())
                    min_ratio = min(ratios.values())
            
                    # Allow maximum 20% difference in ratios
                    if (max_ratio - min_ratio) > 0.2:
                        logging.debug(f"Monthly balance violated for worker {worker_id}: {ratios}")
                        return False, max_ratio - min_ratio

            return True, 0.0

        except Exception as e:
            logging.error(f"Error checking monthly balance for worker {worker_id}: {str(e)}", exc_info=True)
            return True, 0.0

    def _calculate_monthly_targets(self):
        """Calculate target shifts per month for each worker"""
        try:
            self.monthly_targets = {}
        
            # Get available days per month
            month_days = self._get_schedule_months()
            logging.debug(f"Available days per month: {month_days}")
        
            for worker in self.workers_data:
                worker_id = worker['id']
                self.monthly_targets[worker_id] = {}
            
                # Get worker's total target shifts
                total_target = worker.get('target_shifts', 0)
                logging.debug(f"Worker {worker_id} total target: {total_target}")
            
                # Calculate monthly proportion
                total_days = sum(month_days.values())
                for month, days in month_days.items():
                    month_target = round((days / total_days) * total_target)
                    self.monthly_targets[worker_id][month] = month_target
                    logging.debug(f"Worker {worker_id}, Month {month}: {month_target} shifts")
                
        except Exception as e:
            logging.error(f"Error calculating monthly targets: {str(e)}", exc_info=True)
            raise
                
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
        
            # Strictly enforce maximum 1 shift difference between weekdays
            if max_count - min_count > 1:
                logging.debug(f"Weekday balance violated for worker {worker_id}: {weekday_counts}")
                return False

            return True

        except Exception as e:
            logging.error(f"Error checking weekday balance for worker {worker_id}: {str(e)}")
            return True

    def _get_weekday_balance_score(self, worker_id, date):
        """Calculate how well this assignment would maintain weekday balance"""
        weekday = date.weekday()
        weekday_counts = self.worker_weekdays[worker_id].copy()
        weekday_counts[weekday] += 1
    
        # Calculate current and new imbalance
        current_max = max(self.worker_weekdays[worker_id].values())
        current_min = min(self.worker_weekdays[worker_id].values())
        current_imbalance = current_max - current_min
    
        new_max = max(weekday_counts.values())
        new_min = min(weekday_counts.values())
        new_imbalance = new_max - new_min
    
        # Return a score based on how it affects balance
        if new_imbalance < current_imbalance:
            return 3  # Improves balance
        elif new_imbalance == current_imbalance:
            return 2  # Maintains balance
        elif new_imbalance <= 1:
            return 1  # Acceptable imbalance
        else:
            return 0  # Unacceptable imbalance

    def _check_monthly_target(self, worker_id, month_key):
        """Check if worker has reached their monthly target"""
        if month_key not in self.monthly_targets.get(worker_id, {}):
            return False
            
        current_assignments = sum(
            1 for d in self.worker_assignments[worker_id]
            if self._get_month_key(d) == month_key
        )
        
        return current_assignments >= self.monthly_targets[worker_id][month_key]

    def _check_post_rotation(self, worker_id, post):
        """
        Check if assigning this post maintains the required distribution.
        Specifically checks that the last post (highest number) is assigned
        approximately 1/num_shifts of the time.
    
        Args:
            worker_id: Worker's ID
            post: Post number being considered
        Returns:
            bool: True if assignment maintains proper distribution
        """
        try:
            # Only check for last post position
            last_post = self.num_shifts - 1
            if post != last_post:
                return True  # Don't restrict other post assignments
        
            # Get current post counts
            post_counts = self._get_post_counts(worker_id)
        
            # Add the potential new assignment
            new_counts = post_counts.copy()
            new_counts[post] = new_counts.get(post, 0) + 1
        
            # Calculate total assignments including the new one
            total_assignments = sum(new_counts.values())
        
            if total_assignments == 0:
                return True
        
            # Calculate target ratio for last post (1/num_shifts)
            target_ratio = 1.0 / self.num_shifts
            actual_ratio = new_counts[last_post] / total_assignments
        
            # Allow ±1 shift deviation from perfect ratio
            allowed_deviation = 1.0 / total_assignments
        
            if abs(actual_ratio - target_ratio) > allowed_deviation:
                logging.debug(
                    f"Post rotation check failed for worker {worker_id}: "
                    f"Last post ratio {actual_ratio:.2f} deviates too much from "
                    f"target {target_ratio:.2f} (allowed deviation: ±{allowed_deviation:.2f})"
                )    
                return False
        
            return True

        except Exception as e:
            logging.error(f"Error checking post rotation for worker {worker_id}: {str(e)}")
            return True

    def _calculate_post_rotation_coverage(self):
        """Calculate how well the post rotation is working across all workers"""
        worker_scores = {}
        total_score = 0
    
        for worker in self.workers_data:
            worker_id = worker['id']
            post_counts = self._get_post_counts(worker_id)
            total_assignments = sum(post_counts.values())
        
            if total_assignments == 0:
                worker_scores[worker_id] = 100  # No assignments, perfect score
                continue
            
            post_imbalance = 0
            expected_per_post = total_assignments / self.num_shifts
        
            for post in range(self.num_shifts):
                post_count = post_counts.get(post, 0)
                post_imbalance += abs(post_count - expected_per_post)
        
            # Calculate a score where 0 imbalance = 100%
            imbalance_ratio = post_imbalance / total_assignments
            worker_score = max(0, 100 - (imbalance_ratio * 100))
            worker_scores[worker_id] = worker_score
        
        # Calculate overall score
        if worker_scores:
            total_score = sum(worker_scores.values()) / len(worker_scores)
        else:
            total_score = 0
        
        return {
            'overall_score': total_score,
            'worker_scores': worker_scores
        }
            
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
        Calculate available days per month in schedule period
    
        Returns:
            dict: Dictionary with month keys and their available days count
        """
        month_days = {}
        current = self.start_date
        while current <= self.end_date:
            month_key = f"{current.year}-{current.month:02d}"
        
            if month_key not in month_days:
                month_days[month_key] = 0
        
            # Only count days within our schedule period
            if self.start_date <= current <= self.end_date:
                month_days[month_key] += 1
            
            current += timedelta(days=1)
        
            # Move to first day of next month if we've finished current month
            if current.day == 1:
                next_month = current
            else:
                # Get first day of next month
                next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
                current = next_month
    
        return month_days

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

    def remove_worker_assignment(self, worker_id, date):
        """
        Remove a worker's assignment from a given date and update all tracking data
    
        Args:
            worker_id: The worker's ID
            date: Date of the assignment to remove
        """
        try:
            # Check if the worker is actually assigned to this date
            if worker_id not in self.schedule.get(date, []):
                logging.warning(f"Worker {worker_id} is not assigned to {date.strftime('%Y-%m-%d')}")
                return False
            
            # Find the post the worker is assigned to
            post = self.schedule[date].index(worker_id)
        
            # Remove worker from schedule
            self.schedule[date][post] = None
        
            # Remove date from worker assignments
            self.worker_assignments[worker_id].discard(date)
        
            # Update tracking data for the removed assignment
            self._update_worker_stats(worker_id, date, removing=True)
        
            # If the worker has post assignments for this post, remove it if it's the last one
            post_counts = self._get_post_counts(worker_id)
            if post in post_counts and post_counts[post] == 0:
                self.worker_posts[worker_id].discard(post)
            
            logging.info(f"Removed worker {worker_id} assignment from {date.strftime('%Y-%m-%d')}, post {post}")
            return True
        
        except Exception as e:
            logging.error(f"Error removing worker assignment: {str(e)}")
            return False

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
        Clean up schedule while preserving partial assignments
        """
        logging.info("Starting schedule cleanup...")
        incomplete_days = []
        
        current_date = self.start_date
        while current_date <= self.end_date:
            if current_date not in self.schedule:
                incomplete_days.append(current_date)
            else:
                # Keep partial assignments instead of removing them
                assigned_shifts = len(self.schedule[current_date])
                if assigned_shifts > 0 and assigned_shifts < self.num_shifts:
                    logging.info(f"Keeping {assigned_shifts} shifts for {current_date}")
                elif assigned_shifts == 0:
                    incomplete_days.append(current_date)
            
            current_date += timedelta(days=1)

        # Only remove days with zero assignments
        for date in incomplete_days:
            if date in self.schedule:
                logging.info(f"Removing empty day {date}")
                del self.schedule[date]

        logging.info(f"Schedule cleanup complete. Removed {len(incomplete_days)} empty days.")

    def _validate_final_schedule(self):
        """Modified validation to allow for unfilled shifts"""
        errors = []
        warnings = []

        logging.info("Starting final schedule validation...")

        # Check each date in schedule
        for date in sorted(self.schedule.keys()):
            assigned_workers = [w for w in self.schedule[date] if w is not None]
            
            # Check worker incompatibilities
            for i, worker_id in enumerate(assigned_workers):
                for other_id in assigned_workers[i+1:]:
                    if self._are_workers_incompatible(worker_id, other_id):
                        errors.append(
                            f"Incompatible workers {worker_id} and {other_id} "
                            f"on {date.strftime('%Y-%m-%d')}"
                        )

            # Check understaffing (now a warning instead of error)
            filled_shifts = len([w for w in self.schedule[date] if w is not None])
            if filled_shifts < self.num_shifts:
                warnings.append(
                    f"Understaffed on {date.strftime('%Y-%m-%d')}: "
                    f"{filled_shifts} of {self.num_shifts} shifts filled"
                )

        # Check worker-specific constraints
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = sorted([
                date for date, workers in self.schedule.items()
                if worker_id in workers
            ])
            
            # Check weekend constraints
            for date in assignments:
                if self._is_weekend_day(date):
                    window_start = date - timedelta(days=10)
                    window_end = date + timedelta(days=10)
                    
                    weekend_count = sum(
                        1 for d in assignments
                        if window_start <= d <= window_end and self._is_weekend_day(d)
                    )
                    
                    if weekend_count > 3:
                        errors.append(
                            f"Worker {worker_id} has {weekend_count} weekend/holiday "
                            f"shifts in a 3-week period around {date.strftime('%Y-%m-%d')}"
                        )

            # Validate post rotation
            self._validate_post_rotation(worker_id, warnings)

        # Log all warnings
        for warning in warnings:
            logging.warning(warning)

        # Handle errors
        if errors:
            error_msg = "Schedule validation failed:\n" + "\n".join(errors)
            logging.error(error_msg)
            raise SchedulerError(error_msg)

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
        total_assignments = sum(post_counts.values())
        last_post = self.num_shifts - 1
        target_last_post = total_assignments * (1 / self.num_shifts)
        actual_last_post = post_counts.get(last_post, 0)
        
        # Allow for some deviation
        allowed_deviation = 1
        
        if abs(actual_last_post - target_last_post) > allowed_deviation:
            warnings.append(
                f"Worker {worker_id} post rotation imbalance: {post_counts}. "
                f"Last post assignments: {actual_last_post}, Target: {target_last_post:.2f}, "
                f"Allowed deviation: ±{allowed_deviation:.2f}"
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
        """Validate weekend assignments for a worker"""
        try:
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if not assignments:
                return

            for date in assignments:
                # Check 3-week window for each assignment
                window_start = date - timedelta(days=10)
                window_end = date + timedelta(days=10)
            
                weekend_count = sum(
                    1 for d in assignments
                    if window_start <= d <= window_end and
                    (d.weekday() >= 4 or d in self.holidays or 
                     (d + timedelta(days=1)) in self.holidays)
                )
            
                if weekend_count > 3:
                    errors.append(
                        f"Worker {worker_id} has {weekend_count} weekend/holiday "
                        f"shifts in a 3-week period around {date.strftime('%Y-%m-%d')}"
                    )
                    return

        except Exception as e:
            logging.error(f"Error validating consecutive weekends: {str(e)}")
            errors.append(f"Error validating weekends for worker {worker_id}: {str(e)}")

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
     
