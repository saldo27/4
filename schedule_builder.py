# Imports
from datetime import datetime, timedelta
import logging
import random
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from scheduler import Scheduler
else:
    from scheduler import SchedulerError

class ScheduleBuilder:
    """Handles schedule generation and improvement"""
    
    # Methods
    def __init__(self, scheduler):
        """
        Initialize the schedule builder
    
        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
    
        # Store references to frequently accessed attributes
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        self.num_shifts = scheduler.num_shifts
        self.holidays = scheduler.holidays
        self.constraint_checker = scheduler.constraint_checker
    
        logging.info("ScheduleBuilder initialized")
        
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
                        self.data_manager._update_tracking_data(worker_id, date, post)
                        
    def _assign_priority_days(self, forward):
        """Process weekend and holiday assignments first since they're harder to fill"""
        dates_to_process = []
        current = self.start_date
    
        # Get all weekend and holiday dates in the period
        while current <= self.end_date:
            if self._is_weekend_day(current) or current in self.holidays:
                dates_to_process.append(current)
            current += timedelta(days=1)
    
        # Sort based on direction
        if not forward:
            dates_to_process.reverse()
    
        logging.info(f"Processing {len(dates_to_process)} priority days (weekends & holidays)")
    
        # Process these dates first with strict constraints
        for date in dates_to_process:
            if date not in self.schedule:
                self.schedule[date] = []
        
            remaining_shifts = self.num_shifts - len(self.schedule[date])
            if remaining_shifts > 0:
                self._assign_day_shifts_with_relaxation(date, 0, 0)  # Use strict constraints

    def _get_remaining_dates_to_process(self, forward):
        """Get remaining dates that need to be processed"""
        dates_to_process = []
        current = self.start_date
    
        # Get all dates in period that are not weekends or holidays
        # or that already have some assignments but need more
        while current <= self.end_date:
            date_needs_processing = False
        
            if current not in self.schedule:
                # Date not in schedule at all
                date_needs_processing = True
            elif len(self.schedule[current]) < self.num_shifts:
                # Date in schedule but has fewer shifts than needed
                date_needs_processing = True
            
            if date_needs_processing:
                dates_to_process.append(current)
            
            current += timedelta(days=1)
    
        # Sort based on direction
        if forward:
            dates_to_process.sort()
        else:
            dates_to_process.sort(reverse=True)
    
        return dates_to_process
    
    def _assign_day_shifts_with_relaxation(self, date, attempt_number=0, relaxation_level=0):
        """Assign shifts for a given date with optional constraint relaxation"""
        logging.debug(f"Assigning shifts for {date.strftime('%Y-%m-%d')} (relaxation level: {relaxation_level})")
    
        if date not in self.schedule:
            self.schedule[date] = []

        remaining_shifts = self.num_shifts - len(self.schedule[date])
    
        for post in range(len(self.schedule[date]), self.num_shifts):
            # Try each relaxation level until we succeed or run out of options
            for relax_level in range(relaxation_level + 1):
                candidates = self._get_candidates(date, post, relax_level)
            
                if candidates:
                    # Sort candidates by score (descending)
                    candidates.sort(key=lambda x: x[1], reverse=True)
                
                    # Group candidates with similar scores (within 10% of max score)
                    max_score = candidates[0][1]
                    top_candidates = [c for c in candidates if c[1] >= max_score * 0.9]
                
                    # Add some randomness to selection based on attempt number
                    random.Random(attempt_number + date.toordinal() + post).shuffle(top_candidates)
                
                    # Select the first candidate
                    best_worker = top_candidates[0][0]
                    worker_id = best_worker['id']
                
                    # Assign the worker
                    self.schedule[date].append(worker_id)
                    self.worker_assignments[worker_id].add(date)
                    self.data_manager._update_tracking_data(worker_id, date, post)
                
                    logging.debug(f"Assigned worker {worker_id} to {date}, post {post}")
                    break  # Success at this relaxation level
            else:
                # If we've tried all relaxation levels and still failed, leave shift unfilled
                self.schedule[date].append(None)
                logging.debug(f"No suitable worker found for {date}, post {post} - shift unfilled")
                                
    def _get_candidates(self, date, post, relaxation_level=0):
        """
        Get suitable candidates with their scores using the specified relaxation level
    
        Args:
            date: The date to assign
            post: The post number to assign
            relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
        """
        candidates = []
    
        for worker in self.workers_data:
            worker_id = worker['id']
            work_percentage = float(worker.get('work_percentage', 100))

            # Skip if max shifts reached
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                continue

            # Skip if already assigned to this date
            if worker_id in self.schedule[date]:
                continue
        
            # CRITICAL: Hard constraints that should never be relaxed
        
            # 1. Check worker availability (days off)
            if self._is_worker_unavailable(worker_id, date):
                continue
        
            # 2. Check for incompatibilities - CRITICAL
            if not self._check_incompatibility(worker_id, date):
                continue
    
            # Check gap constraints with appropriate relaxation
            passed_gap = True
            assignments = sorted(self.worker_assignments[worker_id])
    
            if assignments:
                # CRITICAL: Always maintain minimum gap of 2 days regardless of relaxation
                min_gap = 2  # Never go below 2 days
        
                # Check minimum gap
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
            
                    # CRITICAL: Basic minimum gap check - never relax below 2
                    if days_between < min_gap:
                        passed_gap = False
                        break
            
                    # Relax some non-critical gap constraints to improve coverage
                    if relaxation_level < 2:  # Only enforce at lower relaxation levels
                        # Special rule for full-time workers: No Friday -> Monday assignments
                        if ((prev_date.weekday() == 4 and date.weekday() == 0) or 
                            (date.weekday() == 4 and prev_date.weekday() == 0)):
                            if days_between == 3:  # The gap between Friday and Monday
                                passed_gap = False
                                break
            
                    # Allow same day of week in consecutive weeks at higher relaxation
                    if relaxation_level == 0 and days_between in [7, 14, 21]:
                        passed_gap = False
                        break
    
            if not passed_gap:
                continue
    
            # CRITICAL: Check weekend limit constraints - never relax completely
            # But allow more flexibility at higher relaxation levels
            if relaxation_level == 0 and self._would_exceed_weekend_limit(worker_id, date):
                continue
    
            # Calculate score
            score = self._calculate_worker_score(worker, date, post, relaxation_level)
            if score > float('-inf'):
                candidates.append((worker, score))

        return candidates
    
    def _calculate_worker_score(self, worker, date, post, relaxation_level=0):
        """
        Calculate score for a worker assignment with optional relaxation of constraints
    
        Args:
            worker: The worker to evaluate
            date: The date to assign
            post: The post number to assign
            relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
    
        Returns:
            float: Score for this worker-date-post combination, higher is better
                  Returns float('-inf') if assignment is invalid
        """
        try:
            worker_id = worker['id']
            score = 0
        
            # --- Hard Constraints (never relaxed) ---
        
            # Basic availability check
            if self._is_worker_unavailable(worker_id, date) or worker_id in self.schedule.get(date, []):
                return float('-inf')
            
            # --- Check for mandatory shifts ---
            worker_data = worker
            mandatory_days = worker_data.get('mandatory_days', [])
            mandatory_dates = self._parse_dates(mandatory_days)
        
            # If this is a mandatory date for this worker, give it maximum priority
            if date in mandatory_dates:
                return float('inf')  # Highest possible score to ensure mandatory shifts are assigned
        
            # --- Target Shifts Check (excluding mandatory shifts) ---
            current_shifts = len(self.worker_assignments[worker_id])
            target_shifts = worker.get('target_shifts', 0)
        
            # Count mandatory shifts that are already assigned
            mandatory_shifts_assigned = sum(
                1 for d in self.worker_assignments[worker_id] if d in mandatory_dates
            )
        
            # Count mandatory shifts still to be assigned
            mandatory_shifts_remaining = sum(
                1 for d in mandatory_dates 
                if d >= date and d not in self.worker_assignments[worker_id]
            )
        
            # Calculate non-mandatory shifts target
            non_mandatory_target = target_shifts - len(mandatory_dates)
            non_mandatory_assigned = current_shifts - mandatory_shifts_assigned
        
            # Check if we've already met or exceeded non-mandatory target
            shift_difference = non_mandatory_target - non_mandatory_assigned
        
            # Reserve capacity for remaining mandatory shifts
            if non_mandatory_assigned + mandatory_shifts_remaining >= target_shifts and relaxation_level < 2:
                return float('-inf')  # Need to reserve remaining slots for mandatory shifts
        
            # Stop if worker already met or exceeded non-mandatory target (except at higher relaxation)
            if shift_difference <= 0:
                if relaxation_level < 2:
                    return float('-inf')  # Strict limit at relaxation levels 0-1
                else:
                    score -= 10000  # Severe penalty but still possible at highest relaxation
            else:
                # Higher priority for workers further from their non-mandatory target
                score += shift_difference * 1000
        
            # --- Gap Constraints ---
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if assignments:
                work_percentage = worker.get('work_percentage', 100)
                # Minimum gap is higher for part-time workers
                min_gap = 3 if work_percentage < 100 else 2
            
                # Check if any previous assignment violates minimum gap
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                
                    # Basic minimum gap check
                    if days_between < min_gap:
                        return float('-inf')
                
                    # Special rule for full-time workers: No Friday + Monday (3-day gap)
                    if work_percentage >= 100 and relaxation_level == 0:
                        if ((prev_date.weekday() == 4 and date.weekday() == 0) or 
                            (date.weekday() == 4 and prev_date.weekday() == 0)):
                            if days_between == 3:
                                return float('-inf')
                
                    # Prevent same day of week in consecutive weeks (can be relaxed)
                    if relaxation_level < 2 and days_between in [7, 14, 21]:
                        return float('-inf')
        
            # --- Weekend Limits ---
            if relaxation_level < 2 and self._would_exceed_weekend_limit(worker_id, date):
                return float('-inf')
        
            # --- Weekday Balance Check ---
            weekday = date.weekday()
            weekday_counts = self.worker_weekdays[worker_id].copy()
            weekday_counts[weekday] += 1  # Simulate adding this assignment
        
            max_weekday = max(weekday_counts.values())
            min_weekday = min(weekday_counts.values())
        
            # If this assignment would create more than 1 day difference, reject it
            if (max_weekday - min_weekday) > 1 and relaxation_level < 1:
                return float('-inf')
        
            # --- Scoring Components (softer constraints) ---
        
            # 1. Weekend Balance Score
            if date.weekday() >= 4:  # Friday, Saturday, Sunday
                weekend_assignments = sum(
                    1 for d in self.worker_assignments[worker_id]
                    if d.weekday() >= 4
                )
                # Lower score for workers with more weekend assignments
                score -= weekend_assignments * 300
        
            # 2. Post Rotation Score - focus especially on last post distribution
            last_post = self.num_shifts - 1
            if post == last_post:  # Special handling for the last post
                post_counts = self._get_post_counts(worker_id)
                total_assignments = sum(post_counts.values()) + 1  # +1 for this potential assignment
                target_last_post = total_assignments * (1 / self.num_shifts)
                current_last_post = post_counts.get(last_post, 0)
            
                # Encourage assignments when below target
                if current_last_post < target_last_post - 1:
                    score += 1000
                # Discourage assignments when above target
                elif current_last_post > target_last_post + 1:
                    score -= 1000
        
            # 3. Weekly Balance Score - avoid concentration in some weeks
            week_number = date.isocalendar()[1]
            week_counts = {}
            for d in self.worker_assignments[worker_id]:
                w = d.isocalendar()[1]
                week_counts[w] = week_counts.get(w, 0) + 1
        
            current_week_count = week_counts.get(week_number, 0)
            avg_week_count = len(assignments) / max(1, len(week_counts))
        
            if current_week_count < avg_week_count:
                score += 500  # Bonus for weeks with fewer assignments
        
            # 4. Schedule Progression Score - adjust priority as schedule fills up
            schedule_completion = sum(len(shifts) for shifts in self.schedule.values()) / (
                (self.end_date - self.start_date).days * self.num_shifts)
        
            # Higher weight for target difference as schedule progresses
            score += shift_difference * 500 * schedule_completion
        
            # Log the score calculation
            logging.debug(f"Score for worker {worker_id}: {score} "
                        f"(current: {current_shifts}, target: {target_shifts}, "
                        f"relaxation: {relaxation_level})")
        
            return score
    
        except Exception as e:
            logging.error(f"Error calculating score for worker {worker['id']}: {str(e)}")
            return float('-inf')
        
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
    
    def _try_fill_empty_shifts(self):
        """
        Try to fill empty shifts while STRICTLY enforcing all mandatory constraints:
        - Minimum 2-day gap between shifts
        - Maximum 3 weekend shifts in any 3-week period
        - No overriding of mandatory shifts
        """
        empty_shifts = []

        # Find all empty shifts
        for date, workers in self.schedule.items():
            for post, worker in enumerate(workers):
                if worker is None:
                    empty_shifts.append((date, post))
    
        if not empty_shifts:
            return False
    
        logging.info(f"Attempting to fill {len(empty_shifts)} empty shifts")
        for worker in self.workers_data:
            worker_id = worker['id']

       # Sort empty shifts by date (earlier dates first)
        empty_shifts.sort(key=lambda x: x[0])
    
        shifts_filled = 0
    
        # Try to fill each empty shift
        for date, post in empty_shifts:
            # Get candidates that satisfy ALL constraints (no relaxation)
            candidates = []
        
            for worker in self.workers_data:
                worker_id = worker['id']
            
                # Skip if already assigned to this date
                if worker_id in self.schedule[date]:
                    continue
            
                # Check if worker can be assigned (with strict constraints)
                if self._can_assign_worker(worker_id, date, post):
                    # Calculate score for this assignment
                    score = self._calculate_worker_score(worker, date, post, relaxation_level=0)
                    if score > float('-inf'):
                        candidates.append((worker, score))
        
            if candidates:
                # Sort candidates by score (highest first)
                candidates.sort(key=lambda x: x[1], reverse=True)
            
                # Select the best candidate
                best_worker = candidates[0][0]
                worker_id = best_worker['id']
            
                # Assign the worker
                self.schedule[date][post] = worker_id
                self.worker_assignments[worker_id].add(date)
                self.data_manager._update_tracking_data(worker_id, date, post)
            
                logging.info(f"Filled empty shift on {date} post {post} with worker {worker_id}")
                shifts_filled += 1
    
        logging.info(f"Filled {shifts_filled} of {len(empty_shifts)} empty shifts")
        return shifts_filled > 0
    
    def _balance_workloads(self):
        """
        Balance the total number of assignments among workers based on their work percentages
        While strictly enforcing all mandatory constraints:
        - Minimum 2-day gap between shifts
        - Maximum 3 weekend shifts in any 3-week period
        """
        logging.info("Attempting to balance worker workloads")
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()

        # First verify and fix data consistency
        self._verify_assignment_consistency()

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
                # Skip if this date is mandatory for this worker
                worker_data = next((w for w in self.workers_data if w['id'] == over_worker_id), None)
                mandatory_days = worker_data.get('mandatory_days', []) if worker_data else []
                mandatory_dates = self._parse_dates(mandatory_days)
        
                if date in mandatory_dates:
                    continue
            
                # Make sure the worker is actually in the schedule for this date
                if date not in self.schedule:
                    # This date is in worker_assignments but not in schedule
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date} but date is not in schedule")
                    continue
                
                try:
                    # Find the post this worker is assigned to
                    if over_worker_id not in self.schedule[date]:
                        # Worker is supposed to be assigned to this date but isn't in the schedule
                        logging.warning(f"Worker {over_worker_id} has assignment for date {date} but is not in schedule")
                        continue
                    
                    post = self.schedule[date].index(over_worker_id)
                    possible_shifts.append((date, post))
                except ValueError:
                    # Worker not found in schedule for this date
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date} but is not in schedule")
                    continue
    
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
                        self.data_manager._update_tracking_data(worker_id, date, post)
                
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

    def _improve_post_rotation(self):
        """Improve post rotation by swapping assignments"""
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()
    
        # Verify and fix data consistency before proceeding
        self._verify_assignment_consistency()        # Find workers with imbalanced post distribution
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
                            self.data_manager._update_tracking_data(worker_id, date, post)
                        
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

    def _improve_weekend_distribution(self):
        """
        Improve weekend distribution by balancing weekend shifts more evenly among workers
        and attempting to resolve weekend overloads
        """
        logging.info("Attempting to improve weekend distribution")
    
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()

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
                            self.data_manager._update_tracking_data(worker_id, date, post)
                        
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
    
    def _fix_incompatibility_violations(self):
            """
            Check the entire schedule for incompatibility violations and fix them
            by reassigning incompatible workers to different days
            """
            logging.info("Checking and fixing incompatibility violations")
    
            violations_fixed = 0
            violations_found = 0
    
            # Check each date for incompatible worker assignments
            for date in sorted(self.schedule.keys()):
                workers_today = [w for w in self.schedule[date] if w is not None]
        
                # Check each pair of workers
                for i, worker1_id in enumerate(workers_today):
                    for worker2_id in workers_today[i+1:]:
                        # Check if these workers are incompatible
                        if self._are_workers_incompatible(worker1_id, worker2_id):
                            violations_found += 1
                            logging.warning(f"Found incompatibility violation: {worker1_id} and {worker2_id} on {date}")
                    
                            # Try to fix the violation by moving one of the workers
                            # Let's try to move the second worker first
                            if self._try_reassign_worker(worker2_id, date):
                                violations_fixed += 1
                                logging.info(f"Fixed by reassigning {worker2_id} from {date}")
                            # If that didn't work, try moving the first worker
                            elif self._try_reassign_worker(worker1_id, date):
                                violations_fixed += 1
                                logging.info(f"Fixed by reassigning {worker1_id} from {date}")
    
            logging.info(f"Incompatibility check: found {violations_found} violations, fixed {violations_fixed}")
            return violations_fixed > 0
        
    def _try_reassign_worker(self, worker_id, date):
        """
        Try to find a new date to assign this worker to fix an incompatibility
        """
        # Find the position this worker is assigned to
        try:
            post = self.schedule[date].index(worker_id)
        except ValueError:
            return False
    
        # First, try to find a date with an empty slot for the same post
        current_date = self.start_date
        while current_date <= self.end_date:
            # Skip the current date
            if current_date == date:
                current_date += timedelta(days=1)
                continue
            
            # Check if this date has an empty slot at the same post
            if (current_date in self.schedule and 
                len(self.schedule[current_date]) > post and 
                self.schedule[current_date][post] is None):
            
                # Check if worker can be assigned to this date
                if self._can_assign_worker(worker_id, current_date, post):
                    # Remove from original date
                    self.schedule[date][post] = None
                    self.worker_assignments[worker_id].remove(date)
                
                    # Assign to new date
                    self.schedule[current_date][post] = worker_id
                    self.worker_assignments[worker_id].add(current_date)
                
                    # Update tracking data
                    self._update_worker_stats(worker_id, date, removing=True)
                    self.data_manager._update_tracking_data(worker_id, date, post)
                
                    return True
                
            current_date += timedelta(days=1)
    
        # If we couldn't find a new assignment, just remove this worker
        self.schedule[date][post] = None
        self.worker_assignments[worker_id].remove(date)
        self._update_worker_stats(worker_id, date, removing=True)
    
        return True
    
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
