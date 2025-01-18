from datetime import datetime, timedelta
import calendar

class Scheduler:
    def __init__(self, config):
        self.config = config
        self.start_date = config['start_date']
        self.end_date = config['end_date']
        self.num_shifts = config['num_shifts']
        self.workers_data = config['workers_data']
        self.schedule = {}
        self.worker_assignments = {w['id']: [] for w in self.workers_data}
        self.current_datetime = datetime(2025, 1, 17, 23, 39, 1)  # Updated timestamp
        self.current_user = 'saldo27'

    def generate_schedule(self):
        """Generate the complete guard schedule following all conditions"""
        print(f"Starting schedule generation at {self.current_datetime}")
        print(f"Generator: {self.current_user}")

        try:
            # Step 1: Process mandatory guards first
            self._assign_mandatory_guards()

            # Step 2: Calculate target shifts for each worker
            self._calculate_target_shifts()

            # Step 3: Fill remaining shifts
            self._fill_remaining_shifts()

            return self.schedule

        except Exception as e:
            print(f"Error generating schedule: {str(e)}")
            raise

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
        if date not in self.schedule:
            self.schedule[date] = []

        while len(self.schedule[date]) < self.num_shifts:
            best_worker = self._find_best_worker(date)
            if best_worker:
                self.schedule[date].append(best_worker['id'])
                self.worker_assignments[best_worker['id']].append(date)
                print(f"Assigned shift: {best_worker['id']} on {date.strftime('%Y-%m-%d')}")
            else:
                print(f"Warning: Could not find suitable worker for {date.strftime('%Y-%m-%d')}")
                break

    def _find_best_worker(self, date):
        """Find the most suitable worker for a given date"""
        candidates = []
        
        for worker in self.workers_data:
            if self._can_assign_worker(worker['id'], date):
                score = self._calculate_worker_score(worker, date)
                candidates.append((worker, score))

        if not candidates:
            return None

        return max(candidates, key=lambda x: x[1])[0]

    def _can_assign_worker(self, worker_id, date):
        """Check if a worker can be assigned to a specific date"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)

        # Check 1: Already assigned that day
        if date in self.worker_assignments[worker_id]:
            return False

        # Check 2: Days off (moved up in priority)
        if worker.get('days_off'):
            off_days = self._parse_dates(worker['days_off'])
            if date in off_days:
                return False

        # Check 3: Within work periods
        if worker.get('work_periods'):
            work_periods = self._parse_date_ranges(worker['work_periods'])
            if not any(start <= date <= end for start, end in work_periods):
                return False

        # Check 4: Minimum distance between guards (4/percentage)
        min_distance = max(2, int(4 / (float(worker.get('work_percentage', 100)) / 100)))
        assignments = sorted(self.worker_assignments[worker_id])

        if assignments:
            # Check past assignments
            for prev_date in reversed(assignments):
                days_between = abs((date - prev_date).days)
                if days_between < min_distance:
                    return False
                if days_between in [7, 14, 21]:  # Prohibited intervals
                    return False

        return True

    def _calculate_worker_score(self, worker, date):
        """Calculate a score for worker assignment suitability"""
        score = 0.0

        # Factor 1: Mandatory days (highest priority - 50% weight)
        if worker.get('mandatory_days'):
            mandatory_dates = self._parse_dates(worker['mandatory_days'])
            if date in mandatory_dates:
                score += 1000  # Very high score for mandatory days

        # Factor 2: Distance from target shifts (25% weight)
        current_shifts = len(self.worker_assignments[worker['id']])
        shift_difference = worker['target_shifts'] - current_shifts
        score += shift_difference * 25

        # Factor 3: Monthly balance (25% weight)
        month_shifts = sum(1 for d in self.worker_assignments[worker['id']]
                          if d.year == date.year and d.month == date.month)
        target_month_shifts = worker['target_shifts'] / ((self.end_date.year * 12 + self.end_date.month) -
                                                    (self.start_date.year * 12 + self.start_date.month) + 1)
        monthly_balance = target_month_shifts - month_shifts
        score += monthly_balance * 25

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

    def validate_schedule(self):
        """Validate the generated schedule"""
        errors = []
        warnings = []

        # Check 1: Every day has guards
        current = self.start_date
        while current <= self.end_date:
            if current not in self.schedule:
                errors.append(f"No guards assigned on {current.strftime('%Y-%m-%d')}")
            elif len(self.schedule[current]) < self.num_shifts:
                warnings.append(f"Insufficient guards on {current.strftime('%Y-%m-%d')}")
            current += timedelta(days=1)

        # Check 2: Guard spacing rules
        for worker_id, dates in self.worker_assignments.items():
            sorted_dates = sorted(dates)
            for i in range(len(sorted_dates) - 1):
                days_between = (sorted_dates[i+1] - sorted_dates[i]).days
                if days_between in [7, 14, 21]:
                    errors.append(f"Invalid spacing ({days_between} days) for worker {worker_id}")

        # Check 3: Monthly distribution
        for worker in self.workers_data:
            worker_id = worker['id']
            monthly_counts = {}
            for date in self.worker_assignments[worker_id]:
                key = f"{date.year}-{date.month}"
                monthly_counts[key] = monthly_counts.get(key, 0) + 1

            if monthly_counts:
                avg = sum(monthly_counts.values()) / len(monthly_counts)
                max_deviation = max(abs(count - avg) for count in monthly_counts.values())
                if max_deviation > avg * 0.2:  # 20% deviation threshold
                    warnings.append(f"Uneven monthly distribution for worker {worker_id}")

        return errors, warnings
