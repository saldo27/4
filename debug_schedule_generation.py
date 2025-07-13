"""
Debug schedule generation to understand why constraint violations are still occurring.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler

# Configure logging for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def debug_schedule_generation():
    """Debug why schedule generation allows constraint violations"""
    print("\n" + "="*60)
    print("DEBUG: Schedule Generation Constraint Violations")
    print("="*60)
    
    # Create test configuration - same as the failing test
    start_date = datetime(2024, 1, 1)  # Monday
    end_date = datetime(2024, 2, 1)    # Full month
    
    workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': []},
        {'id': 'W002', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': []},
    ]
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_workers': len(workers_data),
        'num_shifts': 1,
        'gap_between_shifts': 2,
        'max_consecutive_weekends': 2,
        'holidays': [],
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    print(f"Configuration:")
    print(f"  Workers: {len(workers_data)}")
    print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Gap between shifts: {config['gap_between_shifts']}")
    print(f"  Max consecutive weekends: {config['max_consecutive_weekends']}")
    
    # Generate schedule 
    print(f"\nGenerating schedule...")
    result = scheduler.generate_schedule()
    print(f"Schedule generation result: {result}")
    
    # Analyze the generated schedule for violations
    print(f"\nAnalyzing generated schedule for constraint violations...")
    
    violations = []
    
    # Check for 7/14 day pattern violations (weekdays only)
    print(f"Checking for 7/14 day pattern violations (weekdays only)...")
    for worker_id in ['W001', 'W002']:
        worker_dates = []
        for date, shifts in scheduler.schedule.items():
            for i, assigned_worker in enumerate(shifts):
                if assigned_worker == worker_id:
                    worker_dates.append(date)
        
        worker_dates.sort()
        
        for i, date1 in enumerate(worker_dates):
            for j, date2 in enumerate(worker_dates[i+1:], i+1):
                days_diff = (date2 - date1).days
                # Only check weekdays (0-3 = Mon-Thu) for 7/14 day pattern
                if (days_diff == 7 or days_diff == 14) and date1.weekday() == date2.weekday():
                    if date1.weekday() < 4 and date2.weekday() < 4:  # Both are weekdays
                        violations.append(f"Worker {worker_id}: 7/14 day pattern violation - {date1.strftime('%Y-%m-%d')} and {date2.strftime('%Y-%m-%d')} ({days_diff} days apart, both {date1.strftime('%A')}s)")
    
    # Check for consecutive weekend violations  
    print(f"Checking for consecutive weekend violations...")
    for worker_id in ['W001', 'W002']:
        weekend_dates = []
        for date, shifts in scheduler.schedule.items():
            for i, assigned_worker in enumerate(shifts):
                if assigned_worker == worker_id and date.weekday() >= 4:  # Fri, Sat, Sun
                    weekend_dates.append(date)
        
        weekend_dates.sort()
        
        # Check for consecutive weekends using the same logic as constraint_checker
        if len(weekend_dates) > 0:
            # Group weekend dates that are on consecutive calendar weekends  
            consecutive_groups = []
            current_group = []
            
            for i, weekend_date in enumerate(weekend_dates):
                if not current_group:
                    current_group = [weekend_date]
                else:
                    prev_date = current_group[-1]
                    days_between = (weekend_date - prev_date).days
                    
                    # Consecutive weekends typically have 5-10 days between them
                    if 5 <= days_between <= 10:
                        current_group.append(weekend_date)
                    else:
                        consecutive_groups.append(current_group)
                        current_group = [weekend_date]
            
            if current_group:
                consecutive_groups.append(current_group)
            
            # Find groups that exceed the limit
            for group in consecutive_groups:
                if len(group) > config['max_consecutive_weekends']:
                    violations.append(f"Worker {worker_id}: Consecutive weekend violation - {len(group)} consecutive weekends (limit: {config['max_consecutive_weekends']}): {[d.strftime('%Y-%m-%d') for d in group]}")
    
    # Print detailed schedule for weekend analysis
    print(f"\n📅 Generated Schedule Analysis:")
    print(f"Total dates in schedule: {len(scheduler.schedule)}")
    
    weekend_assignments = {}
    for date, shifts in scheduler.schedule.items():
        for i, worker in enumerate(shifts):
            if worker and date.weekday() >= 4:  # Weekend day
                if worker not in weekend_assignments:
                    weekend_assignments[worker] = []
                weekend_assignments[worker].append(date)
    
    for worker, dates in weekend_assignments.items():
        dates.sort()
        print(f"  {worker} weekend assignments: {[d.strftime('%Y-%m-%d (%a)') for d in dates]}")
    
    # Results
    print(f"\n📊 Constraint Violation Analysis:")
    print(f"  Total violations found: {len(violations)}")
    
    if violations:
        print(f"\n❌ VIOLATIONS DETECTED:")
        for violation in violations:
            print(f"     {violation}")
    else:
        print(f"\n✅ No constraint violations found!")
    
    # Test constraint checker directly on violations
    if violations:
        print(f"\n🔍 Testing constraint checker on detected violations...")
        
        # Test a specific weekend violation if found
        for violation in violations:
            if "Consecutive weekend violation" in violation:
                worker_id = violation.split(":")[0].split()[-1]
                # Find the dates for this worker
                weekend_dates = []
                for date, shifts in scheduler.schedule.items():
                    for i, assigned_worker in enumerate(shifts):
                        if assigned_worker == worker_id and date.weekday() >= 4:
                            weekend_dates.append(date)
                weekend_dates.sort()
                
                if len(weekend_dates) >= 3:
                    third_date = weekend_dates[2]  # Third weekend in sequence
                    print(f"  Testing constraint checker for {worker_id} on {third_date.strftime('%Y-%m-%d')}...")
                    
                    # Clear current assignment temporarily to test
                    original_schedule = dict(scheduler.schedule)
                    original_assignments = dict(scheduler.worker_assignments)
                    
                    # Rebuild assignments up to the second weekend
                    scheduler.worker_assignments[worker_id] = set(weekend_dates[:2])
                    
                    # Test if constraint checker would block the third
                    would_exceed = scheduler.constraint_checker._would_exceed_weekend_limit(worker_id, third_date)
                    print(f"    Constraint checker says would_exceed_weekend_limit: {would_exceed}")
                    
                    # Restore original state
                    scheduler.schedule = original_schedule
                    scheduler.worker_assignments = original_assignments
                
                break
    
    return len(violations) == 0


if __name__ == "__main__":
    success = debug_schedule_generation()
    if success:
        print("\n✅ Schedule generation respects all constraints!")
    else:
        print("\n❌ Schedule generation violates constraints!")