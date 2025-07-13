"""
Simple weekend consecutive test
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
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def simple_weekend_test():
    """Simple test to understand weekend consecutive logic"""
    start_date = datetime(2024, 1, 1)  
    end_date = datetime(2024, 1, 21)   
    
    workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': []},
    ]
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_workers': len(workers_data),
        'num_shifts': 1,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2,  # Maximum 2 consecutive weekends
        'holidays': [],
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    # Test with consecutive Saturday assignments
    sat1 = datetime(2024, 1, 6)   # Saturday week 1
    sat2 = datetime(2024, 1, 13)  # Saturday week 2 (7 days later)
    sat3 = datetime(2024, 1, 20)  # Saturday week 3 (7 days later)
    
    print(f"Testing consecutive Saturdays:")
    print(f"  Sat 1: {sat1.strftime('%Y-%m-%d')}")
    print(f"  Sat 2: {sat2.strftime('%Y-%m-%d')} (7 days later)")
    print(f"  Sat 3: {sat3.strftime('%Y-%m-%d')} (7 days later)")
    
    # Assign first two Saturdays
    print(f"\nAssigning first two Saturdays...")
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', sat1, 0, force=True)
    result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', sat2, 0, force=True)
    print(f"  Results: {result1.success}, {result2.success}")
    
    # Test third Saturday
    print(f"\nTesting third Saturday (should fail)...")
    would_exceed = scheduler.constraint_checker._would_exceed_weekend_limit('W001', sat3)
    print(f"  Would exceed weekend limit: {would_exceed}")
    
    # Let's manually check the consecutive grouping logic
    assignments = list(scheduler.worker_assignments.get('W001', set()))
    assignments.append(sat3)  # Add the prospective date
    weekend_dates = [d for d in assignments if d.weekday() >= 4]  # Only Saturday/Sunday
    weekend_dates.sort()
    
    print(f"\nWeekend dates being analyzed: {[d.strftime('%Y-%m-%d') for d in weekend_dates]}")
    
    # Replicate the consecutive grouping logic
    consecutive_groups = []
    current_group = []
    
    for i, weekend_date in enumerate(weekend_dates):
        if not current_group:
            current_group = [weekend_date]
        else:
            prev_date = current_group[-1]
            days_between = (weekend_date - prev_date).days
            print(f"  Days between {prev_date.strftime('%Y-%m-%d')} and {weekend_date.strftime('%Y-%m-%d')}: {days_between}")
        
            # Consecutive weekends typically have 5-10 days between them
            if 5 <= days_between <= 10:
                current_group.append(weekend_date)
                print(f"    Added to current group (now {len(current_group)} items)")
            else:
                consecutive_groups.append(current_group)
                current_group = [weekend_date]
                print(f"    Started new group")
    
    if current_group:
        consecutive_groups.append(current_group)
    
    print(f"\nConsecutive groups: {[[d.strftime('%Y-%m-%d') for d in group] for group in consecutive_groups]}")
    max_consecutive = max(len(group) for group in consecutive_groups) if consecutive_groups else 0
    print(f"Max consecutive: {max_consecutive}")
    print(f"Limit: {scheduler.max_consecutive_weekends}")
    print(f"Would exceed: {max_consecutive > scheduler.max_consecutive_weekends}")

if __name__ == "__main__":
    simple_weekend_test()