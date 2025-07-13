"""
Debug weekend constraint checking to understand why it's failing.
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

def debug_weekend_constraint():
    """Debug weekend constraint in detail"""
    print("\n" + "="*60)
    print("DEBUG: Weekend Constraint Analysis")
    print("="*60)
    
    # Create test configuration
    start_date = datetime(2024, 1, 1)  # Monday
    end_date = datetime(2024, 1, 21)   # 3 weeks
    
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
    
    # Weekend dates  
    saturday1 = datetime(2024, 1, 6)  # Saturday week 1
    saturday2 = datetime(2024, 1, 13) # Saturday week 2 (consecutive weekend)
    saturday3 = datetime(2024, 1, 20) # Saturday week 3 (would be 3rd consecutive)
    
    print(f"Testing consecutive Saturday scenario:")
    print(f"  {saturday1.strftime('%Y-%m-%d')} (Saturday week 1) - weekday: {saturday1.weekday()}")
    print(f"  {saturday2.strftime('%Y-%m-%d')} (Saturday week 2) - weekday: {saturday2.weekday()}")
    print(f"  {saturday3.strftime('%Y-%m-%d')} (Saturday week 3) - weekday: {saturday3.weekday()}")
    
    # Test constraint checker directly
    print(f"\n--- Testing Constraint Checker Directly ---")
    
    # Assign first Saturday manually 
    scheduler.schedule[saturday1] = ['W001']
    scheduler.worker_assignments['W001'] = {saturday1}
    print(f"Manually assigned W001 to {saturday1.strftime('%Y-%m-%d')}")
    
    # Check if second Saturday would violate constraints
    print(f"Checking if W001 can be assigned to {saturday2.strftime('%Y-%m-%d')}:")
    would_exceed_2 = scheduler.constraint_checker._would_exceed_weekend_limit('W001', saturday2)
    print(f"  _would_exceed_weekend_limit result: {would_exceed_2}")
    
    # Assign second Saturday manually
    scheduler.schedule[saturday2] = ['W001']
    scheduler.worker_assignments['W001'].add(saturday2)
    print(f"Manually assigned W001 to {saturday2.strftime('%Y-%m-%d')}")
    
    # Check if third Saturday would violate constraints
    print(f"Checking if W001 can be assigned to {saturday3.strftime('%Y-%m-%d')}:")
    would_exceed_3 = scheduler.constraint_checker._would_exceed_weekend_limit('W001', saturday3)
    print(f"  _would_exceed_weekend_limit result: {would_exceed_3}")
    
    # Print current assignments
    print(f"\nCurrent assignments for W001: {sorted(list(scheduler.worker_assignments['W001']))}")
    
    # Test with incremental updater WITHOUT force
    print(f"\n--- Testing Incremental Updater (no force) ---")
    
    # Clear schedule and start fresh
    scheduler.schedule = {}
    scheduler.worker_assignments = {'W001': set()}
    
    # Assign first Saturday via incremental updater
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday1, 0)
    print(f"Assign Saturday 1: {result1.success} - {result1.message}")
    
    # Assign second Saturday via incremental updater
    result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday2, 0)
    print(f"Assign Saturday 2: {result2.success} - {result2.message}")
    
    # Try to assign third Saturday via incremental updater (should fail)
    result3 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday3, 0)
    print(f"Assign Saturday 3: {result3.success} - {result3.message}")
    
    print(f"\nExpected results:")
    print(f"  Saturday 1: ✓ Should succeed")
    print(f"  Saturday 2: ✓ Should succeed (within limit of 2)")
    print(f"  Saturday 3: ✗ Should fail (would exceed limit of 2)")
    
    # Analyze why it might be failing
    print(f"\n--- Analyzing Weekend Detection ---")
    dates_to_check = [saturday1, saturday2, saturday3]
    for date in dates_to_check:
        is_weekend = (date.weekday() >= 4 or 
                     date in scheduler.holidays or
                     (date + timedelta(days=1)) in scheduler.holidays)
        print(f"  {date.strftime('%Y-%m-%d')} (weekday {date.weekday()}): is_weekend={is_weekend}")
    
    return result1.success and result2.success and not result3.success


if __name__ == "__main__":
    success = debug_weekend_constraint()
    if success:
        print("\n✅ Weekend constraint is working correctly!")
    else:
        print("\n❌ Weekend constraint is NOT working properly!")