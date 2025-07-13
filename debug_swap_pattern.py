"""
Debug swap 7/14 day pattern issue
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

def debug_swap_pattern():
    """Debug the swap 7/14 day pattern issue"""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 21)
    
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
    
    # Setup the scenario
    monday1 = datetime(2024, 1, 1)  # Monday week 1
    monday2 = datetime(2024, 1, 8)  # Monday week 2 (7 days apart, same weekday)
    
    print(f"Setting up swap scenario:")
    print(f"  W001 -> {monday1.strftime('%Y-%m-%d')} (Monday)")
    print(f"  W002 -> {monday2.strftime('%Y-%m-%d')} (Monday, 7 days later)")
    print(f"  Swapping should violate 7/14 day pattern for both workers")
    
    # Assign workers
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', monday1, 0, force=True)
    result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W002', monday2, 0, force=True)
    print(f"  Assignment results: {result1.success}, {result2.success}")
    
    # Check current assignments
    w001_assignments = scheduler.worker_assignments.get('W001', set())
    w002_assignments = scheduler.worker_assignments.get('W002', set())
    print(f"  W001 assignments: {[d.strftime('%Y-%m-%d') for d in w001_assignments]}")
    print(f"  W002 assignments: {[d.strftime('%Y-%m-%d') for d in w002_assignments]}")
    
    # Now test what happens when we check constraints for the swap
    print(f"\nTesting swap constraint logic:")
    
    # Test W001 going to monday2 (where W002 currently is)
    print(f"  Can W001 go to {monday2.strftime('%Y-%m-%d')}?")
    can_w001_go_to_monday2, reason1 = scheduler.constraint_checker._check_constraints('W001', monday2)
    print(f"    Direct constraint check: {can_w001_go_to_monday2} ({reason1})")
    
    # Test W002 going to monday1 (where W001 currently is)
    print(f"  Can W002 go to {monday1.strftime('%Y-%m-%d')}?")
    can_w002_go_to_monday1, reason2 = scheduler.constraint_checker._check_constraints('W002', monday1)
    print(f"    Direct constraint check: {can_w002_go_to_monday1} ({reason2})")
    
    # Test the swap constraint checking method directly
    print(f"\nTesting swap constraint method:")
    swap_result = scheduler.real_time_engine.incremental_updater._check_swap_constraints(
        'W001', monday1, 0, 'W002', monday2, 0
    )
    print(f"  Swap constraint result: {swap_result.success} - {swap_result.message}")
    if swap_result.conflicts:
        print(f"  Conflicts: {swap_result.conflicts}")
    
    # Test the actual swap
    print(f"\nTesting actual swap:")
    actual_swap = scheduler.real_time_engine.incremental_updater.swap_workers(monday1, 0, monday2, 0)
    print(f"  Actual swap result: {actual_swap.success} - {actual_swap.message}")
    if actual_swap.conflicts:
        print(f"  Conflicts: {actual_swap.conflicts}")
    
    # Final analysis
    print(f"\n📊 Analysis:")
    print(f"  Expected: Both workers should fail 7/14 day pattern check")
    print(f"  W001->Monday2: {'❌ FAIL' if not can_w001_go_to_monday2 else '✅ UNEXPECTED PASS'}")
    print(f"  W002->Monday1: {'❌ FAIL' if not can_w002_go_to_monday1 else '✅ UNEXPECTED PASS'}")
    print(f"  Swap should fail: {'✅ CORRECTLY FAILED' if not actual_swap.success else '❌ INCORRECTLY SUCCEEDED'}")

if __name__ == "__main__":
    debug_swap_pattern()