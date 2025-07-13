"""
Quick test to debug weekend limit issue 
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

def debug_weekend_test_v2():
    """Debug the weekend limit test with proper scenario"""
    start_date = datetime(2024, 1, 1)  # Monday
    end_date = datetime(2024, 1, 28)   # 4 weeks
    
    workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': []},
    ]
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_workers': len(workers_data),
        'num_shifts': 1,
        'gap_between_shifts': 1,  # Allow closer assignments for this test
        'max_consecutive_weekends': 2,  # Maximum 2 consecutive weekends
        'holidays': [datetime(2024, 1, 15)],  # Add a holiday Monday
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    print(f"Max consecutive weekends: {scheduler.max_consecutive_weekends}")
    
    # Scenario: Assign to multiple consecutive weekend periods to test the limit
    friday1 = datetime(2024, 1, 5)    # Friday week 1
    saturday1 = datetime(2024, 1, 6)  # Saturday week 1
    friday2 = datetime(2024, 1, 12)   # Friday week 2  
    saturday2 = datetime(2024, 1, 13) # Saturday week 2
    
    # Different weekday for third test to avoid 7/14 day pattern conflict
    sunday3 = datetime(2024, 1, 21)   # Sunday week 3 (different from Fri/Sat pattern)
    
    print(f"\nAssigning to consecutive weekend periods:")
    
    # First weekend period
    print(f"1. Assigning to {friday1.strftime('%Y-%m-%d')} (Friday week 1)")
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', friday1, 0, force=True)
    print(f"   Success: {result1.success}")
    
    print(f"2. Assigning to {saturday1.strftime('%Y-%m-%d')} (Saturday week 1)")
    result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday1, 0, force=True)
    print(f"   Success: {result2.success}")
    
    # Second weekend period
    print(f"3. Assigning to {friday2.strftime('%Y-%m-%d')} (Friday week 2)")
    result3 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', friday2, 0, force=True)
    print(f"   Success: {result3.success}")
    
    print(f"4. Assigning to {saturday2.strftime('%Y-%m-%d')} (Saturday week 2)")
    result4 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday2, 0, force=True)
    print(f"   Success: {result4.success}")
    
    # Check assignments
    assignments = sorted([d.strftime('%Y-%m-%d') for d in scheduler.worker_assignments.get('W001', set())])
    print(f"\nCurrent assignments: {assignments}")
    
    # Now test third weekend period (should fail due to consecutive limit)
    print(f"\n5. Testing {sunday3.strftime('%Y-%m-%d')} (Sunday week 3) - should fail consecutive limit")
    
    # Test all three components
    would_exceed = scheduler.constraint_checker._would_exceed_weekend_limit('W001', sunday3)
    print(f"   Constraint checker would_exceed: {would_exceed}")
    
    validation_result = scheduler.real_time_engine.live_validator.validate_assignment('W001', sunday3, 0)
    print(f"   Live validator valid: {validation_result.is_valid} - {validation_result.message}")
    
    update_result = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', sunday3, 0)
    print(f"   Incremental updater success: {update_result.success} - {update_result.message}")
    
    # Summary
    print(f"\n📊 Results:")
    print(f"   Weekend limit should prevent: {'✓ PASS' if would_exceed else '✗ FAIL'}")
    print(f"   Live validator blocks:        {'✓ PASS' if not validation_result.is_valid else '✗ FAIL'}")
    print(f"   Incremental updater blocks:   {'✓ PASS' if not update_result.success else '✗ FAIL'}")
    
    # Check consistency
    consistent = (would_exceed == (not validation_result.is_valid))
    print(f"   Consistency:                  {'✅ CONSISTENT' if consistent else '❌ INCONSISTENT'}")

if __name__ == "__main__":
    debug_weekend_test_v2()