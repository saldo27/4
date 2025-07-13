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

def debug_weekend_test():
    """Debug the weekend limit test"""
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
        'gap_between_shifts': 1,  # Allow closer assignments for this test
        'max_consecutive_weekends': 2,  # Maximum 2 consecutive weekends
        'holidays': [datetime(2024, 1, 15)],  # Add a holiday Monday
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    # Weekend/Holiday dates
    friday1 = datetime(2024, 1, 5)    # Friday week 1
    saturday1 = datetime(2024, 1, 6)  # Saturday week 1
    friday2 = datetime(2024, 1, 12)   # Friday week 2
    saturday2 = datetime(2024, 1, 13) # Saturday week 2
    friday3 = datetime(2024, 1, 19)   # Friday week 3
    
    print("Testing weekend assignment scenario:")
    print(f"Max consecutive weekends: {scheduler.max_consecutive_weekends}")
    
    # Assign first weekend
    print(f"\n1. Assigning to {friday1.strftime('%Y-%m-%d')} (Friday week 1)")
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', friday1, 0, force=True)
    print(f"   Result: {result1.success}")
    
    print(f"\n2. Assigning to {saturday1.strftime('%Y-%m-%d')} (Saturday week 1)")
    result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday1, 0, force=True)
    print(f"   Result: {result2.success}")
    
    # Assign second weekend
    print(f"\n3. Assigning to {friday2.strftime('%Y-%m-%d')} (Friday week 2)")
    result3 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', friday2, 0, force=True)
    print(f"   Result: {result3.success}")
    
    print(f"\n4. Assigning to {saturday2.strftime('%Y-%m-%d')} (Saturday week 2)")
    result4 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday2, 0, force=True)
    print(f"   Result: {result4.success}")
    
    # Check current assignments
    assignments = scheduler.worker_assignments.get('W001', set())
    print(f"\nCurrent assignments for W001: {sorted([d.strftime('%Y-%m-%d') for d in assignments])}")
    
    # Now test third weekend (should fail)
    print(f"\n5. Testing assignment to {friday3.strftime('%Y-%m-%d')} (Friday week 3 - should fail)")
    
    # Test constraint checker directly
    would_exceed = scheduler.constraint_checker._would_exceed_weekend_limit('W001', friday3)
    print(f"   Constraint checker says would exceed: {would_exceed}")
    
    # Test live validator
    validation_result = scheduler.real_time_engine.live_validator.validate_assignment('W001', friday3, 0)
    print(f"   Live validator says valid: {validation_result.is_valid} - {validation_result.message}")
    
    # Test incremental updater (without force)
    update_result = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', friday3, 0)
    print(f"   Incremental updater success: {update_result.success} - {update_result.message}")

if __name__ == "__main__":
    debug_weekend_test()