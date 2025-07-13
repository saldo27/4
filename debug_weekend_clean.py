"""
Debug weekend constraint checking more carefully with clean state.
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
    level=logging.INFO,  # Reduce log level to INFO to reduce noise
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def debug_weekend_constraint_clean():
    """Debug weekend constraint with clean state"""
    print("\n" + "="*60)
    print("DEBUG: Weekend Constraint - Clean State Test")
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
    
    # Initialize scheduler with clean state
    scheduler = Scheduler(config)
    
    # Weekend dates  
    saturday1 = datetime(2024, 1, 6)  # Saturday week 1
    saturday2 = datetime(2024, 1, 13) # Saturday week 2 (consecutive weekend)
    saturday3 = datetime(2024, 1, 20) # Saturday week 3 (would be 3rd consecutive)
    
    print(f"Testing consecutive Saturday scenario:")
    print(f"  {saturday1.strftime('%Y-%m-%d')} (Saturday week 1) - weekday: {saturday1.weekday()}")
    print(f"  {saturday2.strftime('%Y-%m-%d')} (Saturday week 2) - weekday: {saturday2.weekday()}")
    print(f"  {saturday3.strftime('%Y-%m-%d')} (Saturday week 3) - weekday: {saturday3.weekday()}")
    
    print(f"\n--- Testing Incremental Updater Step by Step ---")
    
    # Step 1: Assign first Saturday
    print(f"Step 1: Assign W001 to {saturday1.strftime('%Y-%m-%d')}")
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday1, 0)
    print(f"  Result: {result1.success} - {result1.message}")
    
    if result1.success:
        # Step 2: Assign second Saturday
        print(f"Step 2: Assign W001 to {saturday2.strftime('%Y-%m-%d')}")
        result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday2, 0)
        print(f"  Result: {result2.success} - {result2.message}")
        
        if result2.success:
            # Step 3: Try to assign third Saturday (should fail)
            print(f"Step 3: Try to assign W001 to {saturday3.strftime('%Y-%m-%d')} (should fail)")
            result3 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday3, 0)
            print(f"  Result: {result3.success} - {result3.message}")
            
            # Test the constraint checker directly at this point
            print(f"\nDirect constraint check at this point:")
            would_exceed = scheduler.constraint_checker._would_exceed_weekend_limit('W001', saturday3)
            print(f"  _would_exceed_weekend_limit('W001', {saturday3.strftime('%Y-%m-%d')}): {would_exceed}")
            
            # Check current assignments
            print(f"\nCurrent assignments:")
            print(f"  Schedule: {[(d.strftime('%Y-%m-%d'), s) for d, s in scheduler.schedule.items() if any(s)]}")
            print(f"  Worker assignments: {[(d.strftime('%Y-%m-%d')) for d in scheduler.worker_assignments.get('W001', set())]}")
            
            return result1.success and result2.success and not result3.success
        else:
            print(f"❌ Second Saturday assignment failed unexpectedly")
            return False
    else:
        print(f"❌ First Saturday assignment failed unexpectedly") 
        return False


if __name__ == "__main__":
    success = debug_weekend_constraint_clean()
    if success:
        print("\n✅ Weekend constraint is working correctly!")
    else:
        print("\n❌ Weekend constraint is NOT working properly!")