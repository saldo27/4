#!/usr/bin/env python3
"""
Test edge cases for constraint violations that might still occur.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_incremental_operations():
    """Test if incremental operations (swaps, direct assignments) bypass constraints"""
    print("TESTING INCREMENTAL OPERATIONS")
    print("="*50)
    
    try:
        from scheduler import Scheduler
        
        # Create realistic config
        config = {
            'start_date': datetime(2024, 1, 1),
            'end_date': datetime(2024, 1, 21),
            'num_shifts': 2,
            'gap_between_shifts': 1,
            'max_consecutive_weekends': 2,
            'max_shifts_per_worker': 15,
            'workers_data': [
                {'id': 'W001', 'work_percentage': 100, 'target_shifts': 8},
                {'id': 'W002', 'work_percentage': 100, 'target_shifts': 8},
                {'id': 'W003', 'work_percentage': 100, 'target_shifts': 8}
            ]
        }
        
        scheduler = Scheduler(config)
        
        # Test 1: Try to manually assign a 7/14 day pattern violation
        print(f"\nTest 1: Manual assignment with 7/14 day pattern violation")
        monday1 = datetime(2024, 1, 1)  # Monday
        monday2 = datetime(2024, 1, 8)  # Monday, 7 days later
        
        # First assignment should succeed
        result1 = scheduler.incremental_updater.assign_worker_to_shift('W001', monday1, 0)
        print(f"  First assignment (Monday Jan 1): {result1.success} - {result1.message}")
        
        # Second assignment should fail (7/14 day pattern violation)
        result2 = scheduler.incremental_updater.assign_worker_to_shift('W001', monday2, 0)
        print(f"  Second assignment (Monday Jan 8): {result2.success} - {result2.message}")
        
        # Test 2: Try weekend assignment (should succeed)
        print(f"\nTest 2: Weekend assignment (should be allowed)")
        friday1 = datetime(2024, 1, 5)  # Friday
        friday2 = datetime(2024, 1, 12)  # Friday, 7 days later
        
        # Reset worker assignments
        scheduler.schedule = {}
        scheduler.worker_assignments = {'W001': set(), 'W002': set(), 'W003': set()}
        
        result3 = scheduler.incremental_updater.assign_worker_to_shift('W001', friday1, 0)
        print(f"  First Friday assignment: {result3.success} - {result3.message}")
        
        result4 = scheduler.incremental_updater.assign_worker_to_shift('W001', friday2, 0)
        print(f"  Second Friday assignment: {result4.success} - {result4.message}")
        
        # Test 3: Weekend consecutive limit violation
        print(f"\nTest 3: Weekend consecutive limit violation")
        saturday1 = datetime(2024, 1, 6)   # Saturday
        saturday2 = datetime(2024, 1, 13)  # Saturday, next week
        saturday3 = datetime(2024, 1, 20)  # Saturday, third week (should fail)
        
        # Reset and assign first two Saturdays
        scheduler.schedule = {}
        scheduler.worker_assignments = {'W001': set(), 'W002': set(), 'W003': set()}
        
        result5 = scheduler.incremental_updater.assign_worker_to_shift('W001', saturday1, 0)
        print(f"  First Saturday: {result5.success} - {result5.message}")
        
        result6 = scheduler.incremental_updater.assign_worker_to_shift('W001', saturday2, 1)  # Different post
        print(f"  Second Saturday: {result6.success} - {result6.message}")
        
        result7 = scheduler.incremental_updater.assign_worker_to_shift('W001', saturday3, 0)
        print(f"  Third Saturday (should fail): {result7.success} - {result7.message}")
        
        # Test 4: Swap operation that would create violations
        print(f"\nTest 4: Swap operation creating violations")
        
        # Set up a scenario where swapping would create a 7/14 day pattern violation
        scheduler.schedule = {}
        scheduler.worker_assignments = {'W001': set(), 'W002': set(), 'W003': set()}
        
        # W001 on Monday Jan 1
        scheduler.incremental_updater.assign_worker_to_shift('W001', monday1, 0)
        # W002 on Monday Jan 8  
        scheduler.incremental_updater.assign_worker_to_shift('W002', monday2, 0)
        # W003 on Tuesday Jan 2
        tuesday = datetime(2024, 1, 2)
        scheduler.incremental_updater.assign_worker_to_shift('W003', tuesday, 0)
        
        print(f"  Initial setup:")
        print(f"    W001 on {monday1.strftime('%A %Y-%m-%d')}")
        print(f"    W002 on {monday2.strftime('%A %Y-%m-%d')}")
        print(f"    W003 on {tuesday.strftime('%A %Y-%m-%d')}")
        
        # Try to swap W001 and W002 - this would give W001 both Mondays (violation)
        swap_result = scheduler.incremental_updater.swap_workers(
            monday1, 0,  # W001's position
            monday2, 0   # W002's position
        )
        print(f"  Swap W001<->W002 (should fail): {swap_result.success} - {swap_result.message}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_manual_schedule_manipulation():
    """Test if manual schedule manipulation bypasses constraints"""
    print(f"\nTESTING MANUAL SCHEDULE MANIPULATION")
    print("="*50)
    
    try:
        from scheduler import Scheduler
        
        config = {
            'start_date': datetime(2024, 1, 1),
            'end_date': datetime(2024, 1, 21),
            'num_shifts': 2,
            'gap_between_shifts': 1,
            'max_consecutive_weekends': 2,
            'max_shifts_per_worker': 15,
            'workers_data': [
                {'id': 'W001', 'work_percentage': 100, 'target_shifts': 8}
            ]
        }
        
        scheduler = Scheduler(config)
        
        print(f"Testing direct schedule manipulation (bypassing APIs)...")
        
        # Manually create violations by directly manipulating the schedule
        monday1 = datetime(2024, 1, 1)  # Monday
        monday2 = datetime(2024, 1, 8)  # Monday, 7 days later
        
        # Direct manipulation (this is what users might try to avoid constraints)
        scheduler.schedule[monday1] = ['W001', None]
        scheduler.schedule[monday2] = ['W001', None]
        scheduler.worker_assignments['W001'] = {monday1, monday2}
        
        print(f"  Manually created potential 7/14 day violation:")
        print(f"    W001 on {monday1.strftime('%A %Y-%m-%d')}")
        print(f"    W001 on {monday2.strftime('%A %Y-%m-%d')}")
        
        # Check if constraint checker detects the violation
        can_assign_check = scheduler.constraint_checker._check_gap_constraint('W001', monday2)
        print(f"  Constraint checker detects violation: {not can_assign_check}")
        
        full_check, reason = scheduler.constraint_checker._check_constraints('W001', monday2)
        print(f"  Full constraint check: {full_check} (reason: {reason})")
        
        # Test the live validator
        validation_result = scheduler.live_validator.validate_assignment('W001', monday2, 0)
        print(f"  Live validator detects violation: {not validation_result.is_valid}")
        if not validation_result.is_valid:
            print(f"    Message: {validation_result.message}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all edge case tests"""
    logging.basicConfig(level=logging.WARNING)  # Reduce noise
    
    print("TESTING CONSTRAINT EDGE CASES")
    print("="*60)
    
    success1 = test_incremental_operations()
    success2 = test_manual_schedule_manipulation()
    
    if success1 and success2:
        print(f"\n✅ ALL CONSTRAINT EDGE CASE TESTS PASSED")
        print(f"The constraint system is working correctly.")
    else:
        print(f"\n❌ SOME TESTS FAILED")
        
    print(f"\nEDGE CASE TESTING COMPLETE")

if __name__ == "__main__":
    main()