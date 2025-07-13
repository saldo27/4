#!/usr/bin/env python3
"""
Debug constraints to understand why violations still occur.
This script will test the constraint checking in all modules.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler
from constraint_checker import ConstraintChecker
from live_validator import LiveValidator
from incremental_updater import IncrementalUpdater

def setup_logging():
    """Set up detailed logging"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def test_7_14_day_pattern():
    """Test the 7/14 day pattern constraint enforcement"""
    print("\n" + "="*60)
    print("TESTING 7/14 DAY PATTERN CONSTRAINT")
    print("="*60)
    
    # Create minimal config for testing
    config = {
        'start_date': '01-01-2024',
        'end_date': '31-01-2024', 
        'num_shifts': 2,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2,
        'max_shifts_per_worker': 10
    }
    
    # Create minimal scheduler for testing
    scheduler = Scheduler(config)
    
    # Create test workers
    scheduler.workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'target_shifts': 8},
        {'id': 'W002', 'work_percentage': 100, 'target_shifts': 8}
    ]
    
    # Initialize scheduler components
    scheduler._initialize_components()
    
    # Test scenario: Worker assigned Monday Jan 1, trying to assign Monday Jan 8 (7 days, same weekday)
    test_date1 = datetime(2024, 1, 1)  # Monday
    test_date2 = datetime(2024, 1, 8)  # Monday, 7 days later
    worker_id = 'W001'
    
    print(f"Test scenario: {worker_id} assigned {test_date1.strftime('%A %Y-%m-%d')}")
    print(f"Trying to assign {test_date2.strftime('%A %Y-%m-%d')} (7 days later, same weekday)")
    
    # Manually assign first date
    scheduler.schedule[test_date1] = [worker_id, None]
    scheduler.worker_assignments[worker_id] = {test_date1}
    
    # Test constraint_checker
    print(f"\n--- Testing constraint_checker ---")
    can_assign_cc, reason_cc = scheduler.constraint_checker._check_constraints(worker_id, test_date2)
    print(f"constraint_checker result: {can_assign_cc} (reason: {reason_cc})")
    
    # Test live_validator
    print(f"\n--- Testing live_validator ---")
    result_lv = scheduler.live_validator.validate_assignment(worker_id, test_date2, 0)
    print(f"live_validator result: {result_lv.is_valid} (message: {result_lv.message})")
    
    # Test incremental_updater
    print(f"\n--- Testing incremental_updater ---")
    result_iu = scheduler.incremental_updater.assign_worker_to_shift(worker_id, test_date2, 0)
    print(f"incremental_updater result: {result_iu.success} (message: {result_iu.message})")
    
    # Also test weekend days (should be allowed)
    print(f"\n--- Testing weekend exception ---")
    friday1 = datetime(2024, 1, 5)  # Friday
    friday2 = datetime(2024, 1, 12)  # Friday, 7 days later
    
    # Reset for weekend test
    scheduler.worker_assignments[worker_id] = {friday1}
    scheduler.schedule = {friday1: [worker_id, None]}
    
    print(f"Weekend test: {worker_id} assigned {friday1.strftime('%A %Y-%m-%d')}")
    print(f"Trying to assign {friday2.strftime('%A %Y-%m-%d')} (7 days later, same weekday - SHOULD BE ALLOWED)")
    
    can_assign_cc_weekend, reason_cc_weekend = scheduler.constraint_checker._check_constraints(worker_id, friday2)
    print(f"constraint_checker weekend result: {can_assign_cc_weekend} (reason: {reason_cc_weekend})")
    
    result_lv_weekend = scheduler.live_validator.validate_assignment(worker_id, friday2, 0)
    print(f"live_validator weekend result: {result_lv_weekend.is_valid} (message: {result_lv_weekend.message})")

def test_weekend_consecutive_limits():
    """Test weekend consecutive limits enforcement"""
    print("\n" + "="*60)
    print("TESTING WEEKEND CONSECUTIVE LIMITS")
    print("="*60)
    
    # Create minimal config for testing
    config = {
        'start_date': '01-01-2024',
        'end_date': '31-01-2024', 
        'num_shifts': 2,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2,
        'max_shifts_per_worker': 10
    }
    
    # Create minimal scheduler for testing
    scheduler = Scheduler(config)
    
    # Create test workers
    scheduler.workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'target_shifts': 8},
        {'id': 'W002', 'work_percentage': 100, 'target_shifts': 8}
    ]
    
    # Initialize scheduler components
    scheduler._initialize_components()
    
    # Test scenario: 3 consecutive Saturdays (should fail on 3rd)
    saturday1 = datetime(2024, 1, 6)   # Saturday
    saturday2 = datetime(2024, 1, 13)  # Saturday, next week
    saturday3 = datetime(2024, 1, 20)  # Saturday, third week
    worker_id = 'W001'
    
    print(f"Test scenario: Consecutive Saturday assignments")
    print(f"Saturday 1: {saturday1.strftime('%Y-%m-%d')}")
    print(f"Saturday 2: {saturday2.strftime('%Y-%m-%d')}")
    print(f"Saturday 3: {saturday3.strftime('%Y-%m-%d')} (should be BLOCKED)")
    
    # Assign first two Saturdays
    scheduler.schedule[saturday1] = [worker_id, None]
    scheduler.schedule[saturday2] = [worker_id, None]
    scheduler.worker_assignments[worker_id] = {saturday1, saturday2}
    
    print(f"\nAfter assigning first 2 Saturdays:")
    print(f"Worker {worker_id} assignments: {sorted([d.strftime('%Y-%m-%d') for d in scheduler.worker_assignments[worker_id]])}")
    
    # Test constraint_checker
    print(f"\n--- Testing constraint_checker for 3rd Saturday ---")
    can_assign_cc, reason_cc = scheduler.constraint_checker._check_constraints(worker_id, saturday3)
    print(f"constraint_checker result: {can_assign_cc} (reason: {reason_cc})")
    
    # More detailed check
    would_exceed = scheduler.constraint_checker._would_exceed_weekend_limit(worker_id, saturday3)
    print(f"_would_exceed_weekend_limit result: {would_exceed}")
    
    # Test live_validator
    print(f"\n--- Testing live_validator for 3rd Saturday ---")
    result_lv = scheduler.live_validator.validate_assignment(worker_id, saturday3, 0)
    print(f"live_validator result: {result_lv.is_valid} (message: {result_lv.message})")
    
    # Test incremental_updater
    print(f"\n--- Testing incremental_updater for 3rd Saturday ---")
    result_iu = scheduler.incremental_updater.assign_worker_to_shift(worker_id, saturday3, 0)
    print(f"incremental_updater result: {result_iu.success} (message: {result_iu.message})")

def test_schedule_generation_integration():
    """Test if schedule generation bypasses constraints"""
    print("\n" + "="*60)
    print("TESTING SCHEDULE GENERATION INTEGRATION")
    print("="*60)
    
    # Create minimal config for testing
    config = {
        'start_date': '01-01-2024',
        'end_date': '21-01-2024', 
        'num_shifts': 1,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 1,
        'max_shifts_per_worker': 15
    }
    
    # Create a real scheduler instance
    scheduler = Scheduler(config)
    
    # Create workers with high target shifts to force violations
    scheduler.workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'target_shifts': 12},
        {'id': 'W002', 'work_percentage': 100, 'target_shifts': 12}
    ]
    
    print(f"Generating schedule with restrictive constraints:")
    print(f"Period: {scheduler.start_date.strftime('%Y-%m-%d')} to {scheduler.end_date.strftime('%Y-%m-%d')}")
    print(f"Max consecutive weekends: {scheduler.max_consecutive_weekends}")
    print(f"Gap between shifts: {scheduler.gap_between_shifts}")
    print(f"Workers have high target shifts to force constraint violations")
    
    try:
        # Initialize and generate schedule
        scheduler._initialize_components()
        result = scheduler.generate_schedule()
        
        print(f"\nSchedule generation result: {result}")
        
        # Check for violations in the generated schedule
        print(f"\n--- Checking generated schedule for violations ---")
        
        violations_found = []
        
        # Check 7/14 day patterns
        for worker_id, assignments in scheduler.worker_assignments.items():
            assignments_list = sorted(list(assignments))
            for i, date1 in enumerate(assignments_list):
                for date2 in assignments_list[i+1:]:
                    days_between = abs((date2 - date1).days)
                    if (days_between == 7 or days_between == 14) and date1.weekday() == date2.weekday():
                        # Check if both are weekdays (constraint should apply)
                        if date1.weekday() < 4 and date2.weekday() < 4:  # Mon-Thu
                            violations_found.append(f"7/14 day pattern: Worker {worker_id} on {date1.strftime('%A %Y-%m-%d')} and {date2.strftime('%A %Y-%m-%d')}")
        
        # Check weekend consecutive limits
        for worker_id, assignments in scheduler.worker_assignments.items():
            weekend_dates = []
            for date in assignments:
                if date.weekday() >= 4:  # Fri, Sat, Sun
                    weekend_dates.append(date)
            
            weekend_dates.sort()
            consecutive_count = 1
            max_consecutive = 1
            
            for i in range(1, len(weekend_dates)):
                days_diff = (weekend_dates[i] - weekend_dates[i-1]).days
                if 5 <= days_diff <= 10:  # Consecutive weekends
                    consecutive_count += 1
                    max_consecutive = max(max_consecutive, consecutive_count)
                else:
                    consecutive_count = 1
            
            if max_consecutive > scheduler.max_consecutive_weekends:
                violations_found.append(f"Weekend consecutive limit: Worker {worker_id} has {max_consecutive} consecutive weekends (limit: {scheduler.max_consecutive_weekends})")
        
        if violations_found:
            print(f"CONSTRAINT VIOLATIONS FOUND:")
            for violation in violations_found:
                print(f"  - {violation}")
        else:
            print(f"No constraint violations found in generated schedule")
            
        # Print schedule summary
        print(f"\n--- Schedule Summary ---")
        for worker_id, assignments in scheduler.worker_assignments.items():
            print(f"Worker {worker_id}: {len(assignments)} assignments")
            for date in sorted(assignments):
                print(f"  {date.strftime('%A %Y-%m-%d')}")
                
    except Exception as e:
        print(f"Error during schedule generation: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all constraint tests"""
    setup_logging()
    
    print("CONSTRAINT DEBUGGING SUITE")
    print("This will test constraint enforcement across all modules")
    
    try:
        # Test individual constraint functions
        test_7_14_day_pattern()
        test_weekend_consecutive_limits()
        
        # Test full schedule generation
        test_schedule_generation_integration()
        
        print(f"\n" + "="*60)
        print("CONSTRAINT TESTING COMPLETE")
        print("="*60)
        
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()