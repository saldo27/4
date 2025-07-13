#!/usr/bin/env python3
"""
Simple test to check constraint enforcement by using existing data.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_existing_constraint_functions():
    """Test constraint functions directly"""
    print("TESTING CONSTRAINT FUNCTIONS DIRECTLY")
    print("="*50)
    
    try:
        from constraint_checker import ConstraintChecker
        from scheduler import Scheduler
        
        # Create minimal config  
        config = {
            'start_date': datetime(2024, 1, 1),
            'end_date': datetime(2024, 1, 31),
            'num_shifts': 2,
            'gap_between_shifts': 1,
            'max_consecutive_weekends': 2,
            'max_shifts_per_worker': 10,
            'workers_data': [
                {'id': 'W001', 'work_percentage': 100, 'target_shifts': 8},
                {'id': 'W002', 'work_percentage': 100, 'target_shifts': 8}
            ]
        }
        
        # Initialize scheduler with minimal config
        scheduler = Scheduler(config)
        
        # Test dates
        monday1 = datetime(2024, 1, 1)  # Monday
        monday2 = datetime(2024, 1, 8)  # Monday, 7 days later
        worker_id = 'W001'
        
        print(f"Testing 7/14 day pattern:")
        print(f"  Worker: {worker_id}")
        print(f"  Date 1: {monday1.strftime('%A %Y-%m-%d')} (assigned)")
        print(f"  Date 2: {monday2.strftime('%A %Y-%m-%d')} (testing - should FAIL)")
        
        # Manually assign first date
        scheduler.worker_assignments[worker_id] = {monday1}
        scheduler.schedule[monday1] = [worker_id, None]
        
        # Test the constraint check directly
        gap_check = scheduler.constraint_checker._check_gap_constraint(worker_id, monday2)
        print(f"  Gap constraint check result: {gap_check}")
        
        full_check, reason = scheduler.constraint_checker._check_constraints(worker_id, monday2)
        print(f"  Full constraint check: {full_check} (reason: {reason})")
        
        # Test weekend case (should pass)
        print(f"\nTesting weekend exception:")
        friday1 = datetime(2024, 1, 5)  # Friday
        friday2 = datetime(2024, 1, 12)  # Friday, 7 days later
        
        scheduler.worker_assignments[worker_id] = {friday1}
        scheduler.schedule = {friday1: [worker_id, None]}
        
        print(f"  Date 1: {friday1.strftime('%A %Y-%m-%d')} (assigned)")
        print(f"  Date 2: {friday2.strftime('%A %Y-%m-%d')} (testing - should PASS)")
        
        gap_check_weekend = scheduler.constraint_checker._check_gap_constraint(worker_id, friday2)
        print(f"  Gap constraint check result: {gap_check_weekend}")
        
        full_check_weekend, reason_weekend = scheduler.constraint_checker._check_constraints(worker_id, friday2)
        print(f"  Full constraint check: {full_check_weekend} (reason: {reason_weekend})")
        
        # Test weekend consecutive limits
        print(f"\nTesting weekend consecutive limits:")
        saturday1 = datetime(2024, 1, 6)   # Saturday
        saturday2 = datetime(2024, 1, 13)  # Saturday, next week
        saturday3 = datetime(2024, 1, 20)  # Saturday, third week (should fail)
        
        scheduler.worker_assignments[worker_id] = {saturday1, saturday2}
        
        print(f"  Assigned: {saturday1.strftime('%A %Y-%m-%d')}, {saturday2.strftime('%A %Y-%m-%d')}")
        print(f"  Testing: {saturday3.strftime('%A %Y-%m-%d')} (should FAIL)")
        
        weekend_check = scheduler.constraint_checker._would_exceed_weekend_limit(worker_id, saturday3)
        print(f"  Weekend limit check result: {weekend_check}")
        
        full_weekend_check, weekend_reason = scheduler.constraint_checker._check_constraints(worker_id, saturday3)
        print(f"  Full constraint check: {full_weekend_check} (reason: {weekend_reason})")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def check_schedule_builder_constraints():
    """Check if schedule builder enforces constraints"""
    print(f"\nTESTING SCHEDULE BUILDER CONSTRAINT ENFORCEMENT")
    print("="*50)
    
    try:
        from schedule_builder import ScheduleBuilder
        from scheduler import Scheduler
        
        # Create minimal config  
        config = {
            'start_date': datetime(2024, 1, 1),
            'end_date': datetime(2024, 1, 31),
            'num_shifts': 2,
            'gap_between_shifts': 1,
            'max_consecutive_weekends': 2,
            'max_shifts_per_worker': 10,
            'workers_data': [
                {'id': 'W001', 'work_percentage': 100, 'target_shifts': 8},
                {'id': 'W002', 'work_percentage': 100, 'target_shifts': 8}
            ]
        }
        
        scheduler = Scheduler(config)
        builder = ScheduleBuilder(scheduler)
        
        # Test if _can_assign_worker enforces 7/14 day pattern
        monday1 = datetime(2024, 1, 1)  # Monday
        monday2 = datetime(2024, 1, 8)  # Monday, 7 days later
        worker_id = 'W001'
        
        # Manually assign first date
        scheduler.worker_assignments[worker_id] = {monday1}
        scheduler.schedule[monday1] = [worker_id, None]
        
        print(f"Testing schedule builder _can_assign_worker:")
        print(f"  Worker: {worker_id}")
        print(f"  Assigned: {monday1.strftime('%A %Y-%m-%d')}")
        print(f"  Testing: {monday2.strftime('%A %Y-%m-%d')} (should FAIL)")
        
        can_assign = builder._can_assign_worker(worker_id, monday2, 0)
        print(f"  _can_assign_worker result: {can_assign}")
        
        # Test weekend case (should pass)
        friday1 = datetime(2024, 1, 5)  # Friday
        friday2 = datetime(2024, 1, 12)  # Friday, 7 days later
        
        scheduler.worker_assignments[worker_id] = {friday1}
        scheduler.schedule = {friday1: [worker_id, None]}
        
        print(f"\n  Testing weekend exception:")
        print(f"  Assigned: {friday1.strftime('%A %Y-%m-%d')}")
        print(f"  Testing: {friday2.strftime('%A %Y-%m-%d')} (should PASS)")
        
        can_assign_weekend = builder._can_assign_worker(worker_id, friday2, 0)
        print(f"  _can_assign_worker weekend result: {can_assign_weekend}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run constraint tests"""
    logging.basicConfig(level=logging.ERROR)  # Reduce log noise
    
    test_existing_constraint_functions()
    check_schedule_builder_constraints()
    
    print(f"\nCONSTRAINT TESTING COMPLETE")

if __name__ == "__main__":
    main()