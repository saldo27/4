#!/usr/bin/env python3

"""
Test to specifically target potential constraint violation scenarios
"""

import logging
from datetime import datetime, timedelta
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler

def test_mandatory_assignment_violations():
    """Test if mandatory assignments bypass constraint checks"""
    
    print("=" * 80)
    print("TESTING MANDATORY ASSIGNMENT CONSTRAINT BYPASSING")
    print("=" * 80)
    
    # Create config with mandatory assignments that could violate 7/14 day pattern
    config = {
        'workers_data': [
            {
                'id': 'W001',
                'name': 'Worker 1',
                'target_shifts': 10,
                'work_percentage': 100,
                # Mandatory on Mondays - this could create 7-day pattern violations
                'mandatory_days': '8-1-2024;15-1-2024;22-1-2024',  # Three consecutive Mondays
                'days_off': '',
                'is_incompatible': False
            },
            {
                'id': 'W002', 
                'name': 'Worker 2',
                'target_shifts': 10,
                'work_percentage': 100,
                'mandatory_days': '',
                'days_off': '',
                'is_incompatible': False
            }
        ],
        'holidays': [],
        'start_date': datetime(2024, 1, 1),
        'end_date': datetime(2024, 1, 31),
        'num_shifts': 2,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2
    }
    
    print("Testing worker with mandatory assignments on consecutive Mondays...")
    print("Worker W001 mandatory days: 8-1-2024 (Mon), 15-1-2024 (Mon), 22-1-2024 (Mon)")
    print("This should trigger 7-day pattern violations!")
    print()
    
    # Create scheduler
    scheduler = Scheduler(config)
    
    # Generate schedule
    try:
        result = scheduler.generate_schedule()
        if result:
            print("✅ Schedule generation completed")
            
            # Check specifically for W001's assignments
            w001_assignments = sorted(list(scheduler.worker_assignments.get('W001', [])))
            print(f"\nWorker W001 assignments: {[d.strftime('%Y-%m-%d (%A)') for d in w001_assignments]}")
            
            # Check for 7-day pattern violations
            violations = []
            for i, date1 in enumerate(w001_assignments):
                for j in range(i + 1, len(w001_assignments)):
                    date2 = w001_assignments[j]
                    days_diff = (date2 - date1).days
                    
                    if (days_diff == 7 or days_diff == 14) and date1.weekday() == date2.weekday():
                        if date1.weekday() < 4:  # Monday=0 to Thursday=3
                            violations.append(
                                f"7-day pattern violation: {date1.strftime('%Y-%m-%d (%A)')} -> {date2.strftime('%Y-%m-%d (%A)')} ({days_diff} days)"
                            )
            
            # Also check ALL mandatory assignments specifically
            print(f"\nChecking mandatory assignments:")
            mandatory_dates = [datetime(2024, 1, 8), datetime(2024, 1, 15), datetime(2024, 1, 22)]
            for date in mandatory_dates:
                if date in w001_assignments:
                    print(f"  ✓ W001 assigned on {date.strftime('%Y-%m-%d (%A)')} - MANDATORY")
                else:
                    print(f"  ✗ W001 NOT assigned on {date.strftime('%Y-%m-%d (%A)')} - MANDATORY MISSED")
            
            print(f"\nAnalyzing 7-day pattern for mandatory Mondays:")
            if datetime(2024, 1, 8) in w001_assignments and datetime(2024, 1, 15) in w001_assignments:
                print(f"  ❌ VIOLATION: 2024-01-08 (Mon) -> 2024-01-15 (Mon) = 7 days apart, same weekday")
            if datetime(2024, 1, 15) in w001_assignments and datetime(2024, 1, 22) in w001_assignments:
                print(f"  ❌ VIOLATION: 2024-01-15 (Mon) -> 2024-01-22 (Mon) = 7 days apart, same weekday")
            
            if violations:
                print(f"\n❌ FOUND {len(violations)} CONSTRAINT VIOLATIONS:")
                for violation in violations:
                    print(f"  - {violation}")
                return False
            else:
                print("\n✅ No violations found")
                return True
                
        else:
            print("❌ Schedule generation failed")
            return False
            
    except Exception as e:
        print(f"❌ Error during schedule generation: {e}")
        return False

def test_weekend_limit_violations():
    """Test if weekend limits can be exceeded"""
    
    print("\n" + "=" * 80)
    print("TESTING WEEKEND CONSECUTIVE LIMIT VIOLATIONS")
    print("=" * 80)
    
    # Create config that could lead to consecutive weekend violations
    config = {
        'workers_data': [
            {
                'id': 'W001',
                'name': 'Worker 1',
                'target_shifts': 20,
                'work_percentage': 100,
                # Force many weekend assignments
                'mandatory_days': '5-1-2024;6-1-2024;12-1-2024;13-1-2024;19-1-2024;20-1-2024;26-1-2024;27-1-2024',  # 4 consecutive weekends
                'days_off': '',
                'is_incompatible': False
            },
            {
                'id': 'W002', 
                'name': 'Worker 2',
                'target_shifts': 10,
                'work_percentage': 100,
                'mandatory_days': '',
                'days_off': '',
                'is_incompatible': False
            }
        ],
        'holidays': [],
        'start_date': datetime(2024, 1, 1),
        'end_date': datetime(2024, 1, 31),
        'num_shifts': 2,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2  # Only 2 consecutive weekends allowed
    }
    
    print("Testing worker with mandatory assignments on 4 consecutive weekends...")
    print("Max consecutive weekends limit: 2")
    print("Worker W001 mandatory days cover 4 consecutive weekends")
    print("This should trigger weekend consecutive limit violations!")
    print()
    
    # Create scheduler
    scheduler = Scheduler(config)
    
    # Generate schedule
    try:
        result = scheduler.generate_schedule()
        if result:
            print("✅ Schedule generation completed")
            
            # Check specifically for W001's weekend assignments
            w001_assignments = sorted(list(scheduler.worker_assignments.get('W001', [])))
            weekend_assignments = [d for d in w001_assignments if d.weekday() >= 4]  # Fri, Sat, Sun
            
            print(f"\nWorker W001 weekend assignments: {[d.strftime('%Y-%m-%d (%A)') for d in weekend_assignments]}")
            
            # Check for consecutive weekend violations
            violations = []
            consecutive_count = 1
            max_consecutive = scheduler.max_consecutive_weekends
            
            for i in range(1, len(weekend_assignments)):
                days_diff = (weekend_assignments[i] - weekend_assignments[i-1]).days
                
                # Check if it's the next weekend (5-9 days apart typically)
                if 5 <= days_diff <= 9:
                    consecutive_count += 1
                    if consecutive_count > max_consecutive:
                        violations.append(
                            f"Weekend consecutive limit violation: {consecutive_count} consecutive weekends "
                            f"(limit: {max_consecutive}) - ending on {weekend_assignments[i].strftime('%Y-%m-%d (%A)')}"
                        )
                else:
                    consecutive_count = 1
            
            if violations:
                print(f"\n❌ FOUND {len(violations)} CONSTRAINT VIOLATIONS:")
                for violation in violations:
                    print(f"  - {violation}")
                return False
            else:
                print("\n✅ No violations found")
                return True
                
        else:
            print("❌ Schedule generation failed")
            return False
            
    except Exception as e:
        print(f"❌ Error during schedule generation: {e}")
        return False

def test_swap_constraint_violations():
    """Test if swap operations bypass constraint checks"""
    
    print("\n" + "=" * 80)
    print("TESTING SWAP OPERATION CONSTRAINT BYPASSING")
    print("=" * 80)
    
    # First create a schedule
    config = {
        'workers_data': [
            {
                'id': 'W001',
                'name': 'Worker 1',
                'target_shifts': 8,
                'work_percentage': 100,
                'mandatory_days': '',
                'days_off': '',
                'is_incompatible': False
            },
            {
                'id': 'W002', 
                'name': 'Worker 2',
                'target_shifts': 8,
                'work_percentage': 100,
                'mandatory_days': '',
                'days_off': '',
                'is_incompatible': False
            }
        ],
        'holidays': [],
        'start_date': datetime(2024, 1, 1),
        'end_date': datetime(2024, 1, 31),
        'num_shifts': 2,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2
    }
    
    print("Testing swap operations that could violate constraints...")
    
    # Create scheduler
    scheduler = Scheduler(config)
    
    # Generate initial schedule
    try:
        result = scheduler.generate_schedule()
        if not result:
            print("❌ Initial schedule generation failed")
            return False
            
        print("✅ Initial schedule generated")
        
        # Try to manually create a constraint-violating swap
        # We'll use the incremental_updater to test swap operations
        from incremental_updater import IncrementalUpdater
        updater = IncrementalUpdater(scheduler)
        
        # Find two assignments we can swap that would create a violation
        w001_dates = sorted(list(scheduler.worker_assignments.get('W001', [])))
        w002_dates = sorted(list(scheduler.worker_assignments.get('W002', [])))
        
        print(f"\nW001 assignments: {[d.strftime('%Y-%m-%d (%A)') for d in w001_dates[:5]]}")
        print(f"W002 assignments: {[d.strftime('%Y-%m-%d (%A)') for d in w002_dates[:5]]}")
        
        # Try to create a swap that would violate 7-day pattern
        if len(w001_dates) >= 2 and len(w002_dates) >= 2:
            # Try swapping to create a 7-day pattern violation
            date1 = w001_dates[0]
            date2 = w002_dates[0]
            
            # Check if this would create violations
            result = updater.swap_workers(date1, 0, date2, 0)
            
            print(f"\nAttempted swap: W001 {date1.strftime('%Y-%m-%d')} <-> W002 {date2.strftime('%Y-%m-%d')}")
            print(f"Swap result: {result.success}")
            
            if result.success:
                # Check for violations after swap
                violations = check_post_swap_violations(scheduler)
                if violations:
                    print(f"\n❌ SWAP CREATED {len(violations)} CONSTRAINT VIOLATIONS:")
                    for violation in violations:
                        print(f"  - {violation}")
                    return False
                else:
                    print("\n✅ No violations created by swap")
            else:
                print(f"✅ Swap correctly rejected: {result.message or 'Unknown reason'}")
        
        return True
            
    except Exception as e:
        print(f"❌ Error during swap testing: {e}")
        return False

def check_post_swap_violations(scheduler):
    """Check for violations after a swap operation"""
    violations = []
    
    # Check 7/14 day pattern violations
    for worker_id, assigned_dates in scheduler.worker_assignments.items():
        if len(assigned_dates) < 2:
            continue
            
        sorted_dates = sorted(assigned_dates)
        
        for i, date1 in enumerate(sorted_dates):
            for j in range(i + 1, len(sorted_dates)):
                date2 = sorted_dates[j]
                days_diff = (date2 - date1).days
                
                if (days_diff == 7 or days_diff == 14) and date1.weekday() == date2.weekday():
                    if date1.weekday() < 4:  # Monday=0 to Thursday=3
                        violations.append(
                            f"7/14 day pattern violation: Worker {worker_id} on "
                            f"{date1.strftime('%Y-%m-%d (%A)')} and "
                            f"{date2.strftime('%Y-%m-%d (%A)')} - {days_diff} days apart"
                        )
    
    return violations

if __name__ == "__main__":
    success = True
    
    # Run all tests
    success &= test_mandatory_assignment_violations()
    success &= test_weekend_limit_violations()
    success &= test_swap_constraint_violations()
    
    print("\n" + "=" * 80)
    if success:
        print("✅ ALL TESTS PASSED - No constraint violations found")
    else:
        print("❌ SOME TESTS FAILED - Constraint violations detected")
    print("=" * 80)
    
    sys.exit(0 if success else 1)