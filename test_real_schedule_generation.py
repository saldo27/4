#!/usr/bin/env python3
"""
Test real schedule generation to see if constraints are violated.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_schedule_generation_with_constraints():
    """Generate a real schedule and check for constraint violations"""
    print("TESTING REAL SCHEDULE GENERATION")
    print("="*50)
    
    try:
        from scheduler import Scheduler
        
        # Create config that's likely to create violations if constraints aren't enforced
        config = {
            'start_date': datetime(2024, 1, 1),   # Monday
            'end_date': datetime(2024, 1, 28),     # 4 weeks
            'num_shifts': 1,                       # Only 1 shift per day to force conflicts
            'gap_between_shifts': 1,               # Small gap
            'max_consecutive_weekends': 1,         # Very restrictive weekend limit
            'max_shifts_per_worker': 20,
            'workers_data': [
                {'id': 'W001', 'work_percentage': 100, 'target_shifts': 10},
                {'id': 'W002', 'work_percentage': 100, 'target_shifts': 10}
            ]
        }
        
        scheduler = Scheduler(config)
        
        print(f"Generating schedule:")
        print(f"  Period: {config['start_date'].strftime('%Y-%m-%d')} to {config['end_date'].strftime('%Y-%m-%d')}")
        print(f"  Max consecutive weekends: {config['max_consecutive_weekends']}")
        print(f"  Gap between shifts: {config['gap_between_shifts']}")
        print(f"  Only {config['num_shifts']} shift per day")
        print(f"  Workers target high shifts to force potential violations")
        
        # Generate schedule
        result = scheduler.generate_schedule()
        print(f"\nSchedule generation result: {result}")
        
        # Now check for violations
        print(f"\n--- ANALYZING GENERATED SCHEDULE FOR VIOLATIONS ---")
        
        violations = []
        
        # Check 7/14 day pattern violations
        print(f"\nChecking 7/14 day pattern violations...")
        for worker_id, assignments in scheduler.worker_assignments.items():
            assignments_list = sorted(list(assignments))
            for i, date1 in enumerate(assignments_list):
                for date2 in assignments_list[i+1:]:
                    days_between = abs((date2 - date1).days)
                    if (days_between == 7 or days_between == 14) and date1.weekday() == date2.weekday():
                        # Check if both are weekdays (constraint should apply)
                        if date1.weekday() < 4 and date2.weekday() < 4:  # Mon-Thu
                            violation = f"7/14 day pattern: {worker_id} on {date1.strftime('%A %Y-%m-%d')} and {date2.strftime('%A %Y-%m-%d')} ({days_between} days apart)"
                            violations.append(violation)
                            print(f"  VIOLATION: {violation}")
        
        # Check weekend consecutive limit violations
        print(f"\nChecking weekend consecutive limit violations...")
        for worker_id, assignments in scheduler.worker_assignments.items():
            weekend_dates = []
            for date in assignments:
                if date.weekday() >= 4:  # Fri, Sat, Sun
                    weekend_dates.append(date)
            
            if len(weekend_dates) <= 1:
                continue
                
            weekend_dates.sort()
            
            # Check for consecutive weekends
            consecutive_groups = []
            current_group = []
            
            for weekend_date in weekend_dates:
                if not current_group:
                    current_group = [weekend_date]
                else:
                    prev_date = current_group[-1]
                    days_between = (weekend_date - prev_date).days
                    
                    # Consecutive weekends typically have 5-10 days between them
                    if 5 <= days_between <= 10:
                        current_group.append(weekend_date)
                    else:
                        if len(current_group) > 1:
                            consecutive_groups.append(current_group)
                        current_group = [weekend_date]
            
            if len(current_group) > 1:
                consecutive_groups.append(current_group)
            
            # Check if any group exceeds the limit
            for group in consecutive_groups:
                if len(group) > config['max_consecutive_weekends']:
                    violation = f"Weekend consecutive limit: {worker_id} has {len(group)} consecutive weekends (limit: {config['max_consecutive_weekends']})"
                    violation += f" - dates: {', '.join(d.strftime('%Y-%m-%d') for d in group)}"
                    violations.append(violation)
                    print(f"  VIOLATION: {violation}")
        
        # Summary
        print(f"\n--- RESULTS ---")
        if violations:
            print(f"CONSTRAINT VIOLATIONS FOUND: {len(violations)}")
            for violation in violations:
                print(f"  - {violation}")
        else:
            print(f"NO CONSTRAINT VIOLATIONS FOUND")
            
        # Print schedule summary
        print(f"\n--- SCHEDULE SUMMARY ---")
        for worker_id, assignments in scheduler.worker_assignments.items():
            print(f"Worker {worker_id}: {len(assignments)} assignments")
            sorted_assignments = sorted(assignments)
            for date in sorted_assignments:
                print(f"  {date.strftime('%A %Y-%m-%d')}")
                
        # Test constraint validation retroactively
        print(f"\n--- RETROACTIVE CONSTRAINT VALIDATION ---")
        for worker_id, assignments in scheduler.worker_assignments.items():
            for date in assignments:
                can_assign, reason = scheduler.constraint_checker._check_constraints(worker_id, date)
                if not can_assign:
                    print(f"RETROACTIVE VIOLATION: Worker {worker_id} on {date.strftime('%Y-%m-%d')} - {reason}")
                    
        return len(violations) == 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run schedule generation test"""
    logging.basicConfig(level=logging.WARNING)  # Reduce log noise
    
    success = test_schedule_generation_with_constraints()
    
    if success:
        print(f"\n✅ CONSTRAINT ENFORCEMENT WORKING CORRECTLY")
    else:
        print(f"\n❌ CONSTRAINT VIOLATIONS FOUND - NEED TO FIX")
    
    print(f"\nTEST COMPLETE")

if __name__ == "__main__":
    main()