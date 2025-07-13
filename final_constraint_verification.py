#!/usr/bin/env python3
"""
Final comprehensive test to verify constraints are working.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_final_constraint_verification():
    """Final test to verify constraints are working in all scenarios"""
    print("FINAL CONSTRAINT VERIFICATION")
    print("="*50)
    
    try:
        from scheduler import Scheduler
        
        # Test realistic scenario with multiple workers and longer period
        config = {
            'start_date': datetime(2024, 1, 1),
            'end_date': datetime(2024, 2, 29),  # 2 months
            'num_shifts': 2,
            'gap_between_shifts': 1,
            'max_consecutive_weekends': 2,
            'max_shifts_per_worker': 30,
            'workers_data': [
                {'id': 'W001', 'work_percentage': 100, 'target_shifts': 20},
                {'id': 'W002', 'work_percentage': 100, 'target_shifts': 20},
                {'id': 'W003', 'work_percentage': 100, 'target_shifts': 20},
                {'id': 'W004', 'work_percentage': 100, 'target_shifts': 20}
            ]
        }
        
        scheduler = Scheduler(config)
        
        print(f"Generating full schedule with 4 workers over 2 months...")
        print(f"  Max consecutive weekends: {config['max_consecutive_weekends']}")
        print(f"  Gap between shifts: {config['gap_between_shifts']}")
        print(f"  This is a realistic scenario that should stress-test constraints")
        
        # Generate schedule
        result = scheduler.generate_schedule()
        print(f"\nSchedule generation result: {result}")
        
        if not result:
            print("Schedule generation failed!")
            return False
        
        # Comprehensive violation analysis
        print(f"\n--- COMPREHENSIVE VIOLATION ANALYSIS ---")
        
        violations = []
        
        # Check 7/14 day pattern violations with detailed logging
        print(f"\nChecking 7/14 day pattern violations (weekdays only)...")
        weekday_violations = 0
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
                            weekday_violations += 1
                            print(f"  VIOLATION: {violation}")
        
        if weekday_violations == 0:
            print(f"  ✅ No 7/14 day pattern violations found")
        
        # Check weekend consecutive limit violations with detailed logging  
        print(f"\nChecking weekend consecutive limit violations...")
        weekend_violations = 0
        for worker_id, assignments in scheduler.worker_assignments.items():
            weekend_dates = []
            for date in assignments:
                if date.weekday() >= 4:  # Fri, Sat, Sun
                    weekend_dates.append(date)
            
            if len(weekend_dates) <= 1:
                continue
                
            weekend_dates.sort()
            
            # Check for consecutive weekends (more detailed analysis)
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
                    weekend_violations += 1
                    print(f"  VIOLATION: {violation}")
        
        if weekend_violations == 0:
            print(f"  ✅ No weekend consecutive limit violations found")
        
        # Additional gap constraint checks
        print(f"\nChecking basic gap constraints...")
        gap_violations = 0
        for worker_id, assignments in scheduler.worker_assignments.items():
            assignments_list = sorted(list(assignments))
            for i in range(len(assignments_list) - 1):
                days_between = (assignments_list[i+1] - assignments_list[i]).days
                if days_between <= config['gap_between_shifts']:
                    violation = f"Gap constraint: {worker_id} has {days_between} days between {assignments_list[i].strftime('%Y-%m-%d')} and {assignments_list[i+1].strftime('%Y-%m-%d')} (min: {config['gap_between_shifts']+1})"
                    violations.append(violation)
                    gap_violations += 1
                    print(f"  VIOLATION: {violation}")
        
        if gap_violations == 0:
            print(f"  ✅ No basic gap constraint violations found")
        
        # Summary
        print(f"\n--- FINAL RESULTS ---")
        if violations:
            print(f"❌ CONSTRAINT VIOLATIONS FOUND: {len(violations)}")
            print(f"   - 7/14 day pattern violations: {weekday_violations}")
            print(f"   - Weekend consecutive violations: {weekend_violations}")
            print(f"   - Basic gap violations: {gap_violations}")
            return False
        else:
            print(f"✅ NO CONSTRAINT VIOLATIONS FOUND")
            print(f"   - 7/14 day pattern enforcement: WORKING")
            print(f"   - Weekend consecutive limits: WORKING")
            print(f"   - Basic gap constraints: WORKING")
            
        # Print schedule statistics
        total_assignments = sum(len(assignments) for assignments in scheduler.worker_assignments.values())
        total_slots = sum(len(shifts) for shifts in scheduler.schedule.values())
        coverage = (sum(1 for shifts in scheduler.schedule.values() for shift in shifts if shift is not None) / total_slots) * 100
        
        print(f"\n--- SCHEDULE STATISTICS ---")
        print(f"  Total assignments: {total_assignments}")
        print(f"  Total slots: {total_slots}")
        print(f"  Coverage: {coverage:.1f}%")
        print(f"  Workers: {len(scheduler.workers_data)}")
        print(f"  Period: {config['start_date'].strftime('%Y-%m-%d')} to {config['end_date'].strftime('%Y-%m-%d')}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run final verification test"""
    logging.basicConfig(level=logging.WARNING)  # Reduce noise
    
    print("FINAL CONSTRAINT SYSTEM VERIFICATION")
    print("="*60)
    
    success = test_final_constraint_verification()
    
    if success:
        print(f"\n🎉 CONSTRAINT SYSTEM VERIFICATION SUCCESSFUL")
        print(f"The constraint management system is working correctly.")
        print(f"Both 7/14 day pattern and weekend consecutive limits are properly enforced.")
    else:
        print(f"\n⚠️  CONSTRAINT VIOLATIONS DETECTED")
        print(f"The system still has issues that need to be addressed.")
        
    print(f"\nVERIFICATION COMPLETE")

if __name__ == "__main__":
    main()