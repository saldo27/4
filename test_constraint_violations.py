#!/usr/bin/env python3

"""
Simple test to verify if constraint violations are still occurring
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

def test_constraint_violations():
    """Test if constraint violations still occur during schedule generation"""
    
    print("=" * 80)
    print("TESTING CONSTRAINT VIOLATIONS")
    print("=" * 80)
    
    # Create test configuration similar to main.py
    config = {
        'workers_data': [
            {
                'id': 'W001',
                'name': 'Worker 1',
                'target_shifts': 10,
                'work_percentage': 100,
                'mandatory_days': '',
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
            },
            {
                'id': 'W003',
                'name': 'Worker 3', 
                'target_shifts': 10,
                'work_percentage': 100,
                'mandatory_days': '',
                'days_off': '',
                'is_incompatible': False
            },
            {
                'id': 'W004',
                'name': 'Worker 4',
                'target_shifts': 10,
                'work_percentage': 100,
                'mandatory_days': '',
                'days_off': '',
                'is_incompatible': False
            }
        ],
        'holidays': [],
        'start_date': datetime(2024, 1, 1),
        'end_date': datetime(2024, 2, 29),
        'num_shifts': 2,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2
    }
    
    print(f"Generating schedule from {config['start_date'].strftime('%Y-%m-%d')} to {config['end_date'].strftime('%Y-%m-%d')}")
    print(f"Workers: {len(config['workers_data'])}")
    print(f"Posts per day: {config['num_shifts']}")
    print(f"Gap between shifts: {config['gap_between_shifts']}")
    print(f"Max consecutive weekends: {config['max_consecutive_weekends']}")
    print()
    
    # Create scheduler
    scheduler = Scheduler(config)
    
    # Generate schedule
    try:
        result = scheduler.generate_schedule()
        if result:
            print("✅ Schedule generation completed")
            
            # Check for constraint violations
            violations = check_constraint_violations(scheduler)
            
            if violations:
                print(f"\n❌ FOUND {len(violations)} CONSTRAINT VIOLATIONS:")
                for i, violation in enumerate(violations, 1):
                    print(f"{i}. {violation}")
                return False
            else:
                print("\n✅ NO CONSTRAINT VIOLATIONS FOUND")
                return True
                
        else:
            print("❌ Schedule generation failed")
            return False
            
    except Exception as e:
        print(f"❌ Error during schedule generation: {e}")
        logging.error(f"Schedule generation error: {e}", exc_info=True)
        return False

def check_constraint_violations(scheduler):
    """Check for constraint violations in the generated schedule"""
    violations = []
    
    print("\nChecking constraint violations...")
    
    # Check 7/14 day pattern violations
    pattern_violations = check_7_14_day_pattern_violations(scheduler)
    violations.extend(pattern_violations)
    
    # Check weekend consecutive limit violations  
    weekend_violations = check_weekend_limit_violations(scheduler)
    violations.extend(weekend_violations)
    
    return violations

def check_7_14_day_pattern_violations(scheduler):
    """Check for 7/14 day pattern violations"""
    violations = []
    
    print("  Checking 7/14 day pattern violations...")
    
    for worker_id, assigned_dates in scheduler.worker_assignments.items():
        if len(assigned_dates) < 2:
            continue
            
        # Convert to sorted list for easier checking
        sorted_dates = sorted(assigned_dates)
        
        for i, date1 in enumerate(sorted_dates):
            for j in range(i + 1, len(sorted_dates)):
                date2 = sorted_dates[j]
                days_diff = (date2 - date1).days
                
                # Check for 7 or 14 day pattern on same weekday
                if (days_diff == 7 or days_diff == 14) and date1.weekday() == date2.weekday():
                    # Only apply to weekdays (Mon-Thu), not weekends (Fri-Sun)
                    if date1.weekday() < 4:  # Monday=0 to Thursday=3
                        violations.append(
                            f"7/14 day pattern violation: Worker {worker_id} assigned on "
                            f"{date1.strftime('%Y-%m-%d')} ({date1.strftime('%A')}) and "
                            f"{date2.strftime('%Y-%m-%d')} ({date2.strftime('%A')}) - {days_diff} days apart"
                        )
    
    print(f"    Found {len(violations)} 7/14 day pattern violations")
    return violations

def check_weekend_limit_violations(scheduler):
    """Check for weekend consecutive limit violations"""
    violations = []
    
    print("  Checking weekend consecutive limit violations...")
    
    for worker_id, assigned_dates in scheduler.worker_assignments.items():
        if len(assigned_dates) < 2:
            continue
            
        # Get weekend/holiday dates only
        weekend_dates = []
        for date in assigned_dates:
            if date.weekday() >= 4:  # Fri, Sat, Sun
                weekend_dates.append(date)
            elif scheduler.holidays and date in scheduler.holidays:
                weekend_dates.append(date)
        
        if len(weekend_dates) < 2:
            continue
            
        # Sort weekend dates
        weekend_dates.sort()
        
        # Check for consecutive weekends
        consecutive_count = 1
        max_consecutive = scheduler.max_consecutive_weekends
        
        for i in range(1, len(weekend_dates)):
            days_diff = (weekend_dates[i] - weekend_dates[i-1]).days
            
            # Check if it's the next weekend (5-9 days apart typically)
            if 5 <= days_diff <= 9:
                consecutive_count += 1
                if consecutive_count > max_consecutive:
                    violations.append(
                        f"Weekend consecutive limit violation: Worker {worker_id} has "
                        f"{consecutive_count} consecutive weekends (limit: {max_consecutive}) - "
                        f"ending on {weekend_dates[i].strftime('%Y-%m-%d')}"
                    )
            else:
                consecutive_count = 1
    
    print(f"    Found {len(violations)} weekend consecutive limit violations")
    return violations

if __name__ == "__main__":
    success = test_constraint_violations()
    sys.exit(0 if success else 1)