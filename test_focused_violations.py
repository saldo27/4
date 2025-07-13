#!/usr/bin/env python3

"""
Focused test to find constraint violations
"""

import logging
from datetime import datetime, timedelta
import sys
import os

# Configure logging to suppress verbose output
logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')

# Import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler

def test_mandatory_violations():
    """Test mandatory assignments that should violate 7-day pattern"""
    
    print("=" * 60)
    print("TESTING MANDATORY ASSIGNMENT VIOLATIONS")
    print("=" * 60)
    
    config = {
        'workers_data': [
            {
                'id': 'W001',
                'name': 'Worker 1',
                'target_shifts': 10,
                'work_percentage': 100,
                'mandatory_days': '8-1-2024;15-1-2024;22-1-2024',  # Three consecutive Mondays
                'days_off': '',
                'is_incompatible': False
            }
        ],
        'holidays': [],
        'start_date': datetime(2024, 1, 1),
        'end_date': datetime(2024, 1, 31),
        'num_shifts': 1,
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2
    }
    
    # Create scheduler
    scheduler = Scheduler(config)
    
    # Generate schedule
    result = scheduler.generate_schedule()
    
    if not result:
        print("❌ Schedule generation failed")
        return False
    
    # Check W001's assignments
    w001_assignments = sorted(list(scheduler.worker_assignments.get('W001', [])))
    print(f"\nW001 assignments: {[d.strftime('%Y-%m-%d (%A)') for d in w001_assignments]}")
    
    # Check mandatory dates specifically
    mandatory_dates = [datetime(2024, 1, 8), datetime(2024, 1, 15), datetime(2024, 1, 22)]
    assigned_mandatory = []
    
    for date in mandatory_dates:
        if date in w001_assignments:
            assigned_mandatory.append(date)
            print(f"✓ Mandatory assignment: {date.strftime('%Y-%m-%d (%A)')}")
        else:
            print(f"✗ Missing mandatory: {date.strftime('%Y-%m-%d (%A)')}")
    
    # Check for 7-day pattern violations
    violations = []
    for i, date1 in enumerate(assigned_mandatory):
        for j in range(i + 1, len(assigned_mandatory)):
            date2 = assigned_mandatory[j]
            days_diff = (date2 - date1).days
            
            if (days_diff == 7 or days_diff == 14) and date1.weekday() == date2.weekday():
                if date1.weekday() < 4:  # Monday=0 to Thursday=3
                    violations.append(
                        f"7-day pattern violation: {date1.strftime('%Y-%m-%d (%A)')} -> {date2.strftime('%Y-%m-%d (%A)')} ({days_diff} days)"
                    )
    
    print(f"\nViolation analysis:")
    if violations:
        print(f"❌ FOUND {len(violations)} VIOLATIONS:")
        for violation in violations:
            print(f"  - {violation}")
        return True  # Return True if violations found (this is what we expect)
    else:
        print("✅ No violations found")
        return False

if __name__ == "__main__":
    violations_found = test_mandatory_violations()
    
    if violations_found:
        print(f"\n🎯 SUCCESS: Test detected constraint violations as expected!")
        print("This confirms that mandatory assignments can bypass constraint checks.")
        sys.exit(0)
    else:
        print(f"\n❌ UNEXPECTED: No violations found - mandatory constraints may be working properly.")
        sys.exit(1)