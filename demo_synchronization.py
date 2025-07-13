#!/usr/bin/env python3
"""
Demo script showcasing the data synchronization improvements.
This demonstrates that worker_assignments and schedule are now properly synchronized.
"""

import os
import sys
from datetime import datetime, timedelta
import logging

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler
from live_validator import LiveValidator, ValidationSeverity

def setup_test_scheduler():
    """Set up a test scheduler for demonstration"""
    start_date = datetime(2024, 1, 1)  # Monday
    end_date = datetime(2024, 1, 21)   # 3 weeks
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_shifts': 2,
        'variable_shifts': [],
        'workers_data': [
            {
                'id': 'W001',
                'name': 'Worker 1',
                'work_percentage': 100,
                'target_shifts': 5,
                'work_periods': '',
                'days_off': '',
                'mandatory_days': '',
                'incompatible_with': ['W002']
            },
            {
                'id': 'W002',
                'name': 'Worker 2',
                'work_percentage': 100,
                'target_shifts': 5,
                'work_periods': '',
                'days_off': '',
                'mandatory_days': '',
                'incompatible_with': ['W001']
            },
            {
                'id': 'W003',
                'name': 'Worker 3',
                'work_percentage': 100,
                'target_shifts': 4,
                'work_periods': '',
                'days_off': '',
                'mandatory_days': '',
                'incompatible_with': []
            }
        ],
        'holidays': [],
        'gap_between_shifts': 2,
        'max_consecutive_weekends': 2
    }
    
    return Scheduler(config)

def demonstrate_synchronization():
    """Demonstrate the data synchronization features"""
    print("🔄 DEMONSTRATING DATA SYNCHRONIZATION IMPROVEMENTS")
    print("=" * 60)
    
    scheduler = setup_test_scheduler()
    live_validator = LiveValidator(scheduler)
    
    # 1. Test initial synchronization
    print("\n1. Initial State Validation:")
    is_sync, report = scheduler._validate_data_synchronization()
    print(f"   ✅ Initial synchronization: {'OK' if is_sync else 'FAILED'}")
    print(f"   📊 Schedule assignments: {report['summary']['total_assignments_schedule']}")
    print(f"   📊 Tracking assignments: {report['summary']['total_assignments_tracking']}")
    
    # 2. Create a valid assignment
    print("\n2. Making Valid Assignment:")
    monday1 = datetime(2024, 1, 1)
    scheduler.schedule[monday1] = ['W001', None]
    scheduler._update_tracking_data('W001', monday1, 0, removing=False)
    
    is_sync, report = scheduler._validate_data_synchronization()
    print(f"   ✅ After assignment: {'OK' if is_sync else 'FAILED'}")
    print(f"   📊 Total assignments: {report['summary']['total_assignments_schedule']}")
    
    # 3. Test 7-day pattern constraint
    print("\n3. Testing 7-Day Pattern Constraint:")
    monday2 = monday1 + timedelta(days=7)  # Same weekday, 7 days later
    result = live_validator.validate_assignment('W001', monday2, 1)
    print(f"   🚫 7-day pattern constraint: {'BLOCKED' if not result.is_valid else 'ALLOWED'}")
    if not result.is_valid:
        print(f"   📝 Reason: {result.message}")
    
    # 4. Test weekend exception (weekend 7-day patterns are allowed)
    print("\n4. Testing Weekend 7-Day Pattern Exception:")
    friday1 = datetime(2024, 1, 5)  # Friday
    friday2 = friday1 + timedelta(days=7)  # Friday, 7 days later
    
    # Assign to first Friday
    scheduler.schedule[friday1] = ['W002', None]
    scheduler._update_tracking_data('W002', friday1, 0, removing=False)
    
    # Test second Friday
    result = live_validator.validate_assignment('W002', friday2, 1)
    print(f"   ✅ Weekend 7-day pattern: {'ALLOWED' if result.is_valid else 'BLOCKED'}")
    if not result.is_valid and "7/14 day pattern" in result.message:
        print(f"   ❌ Unexpected: Weekend pattern should be allowed")
    
    # 5. Test incompatibility constraint
    print("\n5. Testing Incompatibility Constraint:")
    same_date = datetime(2024, 1, 10)
    scheduler.schedule[same_date] = ['W001', None]
    scheduler._update_tracking_data('W001', same_date, 0, removing=False)
    
    result = live_validator.validate_assignment('W002', same_date, 1)
    print(f"   🚫 Incompatibility constraint: {'BLOCKED' if not result.is_valid else 'ALLOWED'}")
    if not result.is_valid:
        print(f"   📝 Reason: {result.message}")
    
    # 6. Create an intentional synchronization issue and test repair
    print("\n6. Testing Synchronization Repair:")
    print("   Creating intentional desynchronization...")
    
    # Add to schedule but not tracking
    test_date = datetime(2024, 1, 15)
    scheduler.schedule[test_date] = ['W003', None]
    # Don't update worker_assignments - this creates desynchronization
    
    is_sync_before, report_before = scheduler._validate_data_synchronization()
    print(f"   ❌ Before repair: {'SYNC' if is_sync_before else 'DESYNC'}")
    print(f"   📊 Missing from tracking: {report_before['summary']['missing_from_tracking']}")
    
    # Repair the synchronization
    repair_success = scheduler._repair_data_synchronization()
    print(f"   🔧 Repair attempt: {'SUCCESS' if repair_success else 'FAILED'}")
    
    is_sync_after, report_after = scheduler._validate_data_synchronization()
    print(f"   ✅ After repair: {'SYNC' if is_sync_after else 'DESYNC'}")
    
    # 7. Full schedule integrity validation
    print("\n7. Full Schedule Integrity Check:")
    integrity_results = live_validator.validate_schedule_integrity(check_partial=True)
    
    errors = [r for r in integrity_results if r.severity == ValidationSeverity.ERROR]
    warnings = [r for r in integrity_results if r.severity == ValidationSeverity.WARNING]
    
    print(f"   📋 Total checks: {len(integrity_results)}")
    print(f"   ❌ Errors: {len(errors)}")
    print(f"   ⚠️  Warnings: {len(warnings)}")
    
    if errors:
        print("   🔍 Error details:")
        for error in errors[:3]:  # Show first 3 errors
            print(f"      - {error.constraint_type}: {error.message}")
    
    print("\n✨ DEMONSTRATION COMPLETE")
    print("🎯 Key improvements implemented:")
    print("   • Data synchronization validation and repair")
    print("   • Enhanced constraint checking with sync awareness")
    print("   • Proper 7/14 day pattern enforcement (weekdays only)")
    print("   • Weekend pattern exceptions correctly handled")
    print("   • Incompatibility constraint enforcement")
    print("   • Comprehensive integrity validation")

if __name__ == '__main__':
    # Set up minimal logging
    logging.basicConfig(level=logging.WARNING)
    
    demonstrate_synchronization()