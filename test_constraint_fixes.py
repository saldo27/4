"""
Test suite for constraints management fixes.
Validates 7/14 day pattern and weekend limit enforcement.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler
from live_validator import LiveValidator, ValidationSeverity
from incremental_updater import IncrementalUpdater

# Configure logging for testing
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_7_14_day_pattern_constraint():
    """Test that 7/14 day pattern constraint is properly enforced"""
    print("\n" + "="*60)
    print("Testing 7/14 Day Pattern Constraint")
    print("="*60)
    
    # Create test configuration
    start_date = datetime(2024, 1, 1)  # Monday
    end_date = datetime(2024, 1, 28)   # 4 weeks
    
    workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': []},
    ]
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_workers': len(workers_data),
        'num_shifts': 1,  # Single shift per day for simplicity
        'gap_between_shifts': 2,
        'max_consecutive_weekends': 2,
        'holidays': [],
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    # Test dates - same weekday (Monday) 7 days apart
    monday1 = datetime(2024, 1, 1)   # Monday week 1
    monday2 = datetime(2024, 1, 8)   # Monday week 2 (7 days apart)
    monday3 = datetime(2024, 1, 15)  # Monday week 3 (14 days from week 1)
    
    print(f"Testing dates:")
    print(f"  Monday 1: {monday1.strftime('%Y-%m-%d')} (weekday: {monday1.weekday()})")
    print(f"  Monday 2: {monday2.strftime('%Y-%m-%d')} (weekday: {monday2.weekday()}) - 7 days apart")
    print(f"  Monday 3: {monday3.strftime('%Y-%m-%d')} (weekday: {monday3.weekday()}) - 14 days from first")
    
    # Manually assign worker to first Monday
    scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', monday1, 0, force=True)
    print(f"✓ Assigned W001 to {monday1.strftime('%Y-%m-%d')}")
    
    # Test 1: Try to assign same worker to Monday 7 days later (should fail)
    print(f"\nTest 1: Trying to assign W001 to {monday2.strftime('%Y-%m-%d')} (7 days apart, same weekday)")
    
    # Test constraint checker directly
    can_assign_cc = scheduler.constraint_checker._check_gap_constraint('W001', monday2)
    print(f"  Constraint Checker result: {can_assign_cc}")
    
    # Test live validator
    validation_result = scheduler.real_time_engine.live_validator.validate_assignment('W001', monday2, 0)
    print(f"  Live Validator result: {validation_result.is_valid} - {validation_result.message}")
    
    # Test incremental updater
    update_result = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', monday2, 0)
    print(f"  Incremental Updater result: {update_result.success} - {update_result.message}")
    
    # Test 2: Try to assign same worker to Monday 14 days later (should also fail)
    print(f"\nTest 2: Trying to assign W001 to {monday3.strftime('%Y-%m-%d')} (14 days apart, same weekday)")
    
    # Test constraint checker directly
    can_assign_cc_14 = scheduler.constraint_checker._check_gap_constraint('W001', monday3)
    print(f"  Constraint Checker result: {can_assign_cc_14}")
    
    # Test live validator
    validation_result_14 = scheduler.real_time_engine.live_validator.validate_assignment('W001', monday3, 0)
    print(f"  Live Validator result: {validation_result_14.is_valid} - {validation_result_14.message}")
    
    # Test incremental updater
    update_result_14 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', monday3, 0)
    print(f"  Incremental Updater result: {update_result_14.success} - {update_result_14.message}")
    
    # Test 3: Try to assign to a different weekday with sufficient gap (should succeed)
    friday = datetime(2024, 1, 5)  # Friday (different weekday, 4 days gap)
    print(f"\nTest 3: Trying to assign W001 to {friday.strftime('%Y-%m-%d')} (different weekday, sufficient gap)")
    
    can_assign_cc_diff = scheduler.constraint_checker._check_gap_constraint('W001', friday)
    print(f"  Constraint Checker result: {can_assign_cc_diff}")
    
    validation_result_diff = scheduler.real_time_engine.live_validator.validate_assignment('W001', friday, 0)
    print(f"  Live Validator result: {validation_result_diff.is_valid} - {validation_result_diff.message}")
    
    # Results analysis
    print(f"\n📊 7/14 Day Pattern Test Results:")
    print(f"  Constraint Checker 7-day:  {'✓ PASS' if not can_assign_cc else '✗ FAIL'}")
    print(f"  Live Validator 7-day:      {'✓ PASS' if not validation_result.is_valid else '✗ FAIL'}")
    print(f"  Incremental Updater 7-day: {'✓ PASS' if not update_result.success else '✗ FAIL'}")
    print(f"  Constraint Checker 14-day: {'✓ PASS' if not can_assign_cc_14 else '✗ FAIL'}")
    print(f"  Live Validator 14-day:     {'✓ PASS' if not validation_result_14.is_valid else '✗ FAIL'}")
    print(f"  Incremental Updater 14-day:{'✓ PASS' if not update_result_14.success else '✗ FAIL'}")
    print(f"  Different weekday:         {'✓ PASS' if can_assign_cc_diff else '✗ FAIL'}")
    
    # Check for inconsistencies
    inconsistencies = []
    if can_assign_cc != validation_result.is_valid:
        inconsistencies.append("7-day: Constraint Checker vs Live Validator")
    if can_assign_cc_14 != validation_result_14.is_valid:
        inconsistencies.append("14-day: Constraint Checker vs Live Validator")
    
    if inconsistencies:
        print(f"\n⚠️  INCONSISTENCIES DETECTED:")
        for inc in inconsistencies:
            print(f"     {inc}")
    else:
        print(f"\n✅ All modules are consistent")
    
    return len(inconsistencies) == 0


def test_weekend_limit_enforcement():
    """Test that weekend/holiday limit constraints are properly enforced"""
    print("\n" + "="*60)
    print("Testing Weekend/Holiday Limit Enforcement")
    print("="*60)
    
    # Create test configuration with weekend limits
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
        'gap_between_shifts': 1,  # Allow closer assignments for this test
        'max_consecutive_weekends': 2,  # Maximum 2 consecutive weekends
        'holidays': [datetime(2024, 1, 15)],  # Add a holiday Monday
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    # Weekend/Holiday dates  
    saturday1 = datetime(2024, 1, 6)  # Saturday week 1
    saturday2 = datetime(2024, 1, 13) # Saturday week 2 (consecutive weekend)
    saturday3 = datetime(2024, 1, 20) # Saturday week 3 (would be 3rd consecutive)
    
    weekend_dates = [saturday1, saturday2, saturday3]
    
    print(f"Testing consecutive weekend scenario:")
    for i, date in enumerate(weekend_dates[:2], 1):
        day_type = f"{date.strftime('%A')} week {i}"
        print(f"  {date.strftime('%Y-%m-%d')} ({day_type})")
    print(f"  {saturday3.strftime('%Y-%m-%d')} (Saturday week 3 - should fail)")
    
    # Assign worker to first two consecutive Saturdays
    scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday1, 0, force=True)
    scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday2, 0, force=True)
    print(f"✓ Assigned W001 to consecutive Saturdays: {saturday1.strftime('%Y-%m-%d')}, {saturday2.strftime('%Y-%m-%d')}")
    
    # Test 1: Try to assign to third consecutive Saturday (should fail - exceeds consecutive limit)
    print(f"\nTest 1: Trying to assign W001 to {saturday3.strftime('%Y-%m-%d')} (would exceed consecutive weekend limit)")
    
    # Test constraint checker directly
    can_assign_cc = not scheduler.constraint_checker._would_exceed_weekend_limit('W001', saturday3)
    print(f"  Constraint Checker result: {can_assign_cc}")
    
    # Test live validator
    validation_result = scheduler.real_time_engine.live_validator.validate_assignment('W001', saturday3, 0)
    print(f"  Live Validator result: {validation_result.is_valid} - {validation_result.message}")
    
    # Test incremental updater
    update_result = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday3, 0)
    print(f"  Incremental Updater result: {update_result.success} - {update_result.message}")
    
    # Test 2: Try to assign to holiday (separate test)
    holiday = datetime(2024, 1, 15)   # Holiday Monday week 3
    print(f"\nTest 2: Trying to assign W001 to {holiday.strftime('%Y-%m-%d')} (holiday)")
    
    can_assign_cc_holiday = not scheduler.constraint_checker._would_exceed_weekend_limit('W001', holiday)
    print(f"  Constraint Checker result: {can_assign_cc_holiday}")
    
    validation_result_holiday = scheduler.real_time_engine.live_validator.validate_assignment('W001', holiday, 0)
    print(f"  Live Validator result: {validation_result_holiday.is_valid} - {validation_result_holiday.message}")
    
    update_result_holiday = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', holiday, 0)
    print(f"  Incremental Updater result: {update_result_holiday.success} - {update_result_holiday.message}")
    
    # Results analysis
    print(f"\n📊 Weekend Limit Test Results:")
    print(f"  Constraint Checker weekend:  {'✓ PASS' if not can_assign_cc else '✗ FAIL'}")
    print(f"  Live Validator weekend:      {'✓ PASS' if not validation_result.is_valid else '✗ FAIL'}")
    print(f"  Incremental Updater weekend: {'✓ PASS' if not update_result.success else '✗ FAIL'}")
    print(f"  Constraint Checker holiday:  {'✓ PASS' if not can_assign_cc_holiday else '✗ FAIL'}")
    print(f"  Live Validator holiday:      {'✓ PASS' if not validation_result_holiday.is_valid else '✗ FAIL'}")
    print(f"  Incremental Updater holiday: {'✓ PASS' if not update_result_holiday.success else '✗ FAIL'}")
    
    # Check for inconsistencies
    inconsistencies = []
    if can_assign_cc != validation_result.is_valid:
        inconsistencies.append("Weekend: Constraint Checker vs Live Validator")
    if can_assign_cc_holiday != validation_result_holiday.is_valid:
        inconsistencies.append("Holiday: Constraint Checker vs Live Validator")
    
    if inconsistencies:
        print(f"\n⚠️  INCONSISTENCIES DETECTED:")
        for inc in inconsistencies:
            print(f"     {inc}")
    else:
        print(f"\n✅ All modules are consistent")
    
    return len(inconsistencies) == 0


def test_swap_constraint_validation():
    """Test that swap operations properly validate constraints"""
    print("\n" + "="*60)
    print("Testing Swap Constraint Validation")
    print("="*60)
    
    # Create test configuration
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 21)
    
    workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': ['W002']},
        {'id': 'W002', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': ['W001']},
    ]
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_workers': len(workers_data),
        'num_shifts': 2,  # 2 shifts per day
        'gap_between_shifts': 2,
        'max_consecutive_weekends': 2,
        'holidays': [],
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    # Setup initial assignments
    date1 = datetime(2024, 1, 1)  # Monday
    date2 = datetime(2024, 1, 8)  # Monday next week (7 days apart, same weekday)
    
    # Assign workers to same date (different posts) - this should work
    scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', date1, 0, force=True)
    scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W002', date2, 0, force=True)
    
    print(f"Initial assignments:")
    print(f"  W001 -> {date1.strftime('%Y-%m-%d')} post 0")
    print(f"  W002 -> {date2.strftime('%Y-%m-%d')} post 0")
    
    # Test 1: Try to swap (should fail due to 7/14 day pattern)
    print(f"\nTest 1: Swapping W001 and W002 (7-day pattern violation)")
    print(f"  This would put W001 on {date2.strftime('%Y-%m-%d')} (same weekday, 7 days apart)")
    
    swap_result = scheduler.real_time_engine.incremental_updater.swap_workers(date1, 0, date2, 0)
    print(f"  Swap result: {swap_result.success} - {swap_result.message}")
    if swap_result.conflicts:
        print(f"  Conflicts: {swap_result.conflicts}")
    
    # Test 2: Try swap with incompatible workers on same date
    date3 = datetime(2024, 1, 2)  # Tuesday
    scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', date3, 1, force=True)  # Different post
    
    print(f"\nTest 2: Trying to swap incompatible workers to same date")
    print(f"  W001 is on {date3.strftime('%Y-%m-%d')} post 1")
    print(f"  Trying to swap W002 from {date2.strftime('%Y-%m-%d')} to {date3.strftime('%Y-%m-%d')} post 0")
    
    swap_result_incomp = scheduler.real_time_engine.incremental_updater.swap_workers(date2, 0, date3, 0)
    print(f"  Swap result: {swap_result_incomp.success} - {swap_result_incomp.message}")
    if swap_result_incomp.conflicts:
        print(f"  Conflicts: {swap_result_incomp.conflicts}")
    
    # Results analysis
    print(f"\n📊 Swap Constraint Test Results:")
    print(f"  7/14 day pattern prevention: {'✓ PASS' if not swap_result.success else '✗ FAIL'}")
    print(f"  Incompatibility prevention:  {'✓ PASS' if not swap_result_incomp.success else '✗ FAIL'}")
    
    return not swap_result.success and not swap_result_incomp.success


def main():
    """Run all constraint tests"""
    print("🧪 Starting Constraint Management Tests")
    print("=" * 80)
    
    results = []
    
    # Run tests
    results.append(("7/14 Day Pattern", test_7_14_day_pattern_constraint()))
    results.append(("Weekend Limits", test_weekend_limit_enforcement()))
    results.append(("Swap Validation", test_swap_constraint_validation()))
    
    # Summary
    print("\n" + "="*80)
    print("📋 TEST SUMMARY")
    print("="*80)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name:<25} {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! No constraint management issues detected.")
    else:
        print("⚠️  Some tests failed. Constraint management fixes needed.")
    
    return passed == len(results)


if __name__ == "__main__":
    main()