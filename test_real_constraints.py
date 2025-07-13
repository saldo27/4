"""
Test real constraint enforcement without force parameter.
This test validates that constraints are properly enforced in normal operation.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler

# Configure logging for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_real_7_14_day_pattern():
    """Test 7/14 day pattern without force parameter"""
    print("\n" + "="*60)
    print("Testing REAL 7/14 Day Pattern Constraint (without force)")
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
    
    # Step 1: Assign worker to first Monday WITHOUT force
    print(f"\nStep 1: Assigning W001 to {monday1.strftime('%Y-%m-%d')} (no force)")
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', monday1, 0)
    print(f"  Result: {result1.success} - {result1.message}")
    
    if not result1.success:
        print("❌ Failed to assign first Monday - this should work!")
        return False
    
    # Step 2: Try to assign same worker to Monday 7 days later (should fail)
    print(f"\nStep 2: Trying to assign W001 to {monday2.strftime('%Y-%m-%d')} (7 days apart, same weekday) WITHOUT force")
    result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', monday2, 0)
    print(f"  Result: {result2.success} - {result2.message}")
    
    # Step 3: Try to assign same worker to Monday 14 days later (should also fail)
    print(f"\nStep 3: Trying to assign W001 to {monday3.strftime('%Y-%m-%d')} (14 days apart, same weekday) WITHOUT force")
    result3 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', monday3, 0)
    print(f"  Result: {result3.success} - {result3.message}")
    
    # Step 4: Try to assign to a different weekday with sufficient gap (should succeed)
    friday = datetime(2024, 1, 5)  # Friday (different weekday, 4 days gap)
    print(f"\nStep 4: Trying to assign W001 to {friday.strftime('%Y-%m-%d')} (different weekday, sufficient gap) WITHOUT force")
    result4 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', friday, 0)
    print(f"  Result: {result4.success} - {result4.message}")
    
    # Results analysis
    print(f"\n📊 REAL 7/14 Day Pattern Test Results:")
    print(f"  First Monday assignment:    {'✓ PASS' if result1.success else '✗ FAIL'}")
    print(f"  7-day pattern prevention:   {'✓ PASS' if not result2.success else '✗ FAIL'}")
    print(f"  14-day pattern prevention:  {'✓ PASS' if not result3.success else '✗ FAIL'}")
    print(f"  Different weekday allowed:  {'✓ PASS' if result4.success else '✗ FAIL'}")
    
    # Check current schedule
    print(f"\n📅 Current Schedule:")
    for date, shifts in scheduler.schedule.items():
        print(f"  {date.strftime('%Y-%m-%d')}: {shifts}")
    
    success = (result1.success and not result2.success and not result3.success and result4.success)
    
    if not success:
        print(f"\n❌ 7/14 Day Pattern Constraint is NOT working properly!")
    else:
        print(f"\n✅ 7/14 Day Pattern Constraint is working correctly!")
    
    return success


def test_real_weekend_limits():
    """Test weekend limits without force parameter"""
    print("\n" + "="*60)
    print("Testing REAL Weekend Limit Enforcement (without force)")
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
        'holidays': [],
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    # Weekend dates  
    saturday1 = datetime(2024, 1, 6)  # Saturday week 1
    saturday2 = datetime(2024, 1, 13) # Saturday week 2 (consecutive weekend)
    saturday3 = datetime(2024, 1, 20) # Saturday week 3 (would be 3rd consecutive)
    
    print(f"Testing consecutive weekend scenario:")
    print(f"  {saturday1.strftime('%Y-%m-%d')} (Saturday week 1)")
    print(f"  {saturday2.strftime('%Y-%m-%d')} (Saturday week 2 - consecutive)")
    print(f"  {saturday3.strftime('%Y-%m-%d')} (Saturday week 3 - should fail)")
    
    # Step 1: Assign worker to first Saturday WITHOUT force
    print(f"\nStep 1: Assigning W001 to {saturday1.strftime('%Y-%m-%d')} (no force)")
    result1 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday1, 0)
    print(f"  Result: {result1.success} - {result1.message}")
    
    if not result1.success:
        print("❌ Failed to assign first Saturday - this should work!")
        return False
    
    # Step 2: Assign worker to second consecutive Saturday WITHOUT force
    print(f"\nStep 2: Assigning W001 to {saturday2.strftime('%Y-%m-%d')} (consecutive weekend) WITHOUT force")
    result2 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday2, 0)
    print(f"  Result: {result2.success} - {result2.message}")
    
    if not result2.success:
        print("❌ Failed to assign second Saturday - this should work (within limit)!")
        return False
    
    # Step 3: Try to assign to third consecutive Saturday (should fail - exceeds consecutive limit)
    print(f"\nStep 3: Trying to assign W001 to {saturday3.strftime('%Y-%m-%d')} (would exceed consecutive weekend limit) WITHOUT force")
    result3 = scheduler.real_time_engine.incremental_updater.assign_worker_to_shift('W001', saturday3, 0)
    print(f"  Result: {result3.success} - {result3.message}")
    
    # Results analysis
    print(f"\n📊 REAL Weekend Limit Test Results:")
    print(f"  First Saturday assignment:     {'✓ PASS' if result1.success else '✗ FAIL'}")
    print(f"  Second Saturday assignment:    {'✓ PASS' if result2.success else '✗ FAIL'}")
    print(f"  Third Saturday prevention:     {'✓ PASS' if not result3.success else '✗ FAIL'}")
    
    # Check current schedule
    print(f"\n📅 Current Schedule:")
    for date, shifts in scheduler.schedule.items():
        print(f"  {date.strftime('%Y-%m-%d')}: {shifts}")
    
    success = (result1.success and result2.success and not result3.success)
    
    if not success:
        print(f"\n❌ Weekend Limit Constraint is NOT working properly!")
    else:
        print(f"\n✅ Weekend Limit Constraint is working correctly!")
    
    return success


def test_schedule_generation():
    """Test if schedule generation itself violates constraints"""
    print("\n" + "="*60)
    print("Testing Schedule Generation for Constraint Violations")
    print("="*60)
    
    start_date = datetime(2024, 1, 1)  # Monday
    end_date = datetime(2024, 2, 1)    # Full month
    
    workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': []},
        {'id': 'W002', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'incompatible_with': []},
    ]
    
    config = {
        'start_date': start_date,
        'end_date': end_date,
        'num_workers': len(workers_data),
        'num_shifts': 1,
        'gap_between_shifts': 2,
        'max_consecutive_weekends': 2,
        'holidays': [],
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True,
    }
    
    # Initialize scheduler
    scheduler = Scheduler(config)
    
    # Generate a full schedule
    print("Generating full schedule...")
    schedule_result = scheduler.generate_schedule()
    schedule = scheduler.schedule  # Use the actual schedule
    print(f"Schedule generated with {len([d for d, shifts in schedule.items() if any(shifts)])} assigned days")
    
    # Check for 7/14 day pattern violations
    print("\nChecking for 7/14 day pattern violations...")
    violations = []
    
    for worker_id in ['W001', 'W002']:
        worker_dates = []
        for date, shifts in schedule.items():
            for i, assigned_worker in enumerate(shifts):
                if assigned_worker == worker_id:
                    worker_dates.append(date)
        
        worker_dates.sort()
        
        for i, date1 in enumerate(worker_dates):
            for j, date2 in enumerate(worker_dates[i+1:], i+1):
                days_diff = (date2 - date1).days
                if (days_diff == 7 or days_diff == 14) and date1.weekday() == date2.weekday():
                    violations.append(f"Worker {worker_id}: {date1.strftime('%Y-%m-%d')} and {date2.strftime('%Y-%m-%d')} ({days_diff} days apart, same weekday)")
    
    # Check for consecutive weekend violations
    print("Checking for consecutive weekend violations...")
    
    for worker_id in ['W001', 'W002']:
        weekend_dates = []
        for date, shifts in schedule.items():
            for i, assigned_worker in enumerate(shifts):
                if assigned_worker == worker_id and date.weekday() >= 4:  # Fri, Sat, Sun
                    weekend_dates.append(date)
        
        weekend_dates.sort()
        
        # Check for consecutive weekends
        consecutive_count = 1
        for i in range(1, len(weekend_dates)):
            if (weekend_dates[i] - weekend_dates[i-1]).days <= 7:
                consecutive_count += 1
                if consecutive_count > 2:  # Exceeds max_consecutive_weekends
                    violations.append(f"Worker {worker_id}: Consecutive weekend violation at {weekend_dates[i].strftime('%Y-%m-%d')} (count: {consecutive_count})")
            else:
                consecutive_count = 1
    
    print(f"\n📊 Schedule Generation Test Results:")
    print(f"  Generated schedule: {'✓ SUCCESS' if schedule_result else '✗ FAILED'}")
    print(f"  Constraint violations found: {len(violations)}")
    
    if violations:
        print(f"\n❌ CONSTRAINT VIOLATIONS DETECTED:")
        for violation in violations[:10]:  # Show first 10
            print(f"     {violation}")
        if len(violations) > 10:
            print(f"     ... and {len(violations) - 10} more violations")
    else:
        print(f"\n✅ No constraint violations found in generated schedule!")
    
    return len(violations) == 0


def main():
    """Run all real constraint tests"""
    print("🧪 Starting REAL Constraint Management Tests (without force)")
    print("=" * 80)
    
    results = []
    
    # Run tests
    results.append(("Real 7/14 Day Pattern", test_real_7_14_day_pattern()))
    results.append(("Real Weekend Limits", test_real_weekend_limits()))
    results.append(("Schedule Generation", test_schedule_generation()))
    
    # Summary
    print("\n" + "="*80)
    print("📋 REAL TEST SUMMARY")
    print("="*80)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name:<25} {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All real tests passed! Constraints are working properly.")
    else:
        print("⚠️  Some real tests failed. This confirms the user's issue!")
    
    return passed == len(results)


if __name__ == "__main__":
    main()