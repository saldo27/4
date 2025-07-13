#!/usr/bin/env python3
"""
Test suite for worker_assignments and schedule synchronization.
Tests the new synchronization validation and repair mechanisms.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
import logging

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler
from constraint_checker import ConstraintChecker
from live_validator import LiveValidator, ValidationSeverity

class TestDataSynchronization(unittest.TestCase):
    """Test data synchronization between worker_assignments and schedule"""
    
    def setUp(self):
        """Set up test scheduler with basic configuration"""
        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime(2024, 1, 7)  # One week for testing
        
        self.config = {
            'start_date': self.start_date,
            'end_date': self.end_date,
            'num_shifts': 2,
            'variable_shifts': [],
            'workers_data': [
                {
                    'id': 'W001',
                    'name': 'Worker 1',
                    'work_percentage': 100,
                    'target_shifts': 3,
                    'work_periods': '',
                    'days_off': '',
                    'mandatory_days': '',
                    'incompatible_with': []
                },
                {
                    'id': 'W002', 
                    'name': 'Worker 2',
                    'work_percentage': 100,
                    'target_shifts': 3,
                    'work_periods': '',
                    'days_off': '',
                    'mandatory_days': '',
                    'incompatible_with': []
                },
                {
                    'id': 'W003',
                    'name': 'Worker 3', 
                    'work_percentage': 100,
                    'target_shifts': 2,
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
        
        self.scheduler = Scheduler(self.config)
        
    def test_synchronization_validation_empty_schedule(self):
        """Test synchronization validation on empty schedule"""
        is_synchronized, report = self.scheduler._validate_data_synchronization()
        
        self.assertTrue(is_synchronized, "Empty schedule should be synchronized")
        self.assertEqual(report['summary']['total_assignments_schedule'], 0)
        self.assertEqual(report['summary']['total_assignments_tracking'], 0)
        self.assertEqual(len(report['discrepancies']), 0)
        
    def test_synchronization_after_manual_assignment(self):
        """Test synchronization after making manual assignments"""
        date1 = self.start_date
        date2 = self.start_date + timedelta(days=3)
        
        # Manually assign workers to schedule
        self.scheduler.schedule[date1] = ['W001', 'W002']
        self.scheduler.schedule[date2] = ['W003', None]
        
        # Update tracking to match
        self.scheduler.worker_assignments['W001'].add(date1)
        self.scheduler.worker_assignments['W002'].add(date1)
        self.scheduler.worker_assignments['W003'].add(date2)
        
        # Should be synchronized
        is_synchronized, report = self.scheduler._validate_data_synchronization()
        self.assertTrue(is_synchronized, "Manual assignment should be synchronized")
        
    def test_synchronization_detection_missing_from_tracking(self):
        """Test detection of assignments missing from tracking"""
        date1 = self.start_date
        
        # Add to schedule but not to tracking
        self.scheduler.schedule[date1] = ['W001', 'W002']
        # Don't update worker_assignments
        
        is_synchronized, report = self.scheduler._validate_data_synchronization()
        
        self.assertFalse(is_synchronized, "Should detect missing tracking assignments")
        self.assertEqual(report['summary']['missing_from_tracking'], 2)
        self.assertEqual(len(report['discrepancies']), 2)
        
        # Check specific discrepancies
        w001_discrepancy = next((d for d in report['discrepancies'] if d['worker_id'] == 'W001'), None)
        self.assertIsNotNone(w001_discrepancy)
        self.assertEqual(len(w001_discrepancy['missing_from_tracking']), 1)
        self.assertEqual(len(w001_discrepancy['extra_in_tracking']), 0)
        
    def test_synchronization_detection_extra_in_tracking(self):
        """Test detection of extra assignments in tracking"""
        date1 = self.start_date
        
        # Add to tracking but not to schedule
        self.scheduler.worker_assignments['W001'].add(date1)
        self.scheduler.worker_assignments['W002'].add(date1)
        # Don't update schedule
        
        is_synchronized, report = self.scheduler._validate_data_synchronization()
        
        self.assertFalse(is_synchronized, "Should detect extra tracking assignments")
        self.assertEqual(report['summary']['extra_in_tracking'], 2)
        self.assertEqual(len(report['discrepancies']), 2)
        
    def test_synchronization_repair(self):
        """Test synchronization repair functionality"""
        date1 = self.start_date
        date2 = self.start_date + timedelta(days=3)
        
        # Create inconsistent state - schedule has assignments but tracking doesn't
        self.scheduler.schedule[date1] = ['W001', 'W002']
        self.scheduler.schedule[date2] = ['W003', None]
        # worker_assignments remain empty
        
        # Verify inconsistency
        is_synchronized, report = self.scheduler._validate_data_synchronization()
        self.assertFalse(is_synchronized)
        
        # Repair synchronization
        repair_success = self.scheduler._repair_data_synchronization(report)
        self.assertTrue(repair_success, "Repair should succeed")
        
        # Verify repair worked
        is_synchronized_after, _ = self.scheduler._validate_data_synchronization()
        self.assertTrue(is_synchronized_after, "Should be synchronized after repair")
        
        # Check that worker_assignments now match schedule
        self.assertIn(date1, self.scheduler.worker_assignments['W001'])
        self.assertIn(date1, self.scheduler.worker_assignments['W002'])
        self.assertIn(date2, self.scheduler.worker_assignments['W003'])
        
    def test_ensure_data_synchronization(self):
        """Test the _ensure_data_synchronization convenience method"""
        date1 = self.start_date
        
        # Create inconsistent state
        self.scheduler.schedule[date1] = ['W001', None]
        # worker_assignments remain empty
        
        # Use ensure method
        result = self.scheduler._ensure_data_synchronization()
        self.assertTrue(result, "Ensure synchronization should succeed")
        
        # Verify synchronization
        is_synchronized, _ = self.scheduler._validate_data_synchronization()
        self.assertTrue(is_synchronized)
        
    def test_update_tracking_data_consistency(self):
        """Test that _update_tracking_data maintains consistency"""
        date1 = self.start_date
        
        # Manually assign to schedule first
        self.scheduler.schedule[date1] = ['W001', None]
        
        # Use update_tracking_data to add assignment
        self.scheduler._update_tracking_data('W001', date1, 0, removing=False)
        
        # Check consistency
        is_synchronized, _ = self.scheduler._validate_data_synchronization()
        self.assertTrue(is_synchronized, "Update tracking data should maintain synchronization")
        
        # Test removal
        self.scheduler.schedule[date1][0] = None  # Remove from schedule
        self.scheduler._update_tracking_data('W001', date1, 0, removing=True)
        
        # Check consistency after removal
        is_synchronized, _ = self.scheduler._validate_data_synchronization()
        self.assertTrue(is_synchronized, "Update tracking data removal should maintain synchronization")

class TestConstraintCheckingWithSynchronization(unittest.TestCase):
    """Test that constraint checking works correctly with synchronized data"""
    
    def setUp(self):
        """Set up test scheduler with basic configuration"""
        self.start_date = datetime(2024, 1, 1)  # Monday
        self.end_date = datetime(2024, 1, 21)   # 3 weeks for pattern testing
        
        self.config = {
            'start_date': self.start_date,
            'end_date': self.end_date,
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
                    'incompatible_with': ['W002']  # Incompatible with W002
                },
                {
                    'id': 'W002', 
                    'name': 'Worker 2',
                    'work_percentage': 100,
                    'target_shifts': 5,
                    'work_periods': '',
                    'days_off': '',
                    'mandatory_days': '',
                    'incompatible_with': ['W001']  # Incompatible with W001
                }
            ],
            'holidays': [],
            'gap_between_shifts': 2,
            'max_consecutive_weekends': 2
        }
        
        self.scheduler = Scheduler(self.config)
        self.live_validator = LiveValidator(self.scheduler)
        
    def test_7_day_pattern_constraint_with_synchronized_data(self):
        """Test 7-day pattern constraint with properly synchronized data"""
        # Assign worker to Monday Week 1
        monday_week1 = self.start_date  # Jan 1 (Monday)
        self.scheduler.schedule[monday_week1] = ['W001', None]
        self.scheduler.worker_assignments['W001'].add(monday_week1)
        
        # Ensure synchronization
        self.scheduler._ensure_data_synchronization()
        
        # Try to assign same worker to Monday Week 2 (7 days later)
        monday_week2 = monday_week1 + timedelta(days=7)  # Jan 8 (Monday)
        
        # Should fail 7-day pattern constraint for weekday (Monday)
        result = self.live_validator.validate_assignment('W001', monday_week2, 0)
        self.assertFalse(result.is_valid, "Should fail 7-day pattern constraint for weekday")
        self.assertEqual(result.constraint_type, "gap_constraint")
        self.assertIn("7/14 day pattern", result.message)
        
    def test_14_day_pattern_constraint_with_synchronized_data(self):
        """Test 14-day pattern constraint with properly synchronized data"""
        # Assign worker to Tuesday Week 1
        tuesday_week1 = self.start_date + timedelta(days=1)  # Jan 2 (Tuesday)
        self.scheduler.schedule[tuesday_week1] = ['W001', None]
        self.scheduler.worker_assignments['W001'].add(tuesday_week1)
        
        # Ensure synchronization
        self.scheduler._ensure_data_synchronization()
        
        # Try to assign same worker to Tuesday Week 3 (14 days later)
        tuesday_week3 = tuesday_week1 + timedelta(days=14)  # Jan 16 (Tuesday)
        
        # Should fail 14-day pattern constraint for weekday (Tuesday)
        result = self.live_validator.validate_assignment('W001', tuesday_week3, 0)
        self.assertFalse(result.is_valid, "Should fail 14-day pattern constraint for weekday")
        self.assertEqual(result.constraint_type, "gap_constraint")
        self.assertIn("7/14 day pattern", result.message)
        
    def test_weekend_pattern_allowed_with_synchronized_data(self):
        """Test that weekend 7/14 day patterns are allowed with synchronized data"""
        # Assign worker to Friday Week 1
        friday_week1 = self.start_date + timedelta(days=4)  # Jan 5 (Friday)
        self.scheduler.schedule[friday_week1] = ['W001', None]
        self.scheduler.worker_assignments['W001'].add(friday_week1)
        
        # Ensure synchronization
        self.scheduler._ensure_data_synchronization()
        
        # Try to assign same worker to Friday Week 2 (7 days later)
        friday_week2 = friday_week1 + timedelta(days=7)  # Jan 12 (Friday)
        
        # Should be allowed for weekend days (Friday)
        result = self.live_validator.validate_assignment('W001', friday_week2, 0)
        # This might still fail due to gap constraint, but NOT due to 7/14 day pattern
        if not result.is_valid:
            self.assertNotIn("7/14 day pattern", result.message, "Weekend 7-day pattern should be allowed")
        
    def test_incompatibility_constraint_with_synchronized_data(self):
        """Test incompatibility constraint with synchronized data"""
        date1 = self.start_date
        
        # Assign W001 to a shift
        self.scheduler.schedule[date1] = ['W001', None]
        self.scheduler.worker_assignments['W001'].add(date1)
        
        # Ensure synchronization
        self.scheduler._ensure_data_synchronization()
        
        # Try to assign incompatible worker W002 to same date
        result = self.live_validator.validate_assignment('W002', date1, 1)
        self.assertFalse(result.is_valid, "Should fail incompatibility constraint")
        self.assertEqual(result.constraint_type, "incompatibility")
        
    def test_live_validator_data_synchronization_check(self):
        """Test that LiveValidator checks data synchronization"""
        # Create unsynchronized state
        date1 = self.start_date
        self.scheduler.schedule[date1] = ['W001', None]
        # Don't update worker_assignments
        
        # Try validation - should detect synchronization issue
        result = self.live_validator.validate_assignment('W002', date1, 1)
        self.assertFalse(result.is_valid)
        # Should either fail due to synchronization or be fixed automatically
        
        # Test integrity validation
        integrity_results = self.live_validator.validate_schedule_integrity(check_partial=True)
        sync_results = [r for r in integrity_results if r.constraint_type == "data_synchronization"]
        self.assertTrue(len(sync_results) > 0, "Should include synchronization check results")


if __name__ == '__main__':
    # Set up logging for tests
    logging.basicConfig(level=logging.WARNING)  # Reduce noise during testing
    
    # Run the tests
    unittest.main(verbosity=2)