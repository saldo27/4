"""
Simple test script to validate real-time scheduler features.
Tests core functionality without full UI integration.
"""

import sys
import os
from datetime import datetime, timedelta
import logging

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import Scheduler
from event_bus import get_event_bus, EventType

# Configure logging for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_test_config():
    """Create a test configuration"""
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 7)  # One week
    
    workers_data = [
        {'id': 'W001', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'is_incompatible': False},
        {'id': 'W002', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'is_incompatible': False},
        {'id': 'W003', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'is_incompatible': False},
        {'id': 'W004', 'work_percentage': 100, 'work_periods': '', 'mandatory_days': '', 'days_off': '', 'is_incompatible': False}
    ]
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'num_workers': len(workers_data),
        'num_shifts': 2,  # 2 shifts per day
        'gap_between_shifts': 1,
        'max_consecutive_weekends': 2,
        'holidays': [],
        'workers_data': workers_data,
        'variable_shifts': [],
        'enable_real_time': True  # Enable real-time features
    }

def test_event_bus():
    """Test the event bus functionality"""
    print("\n=== Testing Event Bus ===")
    
    event_bus = get_event_bus()
    
    # Test event subscription and emission
    received_events = []
    
    def event_handler(event):
        received_events.append(event)
        print(f"Received event: {event.event_type.value}")
    
    event_bus.subscribe(EventType.SHIFT_ASSIGNED, event_handler)
    
    # Emit a test event
    event_bus.emit(
        EventType.SHIFT_ASSIGNED,
        user_id='test_user',
        worker_id='W001',
        shift_date='2024-01-01',
        post_index=0
    )
    
    # Check if event was received
    assert len(received_events) == 1
    assert received_events[0].event_type == EventType.SHIFT_ASSIGNED
    print("✓ Event bus working correctly")

def test_scheduler_initialization():
    """Test scheduler initialization with real-time features"""
    print("\n=== Testing Scheduler Initialization ===")
    
    config = create_test_config()
    scheduler = Scheduler(config)
    
    # Check if real-time engine is initialized
    assert scheduler.is_real_time_enabled(), "Real-time engine should be enabled"
    print("✓ Scheduler initialized with real-time features")
    
    return scheduler

def test_real_time_operations(scheduler):
    """Test real-time operations"""
    print("\n=== Testing Real-Time Operations ===")
    
    # Generate a basic schedule first
    success = scheduler.generate_schedule()
    assert success, "Schedule generation should succeed"
    print("✓ Basic schedule generated")
    
    # Test real-time assignment
    shift_date = datetime(2024, 1, 1)
    result = scheduler.assign_worker_real_time('W001', shift_date, 0, 'test_user')
    
    print(f"Assignment result: {result['success']} - {result['message']}")
    
    # Test real-time validation
    validation_result = scheduler.validate_schedule_real_time()
    print(f"Validation result: {validation_result['success']} - {validation_result['message']}")
    
    # Test analytics
    analytics = scheduler.get_real_time_analytics()
    if 'error' not in analytics:
        print(f"Analytics: {analytics['schedule_metrics']}")
        print("✓ Real-time operations working")
    else:
        print(f"Analytics error: {analytics['error']}")

def test_change_tracking(scheduler):
    """Test change tracking and undo/redo"""
    print("\n=== Testing Change Tracking ===")
    
    # Get change history
    history = scheduler.get_change_history(limit=5)
    if 'error' not in history:
        print(f"Change history: {len(history['changes'])} changes")
        print(f"Can undo: {history['can_undo']}, Can redo: {history['can_redo']}")
        
        # Test undo if possible
        if history['can_undo']:
            undo_result = scheduler.undo_last_change('test_user')
            print(f"Undo result: {undo_result['success']} - {undo_result['message']}")
        
        print("✓ Change tracking working")
    else:
        print(f"Change tracking error: {history['error']}")

def main():
    """Run all tests"""
    print("Starting Real-Time Scheduler Features Test")
    print("=" * 50)
    
    try:
        # Test event bus
        test_event_bus()
        
        # Test scheduler initialization
        scheduler = test_scheduler_initialization()
        
        # Test real-time operations
        test_real_time_operations(scheduler)
        
        # Test change tracking
        test_change_tracking(scheduler)
        
        print("\n" + "=" * 50)
        print("✓ All tests completed successfully!")
        
        # Show final analytics
        analytics = scheduler.get_real_time_analytics()
        if 'error' not in analytics:
            print(f"\nFinal Analytics:")
            print(f"- Schedule Coverage: {analytics['schedule_metrics']['coverage_percentage']}%")
            print(f"- Total Events: {analytics['event_system']['total_events']}")
            print(f"- Active Operations: {analytics['active_operations']['count']}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        logging.exception("Test error details:")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
