"""
Scheduler Core Module

This module contains the main orchestration logic for the scheduler system,
extracted from the original Scheduler class to improve maintainability and separation of concerns.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any

from scheduler_config import SchedulerConfig
from exceptions import SchedulerError


class SchedulerCore:
    """
    Core orchestration class that manages the high-level scheduling workflow.
    This class focuses on coordination between components rather than implementation details.
    """
    
    def __init__(self, scheduler):
        """
        Initialize the scheduler core with a reference to the main scheduler.
        
        Args:
            scheduler: Reference to the main Scheduler instance
        """
        self.scheduler = scheduler
        self.config = scheduler.config
        self.start_date = scheduler.start_date
        self.end_date = scheduler.end_date
        self.workers_data = scheduler.workers_data
        
        logging.info("SchedulerCore initialized")
    
    def orchestrate_schedule_generation(self, max_improvement_loops: int = 70) -> bool:
        """
        Main orchestration method for schedule generation workflow.
        
        Args:
            max_improvement_loops: Maximum number of improvement iterations
            
        Returns:
            bool: True if schedule generation was successful
        """
        logging.info("Starting schedule generation orchestration...")
        start_time = datetime.now()
        
        try:
            # Phase 1: Initialize schedule structure
            if not self._initialize_schedule_phase():
                raise SchedulerError("Failed to initialize schedule structure")
            
            # Phase 2: Assign mandatory shifts
            if not self._assign_mandatory_phase():
                raise SchedulerError("Failed to assign mandatory shifts")
            
            # Phase 3: Iterative improvement
            if not self._iterative_improvement_phase(max_improvement_loops):
                logging.warning("Iterative improvement phase completed with issues")
            
            # Phase 4: Finalization
            if not self._finalization_phase():
                raise SchedulerError("Failed to finalize schedule")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"Schedule generation orchestration completed successfully in {duration:.2f} seconds.")
            return True
            
        except Exception as e:
            logging.error(f"Schedule generation orchestration failed: {str(e)}", exc_info=True)
            if isinstance(e, SchedulerError):
                raise e
            else:
                raise SchedulerError(f"Orchestration failed: {str(e)}")
    
    def _initialize_schedule_phase(self) -> bool:
        """
        Phase 1: Initialize schedule structure and data.
        
        Returns:
            bool: True if initialization was successful
        """
        logging.info("Phase 1: Initializing schedule structure...")
        
        try:
            # Reset scheduler state
            self.scheduler.schedule = {}
            self.scheduler.worker_assignments = {w['id']: set() for w in self.workers_data}
            self.scheduler.worker_shift_counts = {w['id']: 0 for w in self.workers_data}
            self.scheduler.worker_weekend_counts = {w['id']: 0 for w in self.workers_data}
            self.scheduler.worker_posts = {w['id']: set() for w in self.workers_data}
            self.scheduler.last_assignment_date = {w['id']: None for w in self.workers_data}
            self.scheduler.consecutive_shifts = {w['id']: 0 for w in self.workers_data}
            
            # Initialize schedule with variable shifts
            self.scheduler._initialize_schedule_with_variable_shifts()
            
            # Create schedule builder
            from schedule_builder import ScheduleBuilder
            self.scheduler.schedule_builder = ScheduleBuilder(self.scheduler)
            
            logging.info(f"Schedule structure initialized with {len(self.scheduler.schedule)} dates")
            return True
            
        except Exception as e:
            logging.error(f"Failed to initialize schedule phase: {str(e)}", exc_info=True)
            return False
    
    def _assign_mandatory_phase(self) -> bool:
        """
        Phase 2: Assign mandatory shifts and lock them in place.
        
        Returns:
            bool: True if mandatory assignment was successful
        """
        logging.info("Phase 2: Assigning mandatory shifts...")
        
        try:
            # Pre-assign mandatory shifts
            self.scheduler.schedule_builder._assign_mandatory_guards()
            
            # Synchronize tracking data
            self.scheduler.schedule_builder._synchronize_tracking_data()
            
            # Save initial state as best
            self.scheduler.schedule_builder._save_current_as_best(initial=True)
            
            # Log summary
            self.scheduler.log_schedule_summary("After Mandatory Assignment")
            
            logging.info("Mandatory assignment phase completed")
            return True
            
        except Exception as e:
            logging.error(f"Failed in mandatory assignment phase: {str(e)}", exc_info=True)
            return False
    
    def _iterative_improvement_phase(self, max_improvement_loops: int) -> bool:
        """
        Phase 3: Iterative improvement of the schedule.
        
        Args:
            max_improvement_loops: Maximum number of improvement iterations
            
        Returns:
            bool: True if improvement phase completed successfully
        """
        logging.info("Phase 3: Starting iterative improvement...")
        
        improvement_loop_count = 0
        improvement_made_in_cycle = True
        
        try:
            while improvement_made_in_cycle and improvement_loop_count < max_improvement_loops:
                improvement_made_in_cycle = False
                loop_start_time = datetime.now()
                
                logging.info(f"--- Starting Improvement Loop {improvement_loop_count + 1} ---")
                
                # Apply improvement operations in sequence
                improvement_operations = [
                    ("fill_empty_shifts", self.scheduler.schedule_builder._try_fill_empty_shifts),
                    ("balance_workloads", self.scheduler.schedule_builder._balance_workloads),
                    ("improve_weekend_distribution_1", self.scheduler.schedule_builder._improve_weekend_distribution),
                    ("distribute_holiday_shifts_proportionally", self.scheduler.schedule_builder.distribute_holiday_shifts_proportionally),
                    ("rebalance_weekend_distribution", self.scheduler.schedule_builder.rebalance_weekend_distribution),
                    ("synchronize_tracking_data", self.scheduler.schedule_builder._synchronize_tracking_data),
                    ("improve_weekend_distribution_2", self.scheduler.schedule_builder._improve_weekend_distribution),
                    ("adjust_last_post_distribution", self.scheduler.schedule_builder._adjust_last_post_distribution),
                ]
                
                for operation_name, operation_func in improvement_operations:
                    try:
                        if operation_name == "synchronize_tracking_data":
                            # This operation always runs and doesn't return improvement status
                            operation_func()
                        else:
                            if operation_func():
                                logging.info(f"Improvement Loop: {operation_name} made improvements.")
                                improvement_made_in_cycle = True
                    except Exception as e:
                        logging.warning(f"Operation {operation_name} failed: {str(e)}")
                
                loop_end_time = datetime.now()
                loop_duration = (loop_end_time - loop_start_time).total_seconds()
                logging.info(f"--- Improvement Loop {improvement_loop_count + 1} completed in {loop_duration:.2f}s. Changes made: {improvement_made_in_cycle} ---")
                
                if not improvement_made_in_cycle:
                    logging.info("No further improvements detected. Exiting improvement phase.")
                
                improvement_loop_count += 1
            
            if improvement_loop_count >= max_improvement_loops:
                logging.warning(f"Reached maximum improvement loops ({max_improvement_loops}). Stopping improvements.")
            
            return True
            
        except Exception as e:
            logging.error(f"Error during iterative improvement phase: {str(e)}", exc_info=True)
            return False
    
    def _finalization_phase(self) -> bool:
        """
        Phase 4: Finalize the schedule and perform final optimizations.
        
        Returns:
            bool: True if finalization was successful
        """
        logging.info("Phase 4: Finalizing schedule...")
        
        try:
            # Final adjustment of last post distribution
            logging.info("Performing final last post distribution adjustment...")
            max_iterations = self.config.get('last_post_adjustment_max_iterations', 
                                           SchedulerConfig.DEFAULT_LAST_POST_ADJUSTMENT_ITERATIONS)
            
            if self.scheduler.schedule_builder._adjust_last_post_distribution(
                balance_tolerance=1.0,
                max_iterations=max_iterations
            ):
                logging.info("Final last post distribution adjustment completed.")
            
            # Get the best schedule
            final_schedule_data = self.scheduler.schedule_builder.get_best_schedule()
            
            if not final_schedule_data or not final_schedule_data.get('schedule'):
                logging.error("No best schedule data available for finalization.")
                return self._handle_fallback_finalization()
            
            # Update scheduler state with final schedule
            self._apply_final_schedule(final_schedule_data)
            
            # Final validation and logging
            self._perform_final_validation()
            
            logging.info("Schedule finalization phase completed successfully.")
            return True
            
        except Exception as e:
            logging.error(f"Error during finalization phase: {str(e)}", exc_info=True)
            return False
    
    def _handle_fallback_finalization(self) -> bool:
        """
        Handle fallback finalization when no best schedule is available.
        
        Returns:
            bool: True if fallback was successful
        """
        logging.warning("Using current schedule state as fallback for finalization.")
        
        if not self.scheduler.schedule or all(all(p is None for p in posts) for posts in self.scheduler.schedule.values()):
            logging.error("Current schedule state is also empty. Cannot finalize.")
            return False
        
        # Use current state as final schedule
        final_schedule_data = {
            'schedule': self.scheduler.schedule,
            'worker_assignments': self.scheduler.worker_assignments,
            'worker_shift_counts': self.scheduler.worker_shift_counts,
            'worker_weekend_counts': self.scheduler.worker_weekend_counts,
            'worker_posts': self.scheduler.worker_posts,
            'last_assignment_date': self.scheduler.last_assignment_date,
            'consecutive_shifts': self.scheduler.consecutive_shifts,
            'score': self.scheduler.calculate_score()
        }
        
        return self._apply_final_schedule(final_schedule_data)
    
    def _apply_final_schedule(self, final_schedule_data: Dict[str, Any]) -> bool:
        """
        Apply the final schedule data to the scheduler state.
        
        Args:
            final_schedule_data: Dictionary containing the final schedule data
            
        Returns:
            bool: True if application was successful
        """
        try:
            logging.info("Applying final schedule data to scheduler state...")
            
            self.scheduler.schedule = final_schedule_data['schedule']
            self.scheduler.worker_assignments = final_schedule_data['worker_assignments']
            self.scheduler.worker_shift_counts = final_schedule_data['worker_shift_counts']
            self.scheduler.worker_weekend_counts = final_schedule_data.get(
                'worker_weekend_shifts', 
                final_schedule_data.get('worker_weekend_counts', {})
            )
            self.scheduler.worker_posts = final_schedule_data['worker_posts']
            self.scheduler.last_assignment_date = final_schedule_data['last_assignment_date']
            self.scheduler.consecutive_shifts = final_schedule_data['consecutive_shifts']
            
            final_score = final_schedule_data.get('score', float('-inf'))
            logging.info(f"Final schedule applied with score: {final_score:.2f}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error applying final schedule data: {str(e)}", exc_info=True)
            return False
    
    def _perform_final_validation(self) -> bool:
        """
        Perform final validation and logging of the schedule.
        
        Returns:
            bool: True if validation passed
        """
        try:
            # Calculate final statistics
            total_slots_final = sum(len(slots) for slots in self.scheduler.schedule.values())
            total_assignments_final = sum(
                1 for slots in self.scheduler.schedule.values() 
                for worker_id in slots if worker_id is not None
            )
            
            empty_shifts_final = [
                (date, post_idx) 
                for date, posts in self.scheduler.schedule.items() 
                for post_idx, worker_id in enumerate(posts) 
                if worker_id is None
            ]
            
            # Validate schedule integrity
            if total_slots_final == 0:
                schedule_duration_days = (self.end_date - self.start_date).days + 1
                if schedule_duration_days > 0:
                    logging.error(f"Final schedule has 0 total slots despite valid date range ({schedule_duration_days} days).")
                    return False
            
            if total_assignments_final == 0 and total_slots_final > 0:
                logging.warning(f"Final schedule has {total_slots_final} slots but contains ZERO assignments.")
            
            if empty_shifts_final:
                empty_percentage = (len(empty_shifts_final) / total_slots_final) * 100
                logging.warning(f"Final schedule has {len(empty_shifts_final)} empty shifts ({empty_percentage:.1f}%) out of {total_slots_final} total slots.")
            
            # Log final summary
            self.scheduler.log_schedule_summary("Final Generated Schedule")
            
            return True
            
        except Exception as e:
            logging.error(f"Error during final validation: {str(e)}", exc_info=True)
            return False