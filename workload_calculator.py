"""
Workload calculation module for the scheduler system.

This module handles all target shift calculations and workload distribution logic
extracted from the main Scheduler class.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from collections import defaultdict

from scheduler_config import SchedulerDefaults, ValidationThresholds
from exceptions import SchedulerError


@dataclass
class WorkerWorkload:
    """Represents calculated workload for a single worker"""
    worker_id: str
    available_slots: int
    weight: float
    target_shifts: int
    mandatory_shifts: int
    adjusted_target: int
    work_percentage: float
    
    @property
    def utilization_rate(self) -> float:
        """Calculate utilization rate of available slots"""
        return (self.target_shifts / self.available_slots) if self.available_slots > 0 else 0.0


@dataclass 
class WorkloadDistribution:
    """Represents the complete workload distribution across all workers"""
    total_slots: int
    total_weight: float
    worker_workloads: Dict[str, WorkerWorkload]
    distribution_method: str
    
    @property
    def average_utilization(self) -> float:
        """Calculate average utilization across all workers"""
        if not self.worker_workloads:
            return 0.0
        return sum(w.utilization_rate for w in self.worker_workloads.values()) / len(self.worker_workloads)
    
    @property
    def total_assigned_shifts(self) -> int:
        """Total number of shifts assigned across all workers"""
        return sum(w.target_shifts for w in self.worker_workloads.values())


class WorkloadCalculator:
    """
    Handles workload calculations and target shift distribution.
    
    Extracted from the main Scheduler class to improve separation of concerns.
    """
    
    def __init__(self, scheduler):
        """
        Initialize the calculator with a reference to the scheduler.
        
        Args:
            scheduler: Reference to the main Scheduler instance
        """
        self.scheduler = scheduler
        self.calculation_cache = {}  # Cache for expensive calculations
        
    def calculate_target_shifts(self) -> WorkloadDistribution:
        """
        Calculate target shifts for all workers based on availability and work percentage.
        
        Uses largest-remainder method for proportional allocation.
        
        Returns:
            WorkloadDistribution: Complete workload distribution results
        """
        try:
            logging.info("Calculating target shifts based on availability and percentage")
            
            # 1. Calculate total available slots
            total_slots = self._calculate_total_slots()
            if total_slots <= 0:
                logging.warning("No slots in schedule; skipping allocation")
                return self._create_empty_distribution()
            
            # 2. Calculate available slots per worker
            worker_availability = self._calculate_worker_availability()
            
            # 3. Calculate weights based on availability and work percentage
            worker_weights = self._calculate_worker_weights(worker_availability)
            total_weight = sum(worker_weights.values())
            
            # 4. Distribute slots using largest-remainder method
            workload_distribution = self._distribute_slots_proportionally(
                total_slots, worker_weights, total_weight, worker_availability
            )
            
            # 5. Adjust for mandatory assignments
            self._adjust_for_mandatory_assignments(workload_distribution)
            
            # 6. Update scheduler with calculated targets
            self._update_scheduler_targets(workload_distribution)
            
            logging.info(f"Target calculation complete. Total slots: {total_slots}, "
                        f"Assigned: {workload_distribution.total_assigned_shifts}")
            
            return workload_distribution
            
        except Exception as e:
            logging.error(f"Error calculating target shifts: {str(e)}", exc_info=True)
            raise SchedulerError(f"Failed to calculate target shifts: {str(e)}")
    
    def calculate_monthly_targets(self) -> Dict[str, Dict[str, int]]:
        """
        Calculate monthly target shifts for each worker.
        
        Returns:
            Dict mapping worker_id -> month -> target_shifts
        """
        try:
            monthly_targets = defaultdict(lambda: defaultdict(int))
            
            # Get schedule months
            months = self._get_schedule_months()
            
            for worker in self.scheduler.workers_data:
                worker_id = worker['id']
                total_target = worker.get('target_shifts', 0)
                
                if total_target == 0 or len(months) == 0:
                    continue
                
                # Distribute target shifts across months proportionally
                month_weights = self._calculate_month_weights(worker_id, months)
                monthly_distribution = self._distribute_target_across_months(
                    total_target, month_weights
                )
                
                for month, target in monthly_distribution.items():
                    monthly_targets[worker_id][month] = target
            
            logging.info(f"Monthly targets calculated for {len(monthly_targets)} workers")
            return dict(monthly_targets)
            
        except Exception as e:
            logging.error(f"Error calculating monthly targets: {str(e)}", exc_info=True)
            return {}
    
    def redistribute_excess_shifts(self, excess_shifts: int, excluded_worker_id: str) -> int:
        """
        Redistribute excess shifts from one worker to others.
        
        Args:
            excess_shifts: Number of shifts to redistribute
            excluded_worker_id: Worker to exclude from redistribution
            
        Returns:
            Number of shifts successfully redistributed
        """
        try:
            eligible_workers = [
                w for w in self.scheduler.workers_data 
                if w['id'] != excluded_worker_id
            ]
            
            if not eligible_workers:
                logging.warning("No eligible workers for redistribution")
                return 0
            
            # Sort by work percentage (prioritize higher percentage workers)
            eligible_workers.sort(
                key=lambda w: float(w.get('work_percentage', SchedulerDefaults.DEFAULT_WORK_PERCENTAGE)), 
                reverse=True
            )
            
            redistributed = 0
            for i in range(excess_shifts):
                worker = eligible_workers[i % len(eligible_workers)]
                worker['target_shifts'] = worker.get('target_shifts', 0) + 1
                redistributed += 1
                logging.debug(f"Redistributed 1 shift to worker {worker['id']}")
            
            logging.info(f"Successfully redistributed {redistributed} shifts")
            return redistributed
            
        except Exception as e:
            logging.error(f"Error redistributing shifts: {str(e)}")
            return 0
    
    def _calculate_total_slots(self) -> int:
        """Calculate total number of slots in the schedule"""
        return sum(len(slots) for slots in self.scheduler.schedule.values())
    
    def _calculate_worker_availability(self) -> Dict[str, int]:
        """
        Calculate available slots for each worker based on work periods and days off.
        
        Returns:
            Dict mapping worker_id -> available_slot_count
        """
        available_slots = {}
        
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            
            # Parse work periods and days off
            work_periods = self._parse_work_periods(worker)
            days_off = self._parse_days_off(worker)
            
            # Count available slots
            count = 0
            for date, slots in self.scheduler.schedule.items():
                if self._is_worker_available(date, work_periods, days_off):
                    count += len(slots)
            
            available_slots[worker_id] = count
            logging.debug(f"Worker {worker_id}: {count} available slots")
        
        return available_slots
    
    def _calculate_worker_weights(self, availability: Dict[str, int]) -> Dict[str, float]:
        """
        Calculate worker weights based on availability and work percentage.
        
        Args:
            availability: Dict mapping worker_id -> available_slots
            
        Returns:
            Dict mapping worker_id -> weight
        """
        weights = {}
        
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            available = availability.get(worker_id, 0)
            
            # Parse work percentage safely
            work_percentage = self._parse_work_percentage(worker)
            
            # Calculate weight: available_slots * (work_percentage / 100)
            weight = available * (work_percentage / 100.0)
            weights[worker_id] = weight
            
            logging.debug(f"Worker {worker_id}: weight={weight:.2f} "
                         f"(available={available}, percentage={work_percentage}%)")
        
        return weights
    
    def _distribute_slots_proportionally(self, total_slots: int, weights: Dict[str, float], 
                                       total_weight: float, availability: Dict[str, int]) -> WorkloadDistribution:
        """
        Distribute slots proportionally using largest-remainder method.
        
        Args:
            total_slots: Total number of slots to distribute
            weights: Worker weights for distribution
            total_weight: Sum of all weights
            availability: Worker availability data
            
        Returns:
            WorkloadDistribution: Distribution results
        """
        if total_weight == 0:
            return self._create_empty_distribution()
        
        # Calculate exact fractional targets
        exact_targets = {}
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            weight = weights.get(worker_id, 0)
            exact_targets[worker_id] = (weight / total_weight) * total_slots
        
        # Apply largest-remainder rounding
        floors = {worker_id: int(target) for worker_id, target in exact_targets.items()}
        remainder = int(total_slots - sum(floors.values()))
        
        # Sort by fractional part (descending)
        fractional_parts = [
            (worker_id, exact_targets[worker_id] - floors[worker_id])
            for worker_id in exact_targets.keys()
        ]
        fractional_parts.sort(key=lambda x: x[1], reverse=True)
        
        # Assign remaining slots
        final_targets = floors.copy()
        for i in range(remainder):
            if i < len(fractional_parts):
                worker_id = fractional_parts[i][0]
                final_targets[worker_id] += 1
        
        # Create workload objects
        worker_workloads = {}
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            workload = WorkerWorkload(
                worker_id=worker_id,
                available_slots=availability.get(worker_id, 0),
                weight=weights.get(worker_id, 0),
                target_shifts=final_targets.get(worker_id, 0),
                mandatory_shifts=0,  # Will be calculated in adjustment phase
                adjusted_target=final_targets.get(worker_id, 0),
                work_percentage=self._parse_work_percentage(worker)
            )
            worker_workloads[worker_id] = workload
        
        return WorkloadDistribution(
            total_slots=total_slots,
            total_weight=total_weight,
            worker_workloads=worker_workloads,
            distribution_method="largest_remainder"
        )
    
    def _adjust_for_mandatory_assignments(self, distribution: WorkloadDistribution):
        """
        Adjust target shifts by subtracting mandatory assignments.
        
        Args:
            distribution: WorkloadDistribution to modify in-place
        """
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            workload = distribution.worker_workloads.get(worker_id)
            if not workload:
                continue
            
            # Count mandatory assignments
            mandatory_count = self._count_mandatory_assignments(worker)
            workload.mandatory_shifts = mandatory_count
            
            # Adjust target (ensure non-negative)
            workload.adjusted_target = max(0, workload.target_shifts - mandatory_count)
            
            logging.debug(f"Worker {worker_id}: target={workload.target_shifts}, "
                         f"mandatory={mandatory_count}, adjusted={workload.adjusted_target}")
    
    def _update_scheduler_targets(self, distribution: WorkloadDistribution):
        """Update scheduler worker data with calculated targets"""
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            workload = distribution.worker_workloads.get(worker_id)
            if workload:
                worker['target_shifts'] = workload.adjusted_target
    
    def _parse_work_periods(self, worker: Dict[str, Any]) -> List[Tuple[datetime, datetime]]:
        """Parse work periods from worker configuration"""
        work_periods_str = worker.get('work_periods', '').strip()
        if not work_periods_str:
            return [(self.scheduler.start_date, self.scheduler.end_date)]
        
        try:
            return self.scheduler.date_utils.parse_date_ranges(work_periods_str)
        except Exception as e:
            logging.warning(f"Failed to parse work_periods for worker {worker['id']}: {e}")
            return [(self.scheduler.start_date, self.scheduler.end_date)]
    
    def _parse_days_off(self, worker: Dict[str, Any]) -> List[Tuple[datetime, datetime]]:
        """Parse days off from worker configuration"""
        days_off_str = worker.get('days_off', '').strip()
        if not days_off_str:
            return []
        
        try:
            return self.scheduler.date_utils.parse_date_ranges(days_off_str)
        except Exception as e:
            logging.warning(f"Failed to parse days_off for worker {worker['id']}: {e}")
            return []
    
    def _parse_work_percentage(self, worker: Dict[str, Any]) -> float:
        """Parse work percentage from worker configuration"""
        try:
            work_percentage = float(str(worker.get('work_percentage', SchedulerDefaults.DEFAULT_WORK_PERCENTAGE)).strip())
            return max(0.0, min(100.0, work_percentage))  # Clamp to valid range
        except (ValueError, TypeError):
            logging.warning(f"Invalid work_percentage for worker {worker['id']}, using default")
            return SchedulerDefaults.DEFAULT_WORK_PERCENTAGE
    
    def _is_worker_available(self, date: datetime, work_periods: List[Tuple[datetime, datetime]], 
                           days_off: List[Tuple[datetime, datetime]]) -> bool:
        """Check if worker is available on a specific date"""
        # Check if date is within work periods
        in_work_period = any(start <= date <= end for start, end in work_periods)
        
        # Check if date is in days off
        in_days_off = any(start <= date <= end for start, end in days_off)
        
        return in_work_period and not in_days_off
    
    def _count_mandatory_assignments(self, worker: Dict[str, Any]) -> int:
        """Count mandatory assignments for a worker"""
        mandatory_str = worker.get('mandatory_days', '').strip()
        if not mandatory_str:
            return 0
        
        try:
            mandatory_dates = self.scheduler.date_utils.parse_dates(mandatory_str)
            return sum(
                1 for date in mandatory_dates 
                if self.scheduler.start_date <= date <= self.scheduler.end_date
            )
        except Exception as e:
            logging.error(f"Failed to parse mandatory_days for worker {worker['id']}: {e}")
            return 0
    
    def _get_schedule_months(self) -> List[str]:
        """Get list of months covered by the schedule"""
        months = set()
        current_date = self.scheduler.start_date
        
        while current_date <= self.scheduler.end_date:
            month_key = current_date.strftime('%Y-%m')
            months.add(month_key)
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1, day=1)
        
        return sorted(list(months))
    
    def _calculate_month_weights(self, worker_id: str, months: List[str]) -> Dict[str, float]:
        """Calculate relative weights for each month based on available days"""
        month_weights = {}
        
        for month_str in months:
            year, month = map(int, month_str.split('-'))
            
            # Count available days in this month
            available_days = 0
            current_date = datetime(year, month, 1).date()
            
            # Get last day of month
            if month == 12:
                next_month = datetime(year + 1, 1, 1).date()
            else:
                next_month = datetime(year, month + 1, 1).date()
            
            while current_date < next_month:
                if (self.scheduler.start_date <= current_date <= self.scheduler.end_date and
                    current_date in self.scheduler.schedule):
                    available_days += len(self.scheduler.schedule[current_date])
                
                current_date += timedelta(days=1)
            
            month_weights[month_str] = float(available_days)
        
        return month_weights
    
    def _distribute_target_across_months(self, total_target: int, month_weights: Dict[str, float]) -> Dict[str, int]:
        """Distribute target shifts across months proportionally"""
        total_weight = sum(month_weights.values())
        if total_weight == 0:
            return {month: 0 for month in month_weights.keys()}
        
        # Calculate exact fractional targets
        exact_targets = {
            month: (weight / total_weight) * total_target
            for month, weight in month_weights.items()
        }
        
        # Apply largest-remainder rounding
        floors = {month: int(target) for month, target in exact_targets.items()}
        remainder = int(total_target - sum(floors.values()))
        
        # Sort by fractional part
        fractional_parts = [
            (month, exact_targets[month] - floors[month])
            for month in exact_targets.keys()
        ]
        fractional_parts.sort(key=lambda x: x[1], reverse=True)
        
        # Assign remaining shifts
        final_targets = floors.copy()
        for i in range(remainder):
            if i < len(fractional_parts):
                month = fractional_parts[i][0]
                final_targets[month] += 1
        
        return final_targets
    
    def _create_empty_distribution(self) -> WorkloadDistribution:
        """Create an empty workload distribution"""
        worker_workloads = {}
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            workload = WorkerWorkload(
                worker_id=worker_id,
                available_slots=0,
                weight=0.0,
                target_shifts=0,
                mandatory_shifts=0,
                adjusted_target=0,
                work_percentage=self._parse_work_percentage(worker)
            )
            worker_workloads[worker_id] = workload
        
        return WorkloadDistribution(
            total_slots=0,
            total_weight=0.0,
            worker_workloads=worker_workloads,
            distribution_method="empty"
        )
    
    def clear_cache(self):
        """Clear calculation cache"""
        self.calculation_cache.clear()