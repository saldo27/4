"""
Historical Data Manager for AI-Powered Workload Demand Forecasting System

This module manages the collection, storage, and analysis of historical scheduling data
to enable predictive analytics and demand forecasting.
"""

import logging
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any
from pathlib import Path
import os

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logging.warning("Pandas not available. Historical data will use basic storage.")

from exceptions import SchedulerError


class HistoricalDataManager:
    """Manages historical scheduling data collection and analysis"""
    
    def __init__(self, scheduler, storage_path: str = "historical_data"):
        """
        Initialize the historical data manager
        
        Args:
            scheduler: The main Scheduler object
            storage_path: Path to store historical data files
        """
        self.scheduler = scheduler
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        
        # Historical data containers
        self.shift_fill_rates: Dict[str, List[float]] = {}
        self.worker_availability_patterns: Dict[str, Dict[str, float]] = {}
        self.seasonal_demand_data: Dict[str, List[Dict[str, Any]]] = {}
        self.constraint_violations: List[Dict[str, Any]] = []
        self.efficiency_metrics: List[Dict[str, Any]] = []
        
        logging.info(f"HistoricalDataManager initialized with storage path: {storage_path}")
        
        # Load existing historical data if available
        self._load_historical_data()
    
    def collect_current_schedule_data(self) -> Dict[str, Any]:
        """
        Collect data from the current schedule for historical analysis
        
        Returns:
            Dictionary containing comprehensive schedule metrics
        """
        try:
            # Get current statistics using existing infrastructure
            stats = self.scheduler.stats.gather_statistics()
            
            # Calculate additional metrics for forecasting
            current_data = {
                'timestamp': datetime.now().isoformat(),
                'schedule_period': {
                    'start_date': self.scheduler.start_date.isoformat(),
                    'end_date': self.scheduler.end_date.isoformat(),
                    'total_days': (self.scheduler.end_date - self.scheduler.start_date).days + 1
                },
                'shift_metrics': self._calculate_shift_metrics(),
                'worker_metrics': self._calculate_worker_metrics(stats),
                'coverage_metrics': self._calculate_coverage_metrics(),
                'constraint_metrics': self._extract_constraint_metrics(stats),
                'seasonal_indicators': self._extract_seasonal_indicators(),
                'efficiency_score': self._calculate_efficiency_score(stats)
            }
            
            return current_data
            
        except Exception as e:
            logging.error(f"Error collecting current schedule data: {e}")
            raise SchedulerError(f"Failed to collect schedule data: {str(e)}")
    
    def _calculate_shift_metrics(self) -> Dict[str, Any]:
        """Calculate daily shift fill rates and patterns"""
        shift_metrics = {
            'daily_fill_rates': {},
            'average_fill_rate': 0.0,
            'peak_demand_days': [],
            'low_demand_days': []
        }
        
        total_slots = 0
        filled_slots = 0
        
        for date, shifts in self.scheduler.schedule.items():
            date_str = date.strftime('%Y-%m-%d')
            total_day_slots = len(shifts)
            filled_day_slots = sum(1 for shift in shifts if shift is not None)
            
            fill_rate = filled_day_slots / total_day_slots if total_day_slots > 0 else 0
            shift_metrics['daily_fill_rates'][date_str] = {
                'fill_rate': fill_rate,
                'filled_slots': filled_day_slots,
                'total_slots': total_day_slots,
                'weekday': date.strftime('%A'),
                'is_weekend': self.scheduler.data_manager._is_weekend_day(date),
                'is_holiday': self.scheduler.data_manager._is_holiday(date)
            }
            
            total_slots += total_day_slots
            filled_slots += filled_day_slots
            
            # Identify peak and low demand days
            if fill_rate >= 0.95:
                shift_metrics['peak_demand_days'].append(date_str)
            elif fill_rate <= 0.7:
                shift_metrics['low_demand_days'].append(date_str)
        
        shift_metrics['average_fill_rate'] = filled_slots / total_slots if total_slots > 0 else 0
        
        return shift_metrics
    
    def _calculate_worker_metrics(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate worker availability and performance patterns"""
        worker_metrics = {
            'availability_patterns': {},
            'workload_distribution': {},
            'performance_indicators': {}
        }
        
        for worker_id, worker_stats in stats['workers'].items():
            # Calculate availability patterns
            total_possible_shifts = stats['general']['total_days'] * self.scheduler.num_shifts
            availability_rate = worker_stats['total_shifts'] / total_possible_shifts
            
            worker_metrics['availability_patterns'][worker_id] = {
                'availability_rate': availability_rate,
                'weekend_preference': self._calculate_weekend_preference(worker_id),
                'shift_consistency': self._calculate_shift_consistency(worker_id),
                'post_rotation_balance': self._calculate_post_rotation_score(worker_stats['post_distribution'])
            }
            
            # Workload distribution
            worker_metrics['workload_distribution'][worker_id] = {
                'total_shifts': worker_stats['total_shifts'],
                'target_shifts': worker_stats['target_shifts'],
                'shift_ratio': worker_stats['total_shifts'] / worker_stats['target_shifts'] if worker_stats['target_shifts'] > 0 else 0,
                'monthly_variance': self._calculate_monthly_variance(worker_stats['monthly_stats'])
            }
        
        return worker_metrics
    
    def _calculate_coverage_metrics(self) -> Dict[str, Any]:
        """Calculate overall schedule coverage and gap analysis"""
        coverage_metrics = {
            'overall_coverage': 0.0,
            'post_coverage': {},
            'time_gaps': [],
            'critical_gaps': []
        }
        
        total_slots = 0
        filled_slots = 0
        post_coverage = {}
        
        for date, shifts in self.scheduler.schedule.items():
            for post_idx, worker in enumerate(shifts):
                post_num = post_idx + 1
                if post_num not in post_coverage:
                    post_coverage[post_num] = {'total': 0, 'filled': 0}
                
                post_coverage[post_num]['total'] += 1
                total_slots += 1
                
                if worker is not None:
                    post_coverage[post_num]['filled'] += 1
                    filled_slots += 1
                else:
                    # Track gaps
                    gap_info = {
                        'date': date.strftime('%Y-%m-%d'),
                        'post': post_num,
                        'weekday': date.strftime('%A'),
                        'is_weekend': self.scheduler.data_manager._is_weekend_day(date),
                        'is_holiday': self.scheduler.data_manager._is_holiday(date)
                    }
                    coverage_metrics['time_gaps'].append(gap_info)
                    
                    # Mark critical gaps (weekends/holidays)
                    if gap_info['is_weekend'] or gap_info['is_holiday']:
                        coverage_metrics['critical_gaps'].append(gap_info)
        
        coverage_metrics['overall_coverage'] = filled_slots / total_slots if total_slots > 0 else 0
        
        # Calculate post-specific coverage rates
        for post_num, data in post_coverage.items():
            coverage_metrics['post_coverage'][post_num] = data['filled'] / data['total'] if data['total'] > 0 else 0
        
        return coverage_metrics
    
    def _extract_constraint_metrics(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Extract constraint violation patterns and frequencies"""
        constraint_metrics = {
            'total_violations': 0,
            'violation_types': {},
            'worker_violations': {},
            'temporal_patterns': {}
        }
        
        # Aggregate constraint skips from statistics
        for worker_id, worker_stats in stats['workers'].items():
            worker_violations = 0
            for constraint_type, violations in worker_stats['constraint_skips'].items():
                violation_count = len(violations) if isinstance(violations, list) else violations
                worker_violations += violation_count
                
                if constraint_type not in constraint_metrics['violation_types']:
                    constraint_metrics['violation_types'][constraint_type] = 0
                constraint_metrics['violation_types'][constraint_type] += violation_count
            
            constraint_metrics['worker_violations'][worker_id] = worker_violations
            constraint_metrics['total_violations'] += worker_violations
        
        return constraint_metrics
    
    def _extract_seasonal_indicators(self) -> Dict[str, Any]:
        """Extract seasonal and temporal patterns from the schedule"""
        seasonal_indicators = {
            'monthly_patterns': {},
            'weekday_patterns': {},
            'holiday_impact': {},
            'seasonal_trends': []
        }
        
        # Monthly patterns
        monthly_shifts = {}
        weekday_shifts = [0] * 7  # Monday=0, Sunday=6
        holiday_shifts = 0
        total_holiday_slots = 0
        
        for date, shifts in self.scheduler.schedule.items():
            month_key = f"{date.year}-{date.month:02d}"
            filled_shifts = sum(1 for shift in shifts if shift is not None)
            
            if month_key not in monthly_shifts:
                monthly_shifts[month_key] = {'filled': 0, 'total': 0}
            monthly_shifts[month_key]['filled'] += filled_shifts
            monthly_shifts[month_key]['total'] += len(shifts)
            
            # Weekday patterns
            weekday_shifts[date.weekday()] += filled_shifts
            
            # Holiday impact
            if self.scheduler.data_manager._is_holiday(date):
                holiday_shifts += filled_shifts
                total_holiday_slots += len(shifts)
        
        seasonal_indicators['monthly_patterns'] = monthly_shifts
        seasonal_indicators['weekday_patterns'] = {
            str(i): count for i, count in enumerate(weekday_shifts)
        }
        seasonal_indicators['holiday_impact'] = {
            'fill_rate': holiday_shifts / total_holiday_slots if total_holiday_slots > 0 else 0,
            'total_holiday_shifts': holiday_shifts,
            'total_holiday_slots': total_holiday_slots
        }
        
        return seasonal_indicators
    
    def _calculate_efficiency_score(self, stats: Dict[str, Any]) -> float:
        """Calculate overall scheduling efficiency score"""
        try:
            # Base score on coverage
            total_shifts = stats['general']['total_shifts']
            total_possible = stats['general']['total_days'] * self.scheduler.num_shifts
            coverage_score = total_shifts / total_possible if total_possible > 0 else 0
            
            # Penalty for constraint violations
            total_violations = sum(
                len(violations) if isinstance(violations, list) else violations
                for worker_stats in stats['workers'].values()
                for violations in worker_stats['constraint_skips'].values()
            )
            violation_penalty = min(0.3, total_violations / (total_shifts + 1) * 0.5)
            
            # Balance score (workload distribution)
            shift_counts = [worker_stats['total_shifts'] for worker_stats in stats['workers'].values()]
            if shift_counts:
                avg_shifts = sum(shift_counts) / len(shift_counts)
                variance = sum((count - avg_shifts) ** 2 for count in shift_counts) / len(shift_counts)
                balance_score = max(0, 1 - (variance / (avg_shifts + 1)))
            else:
                balance_score = 0
            
            # Combined efficiency score
            efficiency = (coverage_score * 0.5 + balance_score * 0.3) - violation_penalty
            return max(0, min(1, efficiency))
            
        except Exception as e:
            logging.error(f"Error calculating efficiency score: {e}")
            return 0.0
    
    def _calculate_weekend_preference(self, worker_id: str) -> float:
        """Calculate worker's weekend shift preference/tendency"""
        weekend_count = len(self.scheduler.data_manager.worker_weekends.get(worker_id, []))
        total_shifts = len(self.scheduler.data_manager.worker_assignments.get(worker_id, []))
        return weekend_count / total_shifts if total_shifts > 0 else 0
    
    def _calculate_shift_consistency(self, worker_id: str) -> float:
        """Calculate how consistently a worker is scheduled"""
        assignments = sorted(list(self.scheduler.data_manager.worker_assignments.get(worker_id, [])))
        if len(assignments) < 2:
            return 0.0
        
        gaps = [(assignments[i+1] - assignments[i]).days for i in range(len(assignments) - 1)]
        if not gaps:
            return 0.0
        
        avg_gap = sum(gaps) / len(gaps)
        gap_variance = sum((gap - avg_gap) ** 2 for gap in gaps) / len(gaps)
        
        # Lower variance = higher consistency
        consistency = 1 / (1 + gap_variance / (avg_gap + 1))
        return min(1.0, consistency)
    
    def _calculate_post_rotation_score(self, post_distribution: Dict[str, int]) -> float:
        """Calculate how well balanced post rotation is for a worker"""
        if not post_distribution:
            return 0.0
        
        counts = list(post_distribution.values())
        if len(counts) <= 1:
            return 1.0
        
        avg_count = sum(counts) / len(counts)
        variance = sum((count - avg_count) ** 2 for count in counts) / len(counts)
        
        # Lower variance = better balance
        balance_score = 1 / (1 + variance / (avg_count + 1))
        return min(1.0, balance_score)
    
    def _calculate_monthly_variance(self, monthly_stats: Dict[str, Any]) -> float:
        """Calculate variance in monthly shift distribution"""
        distribution = monthly_stats.get('distribution', {})
        if not distribution:
            return 0.0
        
        counts = list(distribution.values())
        if len(counts) <= 1:
            return 0.0
        
        avg_count = sum(counts) / len(counts)
        variance = sum((count - avg_count) ** 2 for count in counts) / len(counts)
        
        return variance
    
    def store_historical_data(self, data: Dict[str, Any]) -> None:
        """Store collected data for historical analysis"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"schedule_data_{timestamp}.json"
            filepath = self.storage_path / filename
            
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logging.info(f"Historical data stored: {filepath}")
            
            # Also maintain a consolidated history file
            self._update_consolidated_history(data)
            
        except Exception as e:
            logging.error(f"Error storing historical data: {e}")
            raise SchedulerError(f"Failed to store historical data: {str(e)}")
    
    def _update_consolidated_history(self, data: Dict[str, Any]) -> None:
        """Update the consolidated historical database"""
        consolidated_file = self.storage_path / "consolidated_history.json"
        
        try:
            # Load existing data
            if consolidated_file.exists():
                with open(consolidated_file, 'r') as f:
                    history = json.load(f)
            else:
                history = {'records': [], 'summary': {}}
            
            # Add new record
            history['records'].append(data)
            
            # Update summary statistics
            history['summary'] = {
                'total_records': len(history['records']),
                'date_range': {
                    'first_record': history['records'][0]['timestamp'] if history['records'] else None,
                    'last_record': history['records'][-1]['timestamp'] if history['records'] else None
                },
                'last_updated': datetime.now().isoformat()
            }
            
            # Keep only last 100 records to prevent file from growing too large
            if len(history['records']) > 100:
                history['records'] = history['records'][-100:]
            
            # Save updated history
            with open(consolidated_file, 'w') as f:
                json.dump(history, f, indent=2, default=str)
            
        except Exception as e:
            logging.error(f"Error updating consolidated history: {e}")
    
    def _load_historical_data(self) -> None:
        """Load existing historical data from storage"""
        try:
            consolidated_file = self.storage_path / "consolidated_history.json"
            if consolidated_file.exists():
                with open(consolidated_file, 'r') as f:
                    history = json.load(f)
                    records_count = len(history.get('records', []))
                    logging.info(f"Loaded {records_count} historical records")
            else:
                logging.info("No existing historical data found")
        except Exception as e:
            logging.warning(f"Could not load historical data: {e}")
    
    def get_historical_summary(self) -> Dict[str, Any]:
        """Get summary of available historical data"""
        try:
            consolidated_file = self.storage_path / "consolidated_history.json"
            if not consolidated_file.exists():
                return {'status': 'no_data', 'message': 'No historical data available'}
            
            with open(consolidated_file, 'r') as f:
                history = json.load(f)
            
            return {
                'status': 'data_available',
                'summary': history.get('summary', {}),
                'record_count': len(history.get('records', [])),
                'latest_record': history['records'][-1] if history.get('records') else None
            }
            
        except Exception as e:
            logging.error(f"Error getting historical summary: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def get_data_for_forecasting(self, days_back: int = 90) -> Dict[str, Any]:
        """
        Get historical data formatted for forecasting models
        
        Args:
            days_back: Number of days of history to include
            
        Returns:
            Dictionary with time series data suitable for forecasting
        """
        try:
            consolidated_file = self.storage_path / "consolidated_history.json"
            if not consolidated_file.exists():
                return {'status': 'no_data', 'data': None}
            
            with open(consolidated_file, 'r') as f:
                history = json.load(f)
            
            records = history.get('records', [])
            if not records:
                return {'status': 'no_data', 'data': None}
            
            # Filter records within the specified time range
            cutoff_date = datetime.now() - timedelta(days=days_back)
            recent_records = [
                record for record in records
                if datetime.fromisoformat(record['timestamp']) >= cutoff_date
            ]
            
            if not recent_records:
                return {'status': 'insufficient_data', 'data': None}
            
            # Format data for forecasting
            forecasting_data = {
                'timestamps': [record['timestamp'] for record in recent_records],
                'fill_rates': [record['shift_metrics']['average_fill_rate'] for record in recent_records],
                'efficiency_scores': [record['efficiency_score'] for record in recent_records],
                'constraint_violations': [record['constraint_metrics']['total_violations'] for record in recent_records],
                'coverage_rates': [record['coverage_metrics']['overall_coverage'] for record in recent_records],
                'seasonal_indicators': [record['seasonal_indicators'] for record in recent_records]
            }
            
            return {'status': 'success', 'data': forecasting_data}
            
        except Exception as e:
            logging.error(f"Error getting forecasting data: {e}")
            return {'status': 'error', 'data': None, 'message': str(e)}