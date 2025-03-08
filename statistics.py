# Imports
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from scheduler import Scheduler
else:
    from scheduler import SchedulerError

class StatisticsCalculator:
    """Calculates statistics and metrics for schedules"""
    
    def __init__(self, scheduler):
        """
        Initialize the statistics calculator
        
        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
        
        # Store references to frequently accessed attributes
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        self.worker_posts = scheduler.worker_posts
        self.worker_weekdays = scheduler.worker_weekdays
        self.worker_weekends = scheduler.worker_weekends
        self.workers_data = scheduler.workers_data
        self.num_shifts = scheduler.num_shifts
        
        logging.info("StatisticsCalculator initialized")
    
    def get_post_counts(self, worker_id):
        """
        Get the counts of different posts for a worker
        
        Args:
            worker_id: The worker ID to check
        Returns:
            dict: Dictionary mapping post numbers to counts
        """
        if worker_id not in self.worker_posts:
            return {}
            
        post_counts = {}
        for post in self.worker_posts[worker_id]:
            post_counts[post] = post_counts.get(post, 0) + 1
        
        return post_counts
    
    def _get_monthly_distribution(self, worker_id):
        """
        Get monthly shift distribution for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            dict: Monthly shift counts {YYYY-MM: count}
        """
        distribution = {}
        for date in sorted(list(self.worker_assignments[worker_id])):
            month_key = f"{date.year}-{date.month:02d}"
            distribution[month_key] = distribution.get(month_key, 0) + 1
        return distribution
    
    def _analyze_gaps(self, worker_id):
        """
        Analyze gaps between shifts for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            dict: Statistics about gaps between assignments
        """
        assignments = sorted(list(self.worker_assignments[worker_id]))
        if len(assignments) <= 1:
            return {'min_gap': None, 'max_gap': None, 'avg_gap': None}

        gaps = [(assignments[i+1] - assignments[i]).days 
                for i in range(len(assignments)-1)]
        
        return {
            'min_gap': min(gaps),
            'max_gap': max(gaps),
            'avg_gap': sum(gaps) / len(gaps)
        }

    def _get_least_used_weekday(self, worker_id):
        """
        Get the weekday that has been used least often for this worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            int: Weekday number (0-6 for Monday-Sunday)
        """
        weekdays = self.worker_weekdays[worker_id]
        min_count = min(weekdays.values())
        # If there are multiple weekdays with the same minimum count,
        # prefer the earliest one in the week
        for weekday in range(7):  # 0-6 for Monday-Sunday
            if weekdays[weekday] == min_count:
                return weekday
        return 0  # Fallback to Monday if something goes wrong

    def gather_statistics(self):
        """
        Gather comprehensive schedule statistics
        
        Returns:
            dict: Detailed statistics about the schedule and worker assignments
        """
        stats = {
            'general': {
                'total_days': (self.end_date - self.start_date).days + 1,
                'total_shifts': sum(len(shifts) for shifts in self.schedule.values()),
                'constraint_skips': {
                    'gap': sum(len(skips['gap']) for skips in self.constraint_skips.values()),
                    'incompatibility': sum(len(skips['incompatibility']) for skips in self.constraint_skips.values()),
                    'reduced_gap': sum(len(skips['reduced_gap']) for skips in self.constraint_skips.values())
                }
            },
            'workers': {}
        }

        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = self.worker_assignments[worker_id]
            
            monthly_dist = self._get_monthly_distribution(worker_id)
            monthly_stats = {
                'distribution': monthly_dist,
                'min_monthly': min(monthly_dist.values()) if monthly_dist else 0,
                'max_monthly': max(monthly_dist.values()) if monthly_dist else 0,
                'monthly_imbalance': max(monthly_dist.values()) - min(monthly_dist.values()) if monthly_dist else 0
            }
            
            stats['workers'][worker_id] = {
                'total_shifts': len(assignments),
                'target_shifts': worker.get('target_shifts', 0),
                'work_percentage': worker.get('work_percentage', 100),
                'weekend_shifts': len(self.worker_weekends[worker_id]),
                'weekday_distribution': self.worker_weekdays[worker_id],
                'post_distribution': self._get_post_counts(worker_id),
                'constraint_skips': self.constraint_skips[worker_id],
                'monthly_stats': monthly_stats,
                'gaps_analysis': self._analyze_gaps(worker_id)
            }

        # Add monthly balance analysis
        stats['monthly_balance'] = self._analyze_monthly_balance()
        
        return stats

    def _analyze_monthly_balance(self):
        """
        Analyze monthly balance across all workers
        
        Returns:
            dict: Statistics about monthly distribution balance
        """
        monthly_stats = {}
        
        # Get all months in schedule period
        all_months = set()
        for worker_id in self.worker_assignments:
            dist = self._get_monthly_distribution(worker_id)
            all_months.update(dist.keys())
        
        for month in sorted(all_months):
            worker_counts = []
            for worker_id in self.worker_assignments:
                dist = self._get_monthly_distribution(worker_id)
                worker_counts.append(dist.get(month, 0))
            
            if worker_counts:
                monthly_stats[month] = {
                    'min_shifts': min(worker_counts),
                    'max_shifts': max(worker_counts),
                    'avg_shifts': sum(worker_counts) / len(worker_counts),
                    'imbalance': max(worker_counts) - min(worker_counts)
                }
        
        return monthly_stats
    
    def _get_worker_shift_ratio(self, worker_id):
        """
        Calculate the ratio of assigned shifts to target shifts for a worker
        
        Args:
            worker_id: The worker's ID
        Returns:
            float: Ratio of assigned/target shifts (1.0 = perfect match)
        """
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        target = worker.get('target_shifts', 0)
        if target == 0:
            return 0
        return len(self.worker_assignments[worker_id]) / target
   
    def _calculate_post_rotation_coverage(self):
        """Calculate how well the post rotation is working across all workers"""
        worker_scores = {}
        total_score = 0
    
        for worker in self.workers_data:
            worker_id = worker['id']
            post_counts = self._get_post_counts(worker_id)
            total_assignments = sum(post_counts.values())
        
            if total_assignments == 0:
                worker_scores[worker_id] = 100  # No assignments, perfect score
                continue
            
            post_imbalance = 0
            expected_per_post = total_assignments / self.num_shifts
        
            for post in range(self.num_shifts):
                post_count = post_counts.get(post, 0)
                post_imbalance += abs(post_count - expected_per_post)
        
            # Calculate a score where 0 imbalance = 100%
            imbalance_ratio = post_imbalance / total_assignments
            worker_score = max(0, 100 - (imbalance_ratio * 100))
            worker_scores[worker_id] = worker_score
        
        # Calculate overall score
        if worker_scores:
            total_score = sum(worker_scores.values()) / len(worker_scores)
        else:
            total_score = 0
        
        return {
            'overall_score': total_score,
            'worker_scores': worker_scores
        }
    
    def _calculate_weekday_imbalance(self, worker_id, date):
        """Calculate how much this assignment would affect weekday balance"""
        weekday = date.weekday()
        counts = self.worker_weekdays[worker_id].copy()
        counts[weekday] += 1
        return max(counts.values()) - min(counts.values())
    
    def _calculate_coverage(self):
        """Calculate schedule coverage percentage"""
        total_required_shifts = (
            (self.end_date - self.start_date).days + 1
        ) * self.num_shifts
        
        actual_shifts = sum(len(shifts) for shifts in self.schedule.values())
        return (actual_shifts / total_required_shifts) * 100

    def _calculate_balance_score(self):
        """Calculate overall balance score based on various factors"""
        scores = []
    
        # Post rotation balance
        for worker_id in self.worker_assignments:
            post_counts = self._get_post_counts(worker_id)
            if post_counts.values():
                post_imbalance = max(post_counts.values()) - min(post_counts.values())
                scores.append(max(0, 100 - (post_imbalance * 20)))
    
        # Weekday distribution balance
        for worker_id, weekdays in self.worker_weekdays.items():
            if weekdays.values():
                weekday_imbalance = max(weekdays.values()) - min(weekdays.values())
                scores.append(max(0, 100 - (weekday_imbalance * 20)))
    
        return sum(scores) / len(scores) if scores else 0
    
    def _count_constraint_violations(self):
        """Count total constraint violations"""
        return {
            'gap_violations': sum(
                len(skips['gap']) for skips in self.constraint_skips.values()
            ),
            'incompatibility_violations': sum(
                len(skips['incompatibility']) for skips in self.constraint_skips.values()
            ),
            'reduced_gap_violations': sum(
                len(skips['reduced_gap']) for skips in self.constraint_skips.values()
            )
        }

    def _calculate_worker_satisfaction(self):
        """Calculate worker satisfaction score based on preferences and constraints"""
        satisfaction_scores = []
        
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = len(self.worker_assignments[worker_id])
            target = worker.get('target_shifts', 0)
            
            # Calculate basic satisfaction score
            if target > 0:
                target_satisfaction = 100 - (abs(assignments - target) / target * 100)
                satisfaction_scores.append(target_satisfaction)
            
            # Deduct points for constraint violations
            violations = sum(len(v) for v in self.constraint_skips[worker_id].values())
            if violations > 0:
                violation_penalty = min(violations * 10, 50)  # Cap penalty at 50%
                satisfaction_scores.append(100 - violation_penalty)
        
        return sum(satisfaction_scores) / len(satisfaction_scores) if satisfaction_scores else 0
    
    def get_schedule_metrics(self):
        """
        Calculate schedule performance metrics
        
        Returns:
            dict: Dictionary containing various schedule performance metrics
        """
        metrics = {
            'coverage': self._calculate_coverage(),
            'balance_score': self._calculate_balance_score(),
            'constraint_violations': self._count_constraint_violations(),
            'worker_satisfaction': self._calculate_worker_satisfaction()
        }
        
        logging.info("Generated schedule metrics")
        return metrics

    def _generate_schedule_header(self):
        """Generate the header section of the schedule output"""
        return (
            "=== Guard Schedule ===\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Generated by: {self.current_user}\n"
            f"Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}\n"
            f"Total Workers: {len(self.workers_data)}\n"
            f"Shifts per Day: {self.num_shifts}\n"
            "=" * 40 + "\n\n"
        )
    
    def _generate_schedule_body(self):
        """Generate the main body of the schedule output"""
        output = ""
        for date in sorted(self.schedule.keys()):
            # Date header
            output += f"\n{date.strftime('%Y-%m-%d')} ({date.strftime('%A')})"
            if self._is_holiday(date):
                output += " [HOLIDAY]"
            elif self._is_pre_holiday(date):
                output += " [PRE-HOLIDAY]"
            output += "\n"
            
            # Shift assignments
            for i, worker_id in enumerate(self.schedule[date], 1):
                worker = next(w for w in self.workers_data if w['id'] == worker_id)
                output += f"  Shift {i}: Worker {worker_id}"
                
                # Add worker details
                work_percentage = worker.get('work_percentage', 100)
                if float(work_percentage) < 100:
                    output += f" (Part-time: {work_percentage}%)"
                
                # Add post rotation info
                post_counts = self._get_post_counts(worker_id)
                output += f" [Post {i} count: {post_counts.get(i-1, 0)}]"
                
                output += "\n"
            output += "-" * 40 + "\n"
        
        return output

    def _generate_schedule_summary(self):
        """Generate summary statistics for the schedule output"""
        stats = self.gather_statistics()
        
        summary = "\n=== Schedule Summary ===\n"
        summary += f"Total Days: {stats['general']['total_days']}\n"
        summary += f"Total Shifts: {stats['general']['total_shifts']}\n"
        
        # Constraint skip summary
        summary += "\nConstraint Skips:\n"
        for skip_type, count in stats['general']['constraint_skips'].items():
            summary += f"  {skip_type.title()}: {count}\n"
        
        # Worker summary
        summary += "\nWorker Statistics:\n"
        for worker_id, worker_stats in stats['workers'].items():
            summary += f"\nWorker {worker_id}:\n"
            summary += f"  Assigned/Target: {worker_stats['total_shifts']}/{worker_stats['target_shifts']}\n"
            summary += f"  Weekend Shifts: {worker_stats['weekend_shifts']}\n"
            
            # Monthly distribution
            monthly_stats = worker_stats['monthly_stats']
            summary += "  Monthly Distribution:\n"
            for month, count in monthly_stats['distribution'].items():
                summary += f"    {month}: {count}\n"
        
        summary += "\n" + "=" * 40 + "\n"
        return summary

    
    def generate_worker_report(self, worker_id):
        """
        Generate a detailed report for a specific worker
        
        Args:
            worker_id: The worker's ID to generate report for
        Returns:
            str: Formatted report text
        """

    
    

