from fpdf import FPDF
import csv
from datetime import datetime
import calendar

class StatsExporter:
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def gather_worker_statistics(self):
        """Gather comprehensive statistics for all workers"""
        stats = {}
    
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = self.worker_assignments[worker_id]
        
            # Basic stats
            stats[worker_id] = {
                'worker_id': worker_id,
                'work_percentage': worker.get('work_percentage', 100),
                'total_shifts': len(assignments),
                'target_shifts': worker.get('target_shifts', 0),
            
                # Shifts by type
                'weekend_shifts': len(self.worker_weekends[worker_id]),
                'weekday_shifts': len(assignments) - len(self.worker_weekends[worker_id]),
            
                # Monthly distribution
                'monthly_distribution': {},
            
                # Shifts by weekday
                'weekday_distribution': {i: 0 for i in range(7)},  # 0-6 for Monday-Sunday
            
                # Gaps analysis
                'average_gap': 0,
                'min_gap': float('inf'),
                'max_gap': 0
            }
        
            # Calculate monthly distribution
            for date in sorted(assignments):
                month_key = f"{date.year}-{date.month:02d}"
                stats[worker_id]['monthly_distribution'][month_key] = \
                    stats[worker_id]['monthly_distribution'].get(month_key, 0) + 1
                stats[worker_id]['weekday_distribution'][date.weekday()] += 1
        
            # Calculate gaps
            if len(assignments) > 1:
                sorted_dates = sorted(assignments)
                gaps = [(sorted_dates[i+1] - sorted_dates[i]).days 
                       for i in range(len(sorted_dates)-1)]
                stats[worker_id]['average_gap'] = sum(gaps) / len(gaps)
                stats[worker_id]['min_gap'] = min(gaps)
                stats[worker_id]['max_gap'] = max(gaps)
            
        return stats

    def export_worker_stats(self, format='txt'):
        """Export worker statistics to file
        Args:
            format (str): 'txt' or 'pdf'
        Returns:
            str: Path to the generated file
        """
        stats = self.gather_worker_statistics()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
        if format.lower() == 'txt':
            filename = f'worker_stats_{timestamp}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("=== Worker Statistics Report ===\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
                for worker_id, worker_stats in stats.items():
                    f.write(f"\nWorker {worker_id}\n")
                    f.write("="* 20 + "\n")
                    f.write(f"Work Percentage: {worker_stats['work_percentage']}%\n")
                    f.write(f"Total Shifts: {worker_stats['total_shifts']}")
                    f.write(f" (Target: {worker_stats['target_shifts']})\n")
                    f.write(f"Weekend Shifts: {worker_stats['weekend_shifts']}\n")
                    f.write(f"Weekday Shifts: {worker_stats['weekday_shifts']}\n\n")
                
                    f.write("Monthly Distribution:\n")
                    for month, count in worker_stats['monthly_distribution'].items():
                        f.write(f"  {month}: {count} shifts\n")
                
                    f.write("\nWeekday Distribution:\n")
                    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                           'Friday', 'Saturday', 'Sunday']
                    for day_num, count in worker_stats['weekday_distribution'].items():
                        f.write(f"  {days[day_num]}: {count} shifts\n")
                
                    if worker_stats['total_shifts'] > 1:
                        f.write("\nGaps Analysis:\n")
                        f.write(f"  Average gap: {worker_stats['average_gap']:.1f} days\n")
                        f.write(f"  Minimum gap: {worker_stats['min_gap']} days\n")
                        f.write(f"  Maximum gap: {worker_stats['max_gap']} days\n")
                
                    f.write("\n" + "-"*50 + "\n")
    
        elif format.lower() == 'pdf':
            try:
                from reportlab.lib import colors
                from reportlab.lib.pagesizes import letter
                from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
                from reportlab.lib.styles import getSampleStyleSheet
            
                filename = f'worker_stats_{timestamp}.pdf'
                doc = SimpleDocTemplate(filename, pagesize=letter)
                elements = []
                styles = getSampleStyleSheet()
            
                # Add content to PDF
                # ... (PDF generation code would go here)
            # We can implement this part if you want to use PDF format
            
            except ImportError:
                raise ImportError("reportlab is required for PDF export. "
                                "Install it with: pip install reportlab")
    
        return filename
