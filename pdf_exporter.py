from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape, letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from calendar import monthcalendar
from datetime import datetime
import logging

class PDFExporter:
    def __init__(self, schedule_config):
        self.schedule = schedule_config.get('schedule', {})
        self.workers_data = schedule_config.get('workers_data', [])
        self.num_shifts = schedule_config.get('num_shifts', 0)
        self.holidays = schedule_config.get('holidays', [])
        self.styles = getSampleStyleSheet()

    def export_monthly_summary(self, year, month, month_stats, filename=None):
        """Export detailed monthly summary with worker shift listings to PDF"""
        if not filename:
            month_name = datetime(year, month, 1).strftime('%B_%Y')
            filename = f'schedule_summary_{month_name}.pdf'
        
        # Ensure we're using the correct page size constant
        doc = SimpleDocTemplate(
            filename,
            pagesize=letter,
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )
    
        # Prepare story (content)
        story = []
    
        # Add title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=16,
            spaceAfter=30
        )
        title = Paragraph(
            f"Schedule Summary for {datetime(year, month, 1).strftime('%B %Y')}",
            title_style
        )
        story.append(title)
    
        # Add generation info
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date_info = Paragraph(f"Generated on: {current_date}", self.styles['Normal'])
        story.append(date_info)
        story.append(Spacer(1, 20))
    
        # Add overall statistics
        stats_title = Paragraph("Overall Statistics", self.styles['Heading2'])
        story.append(stats_title)
        story.append(Spacer(1, 6))
    
        # Check if month_stats has the expected structure
        if not isinstance(month_stats, dict):
            logging.error(f"Invalid month_stats format: {type(month_stats)}")
            stats_data = [["No valid statistics data available"]]
        else:
            # Safely extract statistics
            workers_count = len(month_stats.get('workers', {}))
            total_shifts = month_stats.get('total_shifts', 0)
            weekend_shifts = month_stats.get('weekend_shifts', 0)
            last_post_shifts = month_stats.get('last_post_shifts', 0)
            
            stats_data = [
                ["Total Workers", str(workers_count)],
                ["Total Shifts", str(total_shifts)],
                ["Weekend Shifts", str(weekend_shifts)],
                ["Last Post Shifts", str(last_post_shifts)]
            ]
    
        stats_table = Table(stats_data, colWidths=[200, 100])
        stats_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        story.append(stats_table)
        story.append(Spacer(1, 20))
    
        # Add post distribution if available
        if 'posts' in month_stats:
            post_title = Paragraph("Post Distribution", self.styles['Heading2'])
            story.append(post_title)
            story.append(Spacer(1, 6))
        
            post_data = [["Post", "Shifts"]]
            for post, count in month_stats.get('posts', {}).items():
                # Make sure post is treated as integer for proper indexing
                try:
                    post_index = int(post)
                    post_data.append([f"Post {post_index+1}", str(count)])
                except (ValueError, TypeError):
                    post_data.append([f"Post {post}", str(count)])
        
            post_table = Table(post_data, colWidths=[200, 100])
            post_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            story.append(post_table)
            story.append(Spacer(1, 20))
    
        # Add worker details with shift listings
        worker_title = Paragraph("Worker Details", self.styles['Heading2'])
        story.append(worker_title)
        story.append(Spacer(1, 12))
    
        # Add detailed worker information with shift lists
        for worker_id, stats in sorted(month_stats.get('workers', {}).items()):
            # Worker header
            worker_header = Paragraph(f"Worker {worker_id}", self.styles['Heading3'])
            story.append(worker_header)
        
            # Worker summary - handle cases where old and new stats format might differ
            worker_summary_data = [
                ["Total Shifts", str(stats.get('total', 0))],
                ["Weekend Shifts", str(stats.get('weekends', 0))],
                ["Last Post Shifts", str(stats.get('last_post', 0))]
            ]
        
            summary_table = Table(worker_summary_data, colWidths=[120, 80])
            summary_table.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            story.append(summary_table)
            story.append(Spacer(1, 6))
        
            # Shift listing header
            shift_header = Paragraph("Assigned Shifts:", self.styles['Normal'])
            story.append(shift_header)
        
            # Get worker shifts with error handling
            worker_shifts = month_stats.get('worker_shifts', {}).get(worker_id, [])
        
            if worker_shifts:
                try:
                    # Create shift table
                    shift_data = [["Date", "Day", "Post", "Type"]]
                    
                    for shift in sorted(worker_shifts, key=lambda x: x.get('date', datetime.max)):
                        # Handle missing date - skip invalid entries
                        if 'date' not in shift:
                            continue
                            
                        date_obj = shift.get('date')
                        if not isinstance(date_obj, datetime):
                            # Try to convert to datetime if it's a string
                            try:
                                date_obj = datetime.strptime(str(date_obj), '%Y-%m-%d')
                            except:
                                continue
                        
                        date_str = date_obj.strftime('%d-%m-%Y')
                        day_str = shift.get('day', date_obj.strftime('%a'))
                        post_str = f"Post {shift.get('post', 'N/A')}"
                    
                        day_type = "Regular"
                        if shift.get('is_holiday', False):
                            day_type = "HOLIDAY"
                        elif shift.get('is_weekend', False):
                            day_type = "WEEKEND"
                    
                        shift_data.append([date_str, day_str, post_str, day_type])
                
                    # Only create table if we have data beyond the header
                    if len(shift_data) > 1:
                        shift_table = Table(shift_data, colWidths=[100, 80, 60, 80])
                        shift_table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black),
                            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ]))
                        story.append(shift_table)
                    else:
                        story.append(Paragraph("No valid shifts data", self.styles['Normal']))
                        
                except Exception as e:
                    logging.error(f"Error processing shifts for worker {worker_id}: {str(e)}")
                    story.append(Paragraph(f"Error processing shifts: {str(e)}", self.styles['Normal']))
            else:
                story.append(Paragraph("No shifts assigned", self.styles['Normal']))
        
            # Add spacer after each worker
            story.append(Spacer(1, 20))
    
        # Build the PDF
        try:
            doc.build(story)
            logging.info(f"Successfully created PDF: {filename}")
            return filename
        except Exception as e:
            logging.error(f"Failed to create PDF: {str(e)}")
            raise
        
    def export_monthly_calendar(self, year, month, filename=None):
        """Export monthly calendar view to PDF"""
        if not filename:
            filename = f'schedule_{year}_{month:02d}.pdf'
            
        doc = SimpleDocTemplate(
            filename,
            pagesize=landscape(A4),
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )
        
        # Prepare story (content)
        story = []
        
        # Add title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=16,
            spaceAfter=30
        )
        title = Paragraph(
            f"Schedule for {datetime(year, month, 1).strftime('%B %Y')}",
            title_style
        )
        story.append(title)
        
        # Create calendar data
        cal = monthcalendar(year, month)
        calendar_data = [['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']]
        
        for week in cal:
            week_data = []
            for day in week:
                if day == 0:
                    cell_content = ''
                else:
                    date = datetime(year, month, day)
                    cell_content = [str(day)]
                    
                    # Add scheduled workers
                    if date in self.schedule:
                        for i, worker_id in enumerate(self.schedule[date]):
                            if worker_id is not None:  # Check for None values
                                cell_content.append(f'S{i+1}: {worker_id}')
                    
                    # Mark holidays
                    if date in self.holidays:
                        cell_content.append('HOLIDAY')
                    
                    cell_content = '\n'.join(cell_content)
                week_data.append(cell_content)
            calendar_data.append(week_data)
        
        # Create table
        table = Table(calendar_data, colWidths=[1.5*inch]*7, rowHeights=[0.5*inch] + [1.2*inch]*len(cal))
        
        # Style the table
        style = TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (5, 1), (6, -1), colors.lightgrey),  # Weekend columns
        ])
        table.setStyle(style)
        
        story.append(table)
        doc.build(story)
        return filename

    def export_worker_statistics(self, filename=None):
        """Export worker statistics to PDF"""
        if not filename:
            filename = 'worker_statistics.pdf'
            
        doc = SimpleDocTemplate(
            filename,
            pagesize=A4,
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30
        )
        
        story = []
        
        # Add title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=16,
            spaceAfter=30
        )
        title = Paragraph("Worker Statistics Report", title_style)
        story.append(title)
        
        # Prepare statistics for each worker
        for worker in self.workers_data:
            worker_id = worker['id']
            
            # Get worker's assignments
            assignments = [
                date for date, workers in self.schedule.items()
                if worker_id in workers
            ]
            
            # Calculate statistics
            total_shifts = len(assignments)
            weekend_shifts = sum(1 for date in assignments if date.weekday() >= 5)
            holiday_shifts = sum(1 for date in assignments if date in self.holidays)
            
            # Calculate post distribution
            post_counts = {i: 0 for i in range(self.num_shifts)}
            for date in assignments:
                if date in self.schedule:
                    try:
                        post = self.schedule[date].index(worker_id)
                        post_counts[post] += 1
                    except ValueError:
                        # Skip if worker_id isn't in the list
                        pass
            
            # Calculate weekday distribution
            weekday_counts = {i: 0 for i in range(7)}
            for date in assignments:
                weekday_counts[date.weekday()] += 1
            
            # Create worker section
            worker_title = Paragraph(
                f"Worker {worker_id}",
                self.styles['Heading2']
            )
            story.append(worker_title)
            
            # Add worker details
            details = [
                f"Work Percentage: {worker.get('work_percentage', 100)}%",
                f"Total Shifts: {total_shifts}",
                f"Weekend Shifts: {weekend_shifts}",
                f"Holiday Shifts: {holiday_shifts}",
                "\nPost Distribution:",
                *[f"  Post {post+1}: {count}" for post, count in post_counts.items()],
                "\nWeekday Distribution:",
                "  Mon Tue Wed Thu Fri Sat Sun",
                "  " + " ".join(f"{weekday_counts[i]:3d}" for i in range(7))
            ]
            
            details_text = Paragraph(
                '<br/>'.join(details),
                self.styles['Normal']
            )
            story.append(details_text)
            story.append(Spacer(1, 20))
        
        doc.build(story)
        return filename
