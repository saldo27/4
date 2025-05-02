from kivy.app import App
from kivy.uix.popup import Popup
from kivy.uix.label import Label
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape, letter # Keep A4 if needed elsewhere
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm # Use cm for better control maybe
from calendar import monthcalendar
from datetime import datetime
import logging

def numeric_sort_key(item):
    """
    Attempts to convert the first element of a tuple (the key) to an integer
    for sorting. Returns a tuple to prioritize numeric keys and handle errors.
    item[0] is assumed to be the worker ID (key).
    """
    try:
        # Try converting the key (worker ID) to an integer
        return (0, int(item[0])) # (0, numeric_value) - sorts numbers first
    except (ValueError, TypeError):
        # If conversion fails, return a tuple indicating it's non-numeric
        return (1, item[0]) # (1, original_string) - sorts non-numbers after numbers

class PDFExporter:
    def __init__(self, schedule_config):
        self.schedule = schedule_config.get('schedule', {})
        self.workers_data = schedule_config.get('workers_data', [])
        self.num_shifts = schedule_config.get('num_shifts', 0)
        self.holidays = schedule_config.get('holidays', [])
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name='SmallNormal', parent=self.styles['Normal'], fontSize=9))
        self.styles.add(ParagraphStyle(name='SmallBold', parent=self.styles['SmallNormal'], fontName='Helvetica-Bold'))

    def export_summary_pdf(self, stats_data): # Takes the whole stats dictionary now
        """Export a detailed GLOBAL summary with shift listings and distributions."""

        # --- Determine Filename and Title from stats_data ---
        start = stats_data.get('period_start')
        end = stats_data.get('period_end')
        if start and end:
            period_str_file = f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
            period_str_title = f"{start.strftime('%d-%m-%Y')} to {end.strftime('%d-%m-%Y')}"
            filename = f"summary_global_{period_str_file}.pdf"
            title_text = f"Schedule Summary ({period_str_title})"
        else:
            filename = "summary_global_full_period.pdf"
            title_text = "Schedule Summary (Full Period)"
        # --- End Filename/Title ---

        try:
            doc = SimpleDocTemplate(
                filename, pagesize=A4, # Portrait A4
                rightMargin=1.5*cm, leftMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm
            )
            styles = self.styles
            story = []
            weekdays_short = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

            # --- PDF Title (using new title_text) ---
            title = Paragraph(title_text, styles['h1'])
            story.append(title)
            story.append(Spacer(1, 0.5*cm))

            # --- Worker Details ---
            worker_title = Paragraph("Worker Details & Distributions", styles['h2'])
            story.append(worker_title)
            story.append(Spacer(1, 0.3*cm))
            story.append(Paragraph("<u>Assigned Shifts:</u>", styles['SmallBold']))

            # Use the workers data directly from stats_data
            workers_stats = stats_data.get('workers', {})
            worker_shifts_all = stats_data.get('worker_shifts', {})

            if not workers_stats:
                 story.append(Paragraph("No worker statistics found for this period.", styles['Normal']))
            else:
                # Numeric sort: 1,2,3,…10,11
                for worker_id, stats in sorted(workers_stats.items(), key=numeric_sort_key):
                    # --- Worker Header ---
                    worker_header = Paragraph(f"Worker {worker_id}", styles['h3'])
                    story.append(worker_header)
                    story.append(Spacer(1, 0.2*cm))

                    # Get stats
                    total_w = stats.get('total', 0)
                    holidays_w = stats.get('holidays', 0)
                    last_post_w = stats.get('last_post', 0)
                    weekday_counts = stats.get('weekday_counts', {})
                    post_counts = stats.get('post_counts', {})
                    worker_shifts = worker_shifts_all.get(worker_id, []) 


                    # --- Worker Summary Stats ---
                    summary_text = f"<b>Total Shifts:</b> {total_w} | <b>Weekend Shifts:</b> {stats.get('weekends', 0)} | <b>Holiday Shifts:</b> {holidays_w} | <b>Last Post Shifts:</b> {last_post_w}"
                    story.append(Paragraph(summary_text, styles['Normal']))
                    story.append(Spacer(1, 0.1*cm))

                    # --- Weekday Distribution ---
                    weekdays_str = "<b>Weekdays:</b> " + " ".join([f"{weekdays_short[i]}:{weekday_counts.get(i, 0)}" for i in range(7)])
                    story.append(Paragraph(weekdays_str, styles['Normal']))
                    story.append(Spacer(1, 0.1*cm))

                    # --- Post Distribution ---
                    posts_str = "<b>Posts:</b> " + " ".join([f"P{post+1}:{count}" for post, count in sorted(post_counts.items())])
                    story.append(Paragraph(posts_str, styles['Normal']))
                    story.append(Spacer(1, 0.3*cm))

                    # --- Assigned Shifts Table ---
                    if worker_shifts:
                        story.append(Paragraph("<u>Assigned Shifts:</u>", styles['SmallBold']))
                        story.append(Spacer(1, 0.1*cm))

                        shifts_data = [['Date', 'Day', 'Post', 'Type']]
                        # Sort shifts by date
                        for shift in sorted(worker_shifts, key=lambda x: x['date']):
                            date_str = shift['date'].strftime('%d-%m-%Y')
                            day_str = shift['day'][:3]
                            post_str = f"P{shift['post']}"
                            day_type = ""
                            if shift['is_holiday']: day_type = "HOL"
                            elif shift['is_weekend']: day_type = "W/E"
                            shifts_data.append([date_str, day_str, post_str, day_type])

                        # Limit table height? Consider splitting if too long? For now, let it flow.
                        shifts_table = Table(shifts_data, colWidths=[2.5*cm, 1.5*cm, 1.5*cm, 1.5*cm], repeatRows=1) # Repeat header row
                        shifts_table.setStyle(TableStyle([
                            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, -1), 9),
                            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
                        ]))
                        story.append(shifts_table)
                    else:
                        story.append(Paragraph("No shifts assigned in this period.", styles['SmallNormal']))

                    story.append(Spacer(1, 0.5*cm)) # Space between workers

            # --- Build PDF ---
            doc.build(story)
            logging.info(f"Successfully created GLOBAL summary PDF: {filename}")
            return filename # Return filename on success

        except Exception as e:
            logging.error(f"Failed to export GLOBAL summary PDF: {str(e)}", exc_info=True)
            raise e # Re-raise for main.py to catch
        
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
            weekend_shifts = sum(1 for date in assignments if date.weekday() >= 4)
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
