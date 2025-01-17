om fpdf import FPDF
import csv
from datetime import datetime
import calendar

class PDFExporter:
    @staticmethod
    def create_monthly_calendar(schedule, year, month, filename):
        pdf = FPDF(orientation='L', format='A4')
        pdf.add_page()
        
        # Add title
        pdf.set_font('Arial', 'B', 16)
        month_name = calendar.month_name[month]
        pdf.cell(0, 10, f'Guard Schedule - {month_name} {year}', 0, 1, 'C')
        
        # Add metadata
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 10, f'Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 0, 1, 'R')
        
        # Create calendar grid
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        cell_width = 37
        cell_height = 20
        
        # Add day headers
        pdf.set_font('Arial', 'B', 10)
        for day in days:
            pdf.cell(cell_width, 10, day, 1, 0, 'C')
        pdf.ln()
        
        # Add calendar days
        cal = calendar.monthcalendar(year, month)
        pdf.set_font('Arial', '', 8)
        
        for week in cal:
            max_height = cell_height
            for day in week:
                if day == 0:
                    pdf.cell(cell_width, max_height, '', 1)
                else:
                    date = datetime(year, month, day)
                    content = f"{day}\n"
                    if date in schedule:
                        content += "\n".join(schedule[date])
                    pdf.multi_cell(cell_width, 5, content, 1)
                    pdf.set_xy(pdf.get_x() + cell_width, pdf.get_y() - max_height)
            pdf.ln(max_height)
        
        pdf.output(filename)

class CSVHandler:
    @staticmethod
    def export_schedule(schedule, filename):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Date', 'Day', 'Month', 'Year', 'Guards'])
            
            for date in sorted(schedule.keys()):
                writer.writerow([
                    date.strftime('%Y-%m-%d'),
                    date.day,
                    date.month,
                    date.year,
                    ';'.join(schedule[date])
                ])

    @staticmethod
    def import_schedule(filename):
        schedule = {}
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                date = datetime.strptime(row['Date'], '%Y-%m-%d')
                guards = row['Guards'].split(';')
                schedule[date] = guards
        return schedule
