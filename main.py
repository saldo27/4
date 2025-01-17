from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.spinner import Spinner
from kivy.uix.popup import Popup
from kivy.uix.scrollview import ScrollView
from datetime import datetime, timedelta
import calendar
from fpdf import FPDF
import csv
import os

# Update the current datetime
current_datetime = datetime(2025, 1, 17, 23, 20, 49)

class InitialSetupScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=20, spacing=10)

        # Title
        title = Label(
            text='Work Shift Manager',
            size_hint_y=0.2,
            font_size='24sp'
        )
        layout.add_widget(title)

        # Current Date/Time and User Info
        info_layout = GridLayout(cols=1, size_hint_y=0.2, spacing=5)
        current_datetime = datetime(2025, 1, 17, 23, 4, 12)
        
        # Updated datetime format
        date_label = Label(
            text=f'Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): {current_datetime.strftime("%Y-%m-%d %H:%M:%S")}',
            size_hint_y=None,
            height=30
        )
        info_layout.add_widget(date_label)
        
        user_label = Label(
            text=f"Current User's Login: saldo27",
            size_hint_y=None,
            height=30
        )
        info_layout.add_widget(user_label)
        layout.add_widget(info_layout)

        # Date Range Input
        date_layout = GridLayout(cols=2, spacing=10, size_hint_y=0.3)
        date_layout.add_widget(Label(text='Start Date (DD-MM-YYYY):'))
        self.start_date = TextInput(
            multiline=False,
            input_filter=lambda text, from_undo: text[:10]
        )
        date_layout.add_widget(self.start_date)

        date_layout.add_widget(Label(text='End Date (DD-MM-YYYY):'))
        self.end_date = TextInput(
            multiline=False,
            input_filter=lambda text, from_undo: text[:10]
        )
        date_layout.add_widget(self.end_date)
        layout.add_widget(date_layout)

        # Shifts per Day Input
        shifts_layout = BoxLayout(orientation='vertical', spacing=5, size_hint_y=0.2)
        shifts_layout.add_widget(Label(text='Number of Shifts per Day:'))
        self.shifts_spinner = Spinner(
            text='Select number of shifts',
            values=[str(i) for i in range(1, 11)],
            size_hint_y=0.7
        )
        shifts_layout.add_widget(self.shifts_spinner)
        layout.add_widget(shifts_layout)

        # Workers Input
        workers_layout = BoxLayout(orientation='vertical', spacing=5, size_hint_y=0.2)
        workers_layout.add_widget(Label(text='Number of Available Workers:'))
        self.workers_input = TextInput(
            multiline=False,
            input_filter='int',
            size_hint_y=0.7
        )
        workers_layout.add_widget(self.workers_input)
        layout.add_widget(workers_layout)

        # Continue Button
        self.continue_btn = Button(
            text='Continue to Worker Details',
            size_hint_y=0.2
        )
        self.continue_btn.bind(on_press=self.validate_and_continue)
        layout.add_widget(self.continue_btn)

        self.add_widget(layout)

    def validate_and_continue(self, instance):
        try:
            # Validate dates
            start = datetime.strptime(self.start_date.text, '%d-%m-%Y')
            end = datetime.strptime(self.end_date.text, '%d-%m-%Y')
            
            if end < start:
                raise ValueError("End date must be after start date")

            if self.shifts_spinner.text == 'Select number of shifts':
                raise ValueError("Please select number of shifts")
            
            num_shifts = int(self.shifts_spinner.text)
            if num_shifts < 1:
                raise ValueError("Must have at least 1 shift")

            if not self.workers_input.text:
                raise ValueError("Please enter number of workers")
            
            num_workers = int(self.workers_input.text)
            if num_workers < 1:
                raise ValueError("Must have at least 1 worker")

            app = App.get_running_app()
            app.schedule_config = {
                'start_date': start,
                'end_date': end,
                'num_shifts': num_shifts,
                'num_workers': num_workers,
                'current_worker_index': 0
            }

            self.manager.current = 'worker_details'

        except ValueError as e:
            error_popup = ErrorPopup(str(e))
            error_popup.open()

class WorkerDetailsScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.layout = BoxLayout(orientation='vertical', padding=20, spacing=10)

        # Title with worker number
        self.title_label = Label(
            text='Worker Details',
            size_hint_y=0.1,
            font_size='24sp'
        )
        self.layout.add_widget(self.title_label)

        # Scrollable content
        scroll = ScrollView(size_hint=(1, 0.8))
        self.form_layout = GridLayout(
            cols=2,
            spacing=10,
            size_hint_y=None,
            padding=10
        )
        self.form_layout.bind(minimum_height=self.form_layout.setter('height'))

        # Worker Identification
        self.form_layout.add_widget(Label(text='Worker ID:'))
        self.worker_id = TextInput(
            multiline=False,
            size_hint_y=None,
            height=40
        )
        self.form_layout.add_widget(self.worker_id)

        # Work Periods
        self.form_layout.add_widget(Label(
            text='Work Periods\n(DD-MM-YYYY - DD-MM-YYYY,\nseparate multiple periods with ;):'
        ))
        self.work_periods = TextInput(
            multiline=True,
            size_hint_y=None,
            height=60
        )
        self.form_layout.add_widget(self.work_periods)

        # Work Percentage
        self.form_layout.add_widget(Label(text='Work Percentage (default 100%):'))
        self.work_percentage = TextInput(
            multiline=False,
            text='100',
            input_filter='float',
            size_hint_y=None,
            height=40
        )
        self.form_layout.add_widget(self.work_percentage)

        # Mandatory Coverage Days
        self.form_layout.add_widget(Label(
            text='Mandatory Coverage Days\n(DD-MM-YYYY, separate with ;):'
        ))
        self.mandatory_days = TextInput(
            multiline=True,
            size_hint_y=None,
            height=60
        )
        self.form_layout.add_widget(self.mandatory_days)

        # Days Off
        self.form_layout.add_widget(Label(
            text='Days Off\n(DD-MM-YYYY or DD-MM-YYYY - DD-MM-YYYY,\nseparate with ;):'
        ))
        self.days_off = TextInput(
            multiline=True,
            size_hint_y=None,
            height=60
        )
        self.form_layout.add_widget(self.days_off)

        scroll.add_widget(self.form_layout)
        self.layout.add_widget(scroll)

        # Buttons layout
        buttons_layout = BoxLayout(
            orientation='horizontal',
            size_hint_y=0.1,
            spacing=10
        )

        self.prev_button = Button(
            text='Previous Worker',
            disabled=True
        )
        self.prev_button.bind(on_press=self.previous_worker)
        buttons_layout.add_widget(self.prev_button)

        self.next_button = Button(
            text='Next Worker'
        )
        self.next_button.bind(on_press=self.save_and_continue)
        buttons_layout.add_widget(self.next_button)

        self.layout.add_widget(buttons_layout)
        self.add_widget(self.layout)

    def on_enter(self):
        app = App.get_running_app()
        current_index = app.schedule_config['current_worker_index']
        total_workers = app.schedule_config['num_workers']
        
        self.title_label.text = f'Worker Details ({current_index + 1} of {total_workers})'
        
        self.prev_button.disabled = (current_index == 0)
        self.next_button.text = 'Finish' if current_index == total_workers - 1 else 'Next Worker'

    def save_and_continue(self, instance):
    try:
        # Validate inputs
        if not self.worker_id.text.strip():
            raise ValueError("Worker ID is required")

        # Validate work percentage
        try:
            work_percentage = float(self.work_percentage.text or '100')
            if not (0 < work_percentage <= 100):
                raise ValueError()
        except ValueError:
            raise ValueError("Invalid work percentage")

        # Validate dates
        if self.work_periods.text:
            if not self.validate_dates(self.work_periods.text):
                raise ValueError("Invalid work period format")
        if self.mandatory_days.text:
            if not self.validate_dates(self.mandatory_days.text):
                raise ValueError("Invalid mandatory days format")
        if self.days_off.text:
            if not self.validate_dates(self.days_off.text):
                raise ValueError("Invalid days off format")

        # Save worker data
        app = App.get_running_app()
        worker_data = {
            'id': self.worker_id.text.strip(),
            'work_periods': self.work_periods.text.strip(),
            'work_percentage': work_percentage,
            'mandatory_days': self.mandatory_days.text.strip(),
            'days_off': self.days_off.text.strip()
        }
        
        if 'workers_data' not in app.schedule_config:
            app.schedule_config['workers_data'] = []
        app.schedule_config['workers_data'].append(worker_data)

        current_index = app.schedule_config['current_worker_index']
        if current_index < app.schedule_config['num_workers'] - 1:
            app.schedule_config['current_worker_index'] = current_index + 1
            self.clear_inputs()
            self.on_enter()
        else:
            # Generate schedule
            from scheduler import Scheduler
            scheduler = Scheduler(app.schedule_config)
            app.schedule_config['schedule'] = scheduler.generate_schedule()
            
            # Validate the schedule
            errors, warnings = scheduler.validate_schedule()
            if errors:
                raise ValueError("\n".join(errors))
            if warnings:
                warning_popup = WarningPopup("\n".join(warnings))
                warning_popup.open()

            success_popup = SuccessPopup("Schedule generated successfully!")
            success_popup.open()
            self.manager.current = 'calendar_view'

    except ValueError as e:
        error_popup = ErrorPopup(str(e))
        error_popup.open()

def validate_dates(self, date_str):
    """Validate date string format"""
    try:
        for period in date_str.split(';'):
            period = period.strip()
            if ' - ' in period:
                start, end = period.split(' - ')
                datetime.strptime(start.strip(), '%d-%m-%Y')
                datetime.strptime(end.strip(), '%d-%m-%Y')
            else:
                datetime.strptime(period, '%d-%m-%Y')
        return True
    except ValueError:
        return False

def clear_inputs(self):
    """Clear all input fields"""
    self.worker_id.text = ''
    self.work_periods.text = ''
    self.work_percentage.text = '100'
    self.mandatory_days.text = ''
    self.days_off.text = ''

    def load_worker_data(self):
        app = App.get_running_app()
        current_index = app.schedule_config['current_worker_index']
        if 'workers_data' in app.schedule_config and current_index < len(app.schedule_config['workers_data']):
            data = app.schedule_config['workers_data'][current_index]
            self.worker_id.text = data['id']
            self.work_periods.text = data['work_periods']
            self.work_percentage.text = str(data['work_percentage'])
            self.mandatory_days.text = data['mandatory_days']
            self.days_off.text = data['days_off']

    def validate_dates(self, date_str):
        if not date_str:
            return True
        try:
            for period in date_str.split(';'):
                period = period.strip()
                if ' - ' in period:
                    start, end = period.split(' - ')
                    datetime.strptime(start.strip(), '%d-%m-%Y')
                    datetime.strptime(end.strip(), '%d-%m-%Y')
                else:
                    datetime.strptime(period, '%d-%m-%Y')
            return True
        except ValueError:
            return False

# Add WarningPopup class
class WarningPopup(Popup):
    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.title = 'Warning'
        self.size_hint = (0.8, 0.4)
        
        content = BoxLayout(orientation='vertical')
        content.add_widget(Label(text=message))
        
        button = Button(
            text='OK',
            size_hint_y=0.2
        )
        button.bind(on_press=self.dismiss)
        content.add_widget(button)
        
        self.content = content

class ScheduleGenerator:
    def __init__(self, config):
        self.config = config
        self.start_date = config['start_date']
        self.end_date = config['end_date']
        self.num_shifts = config['num_shifts']
        self.workers_data = config['workers_data']
        self.schedule = {}
        self.worker_assignments = {w['id']: [] for w in self.workers_data}

    def generate_schedule(self):
        """Main method to generate the schedule following all conditions"""
        print("Starting schedule generation...")
        
        # 1. First assign mandatory coverage days
        self.assign_mandatory_coverage()
        
        # 2. Calculate target shifts per worker based on their percentage
        total_days = (self.end_date - self.start_date).days + 1
        total_shifts = total_days * self.num_shifts
        self.calculate_target_shifts(total_shifts)
        
        # 3. Fill remaining shifts
        current_date = self.start_date
        while current_date <= self.end_date:
            self.fill_day_shifts(current_date)
            current_date += timedelta(days=1)

        return self.schedule

    def assign_mandatory_coverage(self):
        """Assign mandatory coverage days first"""
        print("Assigning mandatory coverage days...")
        
        for worker in self.workers_data:
            if worker.get('mandatory_days'):
                mandatory_days = self.parse_dates(worker['mandatory_days'])
                for date in mandatory_days:
                    if self.start_date <= date <= self.end_date:
                        if date not in self.schedule:
                            self.schedule[date] = []
                        if len(self.schedule[date]) < self.num_shifts:
                            if self.can_assign_worker(worker['id'], date):
                                self.schedule[date].append(worker['id'])
                                self.worker_assignments[worker['id']].append(date)
                                print(f"Assigned mandatory shift: {worker['id']} on {date.strftime('%Y-%m-%d')}")

    def calculate_target_shifts(self, total_shifts):
        """Calculate target number of shifts for each worker based on their percentage"""
        total_percentage = sum(float(w.get('work_percentage', 100)) for w in self.workers_data)
        
        for worker in self.workers_data:
            percentage = float(worker.get('work_percentage', 100))
            worker['target_shifts'] = int((percentage / total_percentage) * total_shifts)
            print(f"Worker {worker['id']} target shifts: {worker['target_shifts']}")

    def fill_day_shifts(self, date):
        """Fill shifts for a specific day"""
        if date not in self.schedule:
            self.schedule[date] = []
            
        while len(self.schedule[date]) < self.num_shifts:
            best_worker = self.find_best_worker_for_date(date)
            if best_worker:
                self.schedule[date].append(best_worker['id'])
                self.worker_assignments[best_worker['id']].append(date)
                print(f"Assigned shift: {best_worker['id']} on {date.strftime('%Y-%m-%d')}")
            else:
                print(f"Warning: Could not find worker for {date.strftime('%Y-%m-%d')}")
                break

    def find_best_worker_for_date(self, date):
        """Find the best worker for a specific date based on all conditions"""
        candidates = []
        
        for worker in self.workers_data:
            if self.can_assign_worker(worker['id'], date):
                score = self.calculate_worker_score(worker, date)
                candidates.append((worker, score))
        
        if not candidates:
            return None
            
        return max(candidates, key=lambda x: x[1])[0]

    def can_assign_worker(self, worker_id, date):
        """Check if a worker can be assigned to a specific date"""
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        
        # Check if worker is already assigned that day
        if date in self.worker_assignments[worker_id]:
            return False
            
        # Check work periods
        if worker.get('work_periods'):
            work_periods = self.parse_date_ranges(worker['work_periods'])
            if not any(start <= date <= end for start, end in work_periods):
                return False
                
        # Check days off
        if worker.get('days_off'):
            off_days = self.parse_dates(worker['days_off'])
            if date in off_days:
                return False
                
        # Check minimum distance between guards
        min_distance = int(4 / (float(worker.get('work_percentage', 100)) / 100))
        assignments = sorted(self.worker_assignments[worker_id])
        
        if assignments:
            # Check previous assignments
            for prev_date in reversed(assignments):
                days_between = (date - prev_date).days
                if days_between < min_distance:
                    return False
                if days_between in [7, 14, 21]:
                    return False
                    
            # Check future assignments (already scheduled)
            for next_date in assignments:
                if next_date > date:
                    days_between = (next_date - date).days
                    if days_between < min_distance:
                        return False
                    if days_between in [7, 14, 21]:
                        return False
        
        return True

    def calculate_worker_score(self, worker, date):
        """Calculate a score for a worker based on various factors"""
        score = 0
        
        # Factor 1: Distance from target shifts
        current_shifts = len(self.worker_assignments[worker['id']])
        shift_difference = worker['target_shifts'] - current_shifts
        score += shift_difference * 10
        
        # Factor 2: Monthly distribution
        month_shifts = sum(1 for d in self.worker_assignments[worker['id']] 
                         if d.year == date.year and d.month == date.month)
        score -= month_shifts * 5
        
        # Factor 3: Distance from last assignment
        if self.worker_assignments[worker['id']]:
            last_assignment = max(self.worker_assignments[worker['id']])
            days_since_last = (date - last_assignment).days
            score += min(days_since_last, 30)
        
        return score

    def parse_dates(self, date_str):
        """Parse semicolon-separated dates"""
        if not date_str:
            return []
            
        dates = []
        for date_text in date_str.split(';'):
            date_text = date_text.strip()
            if date_text:
                dates.append(datetime.strptime(date_text, '%d-%m-%Y'))
        return dates

    def parse_date_ranges(self, date_ranges_str):
        """Parse semicolon-separated date ranges"""
        if not date_ranges_str:
            return []
            
        ranges = []
        for date_range in date_ranges_str.split(';'):
            date_range = date_range.strip()
            if ' - ' in date_range:
                start_str, end_str = date_range.split(' - ')
                start = datetime.strptime(start_str.strip(), '%d-%m-%Y')
                end = datetime.strptime(end_str.strip(), '%d-%m-%Y')
                ranges.append((start, end))
            else:
                date = datetime.strptime(date_range, '%d-%m-%Y')
                ranges.append((date, date))
        return ranges

class CalendarView(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.layout = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # Header with current date/time and user info
        header_info = BoxLayout(orientation='vertical', size_hint_y=0.1)
        header_info.add_widget(Label(
            text=f'Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): 2025-01-17 23:30:42',
            size_hint_y=0.5
        ))
        header_info.add_widget(Label(
            text="Current User's Login: saldo27",
            size_hint_y=0.5
        ))
        self.layout.add_widget(header_info)
        
        # Calendar navigation
        nav_bar = BoxLayout(orientation='horizontal', size_hint_y=0.1)
        self.prev_month_btn = Button(
            text='Previous Month',
            size_hint_x=0.2,
            on_press=self.previous_month
        )
        self.next_month_btn = Button(
            text='Next Month',
            size_hint_x=0.2,
            on_press=self.next_month
        )
        self.month_label = Label(
            text='',
            size_hint_x=0.6
        )
        
        nav_bar.add_widget(self.prev_month_btn)
        nav_bar.add_widget(self.month_label)
        nav_bar.add_widget(self.next_month_btn)
        self.layout.add_widget(nav_bar)
        
        # Calendar grid
        self.calendar_grid = GridLayout(
            cols=7,
            spacing=2,
            size_hint_y=0.7
        )
        self.layout.add_widget(self.calendar_grid)
        
        # Export buttons
        button_bar = BoxLayout(
            orientation='horizontal',
            size_hint_y=0.1,
            spacing=10
        )
        
        export_pdf_btn = Button(
            text='Export to PDF',
            on_press=self.export_pdf
        )
        export_csv_btn = Button(
            text='Export to CSV',
            on_press=self.export_csv
        )
        back_btn = Button(
            text='Back',
            on_press=self.go_back
        )
        
        button_bar.add_widget(export_pdf_btn)
        button_bar.add_widget(export_csv_btn)
        button_bar.add_widget(back_btn)
        
        self.layout.add_widget(button_bar)
        self.add_widget(self.layout)
        
        # Initialize calendar
        self.current_date = datetime.now()
        self.update_calendar()

    def update_calendar(self, year=None, month=None):
        if year is None:
            year = self.current_date.year
        if month is None:
            month = self.current_date.month
            
        self.calendar_grid.clear_widgets()
        
        # Update month label
        self.month_label.text = f'{calendar.month_name[month]} {year}'
        
        # Add weekday headers
        for day in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
            self.calendar_grid.add_widget(
                Label(
                    text=day,
                    bold=True
                )
            )
        
        # Get calendar for month
        cal = calendar.monthcalendar(year, month)
        
        # Add days to grid
        for week in cal:
            for day in week:
                if day == 0:
                    self.calendar_grid.add_widget(Label(text=''))
                else:
                    day_layout = BoxLayout(orientation='vertical')
                    day_layout.add_widget(Label(text=str(day)))
                    
                    # Add schedule if exists
                    date = datetime(year, month, day)
                    app = App.get_running_app()
                    if hasattr(app, 'schedule_config') and 'schedule' in app.schedule_config:
                        schedule = app.schedule_config['schedule'].get(date, [])
                        if schedule:
                            schedule_text = '\n'.join(schedule)
                            day_layout.add_widget(Label(
                                text=schedule_text,
                                font_size='10sp'
                            ))
                    
                    self.calendar_grid.add_widget(day_layout)

    def previous_month(self, instance):
        if self.current_date.month == 1:
            self.current_date = self.current_date.replace(year=self.current_date.year - 1, month=12)
        else:
            self.current_date = self.current_date.replace(month=self.current_date.month - 1)
        self.update_calendar()

    def next_month(self, instance):
        if self.current_date.month == 12:
            self.current_date = self.current_date.replace(year=self.current_date.year + 1, month=1)
        else:
            self.current_date = self.current_date.replace(month=self.current_date.month + 1)
        self.update_calendar()

    def export_pdf(self, instance):
        try:
            app = App.get_running_app()
            if not hasattr(app, 'schedule_config') or 'schedule' not in app.schedule_config:
                raise ValueError("No schedule data available")
            
            filename = f'schedule_{self.current_date.strftime("%Y-%m")}.pdf'
            
            pdf = FPDF()
            pdf.add_page(orientation='L')
            pdf.set_font('Arial', 'B', 16)
            
            pdf.cell(0, 10, f'Schedule - {calendar.month_name[self.current_date.month]} {self.current_date.year}', 0, 1, 'C')
            
            pdf.set_font('Arial', 'B', 12)
            cell_width = 40
            for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
                pdf.cell(cell_width, 10, day, 1)
            pdf.ln()
            
            pdf.set_font('Arial', '', 10)
            cal = calendar.monthcalendar(self.current_date.year, self.current_date.month)
            
            for week in cal:
                max_height = 30
                for day in week:
                    if day == 0:
                        pdf.cell(cell_width, max_height, '', 1)
                    else:
                        date = datetime(self.current_date.year, self.current_date.month, day)
                        content = f"{day}\n"
                        if date in app.schedule_config['schedule']:
                            content += "\n".join(app.schedule_config['schedule'][date])
                        pdf.multi_cell(cell_width, 5, content, 1)
                        pdf.set_xy(pdf.get_x() + cell_width, pdf.get_y() - max_height)
                pdf.ln(max_height)
            
            pdf.output(filename)
            success_popup = SuccessPopup(f"Schedule exported to {filename}")
            success_popup.open()
            
        except Exception as e:
            error_popup = ErrorPopup(str(e))
            error_popup.open()

    def export_csv(self, instance):
        try:
            app = App.get_running_app()
            if not hasattr(app, 'schedule_config') or 'schedule' not in app.schedule_config:
                raise ValueError("No schedule data available")
            
            filename = f'schedule_{self.current_date.strftime("%Y-%m")}.csv'
            
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Date', 'Guards'])
                
                for date in sorted(app.schedule_config['schedule'].keys()):
                    writer.writerow([
                        date.strftime('%Y-%m-%d'),
                        ';'.join(app.schedule_config['schedule'][date])
                    ])
            
            success_popup = SuccessPopup(f"Schedule exported to {filename}")
            success_popup.open()
            
        except Exception as e:
            error_popup = ErrorPopup(str(e))
            error_popup.open()

    def go_back(self, instance):
        self.manager.current = 'worker_details'  # Fixed the syntax error here

class ErrorPopup(Popup):
    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.title = 'Error'
        self.size_hint = (0.8, 0.4)
        self.content = Label(text=message)
        self.auto_dismiss = True

class SuccessPopup(Popup):
    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.title = 'Success'
        self.size_hint = (0.8, 0.4)
        self.content = Label(text=message)
        self.auto_dismiss = True

class ShiftManagerApp(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.schedule_config = {}
        self.current_user = 'saldo27'
        self.current_datetime = datetime(2025, 1, 17, 23, 24, 47)  # Updated datetime

    def build(self):
        sm = ScreenManager()
        sm.add_widget(InitialSetupScreen(name='initial_setup'))
        sm.add_widget(WorkerDetailsScreen(name='worker_details'))
        sm.add_widget(CalendarView(name='calendar_view'))  # Add CalendarView
        return sm

class MainScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=20, spacing=10)

        # Header with current date and user
        header = GridLayout(cols=1, size_hint_y=0.15)
        current_datetime = datetime(2025, 1, 17, 22, 50, 9)
        
        date_label = Label(
            text=f'Current Date and Time (UTC): {current_datetime.strftime("%Y-%m-%d %H:%M:%S")}',
            font_size='16sp'
        )
        user_label = Label(
            text=f"Current User's Login: saldo27",
            font_size='16sp'
        )
        
        header.add_widget(date_label)
        header.add_widget(user_label)
        layout.add_widget(header)

        # Main menu buttons
        menu_layout = GridLayout(cols=2, spacing=10, size_hint_y=0.85)
        
        # Create Schedule Button
        create_schedule_btn = Button(
            text='Create New Schedule',
            on_press=self.go_to_setup
        )
        menu_layout.add_widget(create_schedule_btn)
        
        # View Schedule Button
        view_schedule_btn = Button(
            text='View Schedule',
            on_press=self.go_to_calendar
        )
        menu_layout.add_widget(view_schedule_btn)
        
        # Import CSV Button
        import_btn = Button(
            text='Import from CSV',
            on_press=self.show_import_dialog
        )
        menu_layout.add_widget(import_btn)
        
        # Export Menu Button
        export_btn = Button(
            text='Export Options',
            on_press=self.show_export_options
        )
        menu_layout.add_widget(export_btn)

        layout.add_widget(menu_layout)
        self.add_widget(layout)

    def go_to_setup(self, instance):
        self.manager.current = 'setup'

    def go_to_calendar(self, instance):
        self.manager.current = 'calendar'

    def show_import_dialog(self, instance):
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        self.file_chooser = FileChooserListView(
            path=os.path.expanduser("~"),
            filters=['*.csv']
        )
        content.add_widget(self.file_chooser)
        
        buttons = BoxLayout(size_hint_y=None, height=40, spacing=5)
        
        cancel_btn = Button(text='Cancel')
        import_btn = Button(text='Import')
        
        buttons.add_widget(cancel_btn)
        buttons.add_widget(import_btn)
        content.add_widget(buttons)
        
        popup = Popup(
            title='Select CSV File',
            content=content,
            size_hint=(0.9, 0.9)
        )
        
        cancel_btn.bind(on_press=popup.dismiss)
        import_btn.bind(on_press=lambda x: self.import_csv(self.file_chooser.selection[0], popup))
        
        popup.open()

    def show_export_options(self, instance):
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        pdf_btn = Button(
            text='Export Monthly Calendar (PDF)',
            on_press=lambda x: self.export_pdf()
        )
        csv_btn = Button(
            text='Export Schedule (CSV)',
            on_press=lambda x: self.export_csv()
        )
        
        content.add_widget(pdf_btn)
        content.add_widget(csv_btn)
        
        popup = Popup(
            title='Export Options',
            content=content,
            size_hint=(0.8, 0.4)
        )
        popup.open()

class CSVHandler:
    @staticmethod
    def export_schedule(schedule, filename):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Date', 'Guards'])
            
            for date in sorted(schedule.keys()):
                writer.writerow([
                    date.strftime('%Y-%m-%d'),
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
    
def generate_schedule(self):
    from scheduler import GuardScheduler
    scheduler = GuardScheduler(self.schedule_config)
    schedule = scheduler.generate_schedule()
    return schedule


if __name__ == '__main__':
    ShiftManagerApp().run()
