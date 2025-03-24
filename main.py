from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.scrollview import ScrollView
from kivy.uix.label import Label
from kivy.uix.textinput import TextInput
from kivy.uix.button import Button
from kivy.uix.checkbox import CheckBox
from kivy.uix.popup import Popup
from kivy.graphics import Color, Line, Rectangle
from datetime import datetime, timedelta
from scheduler import Scheduler
from exporters import StatsExporter
from pdf_exporter import PDFExporter

import json
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class WelcomeScreen(Screen):
    def __init__(self, **kwargs):
        super(WelcomeScreen, self).__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=20, spacing=10)
        
        layout.add_widget(Label(text='Bienvenido'))
        
        start_btn = Button(text='Comienza el reparto', size_hint_y=None, height=50)
        start_btn.bind(on_press=self.switch_to_setup)
        layout.add_widget(start_btn)
        
        self.add_widget(layout)

    def switch_to_setup(self, instance):
        self.manager.current = 'setup'

class SetupScreen(Screen):
    def __init__(self, **kwargs):
        super(SetupScreen, self).__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=20, spacing=10)
    
        layout.add_widget(Label(text='Setup'))
    
       # Start Date
        layout.add_widget(Label(text='Fecha de inicio (DD-MM-YYYY):'))
        self.start_date = TextInput(multiline=False)
        layout.add_widget(self.start_date)
    
        # End Date
        layout.add_widget(Label(text='Fecha final (DD-MM-YYYY):'))
        self.end_date = TextInput(multiline=False)
        layout.add_widget(self.end_date)
    
        # Holidays
        layout.add_widget(Label(text='Festivos (DD-MM-YYYY, semicolon-separated):'))
        self.holidays = TextInput(
            multiline=True,
           hint_text='Example: 25-12-2025; 01-01-2026'
        )
        layout.add_widget(self.holidays)
    
        # Number of Workers
        layout.add_widget(Label(text='Número de médicos:'))
        self.num_workers = TextInput(multiline=False, input_filter='int')
        layout.add_widget(self.num_workers)
    
        # Number of Shifts per Day
        layout.add_widget(Label(text='Número de guardias/día:'))
        self.num_shifts = TextInput(multiline=False, input_filter='int')
        layout.add_widget(self.num_shifts)
    
        # Continue Button
        continue_btn = Button(text='Continuar', size_hint_y=None, height=50)
        continue_btn.bind(on_press=self.validate_and_continue)
        layout.add_widget(continue_btn)
    
        self.add_widget(layout)

    def parse_holidays(self, holidays_str):
        """Parse and validate holiday dates"""
        if not holidays_str.strip():
            return []
    
        holidays = []
        try:
            for date_str in holidays_str.split(';'):
                date_str = date_str.strip()
                if date_str:  # Only process non-empty strings
                    try:
                        holiday_date = datetime.strptime(date_str.strip(), '%d-%m-%Y')
                        holidays.append(holiday_date)
                    except ValueError as e:
                        raise ValueError(f"Invalid date format for '{date_str}'. Use DD-MM-YYYY")
            return sorted(holidays)
        except Exception as e:
            raise ValueError(f"Error parsing holidays: {str(e)}")
        
    def validate_and_continue(self, instance):
        try:
            # Validate dates
            start = datetime.strptime(self.start_date.text, '%d-%m-%Y')
            end = datetime.strptime(self.end_date.text, '%d-%m-%Y')
        
            if end <= start:
                raise ValueError("End date must be after start date")
        
            # Validate and parse holidays
            holidays = self.parse_holidays(self.holidays.text)
        
            # Validate holidays are within range
            for holiday in holidays:
                if holiday < start or holiday > end:
                    raise ValueError(f"Holiday {holiday.strftime('%d-%m-%Y')} is outside the schedule period")
        
            # Validate numbers
            num_workers = int(self.num_workers.text)
            num_shifts = int(self.num_shifts.text)
        
            if num_workers < 1:
                raise ValueError("Number of workers must be positive")
            if num_shifts < 1:
                raise ValueError("Number of shifts must be positive")
        
            # Store configuration
            app = App.get_running_app()
            app.schedule_config = {
                'start_date': start,
                'end_date': end,
                'holidays': holidays,  # Add holidays to config
                'num_workers': num_workers,
                'num_shifts': num_shifts,
                'current_worker_index': 0
            }
        
            # Switch to worker details screen
            self.manager.current = 'worker_details'
        
        except ValueError as e:
            popup = Popup(title='Error',
                         content=Label(text=str(e)),
                         size_hint=(None, None), size=(400, 200))
            popup.open()

class WorkerDetailsScreen(Screen):
    def __init__(self, **kwargs):
        super(WorkerDetailsScreen, self).__init__(**kwargs)
        self.layout = BoxLayout(orientation='vertical', padding=20, spacing=10)

        # Title
        self.title_label = Label(text='Worker Details', size_hint_y=0.1)
        self.layout.add_widget(self.title_label)

        # Form layout
        scroll = ScrollView(size_hint=(1, 0.7))  # Reduced size to make room for buttons
        self.form_layout = GridLayout(cols=2, spacing=10, size_hint_y=None, padding=10)
        self.form_layout.bind(minimum_height=self.form_layout.setter('height'))

        # Worker ID
        self.form_layout.add_widget(Label(text='Worker ID:'))
        self.worker_id = TextInput(multiline=False, size_hint_y=None, height=40)
        self.form_layout.add_widget(self.worker_id)

        # Work Periods
        self.form_layout.add_widget(Label(text='Work Periods (DD-MM-YYYY):'))
        self.work_periods = TextInput(multiline=True, size_hint_y=None, height=60)
        self.form_layout.add_widget(self.work_periods)

        # Work Percentage
        self.form_layout.add_widget(Label(text='Work Percentage:'))
        self.work_percentage = TextInput(
            multiline=False,
            text='100',
            input_filter='float',
            size_hint_y=None,
            height=40
        )
        self.form_layout.add_widget(self.work_percentage)

        # Mandatory Days
        self.form_layout.add_widget(Label(text='Mandatory Days:'))
        self.mandatory_days = TextInput(multiline=True, size_hint_y=None, height=60)
        self.form_layout.add_widget(self.mandatory_days)

        # Days Off
        self.form_layout.add_widget(Label(text='Days Off:'))
        self.days_off = TextInput(multiline=True, size_hint_y=None, height=60)
        self.form_layout.add_widget(self.days_off)

        # Incompatibility Checkbox - Updated Layout
        checkbox_label = Label(
            text='Incompatible Worker:',
            size_hint_y=None,
            height=40
        )
        self.form_layout.add_widget(checkbox_label)

        checkbox_container = BoxLayout(
            orientation='horizontal',
            size_hint_y=None,
            height=40
        )
        
        self.incompatible_checkbox = CheckBox(
            size_hint_x=None,
            width=40,
            active=False
        )
        
        checkbox_text = Label(
            text='Cannot work with other incompatible workers',
            size_hint_x=1,
            halign='left'
        )
        
        checkbox_container.add_widget(self.incompatible_checkbox)
        checkbox_container.add_widget(checkbox_text)
        self.form_layout.add_widget(checkbox_container)

        scroll.add_widget(self.form_layout)
        self.layout.add_widget(scroll)

        # Navigation buttons layout
        navigation_layout = BoxLayout(orientation='horizontal', size_hint_y=0.1, spacing=10)
        
        # Previous Button
        self.prev_btn = Button(text='Previous', size_hint_x=0.33)
        self.prev_btn.bind(on_press=self.go_to_previous_worker)
        navigation_layout.add_widget(self.prev_btn)
        
        # Save Button
        self.save_btn = Button(text='Save', size_hint_x=0.33)
        self.save_btn.bind(on_press=self.save_worker_data)
        navigation_layout.add_widget(self.save_btn)
        
        # Next/Finish Button
        self.next_btn = Button(text='Next', size_hint_x=0.33)
        self.next_btn.bind(on_press=self.go_to_next_worker)
        navigation_layout.add_widget(self.next_btn)
        
        self.layout.add_widget(navigation_layout)

        self.add_widget(self.layout)

    def validate_dates(self, date_str, allow_ranges=True):
        """
        Validate date strings
        date_str: The string containing the dates
        allow_ranges: Whether to allow date ranges with hyphen (True for work periods/days off, False for mandatory days)
        """
        if not date_str:
            return True
        try:
            for period in date_str.split(';'):
                period = period.strip()
                if ' - ' in period and allow_ranges:  # Note the spaces around the hyphen
                    start_str, end_str = period.split(' - ')  # Split on ' - ' with spaces
                    datetime.strptime(start_str.strip(), '%d-%m-%Y')
                    datetime.strptime(end_str.strip(), '%d-%m-%Y')
                else:
                    if not allow_ranges and period.count('-') > 2:
                        # For mandatory days, only allow DD-MM-YYYY format
                        return False
                    datetime.strptime(period.strip(), '%d-%m-%Y')
            return True
        except ValueError:
            return False
    
    def validate_worker_data(self):
        """Validate all worker data fields"""
        if not self.worker_id.text.strip():
            self.show_error("Worker ID is required")
            return False

        try:
            work_percentage = float(self.work_percentage.text or '100')
            if not (0 < work_percentage <= 100):
                self.show_error("Work percentage must be between 0 and 100")
                return False
        except ValueError:
            self.show_error("Invalid work percentage")
            return False

        # Validate work periods (allowing ranges)
        if not self.validate_dates(self.work_periods.text, allow_ranges=True):
            self.show_error("Invalid work periods format.\nFormat: DD-MM-YYYY or DD-MM-YYYY - DD-MM-YYYY\nSeparate multiple entries with semicolons")
            return False

        # Validate mandatory days (not allowing ranges)
        if not self.validate_dates(self.mandatory_days.text, allow_ranges=False):
            self.show_error("Invalid mandatory days format.\nFormat: DD-MM-YYYY\nSeparate multiple days with semicolons")
            return False

        # Validate days off (allowing ranges)
        if not self.validate_dates(self.days_off.text, allow_ranges=True):
            self.show_error("Invalid days off format.\nFormat: DD-MM-YYYY or DD-MM-YYYY - DD-MM-YYYY\nSeparate multiple entries with semicolons")
            return False
            
        return True
        
    def save_worker_data(self, instance):
        """Save current worker data without advancing to the next worker"""
        if not self.validate_worker_data():
            return
            
        app = App.get_running_app()
        worker_data = {
            'id': self.worker_id.text.strip(),
            'work_periods': self.work_periods.text.strip(),
            'work_percentage': float(self.work_percentage.text or '100'),
            'mandatory_days': self.mandatory_days.text.strip(),
            'days_off': self.days_off.text.strip(),
            'is_incompatible': self.incompatible_checkbox.active
        }

        # Get current index
        current_index = app.schedule_config.get('current_worker_index', 0)
        
        # Initialize workers_data if needed
        if 'workers_data' not in app.schedule_config:
            app.schedule_config['workers_data'] = []
            
        # Update or append worker data
        if current_index < len(app.schedule_config['workers_data']):
            # Update existing worker
            app.schedule_config['workers_data'][current_index] = worker_data
        else:
            # Add new worker
            app.schedule_config['workers_data'].append(worker_data)
            
        # Show confirmation
        popup = Popup(
            title='Success',
            content=Label(text='Worker data saved!'),
            size_hint=(None, None), 
            size=(300, 150)
        )
        popup.open()
            
    def go_to_previous_worker(self, instance):
        """Navigate to the previous worker"""
        app = App.get_running_app()
        current_index = app.schedule_config.get('current_worker_index', 0)
        
        # Save current worker data
        if self.validate_worker_data():
            self.save_worker_data(None)
            
            if current_index > 0:
                # Move to previous worker
                app.schedule_config['current_worker_index'] = current_index - 1
                self.load_worker_data()
        
    def go_to_next_worker(self, instance):
        """Navigate to the next worker or finalize if last worker"""
        if not self.validate_worker_data():
            return
        
        app = App.get_running_app()
        current_index = app.schedule_config.get('current_worker_index', 0)
        total_workers = app.schedule_config.get('num_workers', 0)
    
        # Save current worker data
        worker_data = {
            'id': self.worker_id.text.strip(),
            'work_periods': self.work_periods.text.strip(),
            'work_percentage': float(self.work_percentage.text or '100'),
            'mandatory_days': self.mandatory_days.text.strip(),
            'days_off': self.days_off.text.strip(),
            'is_incompatible': self.incompatible_checkbox.active
        }

        # Initialize workers_data if needed
        if 'workers_data' not in app.schedule_config:
            app.schedule_config['workers_data'] = []
        
        # Update or append worker data
        if current_index < len(app.schedule_config['workers_data']):
            # Update existing worker
            app.schedule_config['workers_data'][current_index] = worker_data
        else:
            # Add new worker
            app.schedule_config['workers_data'].append(worker_data)
    
        if current_index < total_workers - 1:
            # Move to next worker
            app.schedule_config['current_worker_index'] = current_index + 1
        
            # Clear inputs and load next worker's data (if exists)
            self.clear_inputs()
            if current_index + 1 < len(app.schedule_config.get('workers_data', [])):
                next_worker = app.schedule_config['workers_data'][current_index + 1]
                self.worker_id.text = next_worker.get('id', '')
                self.work_periods.text = next_worker.get('work_periods', '')
                self.work_percentage.text = str(next_worker.get('work_percentage', 100))
                self.mandatory_days.text = next_worker.get('mandatory_days', '')
                self.days_off.text = next_worker.get('days_off', '')
                self.incompatible_checkbox.active = next_worker.get('is_incompatible', False)
        
            # Update title and buttons
            self.title_label.text = f'Worker Details ({current_index + 2}/{total_workers})'
            self.prev_btn.disabled = False
        
            if current_index + 1 == total_workers - 1:
                self.next_btn.text = 'Finish'
        else:
            # We're at the last worker, generate schedule
            self.generate_schedule()
    
    def load_worker_data(self):
        """Load worker data for current index"""
        app = App.get_running_app()
        current_index = app.schedule_config.get('current_worker_index', 0)
        workers_data = app.schedule_config.get('workers_data', [])
        
        # Clear inputs first
        self.clear_inputs()
        
        # If we have data for this worker, load it
        if 0 <= current_index < len(workers_data):
            worker = workers_data[current_index]
            self.worker_id.text = worker.get('id', '')
            self.work_periods.text = worker.get('work_periods', '')
            self.work_percentage.text = str(worker.get('work_percentage', 100))
            self.mandatory_days.text = worker.get('mandatory_days', '')
            self.days_off.text = worker.get('days_off', '')
            self.incompatible_checkbox.active = worker.get('is_incompatible', False)
            
        # Update title
        self.title_label.text = f'Worker Details ({current_index + 1}/{app.schedule_config.get("num_workers", 0)})'
        
        # Update button text based on position
        if current_index == app.schedule_config.get('num_workers', 0) - 1:
            self.next_btn.text = 'Finish'
        else:
            self.next_btn.text = 'Next'
            
        # Disable Previous button if on first worker
        self.prev_btn.disabled = (current_index == 0)

    def show_error(self, message):
        popup = Popup(title='Error',
                     content=Label(text=message),
                     size_hint=(None, None), size=(400, 200))
        popup.open()

    def generate_schedule(self):
        app = App.get_running_app()
        try:
            scheduler = Scheduler(app.schedule_config)
            success = scheduler.generate_schedule()  # This returns True/False
    
            if not success:  # Schedule generation failed
                raise ValueError("Failed to generate schedule - validation errors detected")
        
            # Get the actual schedule from the scheduler object
            schedule = scheduler.schedule  
        
            if not schedule:  # Schedule is empty
                raise ValueError("Generated schedule is empty")
        
            app.schedule_config['schedule'] = schedule
    
            popup = Popup(title='Success',
                         content=Label(text='Schedule generated successfully!'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()
            self.manager.current = 'calendar_view'
    
        except Exception as e:
            error_message = f"Failed to generate schedule: {str(e)}"
            logging.error(error_message)
            self.show_error(error_message)
        
    def clear_inputs(self):
        self.worker_id.text = ''
        self.work_periods.text = ''
        self.work_percentage.text = '100'
        self.mandatory_days.text = ''
        self.days_off.text = ''
        self.incompatible_checkbox.active = False

    def on_enter(self):
        """Initialize the screen when it's entered"""
        app = App.get_running_app()
    
        # Make sure current_worker_index is initialized
        if 'current_worker_index' not in app.schedule_config:
            app.schedule_config['current_worker_index'] = 0
    
        # Initialize workers_data array if needed
        if 'workers_data' not in app.schedule_config:
            app.schedule_config['workers_data'] = []
    
        current_index = app.schedule_config.get('current_worker_index', 0)
        total_workers = app.schedule_config.get('num_workers', 0)
    
        # Update the title with current position
        self.title_label.text = f'Worker Details ({current_index + 1}/{total_workers})'
    
        # Set button states based on position
        self.prev_btn.disabled = (current_index == 0)
    
        if current_index == total_workers - 1:
            self.next_btn.text = 'Finish'
        else:
            self.next_btn.text = 'Next'
    
        # Load data for current worker index
        self.load_worker_data()
    
        # Log entry for debugging
        logging.info(f"Entered WorkerDetailsScreen: Worker {current_index + 1}/{total_workers}")

class CalendarViewScreen(Screen):
    def __init__(self, **kwargs):
        super(CalendarViewScreen, self).__init__(**kwargs)
        self.layout = BoxLayout(orientation='vertical', padding=10, spacing=5)
    
        # Header with title and navigation
        header = BoxLayout(orientation='horizontal', size_hint_y=0.1)
        self.month_label = Label(text='', size_hint_x=0.4)
        prev_month = Button(text='<', size_hint_x=0.2)
        next_month = Button(text='>', size_hint_x=0.2)
        summary_btn = Button(text='Summary', size_hint_x=0.2)
    
        prev_month.bind(on_press=self.previous_month)
        next_month.bind(on_press=self.next_month)
        summary_btn.bind(on_press=self.show_month_summary)
    
        header.add_widget(prev_month)
        header.add_widget(self.month_label)
        header.add_widget(next_month)
        header.add_widget(summary_btn)
        self.layout.add_widget(header)
    
        # Days of week header
        days_header = GridLayout(cols=7, size_hint_y=0.1)
        for day in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
            days_header.add_widget(Label(text=day))
        self.layout.add_widget(days_header)
    
        # Calendar grid
        self.calendar_grid = GridLayout(cols=7, size_hint_y=0.7)
        self.layout.add_widget(self.calendar_grid)
    
        # Scroll view for details
        details_scroll = ScrollView(size_hint_y=0.3)
        self.details_layout = GridLayout(cols=1, size_hint_y=None)
        self.details_layout.bind(minimum_height=self.details_layout.setter('height'))
        details_scroll.add_widget(self.details_layout)
        self.layout.add_widget(details_scroll)
    
        # Export buttons
        button_layout = BoxLayout(orientation='horizontal', size_hint_y=0.1, spacing=10)
        save_btn = Button(text='Save to JSON')
        export_txt_btn = Button(text='Export to TXT')
        export_pdf_btn = Button(text='Export to PDF')  # New button
        stats_btn = Button(text='Worker Stats')

        save_btn.bind(on_press=self.save_schedule)
        export_txt_btn.bind(on_press=self.export_schedule)
        export_pdf_btn.bind(on_press=self.export_to_pdf)  # New binding
        stats_btn.bind(on_press=self.show_worker_stats)

        button_layout.add_widget(save_btn)
        button_layout.add_widget(export_txt_btn)
        button_layout.add_widget(export_pdf_btn)  # Add new button
        button_layout.add_widget(stats_btn)
        self.layout.add_widget(button_layout)
    
        # Year navigation
        year_nav = BoxLayout(orientation='horizontal', size_hint_y=0.1)
        prev_year = Button(text='<<', size_hint_x=0.2)
        next_year = Button(text='>>', size_hint_x=0.2)
        self.year_label = Label(text='', size_hint_x=0.6)
    
        prev_year.bind(on_press=self.previous_year)
        next_year.bind(on_press=self.next_year)
    
        year_nav.add_widget(prev_year)
        year_nav.add_widget(self.year_label)
        year_nav.add_widget(next_year)
        self.layout.add_widget(year_nav)
    
        # Add Today button
        today_btn = Button(text='Today', size_hint_x=0.2)
        today_btn.bind(on_press=self.go_to_today)
        header.add_widget(today_btn)
    
        self.add_widget(self.layout)
        self.current_date = None
        self.schedule = {}
    
    def show_worker_stats(self, instance):
        if not self.schedule:
            return
        
        stats = {}
        for date, workers in self.schedule.items():
            for worker in workers:
                if worker not in stats:
                    stats[worker] = {
                        'total_shifts': 0,
                        'weekends': 0,
                        'holidays': 0
                    }
                stats[worker]['total_shifts'] += 1
            
                if date.weekday() >= 5:
                    stats[worker]['weekends'] += 1
                
                app = App.get_running_app()
                if date in app.schedule_config.get('holidays', []):
                    stats[worker]['holidays'] += 1
    
        # Create stats popup
        content = BoxLayout(orientation='vertical', padding=10)
        content.add_widget(Label(
            text='Worker Statistics',
            size_hint_y=None,
            height=40,
            bold=True
        ))
    
        for worker, data in sorted(stats.items()):
            worker_stats = (
                f"Worker {worker}:\n"
                f"  Total Shifts: {data['total_shifts']}\n"
                f"  Weekend Shifts: {data['weekends']}\n"
                f"  Holiday Shifts: {data['holidays']}\n"
            )
            content.add_widget(Label(text=worker_stats))
    
        popup = Popup(
            title='Worker Statistics',
            content=content,
            size_hint=(None, None),
            size=(400, 600)
        )
        popup.open()
        
    def previous_year(self, instance):
        if self.current_date:
            self.current_date = self.current_date.replace(year=self.current_date.year - 1)
            self.display_month(self.current_date)

    def next_year(self, instance):
        if self.current_date:
            self.current_date = self.current_date.replace(year=self.current_date.year + 1)
            self.display_month(self.current_date)
    
    def go_to_today(self, instance):
        today = datetime.now()
        if self.current_date:
            self.current_date = self.current_date.replace(year=today.year, month=today.month)
            self.display_month(self.current_date)
            
    def get_day_color(self, current_date):
        app = App.get_running_app()
        is_weekend = current_date.weekday() >= 5
        is_holiday = current_date in app.schedule_config.get('holidays', [])
        is_today = current_date.date() == datetime.now().date()
    
        if is_today:
            return (0.2, 0.6, 1, 0.3)  # Light blue for today
        elif is_holiday:
            return (1, 0.8, 0.8, 0.3)  # Light red for holidays
        elif is_weekend:
            return (1, 0.9, 0.9, 0.3)  # Very light red for weekends
        return (1, 1, 1, 1)  # White for regular days
                               
    def on_enter(self):
            app = App.get_running_app()
            self.schedule = app.schedule_config.get('schedule', {})
            if self.schedule:
                # Set current_date to the first date in the schedule
                self.current_date = min(self.schedule.keys())
                self.display_month(self.current_date)

    def display_month(self, date):
        self.calendar_grid.clear_widgets()
        self.details_layout.clear_widgets()
    
        # Update month label
        self.month_label.text = date.strftime('%B %Y')
    
        # Calculate the first day of the month
        first_day = datetime(date.year, date.month, 1)
    
        # Calculate number of days in the month
        if date.month == 12:
            next_month = datetime(date.year + 1, 1, 1)
        else:
            next_month = datetime(date.year, date.month + 1, 1)
        days_in_month = (next_month - first_day).days
    
        # Calculate the weekday of the first day (0 = Monday, 6 = Sunday)
        first_weekday = first_day.weekday()
    
        # Add empty cells for days before the first of the month
        for _ in range(first_weekday):
            empty_cell = Label(text='')
            self.calendar_grid.add_widget(empty_cell)
    
        # Add days of the month
        for day in range(1, days_in_month + 1):
            current = datetime(date.year, date.month, day)
        
            # Create a BoxLayout for the cell with vertical orientation
            cell = BoxLayout(
                orientation='vertical',
                spacing=2,
                padding=[2, 2],  # Add padding inside cells
                size_hint_y=None,
                height=120  # Increase cell height
            )

            # Set background color
            bg_color = self.get_day_color(current)
            with cell.canvas.before:
                Color(*bg_color)
                Rectangle(pos=cell.pos, size=cell.size)
            
                # Add border
                Color(0.7, 0.7, 0.7, 1)  # Gray border
                Line(rectangle=(cell.x, cell.y, cell.width, cell.height))

            # Day number with special formatting for weekends/holidays
            app = App.get_running_app()
            is_weekend = current.weekday() >= 5
            is_holiday = current in app.schedule_config.get('holidays', [])
        
            # Create header box for day number
            header_box = BoxLayout(
                orientation='horizontal',
                size_hint_y=None,
                height=20
            )
        
            day_label = Label(
                text=str(day),
                bold=True,
                color=(1, 0, 0, 1) if is_weekend or is_holiday else (0, 0, 0, 1),
                size_hint_x=0.3
            )
            header_box.add_widget(day_label)
        
            # Add shift count if there are workers
            if current in self.schedule:
                shift_count = len(self.schedule[current])
                total_shifts = App.get_running_app().schedule_config.get('num_shifts', 0)
                count_label = Label(
                    text=f'[{shift_count}/{total_shifts}]',
                    color=(0.4, 0.4, 0.4, 1),
                    size_hint_x=0.7,
                    halign='right'
                )
                header_box.add_widget(count_label)
        
            cell.add_widget(header_box)
        
            # Add worker information
            if current in self.schedule:
                workers = self.schedule[current]
                content_box = BoxLayout(
                    orientation='vertical',
                    padding=[5, 2],
                    spacing=2
                )
            
                for i, worker_id in enumerate(workers):
                    worker_label = Label(
                        text=f'S{i+1}: {worker_id}',
                        color=(0, 0, 0, 1),  # Black text
                        font_size='13sp',     # Adjusted font size
                        size_hint_y=None,
                        height=20,
                        halign='left',
                        valign='middle'
                    )
                    worker_label.bind(size=worker_label.setter('text_size'))
                    content_box.add_widget(worker_label)
            
                cell.add_widget(content_box)
            
                # Make the cell clickable
                btn = Button(
                    background_color=(0.95, 0.95, 0.95, 0.3),
                    background_normal=''
                )
                btn.bind(on_press=lambda x, d=current: self.show_details(d))
                cell.bind(size=btn.setter('size'), pos=btn.setter('pos'))
            
                # Add the button at the beginning of the cell's widgets
                cell.add_widget(btn)
            
            self.calendar_grid.add_widget(cell)
    
        # Fill remaining cells
        remaining_cells = 42 - (first_weekday + days_in_month)  # 42 = 6 rows * 7 days
        for _ in range(remaining_cells):
            empty_cell = Label(text='')
            self.calendar_grid.add_widget(empty_cell)

        # Update the calendar grid's properties
        self.calendar_grid.rows = 6  # Fixed number of rows
        self.calendar_grid.cols = 7  # Fixed number of columns
        self.calendar_grid.spacing = [2, 2]  # Add spacing between cells
        self.calendar_grid.padding = [5, 5]  # Add padding around the grid

    def show_details(self, date):
        self.details_layout.clear_widgets()
        if date in self.schedule:
            # Add date header with day of week
            header = Label(
                text=f'Schedule for {date.strftime("%A, %d-%m-%Y")}',
                size_hint_y=None,
                height=40,
                bold=True
            )
            self.details_layout.add_widget(header)
        
            app = App.get_running_app()
            is_weekend = date.weekday() >= 5
            is_holiday = date in app.schedule_config.get('holidays', [])
        
            # Show if it's a weekend or holiday
            if is_weekend or is_holiday:
                status = Label(
                    text='WEEKEND' if is_weekend else 'HOLIDAY',
                    size_hint_y=None,
                    height=30,
                    color=(1, 0, 0, 1)
                )
                self.details_layout.add_widget(status)
        
            # Show workers with shift numbers
            for i, worker_id in enumerate(self.schedule[date]):
                worker_box = BoxLayout(
                    orientation='horizontal',
                    size_hint_y=None,
                    height=40,
                    padding=(10, 5)
                )
                worker_box.add_widget(Label(
                    text=f'Shift {i+1}: Worker {worker_id}',
                    size_hint_x=1,
                    halign='left'
                ))
                self.details_layout.add_widget(worker_box)

    def previous_month(self, instance):
        if self.current_date:
            if self.current_date.month == 1:
                self.current_date = self.current_date.replace(year=self.current_date.year - 1, month=12)
            else:
                self.current_date = self.current_date.replace(month=self.current_date.month - 1)
            self.display_month(self.current_date)

    def next_month(self, instance):
        if self.current_date:
            if self.current_date.month == 12:
                self.current_date = self.current_date.replace(year=self.current_date.year + 1, month=1)
            else:
                self.current_date = self.current_date.replace(month=self.current_date.month + 1)
            self.display_month(self.current_date)

    def save_schedule(self, instance):
        try:
            schedule_data = {}
            for date, workers in self.schedule.items():
                schedule_data[date.strftime('%Y-%m-%d')] = workers
            
            with open('schedule.json', 'w') as f:
                json.dump(schedule_data, f, indent=2)
            
            popup = Popup(title='Success',
                         content=Label(text='Schedule saved to schedule.json'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()
            
        except Exception as e:
            popup = Popup(title='Error',
                         content=Label(text=f'Failed to save: {str(e)}'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()

    def export_schedule(self, instance):
        try:
            with open('schedule.txt', 'w') as f:
                f.write("SHIFT SCHEDULE\n")
                f.write("=" * 50 + "\n\n")
            
                for date in sorted(self.schedule.keys()):
                    f.write(f"Date: {date.strftime('%A, %Y-%m-%d')}\n")
                    app = App.get_running_app()
                    if date in app.schedule_config.get('holidays', []):
                        f.write("(HOLIDAY)\n")
                    f.write("Assigned Workers:\n")
                    for i, worker in enumerate(self.schedule[date]):
                        f.write(f"  Shift {i+1}: Worker {worker}\n")
                    f.write("\n")
        
            popup = Popup(title='Success',
                         content=Label(text='Schedule exported to schedule.txt'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()
        
        except Exception as e:
            popup = Popup(title='Error',
                         content=Label(text=f'Failed to export: {str(e)}'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()

    # Fix for the CalendarViewScreen Summary button

    def show_summary(self):
        """
        Generate and show a summary of the current schedule
        """
        try:
            # Get the schedule data
            if not hasattr(self, 'scheduler') or not self.scheduler:
                messagebox.showerror("Error", "No schedule data available")
                return
        
            # Create a summary to display and also prepare PDF data
            stats = self.scheduler.stats.calculate_statistics()
            summary_data = self.prepare_summary_data(stats)
        
            # Display summary in a dialog
            self.display_summary_dialog(summary_data)
        
            # Ask if user wants a PDF
            if messagebox.askyesno("Export PDF", "Do you want to export the summary as PDF?"):
                self.export_summary_pdf(summary_data)
            
        except Exception as e:
            logging.error(f"Failed to show summary: {str(e)}", exc_info=True)
            messagebox.showerror("Error", f"Failed to show summary: {str(e)}")

    def prepare_summary_data(self, stats):
        """
        Prepare summary data in a structured way for display and PDF export
        """
        summary_data = {
            'workers': {},
            'totals': {
                'total_shifts': 0,
                'filled_shifts': 0,
                'weekend_shifts': 0,
                'last_post_shifts': 0
            }
        }
    
        # Process worker statistics
        for worker_id, worker_stats in stats.get('workers', {}).items():
            # Safeguard against None values
            worker_name = worker_stats.get('name', worker_id) or worker_id
            total_shifts = worker_stats.get('total_shifts', 0) or 0
            weekend_shifts = worker_stats.get('weekend_shifts', 0) or 0
            weekday_shifts = worker_stats.get('weekday_shifts', 0) or 0
            target_shifts = worker_stats.get('target_shifts', 0) or 0
        
            # Get post distribution (especially last post)
            post_distribution = worker_stats.get('post_distribution', {})
            last_post = max(post_distribution.keys()) if post_distribution else 0
            last_post_shifts = post_distribution.get(last_post, 0) or 0
        
            # Store worker data
            summary_data['workers'][worker_id] = {
                'name': worker_name,
                'total_shifts': total_shifts,
                'weekend_shifts': weekend_shifts,
                'weekday_shifts': weekday_shifts,
                'target_shifts': target_shifts,
                'last_post_shifts': last_post_shifts,
                'post_distribution': post_distribution
            }
        
            # Update totals
            summary_data['totals']['total_shifts'] += total_shifts
            summary_data['totals']['filled_shifts'] += total_shifts
            summary_data['totals']['weekend_shifts'] += weekend_shifts
            summary_data['totals']['last_post_shifts'] += last_post_shifts
    
        return summary_data

    def display_summary_dialog(self, summary_data):
        """
        Display the summary data in a dialog window
        """
        # Create a new top-level window
        summary_window = tk.Toplevel(self.master)
        summary_window.title("Schedule Summary")
        summary_window.geometry("800x600")
    
        # Create a frame with scrollbar
        frame = tk.Frame(summary_window)
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
    
        # Create scrollable text widget
        text = tk.Text(frame, wrap=tk.WORD, padx=10, pady=10)
        scrollbar = tk.Scrollbar(frame, command=text.yview)
        text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    
        # Insert summary text
        text.insert(tk.END, "SCHEDULE SUMMARY\n\n")
        text.insert(tk.END, f"Total Workers: {len(summary_data['workers'])}\n")
        text.insert(tk.END, f"Total Shifts: {summary_data['totals']['total_shifts']}\n")
        text.insert(tk.END, f"Weekend Shifts: {summary_data['totals']['weekend_shifts']}\n")
        text.insert(tk.END, f"Last Post Shifts: {summary_data['totals']['last_post_shifts']}\n\n")
    
        # Worker details
        text.insert(tk.END, "WORKER DETAILS\n\n")
    
        # Sort workers by name
        sorted_workers = sorted(summary_data['workers'].items(), 
                               key=lambda x: x[1]['name'])
    
        for worker_id, data in sorted_workers:
            text.insert(tk.END, f"Worker: {data['name']} ({worker_id})\n")
            text.insert(tk.END, f"  Total Shifts: {data['total_shifts']} (Target: {data['target_shifts']})\n")
            text.insert(tk.END, f"  Weekend Shifts: {data['weekend_shifts']}\n")
            text.insert(tk.END, f"  Last Post Shifts: {data['last_post_shifts']}\n")
            text.insert(tk.END, "\n")
    
        # Make the text widget read-only
        text.configure(state=tk.DISABLED)

    def show_month_summary(self, instance):
        """Display a summary of the current month's schedule"""
        try:
            if not self.current_date:
                return
            
            app = App.get_running_app()
        
            # Get schedule data
            schedule = app.schedule_config.get('schedule', {})
            workers_data = app.schedule_config.get('workers_data', [])
            num_shifts = app.schedule_config.get('num_shifts', 0)
        
            # Calculate basic statistics for this month
            month_stats = {
                'total_shifts': 0,
                'workers': {},
                'weekend_shifts': 0,
                'last_post_shifts': 0,
                'posts': {i: 0 for i in range(num_shifts)},
                'worker_shifts': {}  # Dictionary to store shifts by worker
            }
        
            # Get data for the current month
            for date, workers in schedule.items():
                if date.year == self.current_date.year and date.month == self.current_date.month:
                    month_stats['total_shifts'] += len(workers)
                
                    # Count per worker
                    for i, worker_id in enumerate(workers):
                        if worker_id is None:
                            continue
                        
                        if worker_id not in month_stats['workers']:
                            month_stats['workers'][worker_id] = {
                                'total': 0,
                                'weekends': 0, 
                                'last_post': 0
                            }
                    
                        if worker_id not in month_stats['worker_shifts']:
                            month_stats['worker_shifts'][worker_id] = []
                            
                        # Add this shift to worker's shifts list
                        month_stats['worker_shifts'][worker_id].append({
                            'date': date,
                            'day': date.strftime('%A'),
                            'post': i + 1,
                            'is_weekend': date.weekday() >= 5,
                            'is_holiday': date in app.schedule_config.get('holidays', [])
                        })
                    
                        month_stats['workers'][worker_id]['total'] += 1
                    
                        # Count post distribution
                        if i < num_shifts:
                            month_stats['posts'][i] += 1
                        
                            # Count last post assignments
                            if i == num_shifts - 1:
                                month_stats['last_post_shifts'] += 1
                                month_stats['workers'][worker_id]['last_post'] += 1
                    
                        # Count weekend shifts
                        if date.weekday() >= 5:  # Saturday or Sunday
                            month_stats['weekend_shifts'] += 1
                            month_stats['workers'][worker_id]['weekends'] += 1
            
            # Create a PDF report
            if self.display_summary_dialog(month_stats):
                self.export_summary_pdf(month_stats)
            
        except Exception as e:
            popup = Popup(title='Error',
                         content=Label(text=f'Failed to show summary: {str(e)}'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()
            logging.error(f"Summary error: {str(e)}", exc_info=True)

    def display_summary_dialog(self, month_stats):
        """
        Display the detailed summary dialog with shift listings and ask if user wants to export PDF
        Returns True if user wants PDF
        """
        # Create the content layout
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
    
        # Summary scroll view
        scroll = ScrollView(size_hint=(1, 0.8))
        summary_layout = GridLayout(
            cols=1, 
            spacing=10, 
            size_hint_y=None,
            padding=[10, 10]
        )
        summary_layout.bind(minimum_height=summary_layout.setter('height'))
    
        # Month title
        month_title = Label(
            text=f"Summary for {self.current_date.strftime('%B %Y')}",
            size_hint_y=None,
            height=40,
            bold=True
        )
        summary_layout.add_widget(month_title)
    
        # General stats
        stats_box = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            height=120,
            padding=[5, 5]
        )
    
        stats_box.add_widget(Label(
            text=f"Total Shifts: {month_stats['total_shifts']}",
            size_hint_y=None,
            height=30,
            halign='left'
        ))
    
        stats_box.add_widget(Label(
            text=f"Weekend Shifts: {month_stats['weekend_shifts']}",
            size_hint_y=None,
            height=30,
            halign='left'
        ))
    
        stats_box.add_widget(Label(
            text=f"Last Post Shifts: {month_stats['last_post_shifts']}",
            size_hint_y=None,
            height=30,
            halign='left'
        ))
    
        summary_layout.add_widget(stats_box)
    
        # Worker details header
        worker_header = Label(
            text="Worker Details:",
            size_hint_y=None,
            height=40,
            bold=True
        )
        summary_layout.add_widget(worker_header)
    
        # Add worker details with shift listings
        for worker_id, stats in sorted(month_stats['workers'].items()):
            # Create a container for each worker
            worker_box = BoxLayout(
                orientation='vertical',
                size_hint_y=None,
                padding=[5, 10],
                spacing=5
            )
        
            # Calculate height based on number of shifts (min 120px, 30px per shift)
            worker_shifts = month_stats['worker_shifts'].get(worker_id, [])
            height = max(120, 60 + (len(worker_shifts) * 30))
            worker_box.height = height
        
            # Worker summary
            worker_box.add_widget(Label(
                text=f"Worker {worker_id}:",
                size_hint_y=None,
                height=30,
                bold=True,
                halign='left'
            ))
        
            worker_box.add_widget(Label(
                text=f"Total: {stats['total']} | Weekends: {stats['weekends']} | Last Post: {stats['last_post']}",
                size_hint_y=None,
                height=30,
                halign='left'
            ))
        
            # List of shifts header
            worker_box.add_widget(Label(
                text="Assigned Shifts:",
                size_hint_y=None,
                height=30,
                halign='left'
            ))
        
            # Display each shift
            for shift in sorted(worker_shifts, key=lambda x: x['date']):
                date_str = shift['date'].strftime('%d-%m-%Y')
                post_str = f"Post {shift['post']}"
                day_type = ""
                if shift['is_holiday']:
                    day_type = " [HOLIDAY]"
                elif shift['is_weekend']:
                    day_type = " [WEEKEND]"
                
                shift_label = Label(
                    text=f"• {date_str} ({shift['day']}){day_type}: {post_str}",
                    size_hint_y=None,
                    height=30,
                    halign='left'
                )
                shift_label.bind(size=shift_label.setter('text_size'))
                worker_box.add_widget(shift_label)
                
            # Add separator
            separator = BoxLayout(
                size_hint_y=None,
                height=1
            )
            with separator.canvas:
                from kivy.graphics import Color, Rectangle
                Color(0.7, 0.7, 0.7, 1)  # Light gray
                Rectangle(pos=separator.pos, size=separator.size)
            
            # Add worker box and separator to layout
            summary_layout.add_widget(worker_box)
            summary_layout.add_widget(separator)
    
        scroll.add_widget(summary_layout)
        content.add_widget(scroll)
    
        # Button layout
        button_layout = BoxLayout(
            orientation='horizontal', 
            size_hint_y=0.1,
            spacing=10
        )
    
        pdf_button = Button(text='Export PDF')
        close_button = Button(text='Close')
    
        button_layout.add_widget(pdf_button)
        button_layout.add_widget(close_button)
        content.add_widget(button_layout)
    
        # Create popup
        popup = Popup(
            title='Monthly Summary',
            content=content,
            size_hint=(0.9, 0.9),
            auto_dismiss=False
        )
    
        # Store user's choice
        user_wants_pdf = [False]
    
        def on_pdf(instance):
            user_wants_pdf[0] = True
            popup.dismiss()
        
        def on_close(instance):
            popup.dismiss()
    
        pdf_button.bind(on_press=on_pdf)
        close_button.bind(on_press=on_close)
    
        # Show popup and wait for it to close
        popup.open()
    
        # Return user's choice
        return user_wants_pdf[0]

    def export_summary_pdf(self, month_stats):
        """Export a detailed summary with shift listings as a PDF file"""
        try:
            # Check if reportlab is available
            try:
                from reportlab.lib.pagesizes import letter
                from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
                from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                from reportlab.lib import colors
                import tempfile
                import os
                from reportlab.lib.units import inch
            except ImportError:
                # Show error popup
                popup = Popup(
                    title='Error',
                    content=Label(text='ReportLab is not installed. Please install it with: pip install reportlab'),
                    size_hint=(None, None),
                    size=(400, 200)
                )
                popup.open()
                return
        
            # Create a file name for the PDF
            app = App.get_running_app()
            month_name = self.current_date.strftime('%B_%Y')
            filename = f"schedule_summary_{month_name}.pdf"
        
            # Create PDF document
            doc = SimpleDocTemplate(filename, pagesize=letter)
            styles = getSampleStyleSheet()
            elements = []
        
            # Create custom styles
            styles.add(ParagraphStyle(
                name='Heading3',
                parent=styles['Heading3'],
                fontSize=12,
                leading=14,
                spaceAfter=6
            ))
        
            styles.add(ParagraphStyle(
                name='ShiftList',
                parent=styles['Normal'],
                fontSize=10,
                leading=12,
                leftIndent=20
            ))
        
            # Add title
            title_style = styles['Heading1']
            title = Paragraph(f"Schedule Summary - {month_name}", title_style)
            elements.append(title)
            elements.append(Spacer(1, 12))
        
            # Add generation info
            date_style = styles['Normal']
            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            date_paragraph = Paragraph(f"Generated on: {current_date}", date_style)
            elements.append(date_paragraph)
            elements.append(Spacer(1, 6))
        
            user_paragraph = Paragraph(f"Generated by: {app.current_user}", date_style)
            elements.append(user_paragraph)
            elements.append(Spacer(1, 20))
        
            # Add overall statistics
            stats_style = styles['Heading2']
            stats_title = Paragraph("Overall Statistics", stats_style)
            elements.append(stats_title)
            elements.append(Spacer(1, 6))
        
            stats_data = [
                ["Total Workers", str(len(month_stats['workers']))],
                ["Total Shifts", str(month_stats['total_shifts'])],
                ["Weekend Shifts", str(month_stats['weekend_shifts'])],
                ["Last Post Shifts", str(month_stats['last_post_shifts'])]
            ]
        
            stats_table = Table(stats_data, colWidths=[200, 100])
            stats_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            elements.append(stats_table)
            elements.append(Spacer(1, 20))
        
            # Add post distribution
            post_style = styles['Heading2']
            post_title = Paragraph("Post Distribution", post_style)
            elements.append(post_title)
            elements.append(Spacer(1, 6))
        
            post_data = [["Post", "Shifts"]]
            for post, count in month_stats['posts'].items():
                post_data.append([f"Post {post+1}", str(count)])
        
            post_table = Table(post_data, colWidths=[200, 100])
            post_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            elements.append(post_table)
            elements.append(Spacer(1, 20))
        
            # Add worker details with shift listings
            worker_style = styles['Heading2']
            worker_title = Paragraph("Worker Details", worker_style)
            elements.append(worker_title)
            elements.append(Spacer(1, 12))
        
            # Add detailed worker information with shift lists
            for worker_id, stats in sorted(month_stats['workers'].items()):
                # Worker header
                worker_header = Paragraph(f"Worker {worker_id}", styles['Heading3'])
                elements.append(worker_header)
            
                # Worker summary
                worker_summary_data = [
                    ["Total Shifts", str(stats['total'])],
                    ["Weekend Shifts", str(stats['weekends'])],
                    ["Last Post Shifts", str(stats['last_post'])]
                ]
            
                summary_table = Table(worker_summary_data, colWidths=[120, 80])
                summary_table.setStyle(TableStyle([
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ]))
                elements.append(summary_table)
                elements.append(Spacer(1, 6))
                
                # Shift listing header
                shift_header = Paragraph("Assigned Shifts:", styles['Normal'])
                elements.append(shift_header)
            
                # Get worker shifts
                worker_shifts = month_stats['worker_shifts'].get(worker_id, [])
            
                if worker_shifts:
                    # Create shift table
                    shift_data = [["Date", "Day", "Post", "Type"]]
                
                    for shift in sorted(worker_shifts, key=lambda x: x['date']):
                        date_str = shift['date'].strftime('%d-%m-%Y')
                        day_str = shift['day']
                        post_str = f"Post {shift['post']}"
                    
                        day_type = "Regular"
                        if shift['is_holiday']:
                            day_type = "HOLIDAY"
                        elif shift['is_weekend']:
                            day_type = "WEEKEND"
                    
                        shift_data.append([date_str, day_str, post_str, day_type])
                
                    shift_table = Table(shift_data, colWidths=[100, 80, 60, 80])
                    shift_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        # Highlight weekend/holiday rows
                        ('TEXTCOLOR', (3, 0), (3, -1), colors.black),
                    ]))
                
                    elements.append(shift_table)
                else:
                    elements.append(Paragraph("No shifts assigned", styles['Normal']))
            
                # Add spacer after each worker
                elements.append(Spacer(1, 20))
            
            # Build the PDF
            doc.build(elements)
        
            # Show success message
            popup = Popup(
                title='Success',
                content=Label(text=f'Summary exported to {filename}'),
                size_hint=(None, None),
                size=(400, 200)
            )
            popup.open()
        
        except Exception as e:
            # Show error popup
            popup = Popup(
                title='Error',
                content=Label(text=f'Failed to export PDF: {str(e)}'),
                size_hint=(None, None),
                size=(400, 200)
            )
            popup.open()
            logging.error(f"PDF export error: {str(e)}", exc_info=True)

    def export_to_pdf(self, instance):
        try:
            app = App.get_running_app()
            exporter = PDFExporter(app.schedule_config)
        
            # Create content for export options
            content = BoxLayout(orientation='vertical', spacing=10, padding=10)
        
            # Add radio buttons for export type
            export_type = BoxLayout(orientation='vertical', spacing=5)
            export_type.add_widget(Label(text='Export Type:'))
        
            radio_monthly = CheckBox(group='export_type', active=True)
            radio_stats = CheckBox(group='export_type')
        
            type_1 = BoxLayout()
            type_1.add_widget(radio_monthly)
            type_1.add_widget(Label(text='Monthly Calendar'))
        
            type_2 = BoxLayout()
            type_2.add_widget(radio_stats)
            type_2.add_widget(Label(text='Worker Statistics'))
        
            export_type.add_widget(type_1)
            export_type.add_widget(type_2)
            content.add_widget(export_type)
        
            # Add export button
            export_btn = Button(
                text='Export',
                size_hint_y=None,
                height=40
            )
            content.add_widget(export_btn)
        
            # Create popup
            popup = Popup(
                title='Export to PDF',
                content=content,
                size_hint=(None, None),
                size=(300, 200)
            )
        
            # Define export action
            def do_export(btn):
                try:
                    if radio_monthly.active:
                        # Export current month
                        filename = exporter.export_monthly_calendar(
                            self.current_date.year,
                            self.current_date.month
                        )
                    else:
                        # Export worker statistics
                        filename = exporter.export_worker_statistics()
                
                    success_popup = Popup(
                        title='Success',
                        content=Label(text=f'Exported to {filename}'),
                        size_hint=(None, None),
                        size=(400, 200)
                    )
                    popup.dismiss()
                    success_popup.open()
                
                except Exception as e:
                    error_popup = Popup(
                        title='Error',
                        content=Label(text=f'Export failed: {str(e)}'),
                        size_hint=(None, None),
                        size=(400, 200)
                    )
                    error_popup.open()
        
            export_btn.bind(on_press=do_export)
            popup.open()
        
        except Exception as e:
            popup = Popup(
                title='Error',
                content=Label(text=f'Export failed: {str(e)}'),
                size_hint=(None, None),
                size=(400, 200)
            )
            popup.open()
              
class ShiftManagerApp(App):
    def __init__(self, **kwargs):
        super(ShiftManagerApp, self).__init__(**kwargs)
        self.schedule_config = {}
        self.current_user = 'saldo27'
        # We'll set the datetime when creating the Scheduler instance

    def build(self):
        sm = ScreenManager()
        sm.add_widget(WelcomeScreen(name='welcome'))
        sm.add_widget(SetupScreen(name='setup'))
        sm.add_widget(WorkerDetailsScreen(name='worker_details'))
        sm.add_widget(CalendarViewScreen(name='calendar_view'))
        return sm

if __name__ == '__main__':
    ShiftManagerApp().run()
