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
from kivy.graphics import Color, Line
from datetime import datetime, timedelta
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
        
        layout.add_widget(Label(text='Welcome to Shift Scheduler'))
        
        start_btn = Button(text='Start Scheduling', size_hint_y=None, height=50)
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
        layout.add_widget(Label(text='Start Date (DD-MM-YYYY):'))
        self.start_date = TextInput(multiline=False)
        layout.add_widget(self.start_date)
        
        # End Date
        layout.add_widget(Label(text='End Date (DD-MM-YYYY):'))
        self.end_date = TextInput(multiline=False)
        layout.add_widget(self.end_date)
        
        # Number of Workers
        layout.add_widget(Label(text='Number of Workers:'))
        self.num_workers = TextInput(multiline=False, input_filter='int')
        layout.add_widget(self.num_workers)
        
        # Number of Shifts per Day
        layout.add_widget(Label(text='Number of Shifts per Day:'))
        self.num_shifts = TextInput(multiline=False, input_filter='int')
        layout.add_widget(self.num_shifts)
        
        # Continue Button
        continue_btn = Button(text='Continue', size_hint_y=None, height=50)
        continue_btn.bind(on_press=self.validate_and_continue)
        layout.add_widget(continue_btn)
        
        self.add_widget(layout)

    def validate_and_continue(self, instance):
        try:
            # Validate dates
            start = datetime.strptime(self.start_date.text, '%d-%m-%Y')
            end = datetime.strptime(self.end_date.text, '%d-%m-%Y')
            
            if end <= start:
                raise ValueError("End date must be after start date")
            
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
        scroll = ScrollView(size_hint=(1, 0.8))
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

        # Continue Button
        self.continue_btn = Button(text='Continue', size_hint_y=0.1)
        self.continue_btn.bind(on_press=self.save_and_continue)
        self.layout.add_widget(self.continue_btn)

        self.add_widget(self.layout)

    def validate_dates(self, date_str):
        if not date_str:
            return True
        try:
            for period in date_str.split(';'):
                period = period.strip()
                if '-' in period:
                    start, end = period.split('-')
                    datetime.strptime(start.strip(), '%d-%m-%Y')
                    datetime.strptime(end.strip(), '%d-%m-%Y')
                else:
                    datetime.strptime(period, '%d-%m-%Y')
            return True
        except:
            return False

    def save_and_continue(self, instance):
        if not self.worker_id.text.strip():
            self.show_error("Worker ID is required")
            return

        try:
            work_percentage = float(self.work_percentage.text or '100')
            if not (0 < work_percentage <= 100):
                self.show_error("Work percentage must be between 0 and 100")
                return
        except:
            self.show_error("Invalid work percentage")
            return

        if not self.validate_dates(self.work_periods.text):
            self.show_error("Invalid work periods format")
            return

        if not self.validate_dates(self.mandatory_days.text):
            self.show_error("Invalid mandatory days format")
            return

        if not self.validate_dates(self.days_off.text):
            self.show_error("Invalid days off format")
            return

        app = App.get_running_app()
        worker_data = {
            'id': self.worker_id.text.strip(),
            'work_periods': self.work_periods.text.strip(),
            'work_percentage': work_percentage,
            'mandatory_days': self.mandatory_days.text.strip(),
            'days_off': self.days_off.text.strip(),
            'is_incompatible': self.incompatible_checkbox.active
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
            self.generate_schedule()

    def show_error(self, message):
        popup = Popup(title='Error',
                     content=Label(text=message),
                     size_hint=(None, None), size=(400, 200))
        popup.open()

    def generate_schedule(self):
        app = App.get_running_app()
        try:
            from scheduler import Scheduler
            scheduler = Scheduler(app.schedule_config)
            schedule = scheduler.generate_schedule()
            
            if not schedule:
                raise ValueError("No schedule was generated")
                
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
        app = App.get_running_app()
        current_index = app.schedule_config.get('current_worker_index', 0)
        total_workers = app.schedule_config.get('num_workers', 0)
        self.title_label.text = f'Worker Details ({current_index + 1}/{total_workers})'

class CalendarViewScreen(Screen):
    def __init__(self, **kwargs):
        super(CalendarViewScreen, self).__init__(**kwargs)
        self.layout = BoxLayout(orientation='vertical', padding=10, spacing=5)
        
        # Header with title and navigation
        header = BoxLayout(orientation='horizontal', size_hint_y=0.1)
        self.month_label = Label(text='', size_hint_x=0.6)
        prev_month = Button(text='<', size_hint_x=0.2)
        next_month = Button(text='>', size_hint_x=0.2)
        prev_month.bind(on_press=self.previous_month)
        next_month.bind(on_press=self.next_month)
        
        header.add_widget(prev_month)
        header.add_widget(self.month_label)
        header.add_widget(next_month)
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
        export_btn = Button(text='Export to TXT')
        save_btn.bind(on_press=self.save_schedule)
        export_btn.bind(on_press=self.export_schedule)
        button_layout.add_widget(save_btn)
        button_layout.add_widget(export_btn)
        self.layout.add_widget(button_layout)
        
        self.add_widget(self.layout)
        self.current_date = None
        self.schedule = {}

    def on_enter(self):
        app = App.get_running_app()
        self.schedule = app.schedule_config.get('schedule', {})
        if self.schedule:
            # Set current_date to the first date in the schedule
            self.current_date = min(self.schedule.keys())
            self.display_month(self.current_date)

    def display_month(self, date):  # <-- Correct indentation
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
            self.calendar_grid.add_widget(Label(text=''))
        
        # Add days of the month
        for day in range(1, days_in_month + 1):
            current = datetime(date.year, date.month, day)
            cell = BoxLayout(orientation='vertical', spacing=2)
            
            # Day number
            day_label = Label(
                text=str(day),
                size_hint_y=0.2,
                bold=True
            )
            cell.add_widget(day_label)
            
            # If there are workers scheduled for this day, show their IDs
            if current in self.schedule:
                workers = self.schedule[current]
                # Create a label with worker IDs
                workers_text = '\n'.join(str(w) for w in workers)
                workers_label = Label(
                    text=workers_text,
                    font_size='10sp',
                    size_hint_y=0.8
                )
                cell.add_widget(workers_label)
                
                # Make the cell clickable and highlight it
                btn = Button(
                    size_hint=(1, 1),
                    background_color=(0.8, 0.9, 1, 0.3),
                    background_normal=''
                )
                btn.bind(on_press=lambda x, d=current: self.show_details(d))
                
                # Add the button as a background
                cell.bind(size=btn.setter('size'), pos=btn.setter('pos'))
                cell.add_widget(btn)
            
            # Add a border to make the cell more visible
            with cell.canvas.before:
                Color(0.8, 0.8, 0.8, 1)  # Light gray color for borders
                Line(rectangle=(cell.x, cell.y, cell.width, cell.height))
            
            self.calendar_grid.add_widget(cell)
        
        # Fill remaining cells
        remaining_cells = 42 - (first_weekday + days_in_month)  # 42 = 6 rows * 7 days
        for _ in range(remaining_cells):
            self.calendar_grid.add_widget(Label(text=''))

    def show_details(self, date):
        self.details_layout.clear_widgets()
        if date in self.schedule:
            header = Label(
                text=f'Schedule for {date.strftime("%d-%m-%Y")}',
                size_hint_y=None,
                height=40,
                bold=True
            )
            self.details_layout.add_widget(header)
            
            for worker_id in self.schedule[date]:
                worker_box = BoxLayout(
                    orientation='horizontal',
                    size_hint_y=None,
                    height=40,
                    padding=(10, 5)
                )
                worker_box.add_widget(Label(
                    text=f'Worker {worker_id}',
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
                    f.write(f"Date: {date.strftime('%Y-%m-%d')}\n")
                    f.write("Assigned Workers:\n")
                    for worker in self.schedule[date]:
                        f.write(f"  - Worker {worker}\n")
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
