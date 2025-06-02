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
from scheduler import  Scheduler, SchedulerError
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


class PasswordScreen(Screen):
    def __init__(self, **kwargs):
        super(PasswordScreen, self).__init__(**kwargs)
        layout = BoxLayout(orientation='vertical', padding=50, spacing=20)

        layout.add_widget(Label(text='Introduza la contraseña:', size_hint_y=None, height=40))

        self.password_input = TextInput(
            multiline=False,
            password=True,  
            size_hint_y=None,
            height=40,
            halign='center',
            font_size=20
        )
        layout.add_widget(self.password_input)

        self.error_label = Label(text='', color=(1, 0, 0, 1), size_hint_y=None, height=30) # For error messages
        layout.add_widget(self.error_label)

        submit_btn = Button(text='Login', size_hint_y=None, height=50)
        submit_btn.bind(on_press=self.check_password)
        layout.add_widget(submit_btn)

        # Add some spacing at the bottom
        layout.add_widget(BoxLayout(size_hint_y=0.4))

        self.add_widget(layout)

    def check_password(self, instance):
        print("DEBUG: check_password called.")
        correct_password = "this" # Or your actual password
        entered_password = self.password_input.text
        print(f"DEBUG: Entered Password: '{entered_password}'")
        print(f"DEBUG: Correct Password: '{correct_password}'")
        print(f"DEBUG: self.manager is: {self.manager}")
        if self.manager:
            print(f"DEBUG: Available screens: {[s.name for s in self.manager.screens]}")

        if entered_password == correct_password:
            print("DEBUG: Password MATCHES.")
            self.error_label.text = ''
            try:
                print("DEBUG: Attempting transition to 'welcome'...")
                self.manager.current = 'welcome' 
                print("DEBUG: Transition command sent to 'welcome'.")
            except Exception as e:
                print(f"DEBUG: ERROR during transition: {e}")
                self.error_label.text = f'Transition Error: {e}'
        else:
            print("DEBUG: Password MISMATCH.")
            self.error_label.text = 'Incorrect Password'
            self.password_input.text = ''

class WelcomeScreen(Screen):
    def __init__(self, **kwargs):
        super(WelcomeScreen, self).__init__(**kwargs)
        print("DEBUG: PasswordScreen __init__ called!") 
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
        
        # Use a BoxLayout as the main container with padding
        self.layout = BoxLayout(orientation='vertical', padding=20, spacing=15)
        
        # Title with some vertical space
        title_layout = BoxLayout(size_hint_y=0.1, padding=(0, 10))
        title = Label(text="Schedule Setup", font_size=24, bold=True)
        title_layout.add_widget(title)
        self.layout.add_widget(title_layout)
        
        # Use a ScrollView to make sure all content is accessible
        scroll_view = ScrollView()
        
        # Main form layout - using more vertical space and maintaining proper spacing
        form_layout = GridLayout(
            cols=1,  # Changed to 1 column for better layout
            spacing=15, 
            padding=15, 
            size_hint_y=None
        )
        form_layout.bind(minimum_height=form_layout.setter('height'))
        
        # Create date fields
        date_section = BoxLayout(orientation='horizontal', size_hint_y=None, height=70, spacing=10)
        
        # Start date
        start_date_container = BoxLayout(orientation='vertical')
        start_date_container.add_widget(Label(text='Fecha de inicio (DD-MM-YYYY):', halign='left', size_hint_y=0.4))
        self.start_date = TextInput(multiline=False, size_hint_y=0.6)
        start_date_container.add_widget(self.start_date)
        date_section.add_widget(start_date_container)
        
        # End date
        end_date_container = BoxLayout(orientation='vertical')
        end_date_container.add_widget(Label(text='Fecha final(DD-MM-YYYY):', halign='left', size_hint_y=0.4))
        self.end_date = TextInput(multiline=False, size_hint_y=0.6)
        end_date_container.add_widget(self.end_date)
        date_section.add_widget(end_date_container)
        
        form_layout.add_widget(date_section)
        
        # Add spacing using a BoxLayout instead of Widget
        form_layout.add_widget(BoxLayout(size_hint_y=None, height=10))
        
        # Number inputs section
        numbers_section = GridLayout(cols=2, size_hint_y=None, height=160, spacing=10)
        
        # Number of workers
        workers_container = BoxLayout(orientation='vertical')
        workers_container.add_widget(Label(text='Nº de médicos:', halign='left', size_hint_y=0.4))
        self.num_workers = TextInput(multiline=False, input_filter='int', size_hint_y=0.6)
        workers_container.add_widget(self.num_workers)
        numbers_section.add_widget(workers_container)
        
        # Number of shifts
        shifts_container = BoxLayout(orientation='vertical')
        shifts_container.add_widget(Label(text='Guardias/día:', halign='left', size_hint_y=0.4))
        self.num_shifts = TextInput(multiline=False, input_filter='int', size_hint_y=0.6)
        shifts_container.add_widget(self.num_shifts)
        numbers_section.add_widget(shifts_container)
        
        # Gap between shifts
        gap_container = BoxLayout(orientation='vertical')
        gap_container.add_widget(Label(text='Distancia mínima entre guardias:', halign='left', size_hint_y=0.4))
        self.gap_between_shifts = TextInput(multiline=False, input_filter='int', text='3', size_hint_y=0.6)
        gap_container.add_widget(self.gap_between_shifts)
        numbers_section.add_widget(gap_container)
        
        # Max consecutive weekends
        weekends_container = BoxLayout(orientation='vertical')
        weekends_container.add_widget(Label(text='Máx Findes/Festivos consecutivos:', halign='left', size_hint_y=0.4))
        self.max_consecutive_weekends = TextInput(multiline=False, input_filter='int', text='3', size_hint_y=0.6)
        weekends_container.add_widget(self.max_consecutive_weekends)
        numbers_section.add_widget(weekends_container)
        
        form_layout.add_widget(numbers_section)
        
        # Add spacing using a BoxLayout instead of Widget
        form_layout.add_widget(BoxLayout(size_hint_y=None, height=10))

        # ------ NEW SECTION: Variable Shifts by Date Range ------
        shifts_header = BoxLayout(orientation='vertical', size_hint_y=None, height=40)
        shifts_header.add_widget(Label(
            text='Periodo con variación en guardias/día(opcional):',
            halign='left',
            valign='bottom',
            bold=True
        ))
        form_layout.add_widget(shifts_header)

        # Create container for variable shifts
        self.variable_shifts_container = GridLayout(
            cols=1,
            size_hint_y=None,
            height=0,  # Will be updated dynamically
            spacing=5
        )
        self.variable_shifts_container.bind(minimum_height=self.variable_shifts_container.setter('height'))
        form_layout.add_widget(self.variable_shifts_container)

        # Add button to add new variable shift rows
        add_shift_btn = Button(
            text='+ Añadir Periodo con variación guardias/día',
            size_hint_y=None,
            height=40
        )
        add_shift_btn.bind(on_press=self.add_variable_shift_row)
        form_layout.add_widget(add_shift_btn)

        # Add some spacing
        form_layout.add_widget(BoxLayout(size_hint_y=None, height=10))
        
        # Holidays - given more space with a clear label
        holidays_layout = BoxLayout(orientation='vertical', size_hint_y=None, height=150)
        holidays_layout.add_widget(Label(
            text='Festivos (DD-MM-YYYY, separados por comas):',
            halign='left',
            valign='bottom',
            size_hint_y=0.2
        ))
        
        # TextInput with significant height for holidays
        self.holidays = TextInput(multiline=True, size_hint_y=0.8)
        holidays_layout.add_widget(self.holidays)
        form_layout.add_widget(holidays_layout)
        
        # Add some explanation text
        help_text = Label(
            text="Tip: Introduce Festivos separados por comas. Ej: 25-12-2025, 01-06-2026",
            halign='left',
            valign='top',
            size_hint_y=None,
            height=30,
            text_size=(800, None),
            font_size='12sp',
            color=(0.5, 0.5, 0.5, 1)  # Gray color
        )
        form_layout.add_widget(help_text)
        
        # Add form to scroll view
        scroll_view.add_widget(form_layout)
        self.layout.add_widget(scroll_view)
        
        # Buttons in a separate area at the bottom
        button_section = BoxLayout(
            orientation='horizontal', 
            size_hint_y=0.12,
            spacing=15,
            padding=(0, 10)
        )
        
        # Button styling (simplified to avoid potential issues)
        self.save_button = Button(text='Guardar', font_size='16sp')
        self.save_button.bind(on_press=self.save_config)
        button_section.add_widget(self.save_button)
        
        self.load_button = Button(text='Cargar', font_size='16sp')
        self.load_button.bind(on_press=self.load_config)
        button_section.add_widget(self.load_button)
        
        self.next_button = Button(text='Sigue →', font_size='16sp')
        self.next_button.bind(on_press=self.next_screen)
        button_section.add_widget(self.next_button)
        
        self.layout.add_widget(button_section)
        
        # Add the main layout to the screen
        self.add_widget(self.layout)

        # Keep track of variable shift rows
        self.variable_shift_rows = []

    def save_config(self, instance):
        try:
            start_date_str = self.start_date.text.strip()
            end_date_str = self.end_date.text.strip()
    
            # Validate dates - using DD-MM-YYYY format
            try:
                # Parse using DD-MM-YYYY format
                start_date = datetime.strptime(start_date_str, "%d-%m-%Y").date()
                end_date = datetime.strptime(end_date_str, "%d-%m-%Y").date()
                if start_date > end_date:
                    raise ValueError("Start date must be before end date")
            except ValueError as e:
                self.show_error(f"Invalid date format: {str(e)}\nUse DD-MM-YYYY format")
                return
    
            # Validate numeric inputs
            try:
                num_workers = int(self.num_workers.text)
                num_shifts = int(self.num_shifts.text)
                gap_between_shifts = int(self.gap_between_shifts.text)
                max_consecutive_weekends = int(self.max_consecutive_weekends.text)
        
                if num_workers <= 0 or num_shifts <= 0:
                    raise ValueError("Number of workers and shifts must be positive")
        
                if gap_between_shifts < 0:
                    raise ValueError("Minimum days between shifts cannot be negative")
        
                if max_consecutive_weekends <= 0:
                    raise ValueError("Maximum consecutive weekend shifts must be positive")
            
            except ValueError as e:
                self.show_error(f"Invalid numeric input: {str(e)}")
                return
    
            # Parse holidays - also using DD-MM-YYYY format
            holidays_list = []
            if self.holidays.text.strip():
                for holiday_str in self.holidays.text.strip().split(','):
                    try:
                        # Parse using DD-MM-YYYY format
                        holiday_date = datetime.strptime(holiday_str.strip(), "%d-%m-%Y").date()
                        holidays_list.append(holiday_date)
                    except ValueError:
                        self.show_error(f"Formato de fecha no válido: {holiday_str}\nUse DD-MM-YYYY format")
                        return
    
            # Parse variable shifts data
            variable_shifts = []
            for row_data in self.variable_shift_rows:
                start_str = row_data['start_date'].text.strip()
                end_str = row_data['end_date'].text.strip()
                shifts_str = row_data['shifts'].text.strip()
            
                # Skip empty rows
                if not start_str and not end_str and not shifts_str:
                    continue
            
                # If any field is filled, all must be filled
                if not (start_str and end_str and shifts_str):
                    self.show_error("For variable shifts, all fields (start date, end date, and shifts) must be filled")
                    return
            
                try:
                    range_start = datetime.strptime(start_str, "%d-%m-%Y").date()
                    range_end = datetime.strptime(end_str, "%d-%m-%Y").date()
                    range_shifts = int(shifts_str)
                
                    if range_start > range_end:
                        self.show_error(f"Variable shifts: Start date must be before end date")
                        return
                
                    if range_shifts <= 0:
                        self.show_error(f"Variable shifts: Number of shifts must be positive")
                        return
                
                    # Ensure the range is within the overall period
                    if range_start < start_date or range_end > end_date:
                        self.show_error(f"Variable shifts: Date range must be within the overall schedule period")
                        return
                
                    variable_shifts.append({
                        'start_date': datetime.combine(range_start, datetime.min.time()),
                        'end_date': datetime.combine(range_end, datetime.min.time()),
                        'shifts': range_shifts
                    })
                
                except ValueError as e:
                    self.show_error(f"Invalid data in variable shifts: {str(e)}")
                    return
    
            # Check for overlapping date ranges
            for i, range1 in enumerate(variable_shifts):
                for j, range2 in enumerate(variable_shifts):
                    if i != j:
                        # Check if ranges overlap
                        if (range1['start_date'] <= range2['end_date'] and 
                            range1['end_date'] >= range2['start_date']):
                            self.show_error("Variable shifts: Date ranges cannot overlap")
                            return
    
            # Convert date objects to datetime objects with time set to midnight
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.min.time())
            holidays_datetime = [datetime.combine(holiday, datetime.min.time()) for holiday in holidays_list]
    
            # Save configuration to app
            app = App.get_running_app()
            app.schedule_config = {
                'start_date': start_datetime,
                'end_date': end_datetime,
                'num_workers': num_workers,
                'num_shifts': num_shifts,
                'gap_between_shifts': gap_between_shifts,
                'max_consecutive_weekends': max_consecutive_weekends,
                'holidays': holidays_datetime,
                'workers_data': [],
                'schedule': {},
                'current_worker_index': 0,
                'variable_shifts': variable_shifts  # Make sure this line is present
}
    
            # Notify user
            self.show_message('Introduce los datos de cada médico')

        except Exception as e:
            self.show_error(f"Error saving configuration: {str(e)}")

    def load_config(self, instance):
        try:
            app = App.get_running_app()
    
            # Set input fields from configuration
            if hasattr(app, 'schedule_config') and app.schedule_config:
                if 'start_date' in app.schedule_config:
                    self.start_date.text = app.schedule_config['start_date'].strftime("%d-%m-%Y")
        
                if 'end_date' in app.schedule_config:
                    self.end_date.text = app.schedule_config['end_date'].strftime("%d-%m-%Y")
            
                if 'num_workers' in app.schedule_config:
                    self.num_workers.text = str(app.schedule_config['num_workers'])
            
                if 'num_shifts' in app.schedule_config:
                    self.num_shifts.text = str(app.schedule_config['num_shifts'])
            
                # Load new settings if they exist, otherwise use defaults
                if 'gap_between_shifts' in app.schedule_config:
                    self.gap_between_shifts.text = str(app.schedule_config['gap_between_shifts'])
                else:
                    self.gap_between_shifts.text = "3"  
            
                if 'max_consecutive_weekends' in app.schedule_config:
                    self.max_consecutive_weekends.text = str(app.schedule_config['max_consecutive_weekends'])
                else:
                    self.max_consecutive_weekends.text = "3"  
            
                if 'holidays' in app.schedule_config and app.schedule_config['holidays']:
                    # Format holidays in DD-MM-YYYY format
                    holidays_str = ", ".join([d.strftime("%d-%m-%Y") for d in app.schedule_config['holidays']])
                    self.holidays.text = holidays_str
            
                # Load variable shifts
                if 'variable_shifts' in app.schedule_config and app.schedule_config['variable_shifts']:
                    # Clear existing rows
                    for row_data in list(self.variable_shift_rows):
                        self.variable_shifts_container.remove_widget(row_data['row'])
                    self.variable_shift_rows = []
                
                    # Add rows for each saved variable shift
                    for shift_range in app.schedule_config['variable_shifts']:
                        self.add_variable_shift_row()
                        row_data = self.variable_shift_rows[-1]  # Get the last added row
                    
                        # Fill in the data
                        start_date = shift_range['start_date']
                        end_date = shift_range['end_date']
                        shifts = shift_range['shifts']
                    
                        row_data['start_date'].text = start_date.strftime("%d-%m-%Y")
                        row_data['end_date'].text = end_date.strftime("%d-%m-%Y")
                        row_data['shifts'].text = str(shifts)
            
                self.show_message('Configuration loaded successfully')
            else:
                self.show_message('No saved configuration found')
            
        except Exception as e:
            self.show_error(f"Error loading configuration: {str(e)}")            
        except Exception as e:
            self.show_error(f"Error loading configuration: {str(e)}")
    
    def next_screen(self, instance):
        # Validate and save configuration first
        self.save_config(None)
        
        app = App.get_running_app()
        if hasattr(app, 'schedule_config') and app.schedule_config:
            self.manager.current = 'worker_details'
        else:
            self.show_error('Please complete and save the configuration first')
    
    def show_error(self, message):
        popup = Popup(title='Error',
                     content=Label(text=message),
                     size_hint=(None, None), size=(400, 200))
        popup.open()
    
    def show_message(self, message):
        popup = Popup(title='Information',
                     content=Label(text=message),
                     size_hint=(None, None), size=(400, 200))
        popup.open()

    def add_variable_shift_row(self, instance=None):
        """Add a new row for defining variable shifts for a date range"""
        row = BoxLayout(orientation='horizontal', size_hint_y=None, height=60, spacing=5)
    
        # Start date for this range
        start_range = BoxLayout(orientation='vertical', size_hint_x=0.3)
        start_range.add_widget(Label(text='Desde (DD-MM-YYYY):', size_hint_y=0.3, halign='left'))
        start_date_input = TextInput(multiline=False, size_hint_y=0.7)
        start_range.add_widget(start_date_input)
    
        # End date for this range
        end_range = BoxLayout(orientation='vertical', size_hint_x=0.3)
        end_range.add_widget(Label(text='Hasta (DD-MM-YYYY):', size_hint_y=0.3, halign='left'))
        end_date_input = TextInput(multiline=False, size_hint_y=0.7)
        end_range.add_widget(end_date_input)
    
        # Number of shifts for this range
        shifts_range = BoxLayout(orientation='vertical', size_hint_x=0.2)
        shifts_range.add_widget(Label(text='Guardias/día:', size_hint_y=0.3, halign='left'))
        shifts_input = TextInput(multiline=False, input_filter='int', size_hint_y=0.7)
        shifts_range.add_widget(shifts_input)
    
        # Remove button
        remove_btn = Button(text='X', size_hint_x=0.1)
        remove_btn.row = row  # Store reference to the row for removal
        remove_btn.bind(on_press=self.remove_variable_shift_row)
    
        row.add_widget(start_range)
        row.add_widget(end_range)
        row.add_widget(shifts_range)
        row.add_widget(remove_btn)
    
        # Store the inputs for later retrieval
        row_data = {
            'row': row,
            'start_date': start_date_input,
            'end_date': end_date_input,
            'shifts': shifts_input,
            'remove_btn': remove_btn
        }
        self.variable_shift_rows.append(row_data)
    
        self.variable_shifts_container.add_widget(row)
        self.variable_shifts_container.height = len(self.variable_shift_rows) * 65  # Update container height

    def remove_variable_shift_row(self, instance):
        """Remove a variable shift row"""
        row = instance.row
        # Find the row data entry
        row_data = next((data for data in self.variable_shift_rows if data['row'] == row), None)
    
        if row_data:
            # Remove the row from the container
            self.variable_shifts_container.remove_widget(row)
            # Remove the row data from our tracking list
            self.variable_shift_rows.remove(row_data)
            # Update container height
            self.variable_shifts_container.height = len(self.variable_shift_rows) * 65

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
        self.form_layout.add_widget(Label(text='Periodos de trabajo (DD-MM-YYYY):'))
        self.work_periods = TextInput(multiline=True, size_hint_y=None, height=60)
        self.form_layout.add_widget(self.work_periods)

        # Work Percentage
        self.form_layout.add_widget(Label(text='Porcentaje de jornada:'))
        self.work_percentage = TextInput(
            multiline=False,
            text='100',
            input_filter='float',
            size_hint_y=None,
            height=40
        )
        self.form_layout.add_widget(self.work_percentage)

        # Mandatory Days
        self.form_layout.add_widget(Label(text='Guardias obligatorias:'))
        self.mandatory_days = TextInput(multiline=True, size_hint_y=None, height=60)
        self.form_layout.add_widget(self.mandatory_days)

        # Days Off
        self.form_layout.add_widget(Label(text='Días fuera:'))
        self.days_off = TextInput(multiline=True, size_hint_y=None, height=60)
        self.form_layout.add_widget(self.days_off)

        # Incompatibility Checkbox - Updated Layout
        checkbox_label = Label(
            text='Incompatible:',
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
            text='No puede coincidir con otro incompatible',
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
        self.prev_btn = Button(text='Previo', size_hint_x=0.33)
        self.prev_btn.bind(on_press=self.go_to_previous_worker)
        navigation_layout.add_widget(self.prev_btn)
        
        # Save Button
        self.save_btn = Button(text='Guardar', size_hint_x=0.33)
        self.save_btn.bind(on_press=self.save_worker_data)
        navigation_layout.add_widget(self.save_btn)
        
        # Next/Finish Button
        self.next_btn = Button(text='Siguiente', size_hint_x=0.33)
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
            'is_incompatible': self.incompatible_checkbox.active # <<< Saves a boolean flag
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
            print("DEBUG: generate_schedule - Starting schedule generation...")
            scheduler = Scheduler(app.schedule_config)
            success = scheduler.generate_schedule()

            if not success:
                raise ValueError("Failed to generate schedule - validation errors detected")

            schedule = scheduler.schedule
            if not schedule:
                raise ValueError("Generated schedule is empty")

            app.schedule_config['schedule'] = schedule
            print("DEBUG: generate_schedule - Schedule generated and saved to config.")

            # *** NEW: Automatically export PDF summary after successful generation ***
            try:
                calendar_screen = self.manager.get_screen('calendar_view')
                calendar_screen._auto_export_schedule_summary(scheduler, app.schedule_config)
            except Exception as export_error:
                logging.warning(f"Auto PDF export failed, but schedule was generated successfully: {export_error}")
                # Don't fail the whole operation if PDF export fails

            popup = Popup(title='Success',
                         content=Label(text='Schedule generated successfully!'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()
            print("DEBUG: generate_schedule - Success popup opened.")

            print("DEBUG: generate_schedule - Preparing to switch to calendar_view...")
            self.manager.current = 'calendar_view'
            print("DEBUG: generate_schedule - Switched manager.current to calendar_view.")

        except Exception as e:
            error_message = f"Failed to generate schedule: {str(e)}"
            print(f"DEBUG: generate_schedule - ERROR: {error_message}")
            logging.error(error_message, exc_info=True)
            self.show_error(error_message)

    def generate_schedule_async(self):
        """Generate schedule in a separate thread to prevent UI freezing"""
        import threading
        from kivy.clock import Clock
        
        def run_generation():
            try:
                app = App.get_running_app()
                cfg = app.schedule_config
                
                scheduler = Scheduler(cfg)
                success = scheduler.generate_schedule()
                
                if success:
                    app.schedule_config['schedule'] = scheduler.schedule
                    # Schedule the PDF export on the main thread
                    Clock.schedule_once(lambda dt: self._handle_success(scheduler, cfg))
                else:
                    Clock.schedule_once(lambda dt: self.show_error("No se pudo generar un horario válido."))
                    
            except Exception as e:
                Clock.schedule_once(lambda dt: self.show_error(f"Error: {str(e)}"))
        
        # Start generation in background thread
        thread = threading.Thread(target=run_generation)
        thread.daemon = True
        thread.start()
    
    def _handle_success(self, scheduler, config):
        """Handle successful schedule generation on main thread"""
        try:
            calendar_screen = self.manager.get_screen('calendar_view')
            calendar_screen._auto_export_schedule_summary(scheduler, config)
        except Exception as export_error:
            logging.warning(f"Auto PDF export failed: {export_error}")
        
        popup = Popup(title='Success',
                     content=Label(text='Schedule generated successfully!'),
                     size_hint=(None, None), size=(400, 200))
        popup.open()
        self.manager.current = 'calendar_view'
        
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
            self.next_btn.text = 'Finalizar'
        else:
            self.next_btn.text = 'Siguiente'
    
        # Load data for current worker index
        self.load_worker_data()
    
        # Log entry for debugging
        logging.info(f"Entered WorkerDetailsScreen: Worker {current_index + 1}/{total_workers}")

class CalendarViewScreen(Screen):
    # Replace in your CalendarViewScreen class:

    def __init__(self, **kwargs):
        super(CalendarViewScreen, self).__init__(**kwargs)
        self.layout = BoxLayout(orientation='vertical', padding=10, spacing=5)

        # Header with title and navigation
        header = BoxLayout(orientation='horizontal', size_hint_y=0.1)
        self.month_label = Label(text='', size_hint_x=0.4)
        prev_month = Button(text='<', size_hint_x=0.1) # Adjust sizes if needed
        next_month = Button(text='>', size_hint_x=0.1)
        today_btn = Button(text='Today', size_hint_x=0.2)
        # RENAME the button text and UPDATE the binding
        summary_btn = Button(text='Global Summary', size_hint_x=0.2) # Changed text

        prev_month.bind(on_press=self.previous_month)
        next_month.bind(on_press=self.next_month)
        today_btn.bind(on_press=self.go_to_today)
        # Make sure this binding calls the RENAMED function
        summary_btn.bind(on_press=self.show_global_summary) # Calls the new function

        header.add_widget(prev_month)
        header.add_widget(self.month_label)
        header.add_widget(next_month)
        header.add_widget(today_btn)
        header.add_widget(summary_btn) # Add the summary button
        self.layout.add_widget(header)

        # Days of week header
        days_header = GridLayout(cols=7, size_hint_y=0.1)
        for day in ['Lun', 'Mar', 'Mx', 'Jue', 'Vn', 'Sab', 'Dom']:
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
        save_btn = Button(text='Guardar JSON')
        export_txt_btn = Button(text='Exportar a TXT')
        export_pdf_btn = Button(text='Exportar a PDF')
        reset_btn = Button(text='Resetear')  # Changed from stats_btn

        save_btn.bind(on_press=self.save_schedule)
        export_txt_btn.bind(on_press=self.export_schedule)
        export_pdf_btn.bind(on_press=self.export_to_pdf)
        reset_btn.bind(on_press=self.confirm_reset_schedule)  # New binding

        button_layout.add_widget(save_btn)
        button_layout.add_widget(export_txt_btn)
        button_layout.add_widget(export_pdf_btn)
        button_layout.add_widget(reset_btn)  # Changed from stats_btn
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
            
                if date.weekday() >= 4:
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
                f"  Total Shifts: {data['nº de guardias']}\n"
                f"  Weekend Shifts: {data['Findes']}\n"
                f"  Holiday Shifts: {data['Festivos']}\n"
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
        is_weekend = current_date.weekday() >= 4
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
        print("DEBUG: CalendarViewScreen.on_enter - Entered screen.") # <<< ENSURE THIS IS HERE
        app = App.get_running_app()
        try:
            print("DEBUG: CalendarViewScreen.on_enter - Accessing schedule_config...") # <<< ADD
            self.schedule = app.schedule_config.get('schedule', {})
            print(f"DEBUG: CalendarViewScreen.on_enter - Schedule loaded (is empty: {not self.schedule})") # <<< ADD

            if self.schedule:
                print("DEBUG: CalendarViewScreen.on_enter - Finding min date...") # <<< ADD
                self.current_date = min(self.schedule.keys())
                print(f"DEBUG: CalendarViewScreen.on_enter - Current date set to {self.current_date}") # <<< ADD
                print("DEBUG: CalendarViewScreen.on_enter - Calling display_month...") # <<< ADD
                self.display_month(self.current_date)
                print("DEBUG: CalendarViewScreen.on_enter - display_month finished.") # <<< ADD
            else:
                 print("DEBUG: CalendarViewScreen.on_enter - Schedule is empty, setting current_date to now.") # <<< ADD
                 self.current_date = datetime.now() # Fallback
                 self.month_label.text = self.current_date.strftime('%B %Y') # Update label
                 # Optionally clear the grid if needed
                 # self.calendar_grid.clear_widgets()
                 # self.details_layout.clear_widgets()

        except Exception as e:
            print(f"DEBUG: CalendarViewScreen.on_enter - ERROR: {e}") # <<< ADD ERROR PRINT
            logging.error(f"Error during CalendarViewScreen.on_enter: {e}", exc_info=True) # Keep logging
            # Optionally show an error popup here too
            popup = Popup(title='Screen Load Error', content=Label(text=f'Failed to load calendar: {e}'), size_hint=(None, None), size=(400, 200))
            popup.open()

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
            # Add white background to empty cells
            with empty_cell.canvas.before:
                Color(1, 1, 1, 1)  # White color (RGBA)
                Rectangle(pos=empty_cell.pos, size=empty_cell.size)
                # Add a light gray border
                Color(0.9, 0.9, 0.9, 1)  # Light gray border
                Line(rectangle=(empty_cell.x, empty_cell.y, empty_cell.width, empty_cell.height))
        
            # Bind position and size to ensure the background follows the cell
            empty_cell.bind(pos=self.update_rect, size=self.update_rect)
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

            # Set background color - Always start with white, then modify for special days
            # First set default to white
            bg_color = (1, 1, 1, 1)  # White

            # Then check if it's a special day and adjust color
            app = App.get_running_app()
            is_weekend = current.weekday() >= 5
            is_holiday = current in app.schedule_config.get('holidays', [])
            is_today = current.date() == datetime.now().date()
    
            if is_today:
                bg_color = (0.85, 0.91, 0.98, 1)  # Light blue for today, more solid
            elif is_holiday:
                bg_color = (1, 0.92, 0.92, 1)  # Light red for holidays, more solid
            elif is_weekend:
                bg_color = (1, 0.96, 0.96, 1)  # Very light red for weekends, more solid
        
            # Apply the background color
            with cell.canvas.before:
                Color(*bg_color)
                rect = Rectangle(pos=cell.pos, size=cell.size)
            
                # Add border
                Color(0.7, 0.7, 0.7, 1)  # Gray border
                Line(rectangle=(cell.x, cell.y, cell.width, cell.height))
        
            # Bind position and size to ensure rectangle follows cell
            cell.rect = rect
            cell.bind(pos=self.update_rect, size=self.update_rect)

            # Day number with special formatting for weekends/holidays
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
                    background_color=(0, 0, 0, 0),  # Transparent background
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
            # Add white background to empty cells
            with empty_cell.canvas.before:
                Color(1, 1, 1, 1)  # White color
                Rectangle(pos=empty_cell.pos, size=empty_cell.size)
                # Add a light gray border
                Color(0.9, 0.9, 0.9, 1)  # Light gray border
                Line(rectangle=(empty_cell.x, empty_cell.y, empty_cell.width, empty_cell.height))
        
            # Bind position and size to ensure the background follows the cell
            empty_cell.bind(pos=self.update_rect, size=self.update_rect)
            self.calendar_grid.add_widget(empty_cell)

        # Update the calendar grid's properties
        self.calendar_grid.rows = 6  # Fixed number of rows
        self.calendar_grid.cols = 7  # Fixed number of columns
        self.calendar_grid.spacing = [2, 2]  # Add spacing between cells
        self.calendar_grid.padding = [5, 5]  # Add padding around the grid

    # Add this helper method to your CalendarViewScreen class
    def update_rect(self, instance, value):
        """Update the rectangle position and size when the cell changes."""
        if hasattr(instance, 'rect'):
            instance.rect.pos = instance.pos
            instance.rect.size = instance.size

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
            is_weekend = date.weekday() >= 4
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
                schedule_data[date.strftime('%d-%m-%Y')] = workers
            
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

    def show_summary(self):
        """
        Generate and show a summary of the current schedule
        """
        try:
            # Get the schedule data
            app = App.get_running_app()
            if not app.schedule_config or not app.schedule_config.get('schedule'):
                popup = Popup(
                    title='Error',
                    content=Label(text="No schedule data available"),
                    size_hint=(None, None), 
                    size=(400, 200)
                )
                popup.open()
                return
    
            # Create a summary to display and also prepare PDF data
            month_stats = {
                'total_shifts': 0,
                'workers': {},
                'weekend_shifts': 0,
                'last_post_shifts': 0,
                'posts': {i: 0 for i in range(app.schedule_config.get('num_shifts', 0))},
                'worker_shifts': {}  # Dictionary to store shifts by worker
            }
        
            # Calculate statistics for the current month
            self.prepare_month_statistics(month_stats)
    
            # Display summary in a dialog and ask if user wants PDF
            if self.display_summary_dialog(month_stats):
                try:
                    self.export_summary_pdf(month_stats)
                except Exception as e:
                    logging.error(f"Failed to export summary PDF: {str(e)}", exc_info=True)
                    error_popup = Popup(
                        title='Error',
                        content=Label(text=f"Failed to export PDF: {str(e)}"),
                        size_hint=(None, None), 
                        size=(400, 200)
                    )
                    error_popup.open()
        
        except Exception as e:
            logging.error(f"Failed to show summary: {str(e)}", exc_info=True)
            popup = Popup(
                title='Error',
                content=Label(text=f"Failed to show summary: {str(e)}"),
                size_hint=(None, None), 
                size=(400, 200)
            )
            popup.open()

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
        
    def prepare_month_statistics(self, month_stats):
        """
        Calculate statistics for the current month including distributions.
        """
        if not self.current_date:
            return {} # Return empty if no date

        app = App.get_running_app()
        schedule = app.schedule_config.get('schedule', {})
        num_shifts = app.schedule_config.get('num_shifts', 0)
        holidays = app.schedule_config.get('holidays', []) # Get holidays

        # Initialize overall stats if not already present
        month_stats.setdefault('total_shifts', 0)
        month_stats.setdefault('workers', {})
        month_stats.setdefault('weekend_shifts', 0)
        month_stats.setdefault('last_post_shifts', 0)
        month_stats.setdefault('posts', {i: 0 for i in range(num_shifts)})
        month_stats.setdefault('worker_shifts', {})

        print(f"DEBUG: prepare_month_statistics - Processing month: {self.current_date.year}-{self.current_date.month}")
        for date, workers in schedule.items():
            # Filter for the current month being viewed
            if date.year == self.current_date.year and date.month == self.current_date.month:
                month_stats['total_shifts'] += len(workers)
                is_weekend = date.weekday() >= 4
                is_holiday = date in holidays

                for i, worker_id in enumerate(workers):
                    if worker_id is None:
                        continue

                    # --- Initialize worker stats dict if first time seen ---
                    if worker_id not in month_stats['workers']:
                        month_stats['workers'][worker_id] = {
                            'total': 0,
                            'weekends': 0,
                            'holidays': 0, # Added holiday count per worker
                            'last_post': 0,
                            'weekday_counts': {day: 0 for day in range(7)}, # Mon=0, Sun=6
                            'post_counts': {post: 0 for post in range(num_shifts)} # Post 0, 1, ...
                        }
                    if worker_id not in month_stats['worker_shifts']:
                         month_stats['worker_shifts'][worker_id] = []
                    # --- End initialization ---

                    # Add detailed shift info for the list
                    shift_detail = {
                        'date': date,
                        'day': date.strftime('%A'), # Full day name
                        'post': i + 1, # 1-based post number
                        'is_weekend': is_weekend,
                        'is_holiday': is_holiday
                    }
                    month_stats['worker_shifts'][worker_id].append(shift_detail)
                    # print(f"DEBUG: Added shift for {worker_id}: {shift_detail['date'].strftime('%d-%m-%Y')} Post {shift_detail['post']}") # Optional debug

                    # --- Increment counts ---
                    month_stats['workers'][worker_id]['total'] += 1
                    month_stats['workers'][worker_id]['weekday_counts'][date.weekday()] += 1
                    month_stats['workers'][worker_id]['post_counts'][i] += 1 # Use 0-based index 'i' for dict key

                    if is_weekend:
                        month_stats['weekend_shifts'] += 1 # Overall weekend count
                        month_stats['workers'][worker_id]['weekends'] += 1
                    if is_holiday:
                         month_stats['workers'][worker_id]['holidays'] += 1 # Worker holiday count

                    if i == num_shifts - 1: # Check if it's the last post (0-based index)
                        month_stats['last_post_shifts'] += 1 # Overall last post count
                        month_stats['workers'][worker_id]['last_post'] += 1
                    # --- End increment counts ---

        print(f"DEBUG: prepare_month_statistics - Finished. Worker stats keys: {list(month_stats['workers'].get(list(month_stats['workers'].keys())[0], {}).keys()) if month_stats['workers'] else 'No workers'}")
        return month_stats

    def prepare_statistics(self): # Removed month_stats parameter, it will create it
        """
        Calculate statistics for the ENTIRE schedule period.
        """
        app = App.get_running_app()
        if not hasattr(app, 'schedule_config') or not app.schedule_config:
             print("DEBUG: prepare_statistics - No schedule_config found.")
             return {} # Return empty if no config

        schedule = app.schedule_config.get('schedule', {})
        num_shifts = app.schedule_config.get('num_shifts', 0)
        holidays = app.schedule_config.get('holidays', [])
        start_date = app.schedule_config.get('start_date') # Get overall start
        end_date = app.schedule_config.get('end_date')     # Get overall end

        if not schedule:
            print("DEBUG: prepare_statistics - Schedule is empty.")
            return {}

        # Initialize the stats dictionary
        global_stats = {
            'total_shifts': 0,
            'workers': {},
            'weekend_shifts': 0,
            'last_post_shifts': 0,
            'posts': {i: 0 for i in range(num_shifts)},
            'worker_shifts': {},
            'period_start': start_date, # Store period boundaries
            'period_end': end_date
        }

        print(f"DEBUG: prepare_statistics - Processing ALL dates in schedule...")
        # Iterate through ALL dates in the loaded schedule
        for date, workers in schedule.items():
            # No date filtering needed here for global summary
            global_stats['total_shifts'] += len(workers)
            is_weekend = date.weekday() >= 4
            is_holiday = date in holidays

            for i, worker_id in enumerate(workers):
                if worker_id is None:
                    continue

                # Initialize worker stats dict if first time seen
                if worker_id not in global_stats['workers']:
                    global_stats['workers'][worker_id] = {
                        'total': 0, 'weekends': 0, 'holidays': 0, 'last_post': 0,
                        'weekday_counts': {day: 0 for day in range(7)},
                        'post_counts': {post: 0 for post in range(num_shifts)}
                    }
                if worker_id not in global_stats['worker_shifts']:
                     global_stats['worker_shifts'][worker_id] = []

                # Add detailed shift info for the list
                shift_detail = {
                    'date': date, 'day': date.strftime('%A'), 'post': i + 1,
                    'is_weekend': is_weekend, 'is_holiday': is_holiday
                }
                global_stats['worker_shifts'][worker_id].append(shift_detail)

                # Increment counts
                global_stats['workers'][worker_id]['total'] += 1
                global_stats['workers'][worker_id]['weekday_counts'][date.weekday()] += 1
                if i < num_shifts: # Ensure post index is valid
                    global_stats['workers'][worker_id]['post_counts'][i] += 1

                if is_weekend:
                    global_stats['weekend_shifts'] += 1
                    global_stats['workers'][worker_id]['weekends'] += 1
                if is_holiday:
                     global_stats['workers'][worker_id]['holidays'] += 1

                if i == num_shifts - 1:
                    global_stats['last_post_shifts'] += 1
                    global_stats['workers'][worker_id]['last_post'] += 1

        print(f"DEBUG: prepare_statistics - Finished GLOBAL calculation.")
        return global_stats # Return the newly created dictionary

    def display_summary_dialog(self, stats_data):
        """
        Display the detailed summary dialog (handles global stats).
        """
        print("DEBUG: display_summary_dialog called.")
    
        # Create the main content box
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
    
        # Create a proper ScrollView that takes most of the popup height
        scroll = ScrollView(size_hint=(1, 0.8))
    
        # Create the layout for the summary content that will be scrollable
        summary_layout = GridLayout(cols=1, spacing=10, size_hint_y=None, padding=[10, 10])
        summary_layout.bind(minimum_height=summary_layout.setter('height'))
    
        # --- Modify Title ---
        start = stats_data.get('period_start')
        end = stats_data.get('period_end')
        if start and end:
            period_str = f"{start.strftime('%d-%m-%Y')} to {end.strftime('%d-%m-%Y')}"
            title_text = f"Schedule Summary ({period_str})"
        else:
            title_text = "Schedule Summary (Full Period)"
    
        summary_title = Label(text=title_text, size_hint_y=None, height=40, bold=True)
        summary_layout.add_widget(summary_title)
    
        # --- Worker Details Header ---
        worker_header = Label(text="Worker Details:", size_hint_y=None, height=40, bold=True)
        summary_layout.add_widget(worker_header)
    
        # --- Loop Through Workers ---
        print("DEBUG: display_summary_dialog - Adding worker details...")
        weekdays_short = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        workers_stats = stats_data.get('workers', {})
        worker_shifts_all = stats_data.get('worker_shifts', {})
    
        if not workers_stats:
            # Add a message if there are no workers/stats
            summary_layout.add_widget(Label(text="No worker statistics found for this period.", size_hint_y=None, height=30))
        else:
            for worker_id, stats in sorted(stats_data['workers'].items(), key=numeric_sort_key):
                print(f"DEBUG: display_summary_dialog - Processing worker: {worker_id}")
                worker_box = BoxLayout(orientation='vertical', size_hint_y=None, padding=[5, 10], spacing=3)
                worker_box.bind(minimum_height=worker_box.setter('height'))  # Ensure proper height calculation
            
                # Get calculated stats for this worker
                total_w = stats.get('total', 0)
                weekends_w = stats.get('weekends', 0)
                holidays_w = stats.get('holidays', 0)
                last_post_w = stats.get('last_post', 0)
                weekday_counts = stats.get('weekday_counts', {})
                post_counts = stats.get('post_counts', {})
                worker_shifts = worker_shifts_all.get(worker_id, [])
            
                # --- Worker Summary Line ---
                summary_text = f"Worker {worker_id}: Total: {total_w} | Weekends: {weekends_w} | Holidays: {holidays_w} | Last Post: {last_post_w}"
                summary_label = Label(
                    text=summary_text, 
                    size_hint_y=None, 
                    height=25, 
                    bold=True, 
                    halign='left'
                )
                summary_label.bind(size=summary_label.setter('text_size'))  # Ensure text wrapping
                worker_box.add_widget(summary_label)
            
                # --- Weekday Distribution Line ---
                weekdays_str = "Weekdays: " + " ".join([f"{weekdays_short[i]}:{weekday_counts.get(i, 0)}" for i in range(7)])
                weekday_label = Label(
                    text=weekdays_str, 
                    size_hint_y=None, 
                    height=25, 
                    halign='left'
                )
                weekday_label.bind(size=weekday_label.setter('text_size'))  # Ensure text wrapping
                worker_box.add_widget(weekday_label)
            
                # --- Post Distribution Line ---
                posts_str = "Posts: " + " ".join([f"P{post+1}:{count}" for post, count in sorted(post_counts.items())])
                posts_label = Label(
                    text=posts_str, 
                    size_hint_y=None, 
                    height=25, 
                    halign='left'
                )
                posts_label.bind(size=posts_label.setter('text_size'))  # Ensure text wrapping
                worker_box.add_widget(posts_label)
            
                # --- Assigned Shifts Header ---
                shifts_header = Label(
                    text="Assigned Shifts:", 
                    size_hint_y=None, 
                    height=25, 
                    halign='left', 
                    bold=True
                )
                shifts_header.bind(size=shifts_header.setter('text_size'))  # Ensure text wrapping
                worker_box.add_widget(shifts_header)
            
                # --- List of Shifts ---
                shifts_container = GridLayout(cols=1, size_hint_y=None, spacing=2)
                shifts_container.bind(minimum_height=shifts_container.setter('height'))  # Important for scrolling
            
                if not worker_shifts:
                    no_shifts_label = Label(
                        text="  (No shifts assigned)", 
                        size_hint_y=None, 
                        height=20, 
                        halign='left'
                    )
                    no_shifts_label.bind(size=no_shifts_label.setter('text_size'))
                    shifts_container.add_widget(no_shifts_label)
                else:
                    for shift in sorted(worker_shifts, key=lambda x: x['date']):
                        date_str = shift['date'].strftime('%d-%m-%Y')
                        post_str = f"Post {shift['post']}"
                        day_type = ""
                        if shift['is_holiday']: 
                            day_type = " [HOLIDAY]"
                        elif shift['is_weekend']: 
                            day_type = " [WEEKEND]"
                    
                        shift_text = f" • {date_str} ({shift['day'][:3]}){day_type}: {post_str}"
                        shift_label = Label(
                            text=shift_text, 
                            size_hint_y=None, 
                            height=20, 
                            halign='left'
                        )
                        shift_label.bind(size=shift_label.setter('text_size'))
                        shifts_container.add_widget(shift_label)
            
                # Add the shifts container to the worker box
                shifts_container.height = max(20, len(worker_shifts) * 20)  # Set appropriate height
                worker_box.add_widget(shifts_container)
            
                # --- Calculate Height Dynamically ---
                base_height = 25 * 4  # Summary + Weekday + Posts + Shifts Header
                total_height = base_height + shifts_container.height + 20  # Add some padding
                worker_box.height = total_height
            
                # --- Separator ---
                separator = BoxLayout(size_hint_y=None, height=1)
                with separator.canvas:
                    from kivy.graphics import Color, Rectangle
                    Color(0.7, 0.7, 0.7, 1)
                    Rectangle(pos=separator.pos, size=separator.size)
            
                # Add the worker box and separator to the summary layout
                summary_layout.add_widget(worker_box)
                summary_layout.add_widget(separator)
    
        # Set the height of the summary layout to ensure scrolling works
        # This is critical - we need to ensure this layout takes appropriate space
        summary_layout.height = summary_layout.minimum_height
    
        # Add the summary layout to the scroll view
        scroll.add_widget(summary_layout)
    
        # Add the scroll view to the main content
        content.add_widget(scroll)
    
        # --- Buttons (Export PDF, Close) ---
        button_layout = BoxLayout(orientation='horizontal', size_hint_y=0.1, spacing=10)
        pdf_button = Button(text='Export PDF')
        close_button = Button(text='Close')
        button_layout.add_widget(pdf_button)
        button_layout.add_widget(close_button)
        content.add_widget(button_layout)
    
        # --- Popup Creation ---
        popup = Popup(
            title='Schedule Summary', 
            content=content, 
            size_hint=(0.9, 0.9), 
            auto_dismiss=False
        )
    
        # Button callbacks
        def on_pdf(instance):
            print("DEBUG: on_pdf callback triggered!")
            try:
                print("DEBUG: Calling export_summary_pdf from on_pdf...")
                self.export_summary_pdf(stats_data)
                print("DEBUG: export_summary_pdf call finished.")
            except Exception as e:
                print(f"DEBUG: Error calling export_summary_pdf from on_pdf: {e}")
                logging.error(f"Error during PDF export triggered from popup: {e}", exc_info=True)
                error_popup = Popup(
                    title='PDF Export Error', 
                    content=Label(text=f'Failed export: {e}'),
                    size_hint=(None, None), 
                    size=(400, 200)
                )
                error_popup.open()
            finally:
                popup.dismiss()
                print("DEBUG: Popup dismissed from on_pdf")
    
        def on_close(instance):
            print("DEBUG: on_close callback triggered!")
            popup.dismiss()
            print("DEBUG: Popup dismissed from on_close")
    
        pdf_button.bind(on_press=on_pdf)
        close_button.bind(on_press=on_close)
    
        # Show the popup
        print("DEBUG: Opening summary popup...")
        popup.open()
        print("DEBUG: Summary popup should be open.")
    
        # Return True to indicate success (for compatibility with existing code)
        return True
        
    def show_global_summary(self, instance):
        """Calculate and display a summary of the ENTIRE schedule period."""
        print("DEBUG: show_global_summary called.")
        try:
            print("DEBUG: show_global_summary - Calculating GLOBAL stats...")
            # Call the new global calculation function
            global_stats = self.prepare_statistics()

            if not global_stats:
                 print("DEBUG: show_global_summary - No stats returned.")
                 # Show popup if no stats?
                 popup = Popup(title='Info', content=Label(text='No schedule data available for summary.'),
                              size_hint=(None, None), size=(400, 200))
                 popup.open()
                 return
    
            print(f"DEBUG: show_global_summary - Stats calculated. Keys: {list(global_stats.keys())}")

            # Pass the global stats to the display function
            print("DEBUG: show_global_summary - Calling display_summary_dialog...")
            self.display_summary_dialog(global_stats) # Pass the dictionary
            print("DEBUG: show_global_summary - display_summary_dialog finished.")

        except Exception as e:
            popup = Popup(title='Error', content=Label(text=f'Failed to show summary: {str(e)}'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()
            logging.error(f"Global Summary error: {str(e)}", exc_info=True)
            print(f"DEBUG: show_global_summary - ERROR: {e}")

    def export_summary_pdf(self, stats_data): # Renamed param
        print("DEBUG: export_summary_pdf (main.py) called!")
        logging.info("Attempting to export GLOBAL summary PDF...") # Changed log message
        try:
            app = App.get_running_app()
            print("DEBUG: export_summary_pdf - Creating PDFExporter...")
            exporter = PDFExporter(app.schedule_config)

            print("DEBUG: export_summary_pdf - Calling exporter.export_summary_pdf...") # Calls the method in the OTHER file
            # --- CORRECT THE CALL AGAIN and Pass the correct data ---
            # The pdf_exporter method expects year, month, stats OR just stats?
            # Let's modify pdf_exporter.py to just take stats for the summary.
            filename = exporter.export_summary_pdf(stats_data) # Pass the whole stats dictionary
            # --- END CORRECTION ---

            if filename: # Check if export was successful (returned filename)
                print(f"DEBUG: export_summary_pdf - Export successful: {filename}")
                popup = Popup(title='Success', content=Label(text=f'Summary exported to {filename}'),
                             size_hint=(None, None), size=(400, 200))
                popup.open()
            else:
                 print(f"DEBUG: export_summary_pdf - Export failed (no filename returned).")
                 # Error should have been raised by exporter, but just in case
                 popup = Popup(title='Error', content=Label(text='PDF export failed internally.'),
                              size_hint=(None, None), size=(400, 200))
                 popup.open()
            pass # Placeholder

        except AttributeError as ae:
             print(f"DEBUG: export_summary_pdf - ATTRIBUTE ERROR: {ae}")
             logging.error(f"AttributeError during PDF export: {ae}", exc_info=True)
             popup = Popup(title='Export Error', content=Label(text=f'PDF export failed.\nMethod mismatch.\nError: {ae}'),
                          size_hint=(None, None), size=(400, 250))
             popup.open()
        except Exception as e:
            print(f"DEBUG: export_summary_pdf - ERROR: {e}")
            logging.error(f"Failed to export summary PDF: {str(e)}", exc_info=True)
            popup = Popup(title='Error', content=Label(text=f'Failed to export PDF: {str(e)}'),
                         size_hint=(None, None), size=(400, 200))
            popup.open()
        
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

    def _auto_export_schedule_summary(self, scheduler, config):
        """Automatically export PDF summary after schedule generation"""
        from pdf_exporter import PDFExporter
        from datetime import datetime
        import os
        import subprocess
        import platform
        
        try:
            # Create PDF exporter with the generated schedule
            schedule_config_for_export = {
                'schedule': scheduler.schedule,
                'workers_data': config.get('workers_data', []),
                'num_shifts': config.get('num_shifts', 0),
                'holidays': config.get('holidays', [])
            }
            
            pdf_exporter = PDFExporter(schedule_config_for_export)
            
            # Generate statistics for the PDF
            stats_data = self._generate_stats_for_export(scheduler, config)
            
            # Export the PDF
            filename = pdf_exporter.export_summary_pdf(stats_data)
            
            if filename and os.path.exists(filename):
                logging.info(f"Schedule summary automatically exported to: {filename}")
                
                # Automatically open the PDF
                self._open_pdf_file(filename)
                
                # Show success popup
                from kivy.uix.popup import Popup
                from kivy.uix.label import Label
                popup = Popup(
                    title='Schedule Generated & Exported', 
                    content=Label(text=f'Schedule generated successfully!\nPDF exported to: {filename}\nFile opened automatically.'),
                    size_hint=(None, None), 
                    size=(500, 250)
                )
                popup.open()
                
        except Exception as e:
            logging.error(f"Auto export failed: {e}")
            raise e

    def _generate_stats_for_export(self, scheduler, config):
        """Generate statistics data for PDF export"""
        from datetime import datetime
        
        workers_stats = {}
        worker_shifts_all = {}
        
        for worker in config.get('workers_data', []):
            worker_id = worker['id']
            assignments = scheduler.worker_assignments.get(worker_id, set())
            
            # Basic stats calculation
            total_shifts = len(assignments)
            weekend_shifts = sum(1 for date in assignments if date.weekday() >= 4)
            holiday_shifts = sum(1 for date in assignments if date in config.get('holidays', []))
            
            # Post distribution
            post_counts = {}
            weekday_counts = {i: 0 for i in range(7)}
            
            shifts_list = []
            for date in assignments:
                if date in scheduler.schedule:
                    try:
                        post = scheduler.schedule[date].index(worker_id)
                        post_counts[post] = post_counts.get(post, 0) + 1
                        
                        shifts_list.append({
                            'date': date,
                            'day': date.strftime('%A'),
                            'post': post + 1,
                            'is_weekend': date.weekday() >= 4,
                            'is_holiday': date in config.get('holidays', [])
                        })
                    except ValueError:
                        pass
                
                weekday_counts[date.weekday()] += 1
            
            workers_stats[worker_id] = {
                'total': total_shifts,
                'weekends': weekend_shifts,
                'holidays': holiday_shifts,
                'last_post': post_counts.get(config.get('num_shifts', 1) - 1, 0),
                'weekday_counts': weekday_counts,
                'post_counts': post_counts
            }
            
            worker_shifts_all[worker_id] = shifts_list
        
        return {
            'period_start': config.get('start_date', datetime.now()),
            'period_end': config.get('end_date', datetime.now()),
            'workers': workers_stats,
            'worker_shifts': worker_shifts_all
        }

    def _open_pdf_file(self, filename):
        """Automatically open the generated PDF file"""
        try:
            import os
            import subprocess
            import platform
            
            if platform.system() == 'Windows':
                os.startfile(filename)
            elif platform.system() == 'Darwin':  # macOS
                subprocess.run(['open', filename])
            else:  # Linux
                subprocess.run(['xdg-open', filename])
                
        except Exception as e:
            logging.warning(f"Could not auto-open PDF file: {e}")

    def confirm_reset_schedule(self, instance):
        """Show confirmation dialog before resetting schedule"""
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
    
        # Warning message
        message = Label(
            text='Are you sure you want to reset the schedule?\n'
                 'This will clear all assignments and return to setup.',
            halign='center'
        )
        content.add_widget(message)
    
        # Button layout
        button_layout = BoxLayout(
            orientation='horizontal', 
            size_hint_y=None,
            height=40,
            spacing=10
        )
    
        # Confirm and cancel buttons
        confirm_btn = Button(text='Yes, Reset')
        cancel_btn = Button(text='Cancel')
    
        button_layout.add_widget(confirm_btn)
        button_layout.add_widget(cancel_btn)
        content.add_widget(button_layout)
    
        # Create popup
        popup = Popup(
            title='Confirm Reset',
            content=content,
            size_hint=(None, None),
            size=(400, 200),
            auto_dismiss=False
        )
    
        # Define button actions
        confirm_btn.bind(on_press=lambda x: self.reset_schedule(popup))
        cancel_btn.bind(on_press=popup.dismiss)
    
        popup.open()

    def reset_schedule(self, popup):
        """Reset the schedule but keep worker data and return to worker details screen"""
        try:
            app = App.get_running_app()
    
            # Keep ALL current settings including worker data
            current_config = app.schedule_config.copy()
    
            # Only clear the generated schedule, keep everything else
            current_config['schedule'] = {}
            current_config['current_worker_index'] = 0
        
            # Update the app config
            app.schedule_config = current_config
    
            # Dismiss the popup
            popup.dismiss()
    
            # Show success message
            success = Popup(
                title='Success',
                content=Label(text='Schedule has been reset.\nWorker data preserved.\nReturning to worker details...'),
                size_hint=(None, None),
                size=(400, 200)
            )
            success.open()
    
            # Schedule a callback to close the popup and navigate
            from kivy.clock import Clock
            Clock.schedule_once(lambda dt: self.navigate_after_reset(success), 2)
    
        except Exception as e:
            # Show error
            error_popup = Popup(
                title='Error',
                content=Label(text=f'Failed to reset: {str(e)}'),
                size_hint=(None, None),
                size=(400, 200)
            )
            error_popup.open()
    
            # Dismiss the confirmation popup
            popup.dismiss()

    def navigate_after_reset(self, popup):
        """Navigate to worker details after reset"""
        popup.dismiss()
        self.manager.current = 'worker_details'

class GenerateScheduleScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # A button to kick off scheduling
        btn = Button(text="Generar horario", size_hint=(1, None), height=50)
        btn.bind(on_press=self.on_generate)
        self.add_widget(btn)

    def on_generate(self, instance):
        app = App.get_running_app()
        cfg = getattr(app, "schedule_config", None)
        if not cfg:
            return self._error("No hay configuración de turno guardada.")

        # Sanity‐check that the gap parameters exist
        if "gap_between_shifts" not in cfg or "max_consecutive_weekends" not in cfg:
            return self._error("Faltan parámetros 'gap_between_shifts' o 'max_consecutive_weekends'.")

        # Make sure workers list is ready
        if not cfg.get("workers_data"):
            return self._error("No se han introducido los datos de los médicos.")

        try:
            # Pass the entire dict (with your UI‐entered gaps) into Scheduler:
            scheduler = Scheduler(cfg)
            success   = scheduler.generate_schedule()
            if not success:
                return self._error("No se pudo generar un horario válido.")

            # Store for later display/review
            app.final_schedule = scheduler.schedule

            # *** NEW: Automatically export PDF summary after successful generation ***
            try:
                self._auto_export_schedule_summary(scheduler, cfg)
            except Exception as export_error:
                logging.warning(f"Auto PDF export failed, but schedule was generated successfully: {export_error}")
                # Don't fail the whole operation if PDF export fails

            # Switch to your “review” screen:
            self.manager.current = "schedule_review"

        except SchedulerError as e:
            self._error(f"Error al generar: {e}")

    def _error(self, msg):
        p = Popup(title="Error", content=Label(text=msg),
                  size_hint=(None, None), size=(400, 200))
        p.open()
              
class ShiftManagerApp(App):
    def __init__(self, **kwargs):
        super(ShiftManagerApp, self).__init__(**kwargs)
        self.schedule_config = {}
        self.current_user = 'saldo27'
        # We'll set the datetime when creating the Scheduler instance

    def build(self):
        print("DEBUG: ShiftManagerApp build method started.") # <<< ADD THIS LINE
        sm = ScreenManager()
        # --- Add PasswordScreen FIRST ---
        sm.add_widget(PasswordScreen(name='password'))
        # --- Add other screens AFTER ---
        sm.add_widget(WelcomeScreen(name='welcome'))
        sm.add_widget(SetupScreen(name='setup'))
        sm.add_widget(WorkerDetailsScreen(name='worker_details'))
        sm.add_widget(CalendarViewScreen(name='calendar_view'))

        # Print the list of screens in the manager
        print(f"DEBUG: Screens in manager: {[s.name for s in sm.screens]}") # <<< ADD THIS LINE
        # Print the default current screen
        print(f"DEBUG: Default current screen: {sm.current}") # <<< ADD THIS LINE
        return sm

    def main():
        # Initialize your Kivy app
        app = MyApp()
        app.run()

if __name__ == '__main__':
    ShiftManagerApp().run()
