# Imports
from datetime import datetime, timedelta
import calendar
import logging
import requests
from zoneinfo import ZoneInfo

class DateTimeUtils:
    """Date and time utility functions"""
    
    # Methods
    def __init__(self):
        """Initialize the date/time utilities"""
        logging.info("DateTimeUtils initialized")
        
    def get_spain_time(self):
        """Get current time in Spain timezone with fallback options"""
        try:
            response = requests.get(
                'http://worldtimeapi.org/api/timezone/Europe/Madrid',
                timeout=5,
                verify=True
            )
            
            if response.status_code == 200:
                time_data = response.json()
                return datetime.fromisoformat(time_data['datetime']).replace(tzinfo=None)
                
        except (requests.RequestException, ValueError) as e:
            logging.warning(f"Error getting time from API: {str(e)}")

        try:
            spain_tz = ZoneInfo('Europe/Madrid')
            return datetime.now(spain_tz).replace(tzinfo=None)
        except Exception as e:
            logging.error(f"Fallback time error: {str(e)}")
            return datetime.utcnow()
        
    def parse_dates(self, date_str):
        """Parse semicolon-separated dates"""
        if not date_str:
            return []

        dates = []
        for date_text in date_str.split(';'):
            date_text = date_text.strip()
            if date_text:
                try:
                    dates.append(datetime.strptime(date_text, '%d-%m-%Y'))
                except ValueError as e:
                    logging.warning(f"Invalid date format '{date_text}' - {str(e)}")
        return dates

    def parse_date_ranges(self, date_ranges_str):
        """Parse semicolon-separated date ranges"""
        if not date_ranges_str:
            return []

        ranges = []
        for date_range in date_ranges_str.split(';'):
            date_range = date_range.strip()
            try:
                if ' - ' in date_range:
                    start_str, end_str = date_range.split(' - ')
                    start = datetime.strptime(start_str.strip(), '%d-%m-%Y')
                    end = datetime.strptime(end_str.strip(), '%d-%m-%Y')
                    ranges.append((start, end))
                else:
                    date = datetime.strptime(date_range, '%d-%m-%Y')
                    ranges.append((date, date))
            except ValueError as e:
                logging.warning(f"Invalid date range format '{date_range}' - {str(e)}")
        return ranges
    
    def is_weekend_day(self, date, holidays=None):
        """
        Check if date is a weekend day, holiday, or pre-holiday

        Args:
            date: datetime object
            holidays: optional list of holidays (if None, no holiday check)
        Returns:
            bool: True if weekend/holiday/pre-holiday, False otherwise
        """
        # Check if it's a holiday
        if holidays is not None:
            if date in holidays:
                return True
    
            # Check if it's a pre-holiday (day before a holiday)
            next_day = date + timedelta(days=1)
            if next_day in holidays:
                return True

        # Check if it's a weekend (Friday, Saturday, Sunday)
        return date.weekday() >= 4  # 4 = Friday, 5 = Saturday, 6 = Sunday
        
    def is_holiday(self, date, holidays=None):
        """
        Check if a date is a holiday
    
        Args:
            date: datetime object
            holidays: optional list of holidays
        Returns:
            bool: True if holiday, False otherwise
        """
        if holidays is None:
            return False
        return date in holidays

    def is_pre_holiday(self, date, holidays=None):
        """
        Check if a date is the day before a holiday
    
        Args:
            date: datetime object
            holidays: optional list of holidays
        Returns:
            bool: True if pre-holiday, False otherwise
        """
        if holidays is None:
            return False
        next_day = date + timedelta(days=1)
        return next_day in holidays

    def get_weekend_start(self, date, holidays=None):
        """
        Get the start date (Friday) of the weekend containing this date
    
        Args:
            date: datetime object
            holidays: optional list of holidays
        Returns:
            datetime: Friday date of the weekend (or holiday start)
        """
        if self.is_pre_holiday(date, holidays):
            return date
        elif self.is_holiday(date, holidays):
            return date - timedelta(days=1)
        else:
            # Regular weekend - get to Friday
            weekday = date.weekday()
            if weekday < 4:  # Monday-Thursday
                return date + timedelta(days=4 - weekday)  # Move forward to Friday
            else:  # Friday-Sunday
                return date - timedelta(days=weekday - 4)  # Move back to Friday
        
    def get_effective_weekday(self, date, holidays=None):
        """
        Get the effective weekday, treating holidays as Sundays and pre-holidays as Fridays
    
        Args:
            date: datetime object
            holidays: optional list of holidays
        Returns:
            int: 0-6 representing Monday-Sunday, with holidays as 6 and pre-holidays as 4
        """
        if self.is_holiday(date, holidays):
            return 6  # Sunday
        if self.is_pre_holiday(date, holidays):
            return 4  # Friday
        return date.weekday()
    
    def _get_schedule_months(self):
        """
        Calculate available days per month in schedule period
    
        Returns:
            dict: Dictionary with month keys and their available days count
        """
        month_days = {}
        current = self.start_date
        while current <= self.end_date:
            month_key = f"{current.year}-{current.month:02d}"
        
            if month_key not in month_days:
                month_days[month_key] = 0
        
            # Only count days within our schedule period
            if self.start_date <= current <= self.end_date:
                month_days[month_key] += 1
            
            current += timedelta(days=1)
        
            # Move to first day of next month if we've finished current month
            if current.day == 1:
                next_month = current
            else:
                # Get first day of next month
                next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
                current = next_month
    
        return month_days
    
    def _get_month_key(self, date):
        """
        Get standardized month key for a date
        
        Args:
            date: datetime object
        Returns:
            str: Month key in format 'YYYY-MM'
        """
        return f"{date.year}-{date.month:02d}"

    def _days_between(self, date1, date2):
        """
        Calculate the number of days between two dates
        
        Args:
            date1: datetime object
            date2: datetime object
        Returns:
            int: Absolute number of days between dates
        """
        return abs((date2 - date1).days)

    def _is_same_month(self, date1, date2):
        """
        Check if two dates are in the same month
        
        Args:
            date1: datetime object
            date2: datetime object
        Returns:
            bool: True if same year and month, False otherwise
        """
        return date1.year == date2.year and date1.month == date2.month
      
    def _get_month_dates(self, year, month):
        """
        Get all dates in a specific month
        
        Args:
            year: int
            month: int
        Returns:
            list: List of datetime objects for each day in the month
        """
        num_days = calendar.monthrange(year, month)[1]
        return [
            datetime(year, month, day)
            for day in range(1, num_days + 1)
        ]

    def _get_month_workdays(self, year, month):
        """
        Get all workdays (non-holidays, non-weekends) in a specific month
        
        Args:
            year: int
            month: int
        Returns:
            list: List of datetime objects for workdays in the month
        """
        return [
            date for date in self._get_month_dates(year, month)
            if not self._is_weekend_day(date) and not self._is_holiday(date)
        ]
