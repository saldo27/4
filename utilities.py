# Imports
from datetime import datetime, timedelta
import calendar
import logging
import requests
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional, Union

def numeric_sort_key(item):
    """
    Attempts to convert the first element of a tuple (the key) to an integer
    for sorting. Returns a tuple to prioritize numeric keys and handle errors.
    item[0] is assumed to be the worker ID (key).
    """
    try:
        return (0, int(item[0]))  # (0, numeric_value) - sorts numbers first
    except (ValueError, TypeError):
        return (1, item[0])  # (1, original_string) - sorts non-numbers after numbers


class DateTimeUtils:
    """Centralized date and time utility functions"""
    
    def __init__(self):
        """Initialize the date/time utilities"""
        logging.info("DateTimeUtils initialized")
        
    # ========================================
    # 1. TIME ZONE AND CURRENT TIME
    # ========================================
    
    def get_spain_time(self) -> datetime:
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
    
    # ========================================
    # 2. DATE PARSING AND FORMATTING
    # ========================================
    
    def parse_dates(self, date_str: str) -> List[datetime]:
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

    def parse_date_ranges(self, date_ranges_str: str) -> List[Tuple[datetime, datetime]]:
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
    
    def format_date(self, date: datetime, format_str: str = '%d-%m-%Y') -> str:
        """Format a date according to the specified format"""
        return date.strftime(format_str)
    
    def format_date_range(self, start_date: datetime, end_date: datetime, 
                         format_str: str = '%d-%m-%Y') -> str:
        """Format a date range"""
        return f"{start_date.strftime(format_str)} - {end_date.strftime(format_str)}"
    
    # ========================================
    # 3. WEEKEND AND HOLIDAY LOGIC
    # ========================================
    
    def is_weekend_day(self, date: datetime, holidays_list: Optional[List[datetime]] = None) -> bool:
        """
        Check if a date is a weekend day or holiday
    
        Args:
            date: Date to check
            holidays_list: Optional list of holiday dates to check against
    
        Returns:
            bool: True if date is a weekend day (Fri, Sat, Sun) or holiday
        """
        if holidays_list is None:
            holidays_list = []
        
        # Check if it's Friday, Saturday or Sunday
        if date.weekday() >= 4:  # 4=Friday, 5=Saturday, 6=Sunday
            return True
        
        # Check if it's a holiday
        if date in holidays_list:
            return True
        
        # Check if it's a day before holiday (treated as special)
        next_day = date + timedelta(days=1)
        if next_day in holidays_list:
            return True
        
        return False

    def is_holiday(self, date: datetime, holidays_list: Optional[List[datetime]] = None) -> bool:
        """
        Check if a date is a holiday
        
        Args:
            date: Date to check
            holidays_list: Optional list of holiday dates to check against
        
        Returns:
            bool: True if the date is a holiday, False otherwise
        """
        if holidays_list is None:
            holidays_list = []
        return date in holidays_list

    def is_pre_holiday(self, date: datetime, holidays_list: Optional[List[datetime]] = None) -> bool:
        """
        Check if a date is the day before a holiday
        
        Args:
            date: Date to check
            holidays_list: Optional list of holiday dates to check against
        
        Returns:
            bool: True if the next day is a holiday, False otherwise
        """
        if holidays_list is None:
            holidays_list = []
        next_day = date + timedelta(days=1)
        return next_day in holidays_list

    def is_workday(self, date: datetime, holidays_list: Optional[List[datetime]] = None) -> bool:
        """
        Check if a date is a regular workday (not weekend or holiday)
        
        Args:
            date: Date to check
            holidays_list: Optional list of holiday dates
        
        Returns:
            bool: True if it's a workday, False otherwise
        """
        return not self.is_weekend_day(date, holidays_list)

    def get_weekend_start(self, date: datetime, holidays: Optional[List[datetime]] = None) -> datetime:
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

    def get_effective_weekday(self, date: datetime, holidays: Optional[List[datetime]] = None) -> int:
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
    
    # ========================================
    # 4. DATE CALCULATIONS AND UTILITIES
    # ========================================
    
    def days_between(self, date1: datetime, date2: datetime) -> int:
        """
        Calculate the number of days between two dates
        
        Args:
            date1: datetime object
            date2: datetime object
        Returns:
            int: Absolute number of days between dates
        """
        return abs((date2 - date1).days)

    def is_same_month(self, date1: datetime, date2: datetime) -> bool:
        """
        Check if two dates are in the same month
        
        Args:
            date1: datetime object
            date2: datetime object
        Returns:
            bool: True if same year and month, False otherwise
        """
        return date1.year == date2.year and date1.month == date2.month

    def get_date_range(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """
        Get a list of all dates between start and end (inclusive)
        
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            list: List of datetime objects for each day in the range
        """
        current = start_date
        dates = []
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    def get_workdays_in_range(self, start_date: datetime, end_date: datetime, 
                             holidays_list: Optional[List[datetime]] = None) -> List[datetime]:
        """
        Get all workdays in a date range
        
        Args:
            start_date: Start date
            end_date: End date
            holidays_list: Optional list of holiday dates
        
        Returns:
            list: List of workday datetime objects
        """
        return [
            date for date in self.get_date_range(start_date, end_date)
            if self.is_workday(date, holidays_list)
        ]

    def get_weekends_in_range(self, start_date: datetime, end_date: datetime, 
                             holidays_list: Optional[List[datetime]] = None) -> List[datetime]:
        """
        Get all weekend days in a date range
        
        Args:
            start_date: Start date
            end_date: End date
            holidays_list: Optional list of holiday dates
        
        Returns:
            list: List of weekend datetime objects
        """
        return [
            date for date in self.get_date_range(start_date, end_date)
            if self.is_weekend_day(date, holidays_list)
        ]
    
    # ========================================
    # 5. MONTH-RELATED UTILITIES
    # ========================================
    
    def get_month_key(self, date: datetime) -> str:
        """
        Get standardized month key for a date
        
        Args:
            date: datetime object
        Returns:
            str: Month key in format 'YYYY-MM'
        """
        return f"{date.year}-{date.month:02d}"

    def get_month_dates(self, year: int, month: int) -> List[datetime]:
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

    def get_month_workdays(self, year: int, month: int, 
                          holidays_list: Optional[List[datetime]] = None) -> List[datetime]:
        """
        Get all workdays (non-holidays, non-weekends) in a specific month
        
        Args:
            year: int
            month: int
            holidays_list: Optional list of holiday dates
        Returns:
            list: List of datetime objects for workdays in the month
        """
        return [
            date for date in self.get_month_dates(year, month)
            if self.is_workday(date, holidays_list)
        ]

    def get_schedule_months(self, start_date: datetime, end_date: datetime) -> dict:
        """
        Calculate available days per month in schedule period
        
        Args:
            start_date: Schedule start date
            end_date: Schedule end date
    
        Returns:
            dict: Dictionary with month keys and their available days count
        """
        month_days = {}
        current = start_date
        
        while current <= end_date:
            month_key = self.get_month_key(current)
            
            if month_key not in month_days:
                month_days[month_key] = 0
            
            month_days[month_key] += 1
            current += timedelta(days=1)
    
        return month_days

    def get_months_in_range(self, start_date: datetime, end_date: datetime) -> List[str]:
        """
        Get list of month keys in a date range
        
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            list: List of month keys in format 'YYYY-MM'
        """
        months = set()
        current = start_date
        
        while current <= end_date:
            months.add(self.get_month_key(current))
            current += timedelta(days=1)
        
        return sorted(list(months))
    
    # ========================================
    # 6. ADVANCED DATE UTILITIES
    # ========================================
    
    def get_next_weekday(self, date: datetime, weekday: int) -> datetime:
        """
        Get the next occurrence of a specific weekday
        
        Args:
            date: Starting date
            weekday: Target weekday (0=Monday, 6=Sunday)
        
        Returns:
            datetime: Next occurrence of the weekday
        """
        days_ahead = weekday - date.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        return date + timedelta(days=days_ahead)

    def get_previous_weekday(self, date: datetime, weekday: int) -> datetime:
        """
        Get the previous occurrence of a specific weekday
        
        Args:
            date: Starting date
            weekday: Target weekday (0=Monday, 6=Sunday)
        
        Returns:
            datetime: Previous occurrence of the weekday
        """
        days_behind = date.weekday() - weekday
        if days_behind <= 0:  # Target day hasn't happened this week
            days_behind += 7
        return date - timedelta(days=days_behind)

    def get_week_boundaries(self, date: datetime) -> Tuple[datetime, datetime]:
        """
        Get the start and end of the week containing the given date
        
        Args:
            date: Date within the week
        
        Returns:
            tuple: (week_start, week_end) where week starts on Monday
        """
        days_since_monday = date.weekday()
        week_start = date - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=6)
        return week_start, week_end

    def get_month_boundaries(self, date: datetime) -> Tuple[datetime, datetime]:
        """
        Get the first and last day of the month containing the given date
        
        Args:
            date: Date within the month
        
        Returns:
            tuple: (month_start, month_end)
        """
        month_start = date.replace(day=1)
        next_month = month_start.replace(month=month_start.month % 12 + 1)
        if month_start.month == 12:
            next_month = next_month.replace(year=next_month.year + 1)
        month_end = next_month - timedelta(days=1)
        return month_start, month_end

    # ========================================
    # 7. CONVERSION UTILITIES
    # ========================================
    
    def normalize_to_date(self, date_input: Union[datetime, str]) -> datetime:
        """
        Normalize various date inputs to datetime objects
        
        Args:
            date_input: Can be datetime object or string in DD-MM-YYYY format
        
        Returns:
            datetime: Normalized datetime object
        """
        if isinstance(date_input, datetime):
            return date_input
        elif isinstance(date_input, str):
            try:
                return datetime.strptime(date_input, '%d-%m-%Y')
            except ValueError:
                try:
                    return datetime.strptime(date_input, '%Y-%m-%d')
                except ValueError as e:
                    raise ValueError(f"Unable to parse date string '{date_input}': {e}")
        else:
            raise TypeError(f"Expected datetime or string, got {type(date_input)}")

    def ensure_datetime_list(self, dates: List[Union[datetime, str]]) -> List[datetime]:
        """
        Ensure all items in a list are datetime objects
        
        Args:
            dates: List of dates (can be mixed datetime and string types)
        
        Returns:
            list: List of datetime objects
        """
        return [self.normalize_to_date(date) for date in dates]


# ========================================
# 8. GLOBAL UTILITY FUNCTIONS
# ========================================

# Singleton instance for global use
_date_utils_instance = None

def get_date_utils() -> DateTimeUtils:
    """Get singleton instance of DateTimeUtils"""
    global _date_utils_instance
    if _date_utils_instance is None:
        _date_utils_instance = DateTimeUtils()
    return _date_utils_instance

# Convenience functions for common operations
def parse_date(date_str: str) -> datetime:
    """Parse a single date string"""
    return get_date_utils().normalize_to_date(date_str)

def is_weekend(date: datetime, holidays: Optional[List[datetime]] = None) -> bool:
    """Check if date is weekend or holiday"""
    return get_date_utils().is_weekend_day(date, holidays)

def is_holiday(self, date, holidays_list=None):
    """Check if a date is a holiday"""
    if holidays_list is None:
        holidays_list = []
    return date in holidays_list

def is_pre_holiday(self, date, holidays_list=None):
    """Check if a date is the day before a holiday"""
    if holidays_list is None:
        holidays_list = []
    next_day = date + timedelta(days=1)
    return next_day in holidays_list
    
def is_workday(date: datetime, holidays: Optional[List[datetime]] = None) -> bool:
    """Check if date is a workday"""
    return get_date_utils().is_workday(date, holidays)

def days_between(date1: datetime, date2: datetime) -> int:
    """Calculate days between two dates"""
    return get_date_utils().days_between(date1, date2)
