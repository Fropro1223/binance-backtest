from datetime import datetime, timedelta
import pytz

# Constants
TZ_IST = pytz.timezone('Europe/Istanbul')

def get_current_time_ist():
    """Returns current time in Istanbul timezone."""
    return datetime.now(TZ_IST)

def get_last_completed_week_end():
    """
    Returns the end timestamp of the last completed week.
    Week definition: Sunday 03:00 IST to Sunday 03:00 IST.
    
    If today is Monday, last completed week end was yesterday (Sunday) 03:00.
    If today is Sunday 02:00, last completed week end was LAST Sunday 03:00.
    If today is Sunday 04:00, last completed week end was TODAY 03:00.
    """
    now = get_current_time_ist()
    
    # Is today Sunday? (weekday 6)
    is_sunday = now.weekday() == 6
    
    # Target hour is 03:00
    target_hour = 3
    
    if is_sunday and now.hour >= target_hour:
        # We are past the boundary today
        days_to_subtract = 0
    else:
        # We need to go back to previous Sunday
        # Monday(0) -> -1 day -> Sunday
        # ...
        # Sunday(6) -> -0 days if < 3am -> actually -7 days? No.
        # weekday() returns 0 for Monday, 6 for Sunday.
        # logic: (weekday + 1) % 7 gives days since Sunday?
        # Mon(0) -> 1 day since Sun
        # Tue(1) -> 2 days since Sun
        # Sun(6) -> 0 days since Sun (but handled above)
        
        days_since_sunday = (now.weekday() + 1) % 7
        if days_since_sunday == 0: 
            # It's Sunday < 3am, so we want LAST Sunday
            days_to_subtract = 7
        else:
            days_to_subtract = days_since_sunday
            
    last_sunday = now - timedelta(days=days_to_subtract)
    
    # Set exact time
    last_completed = last_sunday.replace(hour=3, minute=0, second=0, microsecond=0)
    
    # Safety check: if calculation inadvertently resulted in future (shouldn't happen), fix it
    if last_completed > now:
        last_completed -= timedelta(days=7)
        
    return last_completed

def generate_weekly_ranges(start_dt: datetime, end_dt: datetime):
    """
    Generates (start, end) tuples for weeks between start_dt and end_dt.
    All datetimes should be timezone-aware (IST).
    intervals are [start, end).
    """
    current_start = start_dt
    
    # Align start to a Sunday 03:00? 
    # Ideally start_dt should already be aligned by the caller logic, 
    # but let's assume we iterate by 7 days.
    
    while current_start < end_dt:
        current_end = current_start + timedelta(days=7)
        if current_end > end_dt:
            # Should not happen in strict weekly mode, but for robustness:
            break
            
        yield (current_start, current_end)
        current_start = current_end

def format_filename_ts(dt: datetime) -> str:
    """Formats datetime for filenames: YYYY-MM-DD_HH-mm"""
    return dt.strftime("%Y-%m-%d_%H-%M")

def utc_to_ist(dt_utc):
    return dt_utc.astimezone(TZ_IST)
