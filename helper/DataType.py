import datetime

def c_datetime(value):
    try:
        # Adjust the format based on the format of your datetime strings in the database
        datetime_obj = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        return datetime_obj
    except ValueError:
        # Handle any parsing errors here
        return None  # Or raise an exception, depending on your error handling strategy

def c_stringtonum(value):
    return