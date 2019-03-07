import os
import re
import sys
import datetime as dt
import time
import logging
import pandas as pd
from pandas import DataFrame, Series
from dateutil.relativedelta import relativedelta


def dec_err_handler(retries=0):
    """
    Decorator function to handle logging and retries.
    Usage: Call without the retries parameter to have it log exceptions only.
    Assumptions: 1) args[0] is "self", and "self.logger" has been instantiated.
    Ref: https://stackoverflow.com/questions/11731136/python-class-method-decorator-with-self-arguments
    :retries: Number of times to retry, in addition to original try.
    """
    def wrap(f):  # Doing the wrapping here. Called during the decoration of the function.
        def wrapped_err_handler(*args, **kwargs):  # This way, kwargs can be handled too.
            logger = args[0].logger  # args[0] is intended to be "self". Assumes that self.logger has already been created on __init__.

            if not isinstance(logger, logging.Logger):  # Ensures that a Logger object is provided.
                print('[ERROR] Please provide an instance of class: logging.Logger')
                sys.exit('[ERROR] Please provide an instance of class: logging.Logger')

            for i in range(retries + 1):  # First attempt 0 does not count as a retry.
                try:
                    f(*args, **kwargs)
                    #print('No exception encountered')
                    break  # So you don't run f() multiple times!
                except Exception as ex:
                    # To only log exceptions.
                    #print('Exception: ' + str(ex))
                    if i > 0:
                        logger.info(f'[RETRYING] {f.__name__}: {i}/{retries}')
                        time.sleep(2 ** i)  # Exponential backoff. Pause processing for an increasing number of seconds, with each error.
                    logger.error(ex)

        wrapped_err_handler.__name__ = f.__name__  # Nicety. Rename the error handler function name to that of the wrapped function.
        return wrapped_err_handler
    return wrap


def get_date_ranges(str_dt_ref=None, l_periods=[]):
    """ Utility function to return pair-tuples of strings demarcating start_date and end_date of the requested period.
    Note: The period for "past N days" will not include current date. Eg: If today is 8 Jan, period of past 7 days will be 1 Jan to 7 Jan (INCLUSIVE of boundary dates).

    :param str_dt_ref: Reference date from which to make our calculations. Will default to current date if None.
    :param l_periods: Valid values include MTD/YTD/P90D/P7D.
    :return: di_periods: dict structure with period names as key and 2-tuples as value (str_dt_from, str_dt_to). Note that values are STRINGS!
    """
    # Get Reference Date.
    if str_dt_ref is None:
        dt_ref = dt.datetime.today().date()
    else:
        dt_ref = pd.to_datetime(str_dt_ref)

    # CALCULATE PERIODS #
    di_periods = {}
    dt_to = dt_ref + pd.Timedelta(days=-1)
    str_dt_to = dt_to.strftime('%Y-%m-%d')

    for period in l_periods:
        if period == 'MTD':
            dt_from = dt_ref.replace(day=1)
            # "Monthly" case. Where today() is in a later month, but we want data for the preceding month (and not current month).
            if dt_from > dt_to:
                dt_from = dt_from + relativedelta(months=-1)
            str_dt_from = dt_from.strftime('%Y-%m-%d')
            di_periods[period] = (str_dt_from, str_dt_to)  # from first date of month

        elif period == 'YTD':
            str_dt_from = dt_ref.replace(day=1, month=1).strftime('%Y-%m-%d')
            di_periods[period] = (str_dt_from, str_dt_to)  # from first date of year
        elif period == 'P07D':
            str_dt_from = (dt_ref + pd.Timedelta(days=-7)).strftime('%Y-%m-%d')
            di_periods[period] = (str_dt_from, str_dt_to)  # from 7 days ago
        elif period == 'P90D':
            str_dt_from = (dt_ref + pd.Timedelta(days=-90)).strftime('%Y-%m-%d')
            di_periods[period] = (str_dt_from, str_dt_to)  # from 90 days ago
    return di_periods


def get_latest_file(str_folder=None, pattern=None):
    """
    Given a folder, return the last updated file in that folder.
    If pattern (regex) is given, apply pattern as a filter first.
    """
    _, _, l_files = next(os.walk(str_folder))  # First, always get all files in the dir.

    # Apply pattern to filter l_files if pattern exists #
    if pattern is not None:
        l_files = [f for f in l_files if re.search(pattern, f)]
        if len(l_files) == 0: raise Exception('No files found that match the given pattern.')

    # Get last modified file, from the filtered list #
    dt_prev = None  # Initialize outside the loop.
    for file in l_files:
        str_fn_curr = os.path.join(str_folder, file)
        dt_curr = dt.datetime.fromtimestamp(os.path.getmtime(str_fn_curr))

        if dt_prev is None:
            dt_prev = dt_curr
            str_fn_prev = str_fn_curr
        else:
            if dt_curr > dt_prev:  # Keep the most recent datetime value.
                dt_prev = dt_curr
                str_fn_prev = str_fn_curr
    return (str_fn_prev, file)


def get_files(str_folder=None, pattern=None, latest_only=False):
    """ Given a directory name, return all full filenames that exist there, and which match the pattern. Can search for latest filename only.
    :param str_folder: Directory to search for files.
    :param pattern: A regex expression, to filter the list of files.
    :param latest_only: True, if you want to get the latest filename only.
    :return: Returns a tuple of (<full filename>, <filename>) if latest_only=True. Otherwise, returns a list of tuples of (<full filename>, <filename>).
    """
    _, _, l_files = next(os.walk(str_folder))  # First, always get all files in the dir.

    # Simple case. Retrieve all files that match the pattern.
    if (str_folder is not None) & ~latest_only:
        if pattern is None:  # Return all files in the directory
            return l_files
        else:  # Return only files which match pattern.
            return [(os.path.join(str_folder, fn), fn) for fn in l_files if re.search(pattern, fn)]
    else:
        return get_latest_file(str_folder=str_folder, pattern=pattern)  # Note: The function will return a 2-values tuple!


def get_curr_time_as_string(format='%Y%m%d_%H%M', dt_date=None, leading_underscore=True):
    """ Returns current timestamp as a string.
    Convenience function. Timestamps are frequently used in filenames, to make them unique.
    Default output format is: '_YYYYMMDD_hhmm'

    :param format: Use this format if specified.
    :param dt: Converts this date object if specified, otherwise uses current timestamp.
    :param leading_underscore: Prefix the return string with an underscore if True.
    :return: The formatted string.
    """
    if dt_date is None:
        dt_temp = dt.datetime.today()  # By default, take current timestamp.
    else:
        dt_temp = dt_date

    str_out = dt_temp.strftime(format=format)

    if leading_underscore:
        str_out = '_' + str_out

    return str_out
