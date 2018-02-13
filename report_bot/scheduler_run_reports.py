#########################################################
# Purpose: Windows Task Scheduler to run this file at specific intervals (eg: every 30 mins). Working assumption = 30 mins.
# For ease of maintenance, put all reports to be run in 1 place. For errors, look up respective logs.
#########################################################
import sys
import datetime as dt
import calendar
import os
import pandas as pd
import logging
from configobj import ConfigObj

# LOG TO GLOBAL LOG FILE #
config = ConfigObj('C:/webapps/report_bot/report_bot.conf')
logger = logging.getLogger('report_bot')
logger.setLevel(logging.INFO)
str_fn_logger_global = os.path.join(config['global']['global_root_folder'], config['global']['global_log'])
fh_logger_global = logging.FileHandler(str_fn_logger_global)
str_format_global = f'[%(asctime)s]-[%(levelname)s] %(message)s'
fh_logger_global.setFormatter(logging.Formatter(str_format_global))
logger.addHandler(fh_logger_global)  # Add global handler.
fh_logger_global.setFormatter(logging.Formatter(str_format_global))
logger.addHandler(fh_logger_global)  # Add global handler.

try:
    sys.path.insert(0, 'C:/webapps')  # Must be here, or the statement below does not work.
    from report_bot.report_bot import OperaEmailQualityMonitorReportBot

    TIME_NOW = dt.datetime.now().time()  # Jobs to run within specific time windows

    # op_email_quality_monitor_weekly AND op_email_quality_monitor_monthly #
    # op_repeat_guest_monitor #
    # RUN TIME: 1) Weekly: Every Friday at 1pm; 2) Monthly: Every month at 1pm, on 3rd day of the month.
    if dt.time(13, 0) <= TIME_NOW < dt.time(13, 30):
        if dt.datetime.today().day == 3:  # op_email_quality_monitor_monthly #
            # MONTHLY. Check that it's the 3rd day of the month. Send at same time as for WEEKLY.
            # Assume triggered run date is in following month. Take today's date, less 30 days, to get year and month from last month.
            year, month = dt.datetime.strftime(dt.datetime.today() - pd.Timedelta('30D'), format='%Y-%m').split('-')
            _, num_days_in_mth = calendar.monthrange(int(year), int(
                month))  # https://stackoverflow.com/questions/36155332/how-to-get-the-first-day-and-last-day-of-current-month-in-python
            str_dt_from = year + '-' + month + '-01'
            str_dt_to = year + '-' + month + '-' + str(num_days_in_mth)
            str_subject = '[op_email_quality_monitor_monthly] Arrival Date Period: {} to {}'.format(str_dt_from, str_dt_to)
            rb = OperaEmailQualityMonitorReportBot()
            rb.get(str_dt_from=str_dt_from, str_dt_to=str_dt_to)
            rb.send(str_listname='op_email_quality_monitor_monthly', str_subject=str_subject)

            # op_repeat_guest_monitor #
            # Assume triggered run date is in following month. Take today's date, less 30 days, to get year and month from last month.
            year, month = dt.datetime.strftime(dt.datetime.today() - pd.Timedelta('30D'), format='%Y-%m').split('-')
            _, num_days_in_mth = calendar.monthrange(int(year), int(
                month))
            str_dt_from = year + '-' + month + '-01'
            str_dt_to = year + '-' + month + '-' + str(num_days_in_mth)
            str_subject = '[op_repeat_guest_monitor] Arrival Date Period: {} to {}'.format(str_dt_from, str_dt_to)
            rb = OperaEmailQualityMonitorReportBot()
            rb.get(str_dt_from=str_dt_from, str_dt_to=str_dt_to)
            rb.send_op_repeat_guest_monitor(str_listname='op_repeat_guest_monitor', str_subject=str_subject)
        else:  # op_email_quality_monitor_weekly #
            if dt.datetime.today().weekday() == 4:  # Friday
                str_dt_from = dt.datetime.strftime((dt.datetime.today() - pd.Timedelta('7D')), format='%Y-%m-%d')
                str_dt_to = dt.datetime.strftime((dt.datetime.today() - pd.Timedelta('1D')), format='%Y-%m-%d')
                str_subject = '[op_email_quality_monitor_weekly] Arrival Date Period: {} to {}'.format(str_dt_from, str_dt_to)
                rb = OperaEmailQualityMonitorReportBot()
                rb.get(str_dt_from=str_dt_from, str_dt_to=str_dt_to)
                rb.send(str_listname='op_email_quality_monitor_weekly', str_subject=str_subject)
except Exception as ex:
    logger.error(ex)


