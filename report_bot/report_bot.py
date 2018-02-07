import datetime as dt
import jinja2
import pandas as pd
from pandas import DataFrame, Series
import os
import sys
import re
import logging
import shutil
import requests
import sqlalchemy
from configobj import ConfigObj
import smtplib
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders

from utils import dec_err_handler


class ReportBot(object):
    config = ConfigObj('C:/webapps/report_bot/report_bot.conf')
    SMTP = config['smtp']  # dict structure containing data related to mail server.
    # MAIL_SERVER = config['smtp']['mail_server']
    # PORT = config['smtp']['port']
    # FROM_NAME = config['smtp']['from_name']
    # FROM_EMAIL = config['smtp']['from_email']

    def __init__(self):
        # CREATE CONNECTION TO 2 DBs #
        # listman
        str_host = self.config['database']['listman']['host']
        str_userid = self.config['database']['listman']['userid']
        str_password = self.config['database']['listman']['password']
        str_schema = self.config['database']['listman']['schema']
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_schema}?charset=utf8'
        engine = sqlalchemy.create_engine(str_conn_mysql, echo=False)
        self.db_listman_conn = engine.connect()

        # fehdw
        str_host = self.config['database']['fehdw']['host']
        str_userid = self.config['database']['fehdw']['userid']
        str_password = self.config['database']['fehdw']['password']
        str_schema = self.config['database']['fehdw']['schema']
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_schema}?charset=utf8'
        engine = sqlalchemy.create_engine(str_conn_mysql, echo=False)
        self.db_fehdw_conn = engine.connect()

    def __del__(self):
        self.db_listman_conn.close()
        self.db_fehdw_conn.close()
        self._free_logger()

    def _init_logger(self, logger_name, source_name=None):
        """ Initialize self.logger for DataReader sub-classes
        If source_name = None, means that we only want to log to global.log.
            Usage: "dr._init_logger(logger_name='global')"
        If source_name is an actual data source name (in fehdw.conf), then 2 file handlers will be created.

        :param logger_name: Unique name used solely in Logger class as id. Format to send in: "<source_name>".
        :param source_name: Unique name for each data source. Comes from CONF file ("source_name"). eg: "opera", "ezrms".
        :return: NA
        """
        self.logger = logging.getLogger(logger_name)  # A specific id for the Logger class use only.
        self.logger.setLevel(logging.INFO)  # By default, logging will start at 'WARNING' unless we tell it otherwise.

        if self.logger.hasHandlers():  # Clear existing handlers, else will have duplicate logging messages.
            self.logger.handlers.clear()

        # LOG TO GLOBAL LOG FILE #
        str_fn_logger_global = os.path.join(self.config['global']['global_root_folder'],
                                            self.config['global']['global_log'])
        fh_logger_global = logging.FileHandler(str_fn_logger_global)

        if source_name:  # if not None, means it's True, and that there is a data source.
            str_format_global = f'[%(asctime)s]-[%(levelname)s]-[{source_name}] %(message)s'
        else:  # Is None. Means global log only.
            str_format_global = f'[%(asctime)s]-[%(levelname)s] %(message)s'

        fh_logger_global.setFormatter(logging.Formatter(str_format_global))
        self.logger.addHandler(fh_logger_global)  # Add global handler.

        if source_name:  # Log to data source specific file, only if a source_name is given.
            # LOG TO <source_name> LOG FILE #
            str_fn_logger = os.path.join(self.config['global']['global_root_folder'],
                                         self.config['data_sources'][source_name]['logfile'])
            fh_logger = logging.FileHandler(str_fn_logger)
            str_format = '[%(asctime)s]-[%(levelname)s]- %(message)s'
            fh_logger.setFormatter(logging.Formatter(str_format))
            self.logger.addHandler(fh_logger)  # Add handler.

    def _free_logger(self):
        """ Frees up all file handlers. Method is to be called on __del__().
        :return:
        """
        # Logging. Close all file handlers to release the lock on the open files.
        handlers = self.logger.handlers[:]  # https://stackoverflow.com/questions/15435652/python-does-not-release-filehandles-to-logfile
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)


class OperaEmailQualityMonitorReportBot(ReportBot):
    # Get labels for Opera columns.
    fn_op_mt = '//10.0.2.251/fesftp/Mapping table/Opera Text File mapping.xlsx'  # Note that to access from outside, must share out the folder. Check via \\10.0.2.251 in WinExplorer.
    df = pd.read_excel(io=fn_op_mt, sheet_name='Sheet2', skiprows=1, keep_default_na=False, na_values=' ')
    df.drop(labels=df.columns[2:], axis=1, inplace=True)  # Drop all columns starting from 3rd column.
    df.columns = ['code', 'name']
    df.sort_values(by=['name'], axis=0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df_op_labels = df

    def __init__(self):
        super().__init__()

        # INIT LOGGER #
        self._init_logger(logger_name='opera_email_quality_monitor_report_bot')

    def __del__(self):
        super().__del__()

    def check_valid_email(self, str_email):
        '''Checks whether a given string has the format of a valid email.
        Returns True if valid, else returns False.
        '''
        emailRegex = '[^@]+@[^@]+\.[^@]+'
        return True if re.match(emailRegex, str_email) else False

    def get_df_from_opera_file(self, fn=None, dt_from=None, dt_to=None):
        """
        Given a filename (with path) of an Opera text file, read it and output a dataframe.
        Filters the rows using 'arrival_date_dt', which should be between dt_from and dt_to, inclusive.
        This way, can handle both data request scenarios (weekly and monthly).
        """
        # Get string representing last month (eg: 'SEP'), for use in filtering later.
        # str_last_month = str.upper((dt.datetime.today() - dt.timedelta(days=30)).strftime('%b'))

        # Read Opera data file
        # Note: 'quoting'=3 (QUOTE_NONE) will prevent an error. Otherwise if a string has a double-quote, python expects a "|".
        df_op_data = pd.read_csv(fn, sep='|', skiprows=2, skipfooter=2, keep_default_na=False, na_values=' ',
                                 engine='python', error_bad_lines=False, quoting=3)
        # Iterate through Opera Code to OperaFieldName mapping table. Swap out codes for names, in the other df.
        for idx, row in self.df_op_labels.iterrows():
            df_op_data.rename(columns={row['code']: row['name']},
                              inplace=True)  # eg: df_op_data.rename(columns={'C93': 'Origin'}

        df_op_data.columns = df_op_data.columns.str.lower()  # Convert all column names to lowercase.
        df_op_data.drop(labels='', axis=1, inplace=True)  # Drop the column for the blank colname. All values are blank.

        # Column names -> underscores instead of spaces.
        df_op_data.columns = [x.replace(' ', '_') for x in list(df_op_data.columns)]

        # Filter away Airline crew and Wholesales Groups, as identified by the market_code field of the Opera transaction.
        df_op_data = df_op_data[~(df_op_data['market_code'].isin(['ALC', 'ALI', 'WHG']))]
        # Filter away 'CANCELLED' and 'NO SHOW' bookings.
        df_op_data = df_op_data[~(df_op_data['reservation_status'].isin(['CANCELLED', 'NO SHOW']))]
        # Filter away pseudo rooms.
        df_op_data = df_op_data[~(df_op_data['stayed_room_type'].isin(['PM']))]
        # Filter away rate_code = 'SHR'. Room sharers are not required to give their emails in the hotel registration card.
        df_op_data = df_op_data[~(df_op_data['rate_code'].isin(['SHR']))]
        # Convert 'arrival_date' to datetime format.
        df_op_data['arrival_date_dt'] = pd.to_datetime(df_op_data['arrival_date'].apply(lambda x: x[0:-2] + '20' + x[-2:]),
                                                       format='%d-%b-%Y')
        # Filter by 'arrival_date_dt' to contain only rows in between dt_from and dt_to.
        df_op_data = df_op_data[(df_op_data['arrival_date_dt'] >= dt_from) & (df_op_data['arrival_date_dt'] <= dt_to)]

        # Filter arrival_date to contain only str_last_month value.
        # DEBUG-20171013 df_op_data = df_op_data[df_op_data['arrival_date'].str.contains(str_last_month)]

        return df_op_data

    def get_df_from_all_opera_files(self, str_dir='//10.0.2.251/fesftp/Opera', dt_from=None, dt_to=None):
        """
        To go through all TXT files (Opera files) in hardcoded directory, keeping only rows where ArrivalDate is between specified date parameters.
        dt_from, dt_to. Input datetime objects.
        Returns a DataFrame.    """

        # str_last_month = str.upper((dt.datetime.today() - dt.timedelta(days=30)).strftime('%b'))
        # str_last_month = 'SEP'  # DEBUG

        r = re.compile('.+Historical.+txt$')  # Format: " *Historical*.txt ".
        l_op_files = list(filter(r.match, list(os.walk(str_dir))[0][
            2]))  # List of raw Opera files to aggregate. Picks ALL files in directory.

        df_in = DataFrame()

        for file in l_op_files:
            fn = os.path.join(str_dir, file)
            df_temp = self.get_df_from_opera_file(fn=fn, dt_from=dt_from, dt_to=dt_to)  # Call the function.
            df_in = df_in.append(df_temp, ignore_index=True)

        # Drop any possible duplicates of 'confirmation_number', keeping the first occurrence.
        df_in = df_in[~df_in['confirmation_number'].duplicated(keep='first')]

        return df_in

    @dec_err_handler(retries=0)
    def get(self, str_dt_from, str_dt_to):
        # Specify Period. By default, program will take last 7 day period (up to the day before).
        # str_dt_from = '2017-12-01'  # DEBUG <= Change this value!
        # str_dt_to = '2017-12-31'

        # Make these accessible to all methods.
        self.str_dt_from = str_dt_from
        self.str_dt_to = str_dt_to

        # Get data from text files.
        df_op = self.get_df_from_all_opera_files(dt_from=pd.to_datetime(str_dt_from), dt_to=pd.to_datetime(str_dt_to))
        self.df_op = df_op  # Work-around. Solely for use with send_op_repeat_guest_monitor().

        # Create XLSX file to send as email attachment; delete file immediately after sending.
        str_fn = 'email_list - {} to {}.xlsx'.format(str_dt_from, str_dt_to)
        self.str_fn = str_fn
        str_email_attach_fn = os.path.join(os.getcwd(), str_fn)  # Fully qualified path to Excel file.
        self.str_email_attach_fn = str_email_attach_fn
        df_email_attach = df_op[
            ['resort', 'confirmation_number', 'email', 'first_name', 'last_name', 'market_code', 'rate_code',
             'arrival_date_dt']]
        df_email_attach.to_excel(str_email_attach_fn, index=False)

        df = df_op[['resort', 'email']]  # taken only relevant columns.
        df['email_is_blank'] = df['email'] == ''
        df['email_is_valid_tech'] = df['email'].apply(self.check_valid_email)

        # PORTFOLIO LEVEL STATISTICS #
        i_size = len(df)
        # Invalid = TechnicallyInvalid - Blanks + Booking.com
        i_bookingdotcom = len(df[df['email'].str.contains('BOOKING.COM', case=False)])
        i_invalid = sum(~df['email_is_valid_tech']) - sum(df['email_is_blank']) + i_bookingdotcom

        # Valid = TechnicallyValid - Booking.com
        str_portfolio_level_stats = """ Here is the email collection information from Opera:
                
        <b>[PORTFOLIO LEVEL STATISTICS]</b>
        Not Collected: {}% 
        Invalid: {}% 
        Valid: {}% 
        """.format(round( (sum(df['email_is_blank']) / i_size) * 100, 1),
                  round( i_invalid / i_size * 100, 1),
                  round( (sum(df['email_is_valid_tech']) - i_bookingdotcom) / i_size * 100, 1)
                  )

        self.str_portfolio_level_stats = str_portfolio_level_stats

        sr_email_is_blank = ( df[df['email_is_blank']==True].groupby(['resort']).size() ) / ( df.groupby(['resort']).size() )
        sr_email_is_blank.fillna(0., inplace=True)
        sr_email_is_invalid_tech = df[df['email_is_valid_tech']==False].groupby(['resort']).size() / ( df.groupby(['resort']).size())
        sr_email_is_invalid_tech.fillna(0., inplace=True)
        sr_email_is_valid_tech = df[df['email_is_valid_tech']==True].groupby(['resort']).size() / ( df.groupby(['resort']).size())
        sr_email_is_invalid_less_blanks = sr_email_is_invalid_tech - sr_email_is_blank  # Percent of emails which are invalid, less blanks.
        sr_email_has_bookingdotcom_domain = df[df['email'].str.contains('BOOKING.COM', case=False)].groupby(['resort']).size() / ( df.groupby(['resort']).size() )
        sr_email_has_bookingdotcom_domain.fillna(0., inplace=True)  # Replace NaN with 0, so that downstream operations will not become NaN unnecessarily.

        # We defined booking.com emails to be invalid.
        sr_email_invalid = sr_email_is_invalid_less_blanks + sr_email_has_bookingdotcom_domain
        sr_email_valid = sr_email_is_valid_tech - sr_email_has_bookingdotcom_domain

        df_out = DataFrame({'not_collected': sr_email_is_blank, 'invalid':sr_email_invalid, 'valid': sr_email_valid})
        #df_out = df_out[['blank_email_percent', 'invalid_email_less_blanks_percent', 'valid_email_percent', 'email_contains_bookingdotcom_percent']]

        # Map new hotel codes to old hotel codes.
        #df_hotel_codes = pd.read_excel('C:/AA/python/mapping/mapping_hotel_codes.xlsx', keep_default_na=False, na_values=[' '])
        df_hotel_codes = pd.read_sql('SELECT * FROM cfg_map_hotel_sr', self.db_fehdw_conn)
        df_out.index = Series(df_out.index).map(Series(list(df_hotel_codes['new_code']), index=df_hotel_codes['old_code']))
        df_out = df_out[['not_collected', 'invalid', 'valid']]
        df_out = round(df_out * 100, 1)  # convert to percentage (based on 100)
        df_out.reset_index(drop=False, inplace=True)
        df_out.columns = ['Hotel', 'Not Collected', 'Invalid', 'Valid']  # rename to preferred column labels.

        df_out['month_year'] = dt.datetime.strftime(dt.datetime.today() - dt.timedelta(days=30), '%m_%Y')  # Last month.
        df_out = df_out.iloc[:, [-1]+list(range(0, len(df_out.columns)-1)) ]

        self.df_out = df_out

    def build_body(self, str_template_file, di_params=None):
        templateLoader = jinja2.FileSystemLoader(searchpath=self.config['global']['global_templates'])
        templateEnv = jinja2.Environment(loader=templateLoader)
        template = templateEnv.get_template(str_template_file)
        return template.render(di_params)  # Return email body, generated with template.

    @dec_err_handler(retries=0)
    def send(self, str_listname=None, str_subject=None):
        df = self.df_out.drop(labels=['month_year'], axis=1, inplace=False)  # Drop unnecessary column.
        str_portfolio_level_stats = self.str_portfolio_level_stats.replace('\n', '<br />')  # Prep Python string for HTML.
        #str_subject = '[opera_email_quality_monitor] Arrival Date Period: {} to {}'.format(self.str_dt_from, self.str_dt_to)
        str_df = df.to_html(index=False, na_rep='', justify='left')
        di_params = {'str_msg': str_portfolio_level_stats,  # Handcrafted msg.
                     'str_df': str_df,
                     'str_msg2': '',
                     'year': dt.datetime.now().year
                     }
        str_html = self.build_body(str_template_file='jinja_basic_frame.html', di_params=di_params)

        # Determine email recipients #
        str_sql = """
        SELECT email FROM mail_list
        WHERE listname = '{}'
        AND subscribed = 1
        """.format(str_listname)  # listname here
        df = pd.read_sql(str_sql, self.db_listman_conn)
        l_email_recipients = df['email'].tolist()

        # SEND EMAIL #
        MAIL_SERVER = self.SMTP['mail_server']
        SENDER = self.SMTP['from_name'] + ' <' + self.SMTP['from_email'] + '>'

        msg = MIMEMultipart()
        msg['From'] = SENDER
        msg['To'] = ','.join(l_email_recipients)
        msg['Subject'] = str_subject
        msg.attach(MIMEText(str_html, 'html'))

        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(self.str_email_attach_fn, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(self.str_fn))
        msg.attach(part)

        # Send the message via our SMTP server #
        s = smtplib.SMTP(host=MAIL_SERVER)
        s.sendmail(SENDER, l_email_recipients, msg.as_string())
        s.quit()
        os.remove(self.str_email_attach_fn)  # Delete the Excel file.

        # Write to log file #
        self.logger.info('Sent email with subject "{}"'.format(str_subject))

    @dec_err_handler(retries=0)
    def send_op_repeat_guest_monitor(self, str_listname=None, str_subject=None):
        """ This method is a hack, to handle the "op_repeat_guest_monitor" report, which is only slightly different
        from the opera_email_quality_monitor reports. This method uses self.df_op, populated in get(), and takes it from there.
        :param str_listname:
        :param str_subject:
        :return: NA
        """
        df_op = self.df_op
        # Calculate percentage
        i_repeat_guests = len(df_op[df_op['vip_code'] == 'Repeat Guests'])
        i_total_guests = len(df_op)
        str_percent = str(round(i_repeat_guests / i_total_guests * 100, 2))
        # Calculate date range of arrival_date for data set used. For user to get a feel of whether the data set used was correct or not.
        str_arr_dt_min = dt.datetime.strftime(df_op['arrival_date_dt'].min(), format='%Y-%m-%d')
        str_arr_dt_max = dt.datetime.strftime(df_op['arrival_date_dt'].max(), format='%Y-%m-%d')

        # Craft the email message #
        str_msg = """
        <p> Here is your information on repeat guests, based on Opera bookings. 
        Recall that a repeat guest is defined as one which has the VIP Code 
        ("VIP 8" in Opera; Description: "Repeat Guests").
        This measurement is done as a management metric to measure for the direct booking initiative a.k.a. 
        Insiders' Programme for 2018.
        </p>
        <b> Percentage of repeat guests: {}% </b> <br />
        <b> Arrival Date Range in Source Data Set: {} </b>
        """.format(str_percent, str_arr_dt_min + ' to ' + str_arr_dt_max)

        di_params = {'str_msg': str_msg,  # Handcrafted msg.
                     'str_df': '',
                     'str_msg2': '',
                     'year': dt.datetime.now().year
                     }
        str_html = self.build_body(str_template_file='jinja_basic_frame.html', di_params=di_params)

        # Determine email recipients #
        str_sql = """
        SELECT email FROM mail_list
        WHERE listname = '{}'
        AND subscribed = 1
        """.format(str_listname)  # listname here
        df = pd.read_sql(str_sql, self.db_listman_conn)
        l_email_recipients = df['email'].tolist()

        # SEND EMAIL #
        MAIL_SERVER = self.SMTP['mail_server']
        SENDER = self.SMTP['from_name'] + ' <' + self.SMTP['from_email'] + '>'

        msg = MIMEMultipart()
        msg['From'] = SENDER
        msg['To'] = ','.join(l_email_recipients)
        msg['Subject'] = str_subject
        msg.attach(MIMEText(str_html, 'html'))

        # Send the message via our SMTP server #
        s = smtplib.SMTP(host=MAIL_SERVER)
        s.sendmail(SENDER, l_email_recipients, msg.as_string())
        s.quit()
        os.remove(self.str_email_attach_fn)  # Delete the Excel file.

        # Write to log file #
        self.logger.info('Sent email with subject "{}"'.format(str_subject))
