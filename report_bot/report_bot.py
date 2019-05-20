import datetime as dt
import jinja2
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import os
import re
import logging
import sqlalchemy
from configobj import ConfigObj
import smtplib
import pysftp
import shutil
import time
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders

from utils import dec_err_handler, get_curr_time_as_string, get_date_ranges, get_files
from selenium.webdriver.common.action_chains import ActionChains


class ReportBot(object):
    config = ConfigObj('C:/webapps/report_bot/report_bot.conf')
    # SMTP = config['smtp']  # dict structure containing data related to mail server.
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
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_schema}?charset=utf8mb4'
        engine = sqlalchemy.create_engine(str_conn_mysql, echo=False)
        self.db_listman_conn = engine.connect()

        # fehdw
        str_host = self.config['database']['fehdw']['host']
        str_userid = self.config['database']['fehdw']['userid']
        str_password = self.config['database']['fehdw']['password']
        str_schema = self.config['database']['fehdw']['schema']
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_schema}?charset=utf8mb4'
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

    def build_body(self, str_template_file, di_params=None):
        templateLoader = jinja2.FileSystemLoader(searchpath=self.config['global']['global_templates'])
        templateEnv = jinja2.Environment(loader=templateLoader)
        template = templateEnv.get_template(str_template_file)
        return template.render(di_params)  # Return email body, generated with template.

    def check_valid_email(self, str_email):
        '''Checks whether a given string has the format of a valid email.
        Returns True if valid, else returns False.
        '''
        emailRegex = '[^@]+@[^@]+\.[^@]+'
        return True if re.match(emailRegex, str_email) else False


class AdminReportBot(ReportBot):
    def __init__(self):
        super().__init__()
        # INIT LOGGER #
        self._init_logger(logger_name='admin_report_bot')

    def __del__(self):
        super().__del__()

    @dec_err_handler(retries=0)
    def send(self, str_listname=None, str_subject=None, **kwargs):
        str_msg = kwargs['str_msg']
        str_msg2 = kwargs['str_msg2']
        df = kwargs['df']

        # This will allow df to be None!
        try:
            str_df = df.to_html(index=False, na_rep='', justify='left')
        except AttributeError:  # handle "AttributeError: 'NoneType' object has no attribute 'to_html'"
            str_df = ''

        di_params = {'str_msg': str_msg,
                     'str_df': str_df,
                     'str_msg2': str_msg2,
                     'year': dt.datetime.now().year
                     }
        str_html = self.build_body(str_template_file='jinja_basic_frame.html', di_params=di_params)

        # Determine email recipients #
        str_sql = """
        SELECT email FROM mail_list
        WHERE listname = '{}'
        AND subscribed = 1
        ORDER BY email
        """.format(str_listname)  # listname here
        df = pd.read_sql(str_sql, self.db_listman_conn)
        l_email_recipients = df['email'].tolist()

        # SEND EMAIL #
        MAIL_SERVER = self.config['smtp']['mail_server']
        SENDER = self.config['smtp']['from_name'] + ' <' + self.config['smtp']['from_email'] + '>'
        PORT = self.config['smtp']['port']

        msg = MIMEMultipart()
        msg['From'] = SENDER
        str = ','.join(l_email_recipients)  # Assumes no non-ASCII chars in emails.
        msg['To'] = str
        msg['Subject'] = str_subject
        msg.attach(MIMEText(str_html, 'html'))

        # part = MIMEBase('application', "octet-stream")
        # part.set_payload(open(self.str_email_attach_fn, 'rb').read())
        # encoders.encode_base64(part)
        # part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(self.str_fn))
        # msg.attach(part)

        # Send the message via our SMTP server #
        s = smtplib.SMTP(host=MAIL_SERVER, port=PORT)
        s.sendmail(SENDER, l_email_recipients, msg.as_string())
        s.quit()

        # os.remove(self.str_email_attach_fn)  # Delete the Excel file.

        # Write to log file #
        # Note: All sent emails will be logged. This includes emails which were sent by other applications, which leverage upon ReportBot's functionalities!
        self.logger.info('Sent email with subject "{}"'.format(str_subject))


class STRReportBot(ReportBot):
    def __init__(self):
        super().__init__()
        # INIT LOGGER #
        self._init_logger(logger_name='str_report_bot')

    def __del__(self):
        super().__del__()

    @dec_err_handler(retries=3)
    def send_str_perf(self, str_listname='str_perf_rpt_weekly', str_type='Weekly'):
        """ Downloads STR data, and emails it to the specified mailing list.
        NOTE: This same code has been copied over to the ReportBot application, and it is THAT copy which is running every week!
        There was some problem with circular imports (AdminReportBot is imported by feh.datareader <- feh.utils, so we cannot import feh.datareader from STRReportBot!)
        So a copy of this code, along with the supporting config, were copied over.

        DESIGN: Firstly, all the files are download for a specific period (eg: 'MTD'). This is done in
        download_rpt_basic_perf_01(), download_rpt_basic_perf_01a(), download_rpt_basic_perf_01b()
        Next the read_rpt_basic_perf_01() and read_rpt_basic_perf_01_all() are used to read the files in a df.
        The df is appended, for all period_name.
        """

        #str_listname = 'test_aa'  # DEBUG

        # ALWAYS CLEAR DOWNLOAD FOLDER OF XLS FILES BEFORE STARTING, TO ALLOW A CLEAN RETRY AFTER EXCEPTION.
        str_dl_folder = os.path.join(os.getenv('USERPROFILE'), 'Downloads')
        l_str_fn_with_path = get_files(str_folder=str_dl_folder, pattern='xls$')
        for fp, _ in l_str_fn_with_path:
            os.remove(fp)

        str_subject = 'STR {} Report - Raw Data'.format(str_type)  # 'Weekly'/'Monthly'
        str_msg = """
        <strong>Hello team!</strong>
        <br>
        I've finished copying-and-pasting the data from STR, as attached, for your reporting activity.
        I've also included the execution time of the report in the filename, for your convenience.
        <br><br>
        Best regards,
        <br>
        Ah Bee
        """
        str_msg2 = ''
        str_df = ''

        di_params = {'str_msg': str_msg,
                     'str_df': str_df,
                     'str_msg2': str_msg2,
                     'year': dt.datetime.now().year
                     }
        str_html = self.build_body(str_template_file='jinja_basic_frame.html', di_params=di_params)

        # Determine email recipients from listname #
        str_sql = """
        SELECT email FROM mail_list
        WHERE listname = '{}'
        AND subscribed = 1
        ORDER BY email
        """.format(str_listname)  # listname here
        df = pd.read_sql(str_sql, self.db_listman_conn)
        l_email_recipients = df['email'].tolist()

        # GET DATAFRAME AND OUTPUT TO XLSX FILE #
        if str_type == 'Weekly':
            df_str = self.get_str_perf_weekly()
        elif str_type == 'Monthly':
            df_str = self.get_str_perf_monthly()

        str_fn_out = 'STR_{}_Report'.format(str_type) + get_curr_time_as_string() + '.xlsx'
        str_fp_out = 'C:/fehdw/temp/' + str_fn_out
        df_str.to_excel(str_fp_out, index=False)

        # SEND EMAIL #
        MAIL_SERVER = self.config['smtp']['mail_server']
        SENDER = self.config['smtp']['from_name'] + ' <' + self.config['smtp']['from_email'] + '>'
        PORT = self.config['smtp']['port']

        msg = MIMEMultipart()
        msg['From'] = SENDER
        str_temp = ','.join(l_email_recipients)  # Assumes no non-ASCII chars in emails.
        msg['To'] = str_temp
        msg['Subject'] = str_subject
        msg.attach(MIMEText(str_html, 'html'))

        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(str_fp_out, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(str_fn_out))
        msg.attach(part)

        # Send the message via our SMTP server #
        s = smtplib.SMTP(host=MAIL_SERVER, port=PORT)
        s.sendmail(SENDER, l_email_recipients, msg.as_string())
        s.quit()

        # os.remove(self.str_fp_out)  # Delete the Excel file.

        # Write to log file #
        # Note: All sent emails will be logged. This includes emails which were sent by other applications, which leverage upon ReportBot's functionalities!
        self.logger.info('Sent email with subject "{}"'.format(str_subject))

    def download_rpt_basic_perf_01(self, str_dt_from, str_dt_to):
        """ Downloads the STR STAR basic report by Property, for specified date range.
        Note: OSKL is requested to be included in the individual properties, but NOT in the "ALL".
        Note 2: Total should be (11+3)x4 -> 64.
        """
        from selenium import webdriver
        from selenium.common.exceptions import NoSuchElementException

        # GET LIST OF HOTELS #
        # 11 hotels (excl VHS).
        # 17 May 2019: ML asked that VHS to be included, so commented out in the SQL. This covers TOH as well, because same compset as VHS.
        str_sql = """
        SELECT str_hotel_id, str_hotel_name FROM cfg_map_properties
        WHERE operator = 'feh'
        AND asset_type = 'hotel'
        AND str_hotel_id IS NOT NULL
        -- AND cluster <> 'Sentosa'  -- Excl Sentosa hotels.
        ORDER BY str_hotel_name
        """
        df_hotels = pd.read_sql(str_sql, self.db_fehdw_conn)

        # Path to chromedriver executable. Get latest versions from https://sites.google.com/a/chromium.org/chromedriver/downloads. Also see http://chromedriver.chromium.org/downloads/version-selection
        # LOGIN #
        str_fn_chromedriver = os.path.join(self.config['global']['global_bin'], self.config['chromedriver']['exe_name'])
        driver = webdriver.Chrome(executable_path=str_fn_chromedriver)  # NOTE: Check for presence of 'options!'.
        driver.get('https://clients.str.com/ReportsOnline.aspx')
        input_email = driver.find_element_by_xpath('//*[@id="username"]')
        input_email.send_keys(self.config['data_sources']['str']['userid'])
        input_password = driver.find_element_by_xpath('//*[@id="password"]')
        input_password.send_keys(self.config['data_sources']['str']['password'])
        input_password.submit()  # Walks up the tree until it finds the enclosing Form, and submits that. http://selenium-python.readthedocs.io/navigating.html

        # Go to STAR report selection page.
        time.sleep(2)  # Wait for page to load before trying to access it.
        driver.find_element_by_xpath('//*[@id="menu-reports"]/a').click()

        wn_handle = driver.current_window_handle  # original window handle of the main browser tab.

        for idx, row in df_hotels.iterrows():
            # SELECT HOTEL IN DDLB #
            self.logger.info('DOWNLOADING STR REPORT (PROPERTY): ' + row['str_hotel_name'])
            str_xpath = "//*[@id='ctl00_CensusID']/option[@value='{}']".format(row['str_hotel_id'])

            # el_hotel_ddlb = driver.find_element_by_xpath(
            #     '//*[@id="ctl00_CensusID"]')  # The dropdown at the top select for hotel selection. Selecting the DDLB is different from selecting its Options!
            el_hotel = driver.find_element_by_xpath(str_xpath)
            el_hotel.click()

            # CHANGE DATE RANGE SELECTION #
            input_dt_from = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_txtStartDate"]')
            input_dt_from.clear()
            input_dt_from.send_keys(str_dt_from)
            input_dt_to = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_txtEndDate"]')
            input_dt_to.clear()
            input_dt_to.send_keys(str_dt_to)

            # SUBMIT #
            driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_btnSubmit2"]').click()

            # If wait time is too short, XLS file may not have finished downloading! Wait for report to be downloaded before closing window below!
            # 4s seems too short; 10s seems okay.
            time.sleep(10)
            # Switches back to the original browser tab (Note: For some reason, this will leave the new tabs opened until browser is closed)
            for wn in driver.window_handles:
                if wn_handle != wn:
                    # Close all windows other than the original one.
                    # Switch to the window, then close it. Then later switch back to original window.
                    driver.switch_to.window(wn)
                    driver.close()
            driver.switch_to.window(wn_handle)

        # LOGOUT #
        # driver.find_element_by_xpath('//*[@id="str-universal"]/a/i').click()  # Click the square icon.
        # driver.find_element_by_xpath('//*[@id="um-logout"]/div/strong').click()
        time.sleep(3)  # Wait a bit, in case the last download has not completed! Symptom is if the last download does not seem to be there.
        driver.quit()  # Quit the browser

    def download_rpt_basic_perf_01a(self, str_dt_from, str_dt_to):
        """ Downloads the STR STAR basic report for ALL portfolio properties, for specified date range.
        Differs from download_rpt_basic_perf_01() in that we're running STAR report on the PORTFOLIO instead of property.
        """
        from selenium import webdriver
        from selenium.common.exceptions import NoSuchElementException

        self.logger.info('DOWNLOADING STR REPORT (ALL)')

        # GET LIST OF HOTELS #
        # 10 hotels (excl VHS, OSKL).
        # 17 May 2019: ML asked that VHS to be included, so commented out in the SQL. This covers TOH as well, because same compset as VHS.
        str_sql = """
        SELECT str_hotel_id, str_hotel_name FROM cfg_map_properties
        WHERE operator = 'feh'
        AND asset_type = 'hotel'
        AND country = 'SG'  -- Excl 'OSKL'
        AND str_hotel_id IS NOT NULL
        -- AND cluster <> 'Sentosa'  -- Excl Sentosa hotels.
        ORDER BY str_hotel_name
        """
        df_hotels = pd.read_sql(str_sql, self.db_fehdw_conn)

        # Path to chromedriver executable. Get latest versions from https://sites.google.com/a/chromium.org/chromedriver/downloads
        # LOGIN #
        str_fn_chromedriver = os.path.join(self.config['global']['global_bin'], self.config['chromedriver']['exe_name'])
        driver = webdriver.Chrome(executable_path=str_fn_chromedriver)  # NOTE: Check for presence of 'options!'.
        driver.get('https://clients.str.com/ReportsOnline.aspx')
        input_email = driver.find_element_by_xpath('//*[@id="username"]')
        input_email.send_keys(self.config['data_sources']['str']['userid'])
        input_password = driver.find_element_by_xpath('//*[@id="password"]')
        input_password.send_keys(self.config['data_sources']['str']['password'])
        input_password.submit()  # Walks up the tree until it finds the enclosing Form, and submits that. http://selenium-python.readthedocs.io/navigating.html

        # Go to STAR report selection page.
        time.sleep(2)  # Wait for page to load before trying to access it.
        driver.find_element_by_xpath('//*[@id="menu-reports"]/a').click()

        for idx, row in df_hotels.iterrows():
            # SELECT HOTELS IN MULTISELECT #
            str_xpath = "option[@value='{}']".format(row['str_hotel_id'])
            try:
                el_multiselect = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_sProperty"]')  # Focus on the multiselect (list of hotels).
                opt_hotel = el_multiselect.find_element_by_xpath(str_xpath)
            except NoSuchElementException:
                continue  # Cannot find in the multiselect, so skip this hotel
            if opt_hotel is not None:
                actions = ActionChains(driver)  # For some reason, must always re-instantiate to get new one. Otherwise cannot double-click a second time.
                actions.double_click(opt_hotel).perform()

        # Click the "Duplicates?" checkbox if not checked already. We want to EXCLUDE duplicates by unchecking.
        el_cb = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_ckIncDups"]')
        if el_cb.get_attribute('checked'):
            el_cb.click()

        # # CHANGE DATE RANGE SELECTION #
        input_dt_from = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_txtStartDate"]')
        input_dt_from.clear()
        input_dt_from.send_keys(str_dt_from)
        input_dt_to = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_txtEndDate"]')
        input_dt_to.clear()
        input_dt_to.send_keys(str_dt_to)

        # SUBMIT #
        driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_btnSubmit2"]').click()

        # LOGOUT #
        # driver.find_element_by_xpath('//*[@id="str-universal"]/a/i').click()  # Click the square icon.
        # driver.find_element_by_xpath('//*[@id="um-logout"]/div/strong').click()
        time.sleep(10)  # Wait a bit, in case the last download has not completed! Symptom is if the last download does not seem to be there.
        driver.quit()  # Quit the browser

    def download_rpt_basic_perf_01b(self, str_dt_from, str_dt_to, str_ind_seg):
        """ Downloads the STR STAR basic report for ALL portfolio properties, for specified date range.
        Differs from download_rpt_basic_perf_01a(). This one compares the 3 metrics against Industry Segment rather than against compset.
        """
        from selenium import webdriver
        from selenium.common.exceptions import NoSuchElementException

        self.logger.info('DOWNLOADING STR REPORT (IND_SEG: {})'.format(str_ind_seg))

        # GET LIST OF HOTELS #
        # 10 hotels (excl VHS, OSKL).
        # 17 May 2019: ML asked that VHS to be included, so commented out in the SQL. This covers TOH as well, because same compset as VHS.
        str_sql = """
        SELECT str_hotel_id, str_hotel_name FROM cfg_map_properties
        WHERE operator = 'feh'
        AND asset_type = 'hotel'
        AND country = 'SG'  -- Excl 'OSKL'
        AND str_hotel_id IS NOT NULL
        -- AND cluster <> 'Sentosa'  -- Excl Sentosa hotels.
        ORDER BY str_hotel_name
        """
        df_hotels = pd.read_sql(str_sql, self.db_fehdw_conn)

        # Path to chromedriver executable. Get latest versions from https://sites.google.com/a/chromium.org/chromedriver/downloads
        # LOGIN #
        str_fn_chromedriver = os.path.join(self.config['global']['global_bin'], self.config['chromedriver']['exe_name'])
        driver = webdriver.Chrome(executable_path=str_fn_chromedriver)  # NOTE: Check for presence of 'options!'.
        driver.get('https://clients.str.com/ReportsOnline.aspx')
        input_email = driver.find_element_by_xpath('//*[@id="username"]')
        input_email.send_keys(self.config['data_sources']['str']['userid'])
        input_password = driver.find_element_by_xpath('//*[@id="password"]')
        input_password.send_keys(self.config['data_sources']['str']['password'])
        input_password.submit()  # Walks up the tree until it finds the enclosing Form, and submits that. http://selenium-python.readthedocs.io/navigating.html

        # Go to STAR report selection page.
        time.sleep(2)  # Wait for page to load before trying to access it.
        driver.find_element_by_xpath('//*[@id="menu-reports"]/a').click()

        for idx, row in df_hotels.iterrows():
            # SELECT HOTELS IN MULTISELECT #
            str_xpath = "option[@value='{}']".format(row['str_hotel_id'])
            try:
                el_multiselect = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_sProperty"]')  # Focus on the multiselect (list of hotels).
                opt_hotel = el_multiselect.find_element_by_xpath(str_xpath)
            except NoSuchElementException:
                continue  # Cannot find in the multiselect, so skip this hotel
            if opt_hotel is not None:
                actions = ActionChains(driver)  # For some reason, must always re-instantiate to get new one. Otherwise cannot double-click a second time.
                actions.double_click(opt_hotel).perform()

        # Click "My industry segments" radiobutton.
        driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_rbIndSegment"]').click()

        if str_ind_seg == 'upscale':
            driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_sSelectGrp2Segment"]/option[@value="Market Class: Singapore - Upscale Class"]').click()
            driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_btnGrp2Select"]').click()
        elif str_ind_seg == 'upper_upscale':
            driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_sSelectGrp2Segment"]/option[@value="Market Class: Singapore - Upper Upscale Class"]').click()
            driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_btnGrp2Select"]').click()

        # # CHANGE DATE RANGE SELECTION #
        input_dt_from = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_txtStartDate"]')
        input_dt_from.clear()
        input_dt_from.send_keys(str_dt_from)
        input_dt_to = driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_txtEndDate"]')
        input_dt_to.clear()
        input_dt_to.send_keys(str_dt_to)

        # SUBMIT #
        driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_btnSubmit2"]').click()

        # LOGOUT #
        # driver.find_element_by_xpath('//*[@id="str-universal"]/a/i').click()  # Click the square icon.
        # driver.find_element_by_xpath('//*[@id="um-logout"]/div/strong').click()
        time.sleep(12)  # Wait a bit, in case the last download has not completed! Symptom is if the last download does not seem to be there.
        driver.quit()  # Quit the browser

    def read_rpt_basic_perf_01(self, str_fn, str_dt_from, str_dt_to, str_period_name):
        """ Given an XLS file, read the pre-prescribed line of data. Obtain only the row of data in the "Period" line.
        Basically, we want to get all columns of data, for the "Period" line.
        Note: "str_hotel_id" is translated to "hotel_code" through the mapping table cfg_map_properties.
        :param str_fn: The STR STAR report XLS file to read.
        :param str_dt_from: Used only to demarcate period start/end.
        :param str_dt_to: Used only to demarcate period start/end.
        :param str_period_name: Used to indicate what this period is (eg: "MTD", "YTD").
        :return:
        """
        df = pd.read_excel(str_fn, skiprows=1, nrows=1, header=None)  # Second row of the STR report contains the keys we want.

        str_hotel_name_row = df[1][0]
        if str_hotel_name_row.count('#') > 1:  # If there are multiple '#' in the raw string, means there are multiple hotels, ie: not for a single property.
            str_hotel_code = 'ALL'
        else:
            # Get the STR Hotel ID from the Excel cell.
            # "str_hotel_id" -> means STR Hotel ID, not "string"!
            str_hotel_id = re.search('(?<=#)\d+', str_hotel_name_row)[0]  # eg: 'The Elizabeth Hotel #123456'

            # GET HOTEL CODE FROM PROPERTY NAME #
            str_sql = """
            SELECT hotel_code FROM cfg_map_properties
            WHERE str_hotel_id = '{}'
            """.format(str_hotel_id)
            df_hotels = pd.read_sql(str_sql, self.db_fehdw_conn)

            str_hotel_code = df_hotels['hotel_code'][0]  # Assumes there's only 1 row returned.

        # Cater for "Industry Segment" report rather than by compset.
        # Note: Do NOT use "else" clause here -- if str_hotel_code is anyway other than expected values, the insertions will not take place and code will crash (expecting EXACTLY 18 columns!).
        if str_hotel_code == 'ALL':
            df = pd.read_excel(str_fn, skiprows=2, nrows=1, header=None)  # Third row of the STR report contains the keys we want.
            str_cell = df[1][0]
            if str_cell == 'Industry: Market Class: Singapore - Upscale':  # SEARCH STRING!
                str_hotel_code = 'ALL_UPSC'
            elif str_cell == 'Industry: Market Class: Singapore - Upper Upscale':  # SEARCH STRING!
                str_hotel_code = 'ALL_UPPER_UPSC'

        # GET PERIOD LINE's ALL DATA #
        df = pd.read_excel(str_fn, skiprows=6)
        df = df[df['Date'] == 'Period']  # "Period" line.
        df.dropna(axis=1, inplace=True)  # Drop blank columns
        df.drop(['Date'], axis=1, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # The file for "ALL" hotels has 3 less columns (15 instead of 18) because the 3 "rank" columns are not there. To insert NaNs into the correct positions.
        if str_hotel_code == 'ALL':
            df.insert(5, column='occ_rank', value=np.nan)
            df.insert(11, column='adr_rank', value=np.nan)
            df.insert(len(df.loc[0, :]), column='revpar_rank', value=np.nan)

        # The file for Industry Segment has 6 less columns (missing "ranking" and the 3 metrics)
        if (str_hotel_code == 'ALL_UPSC') or (str_hotel_code == 'ALL_UPPER_UPSC'):
            # Note: Insertion sequence matters! Processing from right-to-left so less complex (won't shift).
            df.insert(len(df.loc[0, :]), column='revpar_rgi', value=np.nan)
            df.insert(len(df.loc[0, :]), column='revpar_rank', value=np.nan)
            df.insert(8, column='adr_ari', value=np.nan)
            df.insert(9, column='adr_rank', value=np.nan)
            df.insert(4, column='occ_mpi', value=np.nan)
            df.insert(5, column='occ_rank', value=np.nan)

        df.columns = ['occ', 'occ_comp', 'occ_chng_pct', 'occ_comp_chng_pct', 'occ_mpi', 'occ_rank',
                      'adr', 'adr_comp', 'adr_chng_pct', 'adr_comp_chng_pct', 'adr_ari', 'adr_rank',
                      'revpar', 'revpar_comp', 'revpar_chng_pct', 'revpar_comp_chng_pct', 'revpar_rgi', 'revpar_rank']
        # Insert columns at the front of dataframe. These are key columns.
        df.insert(loc=0, column='hotel_code', value=str_hotel_code)
        df.insert(loc=1, column='period_name', value=str_period_name)
        df.insert(loc=2, column='date_from', value=pd.to_datetime(str_dt_from))
        df.insert(loc=3, column='date_to', value=pd.to_datetime(str_dt_to))
        return df

    def read_rpt_basic_perf_01_all(self, str_dt_from, str_dt_to, str_period_name, str_dir_src=None, str_dir_target=None):
        """ Processes read_rpt_basic_perf_01() in a loop, to read multiple files.
        Original downloaded files are ALWAYS DELETED thereafter, with an option to save a copy somewhere else.
        Note: The str_dt_from and str_dt_to do not perform any form of filtering! They are just added as constants in their columns, to indicate which period the row is for.
        :param str_dir_src: Directory where the files to be parsed are found.
        :param str_dt_from:
        :param str_dt_to:
        :param str_period_name: Used to indicate what this period is (eg: "MTD", "YTD").
        :param str_dir_target: If provided, will copy the downloaded files from STR to here, before deleting the originals.
        :return: DataFrame containing all "Period" lines (10+1) for a specific period (str_period_name).
        """
        # By default, download directory is the 'Downloads' folder of the user.
        if str_dir_src is None:
            str_dl_folder = os.path.join(os.getenv('USERPROFILE'), 'Downloads')
        else:
            str_dl_folder = str_dir_src

        l_str_fn_with_path = get_files(str_folder=str_dl_folder, pattern='xls$')  # Filenames can start with 'Cmp_Daily*" or "STR_OnlineReport*"

        df_all = DataFrame()
        for str_fn_with_path, _ in l_str_fn_with_path:
            self.logger.info('READING FILE: ' + str_fn_with_path)
            df = self.read_rpt_basic_perf_01(str_fn_with_path, str_dt_from, str_dt_to, str_period_name)
            df_all = df_all.append(df)

        # Downloaded files from STR will always be deleted. Copy files to str_dir_target if specified. eg: to 'C:/Users/feh_admin/Downloads/temp'
        # Deletion must always happen after use, because there could be another download from STR for a different period, immediately after this! (Note: this is unlikely to be thread-safe!)
        if str_dir_target is not None:
            for str_fn_with_path, str_fn in l_str_fn_with_path:
                str_fn_target = os.path.join(str_dir_target, str_fn)
                shutil.move(src=str_fn_with_path, dst=str_fn_target)  # move() also deletes the original.
        else:
            for str_fn_with_path, str_fn in l_str_fn_with_path:
                os.remove(str_fn_with_path)

        df_all['period_name'] = pd.Categorical(df_all['period_name'], categories=['YTD', 'P90D', 'MTD', 'P07D'])  # For custom sort order.
        df_all.sort_values(by=['hotel_code', 'period_name'], inplace=True)
        df_all.reset_index(drop=True, inplace=True)
        return df_all

    def get_str_perf_weekly(self):
        """ Gets all STR Performance data ("Period" line) for all specified periods (eg: "MTD", "YTD", etc).
        Works by download the XLS files for a period, reading into a df, then repeating for each period.
        (We loop it this way because there's no way to tell the "period" simply by looking at the XLS files) -> "period" is a construct created by us!
        Business Work Process: Run by Jamie every Thurday, on 10+1 hotels, for 4 periods.
        DataFrame containing all "Period" lines (10+1) for ALL specified periods (str_period_name).
        """
        self.logger.info('[get_str_perf_weekly] STARTING RUN')

        di_periods = get_date_ranges(l_periods=['P07D', 'MTD', 'P90D', 'YTD'])  # str_dt_ref defaults to current date.
        df_all = DataFrame()

        # For each period, download the 10 + 1 (hotels + ALL) XLS files.
        for str_period_name, v in di_periods.items():
            self.logger.info('Processing for period type: {} {}'.format(str_period_name, v))

            # DOWNLOAD FILES FOR PERIOD #
            self.download_rpt_basic_perf_01(str_dt_from=v[0], str_dt_to=v[1])   # Individual Hotels
            self.logger.info('COMPLETED: download_rpt_basic_perf_01')
            time.sleep(3)  # Add delay between each call. Fight against ('Connection aborted.', ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host', None, 10054, None)

            self.download_rpt_basic_perf_01a(str_dt_from=v[0], str_dt_to=v[1])  # "ALL"
            self.logger.info('COMPLETED: download_rpt_basic_perf_01a')
            time.sleep(3)

            self.download_rpt_basic_perf_01b(str_dt_from=v[0], str_dt_to=v[1], str_ind_seg='upscale')
            self.logger.info('COMPLETED: download_rpt_basic_perf_01b - upscale')
            time.sleep(3)

            self.download_rpt_basic_perf_01b(str_dt_from=v[0], str_dt_to=v[1], str_ind_seg='upper_upscale')
            self.logger.info('COMPLETED: download_rpt_basic_perf_01b - upper_upscale')
            time.sleep(3)

            # READ FILES #
            df = self.read_rpt_basic_perf_01_all(str_dt_from=v[0], str_dt_to=v[1], str_period_name=str_period_name,
                                                 str_dir_src=None, str_dir_target='C:/Users/feh_admin/Downloads/temp')
            df_all = df_all.append(df)

            # Interim output of df_all. So that if fails mid-way, the costly processing is not wasted. Can just read the CSV and continue.
            str_temp_fn = 'C:/Users/feh_admin/Downloads/temp/df_all' + get_curr_time_as_string() + '.csv'
            df_all.to_csv(str_temp_fn, index=False)

        # Sort again, because we appended period-by-period, so it's not in our desired sort order!
        df_all.sort_values(by=['hotel_code', 'period_name'], inplace=True)
        df_all.reset_index(drop=True, inplace=True)
        return df_all

    def get_str_perf_monthly(self):
        """ Gets all STR Performance data ("Period" line) for all specified periods (eg: "MTD", "YTD", etc).
        Identical to get_str_perf_weekly(), except we give a str_dt_ref of START OF CURRENT MONTH.

        Business Work Process: Run by Jamie on the first week of each month, on 10+1 hotels, for 4 periods.
        DataFrame containing all "Period" lines (10+1) for ALL specified periods (str_period_name).
        """
        self.logger.info('[get_str_perf_monthly] STARTING RUN')

        # Reference date to be set as the 1st day of the current month.
        str_dt_ref = dt.datetime.today().date().replace(day=1).strftime(format='%Y-%m-%d')
        di_periods = get_date_ranges(str_dt_ref=str_dt_ref, l_periods=['P07D', 'MTD', 'P90D', 'YTD'])  # str_dt_ref defaults to current date.

        df_all = DataFrame()

        # For each period, download the 10 + 1 (hotels + ALL) XLS files.
        for str_period_name, v in di_periods.items():
            self.logger.info('Processing for period type: {} {}'.format(str_period_name, v))

            # DOWNLOAD FILES FOR PERIOD #
            self.download_rpt_basic_perf_01(str_dt_from=v[0], str_dt_to=v[1])   # Individual Hotels
            self.logger.info('COMPLETED: download_rpt_basic_perf_01')
            time.sleep(3)  # Add delay between each call. Fight against ('Connection aborted.', ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host', None, 10054, None)

            self.download_rpt_basic_perf_01a(str_dt_from=v[0], str_dt_to=v[1])  # "ALL"
            self.logger.info('COMPLETED: download_rpt_basic_perf_01a')
            time.sleep(2)

            self.download_rpt_basic_perf_01b(str_dt_from=v[0], str_dt_to=v[1], str_ind_seg='upscale')
            self.logger.info('COMPLETED: download_rpt_basic_perf_01b - upscale')
            time.sleep(2)

            self.download_rpt_basic_perf_01b(str_dt_from=v[0], str_dt_to=v[1], str_ind_seg='upper_upscale')
            self.logger.info('COMPLETED: download_rpt_basic_perf_01b - upper_upscale')

            # READ FILES #
            df = self.read_rpt_basic_perf_01_all(str_dt_from=v[0], str_dt_to=v[1], str_period_name=str_period_name,
                                                 str_dir_src=None, str_dir_target='C:/Users/feh_admin/Downloads/temp')
            df_all = df_all.append(df)

        # Sort again, because we appended period-by-period, so it's not in our desired sort order!
        df_all.sort_values(by=['hotel_code', 'period_name'], inplace=True)
        df_all.reset_index(drop=True, inplace=True)
        return df_all


class OperaEmailQualityMonitorReportBot(ReportBot):
    # Get labels for Opera columns.
    fn_op_mt = 'C:/fehdw/config/Opera Text File mapping.xlsx'
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
        """ To go through all TXT files (Opera files) in hardcoded directory, keeping only rows where ArrivalDate is between specified date parameters.
        dt_from, dt_to. Input datetime objects.
        Returns a DataFrame.    """

        # str_last_month = str.upper((dt.datetime.today() - dt.timedelta(days=30)).strftime('%b'))
        # str_last_month = 'SEP'  # DEBUG

        r = re.compile('.+Historical.+txt$')  # Format: " *Historical*.txt ".
        l_op_files = list(filter(r.match, list(os.walk(str_dir))[0][2]))  # List of raw Opera files to aggregate. Picks ALL files in directory.

        df_in = DataFrame()

        for file in l_op_files:
            fn = os.path.join(str_dir, file)
            df_temp = self.get_df_from_opera_file(fn=fn, dt_from=dt_from, dt_to=dt_to)  # Call the function.
            df_in = df_in.append(df_temp, ignore_index=True)

        # Drop any possible duplicates of 'confirmation_number', keeping the first occurrence.
        df_in = df_in[~df_in['confirmation_number'].duplicated(keep='first')]

        return df_in

    def get_df_from_opera_file_sftp(self, sftp_srv=None, str_folder_remote=None, fn=None, str_dt_from=None, str_dt_to=None):
        """ Given a filename fn, open a connection to the configured SFTP server. Changed working directory to str_folder_remote.
        Read the file (specially formatted) and swap the column names as per mapping Excel file.
        Apply filters (very specific logic). Filters include dt_from <= arrival_date <= dt_to.
        Return the DataFrame.
        :param sftp_srv: If SFTP connection is supplied, use this instead of opening a new one.
        :param str_folder_remote: The SFTP folder where we should be based.
        :param str_dt_from:
        :param str_dt_to:
        :return:
        """
        # CREATE SFTP CONNECTION TO REMOTE SERVER. ONLY IF NOT SUPPLIED. THIS MAKES IS FASTER THAN REPEATEDLY RE-OPENING CONNECTIONS. #
        if sftp_srv is None:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            str_host = self.config['sftp']['sftp_server']
            str_userid = self.config['sftp']['userid']
            str_pw = self.config['sftp']['password']
            str_folder_remote = str_folder_remote  #'/C/FESFTP/Opera'
            srv = pysftp.Connection(host=str_host, username=str_userid, password=str_pw, cnopts=cnopts)
            srv.cwd(str_folder_remote)  # Change current working dir to here.
        else:
            srv = sftp_srv

        dt_from = pd.to_datetime(str_dt_from)  # Type conversion, so can do comparison later.
        dt_to = pd.to_datetime(str_dt_to)

        with srv.open(fn) as fo:  # Auto file close.
            df_op_data = pd.read_csv(fo, sep='|', skiprows=2, skipfooter=2, keep_default_na=False, na_values=' ',
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
        df_op_data['arrival_date_dt'] = pd.to_datetime(
            df_op_data['arrival_date'].apply(lambda x: x[0:-2] + '20' + x[-2:]),
            format='%d-%b-%Y')
        # Filter by 'arrival_date_dt' to contain only rows in between dt_from and dt_to.
        df_op_data = df_op_data[(df_op_data['arrival_date_dt'] >= dt_from) & (df_op_data['arrival_date_dt'] <= dt_to)]

        return df_op_data

    def get_df_from_all_opera_files_sftp(self, str_folder_remote='/C/FESFTP/Opera', str_dt_from=None, str_dt_to=None):
        """ Given a (hardcoded) remote folder, read all "*Historical*.txt" files.
        Filter str_dt_from <= arrival_date <= str_dt_to. Return the consolidated DataFrame.
        :param str_folder_remote:
        :param str_dt_from:
        :param str_dt_to:
        :return:
        """
        str_host = self.config['sftp']['sftp_server']
        str_userid = self.config['sftp']['userid']
        str_pw = self.config['sftp']['password']

        # CREATE SFTP CONNECTION TO REMOTE SERVER #
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=str_host, username=str_userid, password=str_pw, cnopts=cnopts)
        srv.cwd(str_folder_remote)  # Change current working dir to here.

        r = re.compile('.+Historical.+txt$')  # Format: " *Historical*.txt ".

        l_op_files = list(filter(r.match, srv.listdir()))  # List of raw Opera files to aggregate. Picks ALL files in directory.

        df_in = DataFrame()

        for file in l_op_files:
            if srv.isfile(file):
                df_temp = self.get_df_from_opera_file_sftp(sftp_srv=srv, str_folder_remote=str_folder_remote, fn=file, str_dt_from=str_dt_from, str_dt_to=str_dt_to)
                df_in = df_in.append(df_temp, ignore_index=True)

        # Drop any possible duplicates of 'confirmation_number', keeping the first occurrence.
        df_in = df_in[~df_in['confirmation_number'].duplicated(keep='first')]

        srv.close()
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
        df_op = self.get_df_from_all_opera_files_sftp(str_dt_from=str_dt_from, str_dt_to=str_dt_to)  # Get using SFTP protocol.
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
        str_sql = """
        SELECT * FROM cfg_map_properties WHERE operator = 'feh' 
        """
        df_hotel_codes = pd.read_sql(str_sql, self.db_fehdw_conn)
        df_out.index = Series(df_out.index).map(Series(list(df_hotel_codes['new_code']), index=df_hotel_codes['hotel_code']))
        df_out = df_out[['not_collected', 'invalid', 'valid']]
        df_out = round(df_out * 100, 1)  # convert to percentage (based on 100)
        df_out.reset_index(drop=False, inplace=True)
        df_out.columns = ['Hotel', 'Not Collected', 'Invalid', 'Valid']  # rename to preferred column labels.

        df_out['month_year'] = dt.datetime.strftime(dt.datetime.today() - dt.timedelta(days=30), '%m_%Y')  # Last month.
        df_out = df_out.iloc[:, [-1]+list(range(0, len(df_out.columns)-1)) ]

        self.df_out = df_out

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
        ORDER BY email
        """.format(str_listname)  # listname here
        df = pd.read_sql(str_sql, self.db_listman_conn)
        l_email_recipients = df['email'].tolist()

        # SEND EMAIL #
        MAIL_SERVER = self.config['smtp']['mail_server']
        SENDER = self.config['smtp']['from_name'] + ' <' + self.config['smtp']['from_email'] + '>'
        PORT = self.config['smtp']['port']

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
        s = smtplib.SMTP(host=MAIL_SERVER, port=PORT)
        s.sendmail(SENDER, l_email_recipients, msg.as_string())
        s.quit()
        os.remove(self.str_email_attach_fn)  # Delete the Excel file.

        # Write to log file #
        self.logger.info('Sent email to list "{}" with subject "{}"'.format(str_listname, str_subject))

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
        # SEND EMAIL #
        MAIL_SERVER = self.config['smtp']['mail_server']
        SENDER = self.config['smtp']['from_name'] + ' <' + self.config['smtp']['from_email'] + '>'
        PORT = self.config['smtp']['port']

        msg = MIMEMultipart()
        msg['From'] = SENDER
        msg['To'] = ','.join(l_email_recipients)
        msg['Subject'] = str_subject
        msg.attach(MIMEText(str_html, 'html'))

        # Send the message via our SMTP server #
        s = smtplib.SMTP(host=MAIL_SERVER, port=PORT)
        s.sendmail(SENDER, l_email_recipients, msg.as_string())
        s.quit()
        os.remove(self.str_email_attach_fn)  # Delete the Excel file.

        # Write to log file #
        self.logger.info('Sent email with subject "{}"'.format(str_subject))
