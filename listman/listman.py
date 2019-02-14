import datetime as dt
import pandas as pd
from pandas import DataFrame, Series
import os
import sys
import re
import logging
import sqlalchemy
from configobj import ConfigObj
import html
from flask import Flask, request


class ListManager(object):
    config = ConfigObj('C:/webapps/listman/listman.conf')
    MAIL_SERVER = config['smtp']['mail_server']
    PORT = config['smtp']['port']

    TAB_MAIL_LIST = config['database']['tables']['mail_list']  # Table name of main mailing list.
    #from_name = List Manager
    #from_email = noreply @ fareast.com.sg

    def __init__(self):
        # CREATE CONNECTION TO DB #  All data movements involve the database, so this is very convenient to have.
        str_host = self.config['database']['host']
        str_userid = self.config['database']['userid']
        str_password = self.config['database']['password']
        str_schema = self.config['database']['schema']
        str_conn_mysql = f'mysql+pymysql://{str_userid}:{str_password}@{str_host}/{str_schema}?charset=utf8mb4'
        engine = sqlalchemy.create_engine(str_conn_mysql, echo=False)
        self.db_conn = engine.connect()

        # INIT LOGGER #
        self._init_logger(logger_name='listman')

    def __del__(self):
        self.db_conn.close()
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

    def is_valid_email(self, str_email):
        '''Checks whether a given string has the format of a valid email.
        Returns True if valid, else returns False.
        '''
        emailRegex = '[^@]+@[^@]+\.[^@]+'
        return True if re.match(emailRegex, str_email) else False

    def is_valid_listname(self, str_listname):
        """ Check if listname is valid.
        :return: True if valid.
        """
        str_sql = f"""
        SELECT * FROM cfg_mail_lists
        WHERE listname = '{str_listname}'
        """
        df = pd.read_sql(str_sql, self.db_conn)
        if len(df) == 0:
            return False
        else:
            return True  # listname can be found in the table. Hence it's "allowed".

    def email_exists(self, str_listname, str_email):
        str_sql = f""" 
        SELECT * FROM {self.TAB_MAIL_LIST}
        WHERE listname = '{str_listname}'
        AND email = '{str_email}'
        """
        try:
            df = pd.read_sql(str_sql, self.db_conn)
            if len(df) == 0:  # Do not use COUNT(*) in above SELECT statement, if you're using this to check!
                return False
            else:
                return True
        except sqlalchemy.exc.ProgrammingError:  # Assume that this exception means that table does not yet exist.
            return False

    def subscribe(self, str_listname, str_email, l_domain_filter=None):
        """ Inserts or Updates listname+email record into mailing list table.
        :param str_listname:
        :param str_email:
        :param l_domain_filter: List of domain names. Only emails from these domains are allowed to subscribe.
        :return:
        """
        str_msg = ''

        # Ensure that email is from list of allowed domains.
        if not any(domain in str_email for domain in l_domain_filter):
            str_msg = 'Only emails from the following domains are allowed to subscribe to this mailing list. <br/>'
            str_msg = str_msg + str(l_domain_filter)
            self.logger.info(str_msg)
            return str_msg

        if self.is_valid_email(str_email):
            if self.is_valid_listname(str_listname):
                # INSERT/UPDATE DATABASE #
                if self.email_exists(str_listname, str_email):
                    str_sql = f"""
                    UPDATE {self.TAB_MAIL_LIST}
                    SET subscribed = True, last_update = '{dt.datetime.now()}'
                    WHERE listname = '{str_listname}'
                    AND email = '{str_email}'
                    """
                    pd.io.sql.execute(str_sql, self.db_conn)
                    str_msg = f'{str_email} subscription to mailing list {str_listname} has been updated.'
                    self.logger.info(str_msg)
                else:
                    df = DataFrame(
                        {'listname': [str_listname], 'email': [str_email], 'subscribed': True,
                         'last_update': dt.datetime.now()})
                    df = df[['listname', 'email', 'subscribed', 'last_update']]  # re-order columns.
                    df.to_sql(self.TAB_MAIL_LIST, self.db_conn, index=False, if_exists='append')
                    str_msg = f'{str_email} added to mailing list {str_listname}'
                    self.logger.info(str_msg)
            else:  # Return error
                str_msg = f'Unable to add {str_email}. List {str_listname} does not exist.'
                self.logger.error(str_msg)
        else:
            str_msg = f'Unable to add email {str_email}. Invalid email provided.'
            self.logger.error(str_msg)
        return str_msg

    def unsubscribe(self, str_listname, str_email):
        str_msg = ''
        if self.email_exists(str_listname, str_email):
            str_sql = f"""
            UPDATE {self.TAB_MAIL_LIST}
            SET subscribed = False, last_update = '{dt.datetime.now()}'
            WHERE listname = '{str_listname}'
            AND email = '{str_email}'
            """
            pd.io.sql.execute(str_sql, self.db_conn)
            str_msg = f'{str_email} has been unsubscribed from mailing list {str_listname}'
            self.logger.info(str_msg)
        else:
            str_msg = 'Unable to unsubscribe. Please ensure that listname and email are valid.'
            self.logger.error(str_msg)
        return str_msg


app = Flask(__name__)
sys.path.insert(0, ListManager.config['global']['global_apps_root'])  # Insert parent dir into path.
from utils import dec_err_handler  # utils.py is a shared resource across webapps.


@app.route('/')
def subscribe_unsubscribe():
    l_mgr = ListManager()
    str_msg = ''

    str_action = request.args.get('action')  # sub/unsub. None if key not found.
    str_listname = request.args.get('listname')
    str_email = request.args.get('email')
    #http://azrorca.fareast.com.sg/listman?action=sub&listname=op_email_quality_monitor&email=amosang@fareast.com.sg

    # VALIDATE INPUT STRINGS #
    if (str_action is None) | (str_listname is None) | (str_email is None):
        str_msg = f"""
        Please ensure that all input parameters are filled correctly. <br/>
        A valid command would be <br/>
        <pre>
        {html.escape('http://azrorca.fareast.com.sg/listman?action=<sub|unsub>&listname=<VALID_LISTNAME>&email=<VALID_EMAIL>')}        
        </pre>
        """
    else:
        if str_action == 'sub':
            str_msg = l_mgr.subscribe(str_listname, str_email, l_domain_filter=['fareast.com.sg'])
        elif str_action == 'unsub':
            # Check if email exists. If yes, toggle flag. If no,
            str_msg = l_mgr.unsubscribe(str_listname, str_email)
        else:  # Invalid action.
            str_msg = 'Invalid action. action = "sub" or "unsub" only.'
    return str_msg


if __name__ == '__main__':
    app.run(debug=True)
