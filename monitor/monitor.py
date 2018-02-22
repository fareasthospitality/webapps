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

class Monitor(object):
    config = ConfigObj('C:/webapps/monitor/monitor.conf')
    MAIL_SERVER = config['smtp']['mail_server']
    PORT = config['smtp']['port']

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


##############
app = Flask(__name__)
sys.path.insert(0, Monitor.config['global']['global_apps_root'])  # Insert parent dir into path.
from utils import dec_err_handler  # utils.py is a shared resource across webapps.


@app.route('/logs')
def show_logs():
    moni = Monitor()
    str_msg = ''

    str_date = request.args.get('date')  # Anchor date.
    str_days = request.args.get('days')  # Number of days to look back.
    #http://fehdw.fareast.com.sg/monitor/logs?date=2018-02-20&days=5

    # VALIDATE INPUT STRINGS #
    # If either parameter is missing, default to str_date = <today> and str_days = 7 (1 week).
    if (str_date is None) | (str_days is None):
        # 6 days ago, plus 1 day today, total of 7 days.
        str_date_from = dt.datetime.strftime(dt.datetime.today() - dt.timedelta(days=6), format('%Y-%m-%d'))
        str_date_to = dt.datetime.strftime(dt.datetime.today() + dt.timedelta(days=1), format('%Y-%m-%d'))  # +1 day, so that today's logs will be included.
    else:
        # To parse out the parameters from the HTTP GET parameters.
        dt_date = pd.to_datetime(str_date, format='%Y-%m-%d')
        str_date_from = dt.datetime.strftime(dt_date - dt.timedelta(days=int(str_days)-1), format('%Y-%m-%d'))  # -1, because it's inclusive of current date.
        str_date_to = dt.datetime.strftime(dt_date + dt.timedelta(days=1), format('%Y-%m-%d'))  # +1 day, so that today's logs will be included.

    # Get data from log tables.
    str_sql = """
    SELECT * FROM sys_log_dataload
    WHERE timestamp >= '{}' AND timestamp < '{}'
    ORDER BY timestamp DESC        
    """.format(str_date_from, str_date_to)

    df = pd.read_sql(str_sql, moni.db_fehdw_conn)
    str_msg += df.to_html(index=False, na_rep='', justify='left')

    return str_msg


if __name__ == '__main__':
    app.run(debug=True)
