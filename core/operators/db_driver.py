# encoding: utf-8
# author TurboChang

import csv
import datetime

from core.exceptions.related_exception import ReadCsvException

DB_LOGGING_START = '-' * 10 + 'DB-START' + '-' * 10
DB_LOGGING_END = '-' * 11 + 'DB-END' + '-' * 11

def db_call(func):
    """
    数据库调用装饰器，用于打印行为的用时
    """
    def inner_wrapper(*args, **kwargs):
        print(DB_LOGGING_START)
        start_time = datetime.datetime.now()
        has_error = None
        ret = None
        try:
            ret = func(*args, **kwargs)
        except Exception as e:
            has_error = e
        stop_time = datetime.datetime.now()
        ms = (stop_time - start_time).microseconds / 1000
        print('Query time: {0}ms'.format(str(ms)))
        print(DB_LOGGING_END)
        if has_error:
            raise has_error
        return ret
    return inner_wrapper

def db_step(step_name):
    """
    数据库操作步骤装饰器，用于打印数据库行为
    """
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            print('[DB]: ' + step_name)
            return func(*args, **kwargs)
        return inner_wrapper
    return wrapper

def back_dbdata(db_data_path, db_data_file):
    """
    :param db_data_path: "../../assets/testData/"
    :param db_data_file: "TestData_5000_Rows.csv"
    :return: [(xxx, xxx, xxx)]
    """
    with open(db_data_path+db_data_file, encoding="utf-8") as fcsv:
        reader = csv.reader(fcsv)
        if reader is None:
            raise ReadCsvException('Csv {0} 中没有数据'.format(db_data_file))
        else:
            drow = [tuple(row) for row in reader]
            return drow
