# encoding: utf-8
# author TurboChang

import cx_Oracle
import os
from loadTest.core.operators.db_driver import *


class InsertOracleDB:
    ALIAS = 'ORACLE'

    def __init__(self, db_config: list, table_name):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        self.table_name = table_name
        self.db = self.__connect()
        self.insert_sql = "insert into {0} (customer_id, first_name, last_name, phone, email, status, birdsday, addr)"
        self.values_sql = " values (:1, :2, :3, :4, :5, :6, :7, :8)"
        self.db_data_path = "../../assets/testData/"
        self.db_data_file = "TestData.csv"

    def __del__(self):
        try:
            self.db.close()
        except cx_Oracle.Error as e:
            print(e)

    def __connect(self):
        print('连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(
            self.user, self.password, self.host, self.port, self.database))

        db.ping()
        return db

    @db_call
    def __execute(self, table_name, sql, para):
        cursor = self.db.cursor()
        cursor.execute("truncate table {0}".format(table_name))
        print('SQL: {0}'.format(sql))
        cursor.executemany(sql, para)
        cursor.close()
        self.db.commit()

    @db_step("Oracle Insert Batch")
    def insert(self):
        print('表名: {0}'.format(self.table_name))
        values = back_dbdata(self.db_data_path, self.db_data_file)
        sql = self.insert_sql.format(self.table_name) + self.values_sql
        self.__execute(self.table_name, sql, values)

class LoadOracleDB:
    ALIAS = 'ORACLE'

    def __init__(self, db_config: list):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        pass

    @db_step("Oracle Sqlldr Data")
    def sqlldr(self):
        print('连接Oracle数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        command = "sqlldr {0}/{1}@{2}:{3}/{4} control=/Users/changliuxin/Programs/datapipeline/loadTest/core/logics/db/ctls/oracle.ctl bindsize=67108864".format(
            self.user,
            self.password,
            self.host,
            self.port,
            self.database)
        os.system(command)
