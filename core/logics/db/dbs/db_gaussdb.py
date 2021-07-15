# encoding: utf-8
# author TurboChang

import psycopg2
from loadTest.core.operators.db_driver import *


class InsertGaussDB:
    ALIAS = 'Gauss'

    def __init__(self, db_config: list, table_name):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        self.table_name = table_name
        self.db = self.__connect()
        self.insert_sql = "insert into {0} (customer_id, first_name, last_name, phone, email, status, birdsday, addr)"
        self.values_sql = " values (%s, %s, %s, %s, %s, %s, %s, %s)"
        self.db_data_path = "../../assets/testData/"
        self.db_data_file = "TestData.csv"

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print('连接GaussDB数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        db = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                              database=self.database)
        return db

    @db_call
    def __execute(self, table_name, sql, para):
        cursor = self.db.cursor()
        cursor.execute("truncate table {0}".format(table_name))
        print('SQL: {0}'.format(sql))
        cursor.executemany(sql, para)
        cursor.close()
        self.db.commit()

    @db_step("GaussDB Insert Batch")
    def insert(self):
        print('表名: {0}'.format(self.table_name))
        values = back_dbdata(self.db_data_path, self.db_data_file)
        sql = self.insert_sql.format(self.table_name) + self.values_sql
        self.__execute(self.table_name, sql, values)

class LoadGaussDB:
    ALIAS = 'GaussDB'

    def __init__(self, db_config: list, table_name):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        self.table_name = table_name
        self.db = self.__connect()
        self.db_data_path = "../../assets/testData/"
        self.db_data_file = "TestData.csv"

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print('连接GaussDB数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}'.format(
            self.host, self.port, self.user, self.password, self.database))
        db = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                              database=self.database)
        return db

    @db_call
    def __execute(self, table_name):
        cursor = self.db.cursor()
        cursor.execute("truncate table {0}".format(table_name))
        with open(self.db_data_path+self.db_data_file, encoding="utf-8") as f:
            cursor.copy_from(f, table_name, sep=',', null='')
        cursor.close()
        self.db.commit()

    @db_step("GaussDB Load Data")
    def copy(self):
        print('表名: {0}'.format(self.table_name))
        self.__execute(self.table_name)




