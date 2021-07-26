# encoding: utf-8
# author TurboChang

import pymysql

from core.operators.db_driver import *


class InsertTiDB:
    ALIAS = 'TIDB'

    def __init__(self, db_config: list, table_name):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        self.table_name = table_name
        self.charset = 'utf8'
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
        print('连接TiDB数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}, charset: {5}'.format(
            self.host, self.port, self.user, self.password, self.database, self.charset))
        db = pymysql.connect(host=self.host, port=self.port, user=self.user,
                             passwd=str(self.password), db=self.database, charset=self.charset)
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

    @db_step("TiDB Insert Batch")
    def insert(self):
        print('表名: {0}'.format(self.table_name))
        values = back_dbdata(self.db_data_path, self.db_data_file)
        sql = self.insert_sql.format(self.table_name) + self.values_sql
        self.__execute(self.table_name, sql, values)

class LoadTiDB:
    ALIAS = 'TIDB'

    def __init__(self, db_config: list, table_name):
        self.host = db_config[2]
        self.port = db_config[5]
        self.user = db_config[3]
        self.password = db_config[4]
        self.database = db_config[6]
        self.table_name = table_name
        self.charset = 'utf8'
        self.db = self.__connect()
        self.db_data_path = "../../assets/testData/"
        self.db_data_file = "TestData.csv"

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print('连接TiDB数据库 host: {0}, port: {1}, user: {2}, passwd: {3}, db: {4}, charset: {5}'.format(
            self.host, self.port, self.user, self.password, self.database, self.charset))
        db = pymysql.connect(host=self.host, port=self.port, user=self.user,
                             passwd=str(self.password), db=self.database, charset=self.charset, local_infile=1)
        db.ping()
        return db

    @db_call
    def __execute(self, table_name, sql):
        cursor = self.db.cursor()
        cursor.execute("truncate table {0}".format(table_name))
        print('SQL: {0}'.format(sql))
        cursor.execute(sql)
        cursor.close()
        self.db.commit()

    @db_step("TiDB Insert Batch")
    def load(self):
        print('表名: {0}'.format(self.table_name))
        sql = "load data local infile '" + self.db_data_path + self.db_data_file + "' into table `" + self.table_name + "` fields terminated by ','"
        self.__execute(self.table_name, sql)