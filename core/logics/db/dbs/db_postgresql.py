# encoding: utf-8
# author TurboChang

import jaydebeapi as jdbc
import os
from core.operators.db_driver import *
from core.operators.param import get_testplan

def db_cost(start_time, step_name, report):
    """
    记录数据库操作耗时
    """
    stop_time = datetime.datetime.now()
    ms = (stop_time - start_time).seconds
    command = "echo {0} {1}s >> /Users/changliuxin/Programs/datapipeline/loadTest/report/save/{2}.txt".format(step_name, ms, report)
    os.system(command)

class InsertJdbcPG:
    ALIAS = 'JDBC'

    def __init__(self, db_config: list, table_name, db_type):
        self.dsn_hostname = db_config[2]
        self.dsn_port = db_config[5]
        self.dsn_uid = db_config[3]
        self.dsn_pwd = db_config[4]
        self.dsn_database = db_config[6]
        self.jdbc_driver_name = "org.postgresql.Driver"
        self.jdbc_driver_path = "/Users/changliuxin/Programs/datapipeline/loadTest/lib/"
        # self.jdbc_driver_path = "../../lib/"
        self.db_type = db_type
        self.jdbc_driver_loc = os.path.join(self.jdbc_driver_path + self.db_type + ".jar")
        self.table_name = table_name
        self.db = self.__connect()
        self.insert_sql = "insert into {0} (customer_id, first_name, last_name, phone, email, status, birdsday, addr)"
        self.values_sql = " values (?, ?, ?, ?, ?, ?, ?, ?)"
        self.db_data_path = "/Users/changliuxin/Programs/datapipeline/loadTest/assets/testData/"
        # self.db_data_path = "../../assets/testData/"
        self.db_data_file = "TestData.csv"

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        print(self.jdbc_driver_loc)
        connection_string = "jdbc:postgresql://" + self.dsn_hostname + ":" + str(self.dsn_port) + "/" + self.dsn_database
        url = '{0}:user={1};password={2}'.format(connection_string, self.dsn_uid, self.dsn_pwd)
        print("Connection String: " + url)

        db = jdbc.connect(self.jdbc_driver_name, connection_string,
                          {'user': self.dsn_uid, 'password': str(self.dsn_pwd)},
                          jars=self.jdbc_driver_loc)
        db.jconn.setAutoCommit(False)
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
        print(sql)
        self.__execute(self.table_name, sql, values)

class LoadPsqlPG:

    def __init__(self, db_config: list, table_name, db_type):
        self.dsn_hostname = db_config[2]
        self.dsn_port = db_config[5]
        self.dsn_uid = db_config[3]
        self.dsn_pwd = db_config[4]
        self.dsn_database = db_config[6]
        self.jdbc_driver_name = "org.postgresql.Driver"
        self.jdbc_driver_path = "/Users/changliuxin/Programs/datapipeline/loadTest/lib/"
        # self.jdbc_driver_path = "../../lib/"
        self.db_type = db_type
        self.jdbc_driver_loc = os.path.join(self.jdbc_driver_path + self.db_type + ".jar")
        self.table_name = table_name
        self.db = self.__connect()
        self.db_data_path = "/Users/changliuxin/Programs/datapipeline/loadTest/assets/testData/"
        self.db_data_file = "TestData.csv"
        self.csv = self.db_data_path + self.db_data_file
        self.copy_sql = """\COPY {0} FROM STDIN with delimiter as ','""".format(table_name)

    def __del__(self):
        try:
            self.db.close()
        except:
            pass

    def __connect(self):
        connection_string = "jdbc:postgresql://" + self.dsn_hostname + ":" + str(self.dsn_port) + "/" + self.dsn_database
        url = '{0}:user={1};password={2}'.format(connection_string, self.dsn_uid, self.dsn_pwd)
        print("Connection String: " + url)

        db = jdbc.connect(self.jdbc_driver_name, connection_string, {'user': self.dsn_uid, 'password': str(self.dsn_pwd)},
                                  jars=self.jdbc_driver_loc)
        db.jconn.setAutoCommit(False)
        return db

    @db_call
    def __execute(self, table_name):
        cursor = self.db.cursor()
        cursor.execute("truncate table {0}".format(table_name))
        cursor.close()
        self.db.commit()

    def __load(self, table_name):
        sql = """psql --command "copy {0} from STDIN with delimiter as ','" < {1} "host={2} hostaddr={2} port={3} user={4} password={5} dbname={6}"
        """.format(table_name, self.csv, self.dsn_hostname, self.dsn_port, self.dsn_uid, self.dsn_pwd, self.dsn_database)
        print(sql)
        os.system(sql)

    @db_step("GaussDB Load Data")
    def copy(self):
        print('表名: {0}'.format(self.table_name))
        self.__execute(self.table_name)
        self.__load(self.table_name)


if __name__ == '__main__':
    file_path = "/Users/changliuxin/Programs/datapipeline/loadTest/report/save/"
    report_path = "/Users/changliuxin/Programs/datapipeline/loadTest/report/"
    root_path = "/Users/changliuxin/Programs/datapipeline/loadTest/assets/testPlan/"
    excel_name = "TestPlan.xlsx"
    sheet_name = "TestDbSource"

    testinfo = get_testplan(root_path, excel_name, sheet_name)
    for cls in testinfo:
        db_type = cls[1]

        if db_type == "postgresql":
            ins_data = InsertJdbcPG(cls, 'customer', 'postgresql')
            load_data = LoadPsqlPG(cls, 'customer', 'postgresql')

            if ins_data:
                start_time = datetime.datetime.now()
                ins_data.insert()
                db_cost(start_time, "Pg insert data cost:", "pg_insert")

            if load_data:
                start_time = datetime.datetime.now()
                load_data.copy()
                db_cost(start_time, "Pg insert data cost:", "pg_load")