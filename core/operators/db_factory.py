# encoding: utf-8
# author TurboChang


from loadTest.core.operators.param import get_testplan
from loadTest.core.logics.db.dbs.db_oracle import *
from loadTest.core.logics.db.dbs.db_postgresql import *
from loadTest.core.logics.db.dbs.db_greenplum import *
from loadTest.core.logics.db.dbs.db_gaussdb import *
from loadTest.core.logics.db.dbs.db_mysql import *
from loadTest.core.logics.db.dbs.db_tidb import *
from loadTest.core.exceptions.related_exception import DBNotSupportedException
import datetime

def db_cost(start_time, step_name, report):
    """
    记录数据库操作耗时
    """
    stop_time = datetime.datetime.now()
    ms = (stop_time - start_time).microseconds / 1000
    command = "echo {0} {1}ms >> ../../report/save/{2}.txt".format(step_name, ms, report)
    os.system(command)

def insert_db_instance(table_name):
    root_path = "../../assets/testPlan/"
    excel_name = "TestPlan.xlsx"
    sheet_name = "TestDbSource"

    testinfo = get_testplan(root_path, excel_name, sheet_name)
    for cls in testinfo:
        db_type = cls[1]

        # Oracle
        if db_type == "oracle":
            ins_data = InsertOracleDB(cls, table_name)
            load_data = LoadOracleDB(cls)

            if ins_data:
                start_time = datetime.datetime.now()
                ins_data.insert()
                db_cost(start_time, "Oracle insert data cost:", "ora_insert")

            if load_data:
                start_time = datetime.datetime.now()
                load_data.sqlldr()
                db_cost(start_time, "Oracle load data cost:", "ora_load")

        # PostgreSQL
        elif db_type == "postgresql":
            ins_data = InsertPostgresDB(cls, table_name)
            load_data = LoadPostgresDB(cls, table_name)

            if ins_data:
                start_time = datetime.datetime.now()
                ins_data.insert()
                db_cost(start_time, "Pg insert data cost:", "pg_insert")

            if load_data:
                start_time = datetime.datetime.now()
                load_data.copy()
                db_cost(start_time, "Pg insert data cost:", "pg_load")

        # GreenPlum
        elif db_type == "greenplum":
            ins_data = InsertGreenPlumDB(cls, table_name)
            load_data = LoadGreenPlumDB(cls, table_name)

            if ins_data:
                start_time = datetime.datetime.now()
                ins_data.insert()
                db_cost(start_time, "Gp insert data cost:", "gp_insert")

            if load_data:
                start_time = datetime.datetime.now()
                load_data.copy()
                db_cost(start_time, "Gp insert data cost:", "gp_load")
            # continue

        # GaussDB
        elif db_type == "gaussdb":
            """psycopg2需要使用opengauss lib重新编译"""
            continue

        # Mysql
        elif db_type == "mysql":
            ins_data = InsertMysqlDB(cls, table_name)
            load_data = LoadMysqlDB(cls, table_name)

            if ins_data:
                start_time = datetime.datetime.now()
                ins_data.insert()
                db_cost(start_time, "Mysql insert data cost:", "mysql_insert")

            if load_data:
                start_time = datetime.datetime.now()
                load_data.load()
                db_cost(start_time, "Oracle load data cost:", "mysql_load")
            # continue

        # DB2
        elif db_type == "db2":
            continue

        # Redshift
        elif db_type == "redshift":
            continue

        # Sequoiadb
        elif db_type == "sequoiadb":
            continue

        # SqlServer
        elif db_type == "sqlserver":
            continue

        # Tidb
        elif db_type == "tidb":
            ins_data = InsertTiDB(cls, table_name)
            load_data = LoadTiDB(cls, table_name)

            if ins_data:
                start_time = datetime.datetime.now()
                ins_data.insert()
                db_cost(start_time, "Mysql insert data cost:", "tidb_insert")

            if load_data:
                start_time = datetime.datetime.now()
                load_data.load()
                db_cost(start_time, "Oracle load data cost:", "tidb_load")

        else:
            raise DBNotSupportedException('{0} 测试不通过'.format(db_type))
            # continue


if __name__ == '__main__':
    for i in range(1, 10):
        insert_db_instance('customer')


