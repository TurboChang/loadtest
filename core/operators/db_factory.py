# encoding: utf-8
# author TurboChang

import xlsxwriter
import re
from core.operators.param import get_testplan
from core.logics.db.dbs.db_oracle import *
from core.logics.db.dbs.db_jdbc import *
from core.logics.db.dbs.db_mysql import *
# from core.logics.db.dbs.db_postgresql import *
# from core.logics.db.dbs.db_tidb import *
from core.exceptions.related_exception import DBNotSupportedException
import datetime

file_path = "../../report/save/"
report_path = "../../report/"

def db_cost(start_time, step_name, report):
    """
    记录数据库操作耗时
    """
    stop_time = datetime.datetime.now()
    ms = (stop_time - start_time).seconds
    command = "echo {0} {1}s >> ../../report/save/{2}.txt".format(step_name, ms, report)
    os.system(command)

def insert_db_instance(table_name):
    root_path = "../../assets/testPlan/"
    excel_name = "TestPlan.xlsx"
    sheet_name = "TestDbSource"
    command = "python /Users/changliuxin/Programs/datapipeline/loadTest/core/logics/db/dbs/"

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

        # # PostgreSQL
        # elif db_type == "postgresql":
        #     ins_data = InsertJdbcPG(cls, 'customer', 'postgresql')
        #     load_data = LoadPsqlPG(cls, 'customer', 'postgresql')
        #
        #     if ins_data:
        #         start_time = datetime.datetime.now()
        #         ins_data.insert()
        #         db_cost(start_time, "Pg insert data cost:", "pg_insert")
        #
        #     if load_data:
        #         start_time = datetime.datetime.now()
        #         load_data.copy()
        #         db_cost(start_time, "Pg insert data cost:", "pg_load")

        # GreenPlum
        # elif db_type == "greenplum":
        #     ins_data = InsertJDBC(cls, 'customer', 'greenplum')
        #     load_data = LoadPSQL(cls, 'customer', 'greenplum')
        #
        #     if ins_data:
        #         start_time = datetime.datetime.now()
        #         ins_data.insert()
        #         db_cost(start_time, "Gp insert data cost:", "gp_insert")
        #
        #     if load_data:
        #         start_time = datetime.datetime.now()
        #         load_data.copy()
        #         db_cost(start_time, "Gp insert data cost:", "gp_load")

        # GP/PG
        elif db_type == "greenplum":
            os.system(command + "db_greenplum.py")
        elif db_type == "postgresql":
            os.system(command + "db_postgresql.py")

        # GaussDB
        elif db_type == "gaussdb":
            ins_data = InsertJDBC(cls, 'customer', 'gaussdb')
            load_data = LoadPSQL(cls, 'customer', 'gaussdb')

            if ins_data:
                start_time = datetime.datetime.now()
                ins_data.insert()
                db_cost(start_time, "Gauss insert data cost:", "gauss_insert")

            if load_data:
                start_time = datetime.datetime.now()
                load_data.copy()
                db_cost(start_time, "Gauss insert data cost:", "gauss_load")

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
            # ins_data = InsertTiDB(cls, table_name)
            # load_data = LoadTiDB(cls, table_name)
            #
            # if ins_data:
            #     start_time = datetime.datetime.now()
            #     ins_data.insert()
            #     db_cost(start_time, "Mysql insert data cost:", "tidb_insert_10w")
            #
            # if load_data:
            #     start_time = datetime.datetime.now()
            #     load_data.load()
            #     db_cost(start_time, "Oracle load data cost:", "tidb_load_10w")
            continue

        else:
            raise DBNotSupportedException('{0} 测试不通过'.format(db_type))
            # continue

def pars_report(file_name):
    """
    提取临时报告中的小数, sum之后再avg
    """
    files = file_path+file_name
    file = open(files, encoding="utf-8").readlines()
    count = len(file)
    sum = 0
    for row in file:
        v1 = re.search("\d+(\.\d+)?", row).group(0)
        data = float(v1)
        sum += data
    if sum == 0.0:
        pass
    else:
        data = round(sum/count, 3)
        return data

def get_file_name():
    files_name = []
    for path, dirs, files in os.walk(file_path):
        files_name.append(files)
    return files_name

def file_group():
    file_name = get_file_name()
    lists = []
    res_dict = {}

    for file in file_name[0]:

        # gp
        if file[0:2] == "gp":
            if file[3:7] == "load":
                if file[8:11] == "10w":
                    lists.append("Gp  10w load avg: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("Gp  5k load avg:  {0}{1}".format(pars_report(file), "s"))
            if file[3:9] == "insert":
                if file[10:13] == "10w":
                    lists.append("Gp  10w insert avg: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("Gp  5k insert avg:  {0}{1}".format(pars_report(file), "s"))

        # pg
        if file[0:2] == "pg":
            if file[3:7] == "load":
                if file[8:11] == "10w":
                    lists.append("Pg  10w load: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("Pg  5k load:  {0}{1}".format(pars_report(file), "s"))
            if file[3:9] == "insert":
                if file[10:13] == "10w":
                    lists.append("Pg  10w insert: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("Pg  5k insert:  {0}{1}".format(pars_report(file), "s"))

        # oracle
        if file[0:3] == "ora":
            if file[4:8] == "load":
                if file[9:12] == "10w":
                    lists.append("ora 10w load: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("ora 5k load:  {0}{1}".format(pars_report(file), "s"))
            if file[4:10] == "insert":
                if file[11:14] == "10w":
                    lists.append("ora 10w insert: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("ora 5k insert:  {0}{1}".format(pars_report(file), "s"))

        # tidb
        if file[0:4] == "tidb":
            if file[5:9] == "load":
                if file[10:13] == "10w":
                    lists.append("tidb 10w load: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("tidb 5k load:  {0}{1}".format(pars_report(file), "s"))
            if file[5:11] == "insert":
                if file[12:15] == "10w":
                    lists.append("tidb 10w insert: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("tidb 5k insert:  {0}{1}".format(pars_report(file), "s"))

        # mysql
        if file[0:5] == "mysql":
            if file[6:10] == "load":
                if file[11:14] == "10w":
                    lists.append("mysql 10w load: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("mysql 5k load:  {0}{1}".format(pars_report(file), "s"))
            if file[6:12] == "insert":
                if file[13:16] == "10w":
                    lists.append("mysql 10w insert: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("mysql 5k insert:  {0}{1}".format(pars_report(file), "s"))

        # gaussdb
        if file[0:5] == "gauss":
            if file[6:10] == "load":
                if file[11:14] == "10w":
                    lists.append("gauss 10w load: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("gauss 5k load:  {0}{1}".format(pars_report(file), "s"))
            if file[6:12] == "insert":
                if file[13:16] == "10w":
                    lists.append("gauss 10w insert: {0}{1}".format(pars_report(file), "s"))
                else:
                    lists.append("gauss 5k insert:  {0}{1}".format(pars_report(file), "s"))

    data = sorted(lists)
    return data

def get_report(report_name):
    report = report_path+report_name
    os.system("touch {0}".format(report))
    report_file = open(report, 'a+')
    for row in file_group():
        report_file.write(row + "\n")

def parse_txt(file_name):
    file = file_path + file_name
    list = []
    indx = []
    with open(file, "r") as op:
        for row in op:
            tmp = row.split()
            data = int(tmp[4].replace("s", ""))
            list.append(data)
    for i in range(1, len(list)+1):
        indx.append(i)
    return [list, indx]

def write_form(sheet_name):
    file_name = report_path + "report.xlsx"
    wb = xlsxwriter.Workbook(file_name)
    ws = wb.add_worksheet(sheet_name)
    bold = wb.add_format({'bold': 1})

if __name__ == '__main__':
    # for i in range(0,40):
    #     insert_db_instance('customer')
    # print(file_group())
    get_report("report.txt")


