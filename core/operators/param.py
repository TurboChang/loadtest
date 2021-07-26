# encoding: utf-8
# author TurboChang

import os

import openpyxl

from core.exceptions.related_exception import ReadExcelException


def get_testplan(root_path, excel_name, sheet_name=None):
    cls = []
    excel_path = os.path.abspath(os.path.join(root_path, excel_name))
    wb = openpyxl.load_workbook(excel_path)
    sheet = wb[sheet_name]
    rows = sheet.rows

    for i, row in enumerate(rows):
        if i == 0:
            continue
        columns = [cell.value for cell in row]
        cls.append(columns)
        if row is False:
            raise ReadExcelException('sheet页{0}中并无数据'.format(sheet_name))
    return cls

if __name__ == '__main__':
    root_path = "../../assets/testPlan/"
    excel_name = "TestPlan.xlsx"
    sheet_name = "TestDbSource"

    # Oracle
    print([i for i in get_testplan(root_path, excel_name, sheet_name)])

