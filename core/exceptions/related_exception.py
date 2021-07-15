# encoding: utf-8
# author TurboChang

class RegressionException(Exception):
    """回归测试级别的异常基类"""

    def __init__(self, msg='', logger=None):
        self.message = msg
        if logger:
            logger.error(msg)

        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

    __str__ = __repr__


class ReadExcelException(RegressionException):
    """读取excel配置时出现错误"""
    pass

class ReadCsvException(RegressionException):
    """读取测试数据时出现错误"""
    pass

class DBNotSupportedException(RegressionException):
    """数据库系统不支持错误"""
    pass