# encoding: utf-8
# author TurboChang


import argparse
import sys
print(sys.path)
from core.operators.db_factory import *

PLAN_TABLE = '执行指定的测试表'
PLAN_COUNT = '执行制定的测试次数'


def _prepare_cli():
    """
    配置runner支持的构建参数
    --table_name: 执行指定的测试表
    --num: 执行指定的测试次数
    :return: argument parser
    """
    print('DP Runner 开始运行!!!')
    parser = argparse.ArgumentParser(prog='operators',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description="多数据节点load/insert性能对比")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--table_name', required=True, type=str, help=PLAN_TABLE)
    group.add_argument('--num', required=True, type=int, default=2, nargs='+', help=PLAN_COUNT)
    (options, args) = parser.parse_known_args()
    return options, args

class TestRunner:

    def __init__(self):
        self.options, self.args = _prepare_cli()
        self.num = self.args.num
        self.table_name = self.args.table_name
        self.repoter_path = "report/save/"

    def runtest(self):
        for i in range(0, self.num):
            insert_db_instance(self.table_name)

    def repoter(self):
        # self.runtest()
        report_name = os.listdir(self.repoter_path)
        print(report_name)



if __name__ == '__main__':
    # exit(TestRunner().runtest())
    import sys
    sys.path.append(os.path.dirname(sys.path[0]))
    f=TestRunner()
    f.repoter()

