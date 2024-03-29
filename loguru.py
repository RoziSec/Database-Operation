#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File : loguru.py
# @Author : Norah C.IV
# @Time : 2022/8/22 17:37
# @Software: PyCharm
from functools import wraps
import os
import datetime
import loguru


# 单例类的装饰器
def singleton_class_decorator(cls):
    """
    装饰器，单例类的装饰器
    """
    # 在装饰器里定义一个字典，用来存放类的实例。
    _instance = {}

    # 装饰器，被装饰的类
    @wraps(cls)
    def wrapper_class(*args, **kwargs):
        # 判断，类实例不在类实例的字典里，就重新创建类实例
        if cls not in _instance:
            # 将新创建的类实例，存入到实例字典中
            _instance[cls] = cls(*args, **kwargs)
        # 如果实例字典中，存在类实例，直接取出返回类实例
        return _instance[cls]

    # 返回，装饰器中，被装饰的类函数
    return wrapper_class


@singleton_class_decorator
class Logger:
    def __init__(self):
        self.logger_add()

    def get_project_path(self, project_path=None):
        if project_path is None:
            # 当前项目文件的，绝对真实路径
            # 路径，一个点代表当前目录，两个点代表当前目录的上级目录
            project_path = os.path.realpath('..')
        # 返回当前项目路径
        return project_path

    def get_log_path(self):
        # 项目目录
        project_path = self.get_project_path()
        # 项目日志目录
        project_log_dir = os.path.join(project_path, 'log')
        # 日志文件名
        project_log_filename = 'runtime_{}.log'.format(datetime.date.today())
        # 日志文件路径
        project_log_path = os.path.join(project_log_dir, project_log_filename)
        # 返回日志路径
        return project_log_path

    def logger_add(self):
        loguru.logger.add(
            # 水槽，分流器，可以用来输入路径
            sink=self.get_log_path(),
            # 日志创建周期
            rotation='00:00',
            # 保存
            retention='1 year',
            # 文件的压缩格式
            compression='zip',
            # 编码格式
            encoding="utf-8",
            # 具有使日志记录调用非阻塞的优点
            enqueue=True
        )

    @property
    def get_logger(self):
        return loguru.logger


'''
# 实例化日志类
'''
logger = Logger().get_logger


if __name__ == '__main__':
    logger.debug('调试代码')
    logger.info('输出信息')
    logger.success('输出成功')
    logger.warning('错误警告')
    logger.error('代码错误')
    logger.critical('崩溃输出')

    """
    在其他.py文件中，只需要直接导入已经实例化的logger类即可
    例如导入访视如下：
    from project.logger import logger
    然后直接使用logger即可
    """
    logger.info('----原始测试----')
