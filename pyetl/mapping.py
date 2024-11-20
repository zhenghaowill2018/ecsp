# -*- coding: utf-8 -*-
"""
@time: 2021/01/06 11:29 上午
@desc:
"""
import logging
import traceback 


class ColumnsMapping(object):

    def __init__(self, columns):
        self.raw_columns = columns
        self.alias, self.columns = self.get_src_columns_alias()

    def get_src_columns_alias(self):
        alias = {}
        for k, v in self.raw_columns.items():
            if isinstance(v, (list, tuple)):
                for i, name in enumerate(v):
                    alias.setdefault(name, "%s_%s" % (k, i))
            else:
                alias.setdefault(v, k)

        columns = {}
        for k, v in self.raw_columns.items():
            if isinstance(v, (list, tuple)):
                columns[k] = tuple(alias[n] for n in v)
            else:
                columns[k] = alias[v]
        return alias, columns


class Mapping(object):

    def __init__(self, columns, functions, apply_function):
        self.columns = columns
        self.functions = functions
        self.apply_function = apply_function
        self.logger = logging.getLogger(f'DatabaseConnection')
        self.total = 0

    def __call__(self, record):
        try:
            result = {}
            for k, v in self.columns.items():
                if isinstance(v, (list, tuple)):
                    result[k] = self.functions.get(k, lambda x: x)(tuple(record.get(n) for n in v))
                    #result[k] = self.functions.get(k, lambda x: ",".join(map(str, x)))(tuple(record.get(n) for n in v))
                else:
                    value = record.get(v)
                    result[k] = self.functions[k](value) if k in self.functions else value
            self.total += 1
        except Exception as e:
            self.logger.error(f'Mapping __call__出现错误!!!{e.args}')
        return self.apply_function(result)


class FlatMapping(object):

    def __init__(self, flatMapFunciton, apply_function):
        self.flatMapFunciton = flatMapFunciton
        self.apply_function = apply_function
        self.logger = logging.getLogger(f'DatabaseConnection')

    def __call__(self, record,conditions):
        try:
            if self.flatMapFunciton:
                result=self.flatMapFunciton(record,conditions)
            else:
                return record
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f'FlatMapping __call__出现错误!!!{e.args}')
        #转换为寄存器
        result=(x for x in result)
        return self.apply_function(result)