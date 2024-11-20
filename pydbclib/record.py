# -*- coding: utf-8 -*-
"""
@time: 2020/4/13 11:28 下午
@desc:
"""
import itertools


def to_df_iterator(records, batch_size):
    import pandas
    while 1:
        _records = records.get(batch_size)
        if _records:
            yield pandas.DataFrame.from_records(_records)
        else:
            return None


class Records(object):

    def __init__(self, rows, as_dict):
        self._rows = rows
        self.as_dict = as_dict
        self._limit_num = None

    def __iter__(self):
        return self

    def next(self):
        return next(self._rows)

    __next__ = next

    def map(self, function):
        self._rows = (function(r) for r in self._rows)
        return self

    def filter(self, function):
        self._rows = (r for r in self._rows if function(r))
        return self

    def rename(self, mapper):
        """
        字段重命名
        """
        def function(record):
            if isinstance(record, dict):
                return {mapper.get(k, k): v for k, v in record.items()}
            else:
                return dict(zip(mapper, record))
        return self.map(function)

    def limit(self, num):
        def rows_limited(rows, limit):
            for i, r in enumerate(rows):
                if i < limit:
                    yield r
                else:
                    return None
        self._rows = rows_limited(self._rows, num)
        return self

    def get_one(self):
        r = self.get(1)
        return r[0] if len(r) > 0 else None

    def get(self, num):
        return [i for i in itertools.islice(self._rows, num)]

    def get_all(self):
        return [r for r in self._rows]

    def to_df(self, batch_size=None):
        if batch_size is None:
            import pandas
            return pandas.DataFrame.from_records(self)
        else:
            return to_df_iterator(self, batch_size)

    def to_csv(self, file_path, sep=',', header=False, columns=None, batch_size=100000, **kwargs):
        """
        用于大数据量分批写入文件
        :param file_path: 文件路径
        :param sep: 分割符号，hive默认\001
        :param header: 是否写入表头
        :param columns: 按给定字段排序
        :param batch_size: 每批次写入文件行数
        """
        mode = "w"
        for df in self.to_df(batch_size=batch_size):
            df.to_csv(file_path, sep=sep, index=False, header=header, columns=columns, mode=mode, **kwargs)
            mode = "a"
            header = False
