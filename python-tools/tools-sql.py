# -- coding: utf-8 --
import re
import json
import os
import pymysql
import copy


class SqlTools(object):
    data_count = 0

    def __init__(self, **kwargs):

        self.source_list = list()
        self.sql_data_list = list()
        # self.filename_json = kwargs.get('json', '')
        self.filename = kwargs.get('file', '')
        self.names = kwargs.get('names', '')
        # self.sql_script_lines = ''
        self.__data_func = kwargs.get('data_func', lambda data: tuple(data))
        self.file_line = True
        self.steam = False
        self.__file_fd = None
        self.sql_line_count = 0

    @staticmethod
    def sq_val(val):
        # 处理python到mysql的各种基础类型数据转换成字符串
        default_type = ('default', 'null')
        ret = ''
        if isinstance(val, str):
            if val in default_type:
                ret += f"{val}"
            else:
                ret += f"'{val}'"
        elif isinstance(val, float):
            ret += f"%.2f" % val
        else:
            ret += f"%s" % val
        return ret

    # 对数据类型进行转换成mysql语句
    @staticmethod
    def script_unpack_values(values):
        ret = ''
        if isinstance(values, list) or isinstance(values, tuple):
            for i, val in enumerate(values):
                if i >= 1:
                    ret += ','

                ret += SqlTools.sq_val(val)

        else:

            ret += SqlTools.sq_val(values)

        ret = f'({ret})'

        return ret

    @staticmethod
    def sq_unpack_names(names):
        # 对name进行mysql语句转换
        _name = ''
        if isinstance(names, str):
            _name = f"`{names}`"

        elif isinstance(names, list) or isinstance(names, tuple):
            for i, name in enumerate(names):
                if i >= 1:
                    _name += ","

                _name += f'`{name}`'

            _name = f'({_name})'
        return _name

    @staticmethod
    def script_insert(table='', values='', names=''):
        # 生成insert语句
        if not table:
            print("table name is None")
            return
        elif not values:
            print("data is empty")
            return
        _values = ''
        _name = ''

        if isinstance(values, list) or isinstance(values, tuple):
            for i, val in enumerate(values):
                if i >= 1:
                    _values += ","
                _values += SqlTools.script_unpack_values(val)
        else:
            _values = f'({SqlTools.sq_val(values)})'

        _name = SqlTools.sq_unpack_names(names)
        ret = f'insert into {table} {_name} values {_values};'
        return ret

    @staticmethod
    def convert_data(data_list, func):
        # 数据转换
        ret = list()
        if len(data_list) == 0:
            return None
        for data in data_list:
            ret.append(func(data))
        return ret

    def files_load_json(self):
        # 载入json文件
        try:
            if self.file_line:
                if self.steam:
                    if not self.__file_fd:
                        self.__file_fd = open(self.filename, 'r')
                    data_line = self.__file_fd.readline()

                    while data_line:
                        self.source_list.clear()
                        data = json.loads(data_line)
                        self.source_list.append(data)
                        yield self.source_list
                        data_line = self.__file_fd.readline()
                    else:
                        self.__file_fd.close()
                        self.__file_fd = None
                else:
                    with open(self.filename, 'r') as f:
                        data_lines = f.readlines()
                        for i in data_lines:
                            data = json.loads(i)
                            self.source_list.append(data)

                    yield self.source_list
            else:
                with open(self.filename, 'r') as f:
                    data = json.loads(f.read())
                    self.source_list.extend(data)
                    yield self.source_list
        except Exception as ret:
            print('open Json Exception', ret)

    def json_files(self, names, **kwargs):
        self.filename = kwargs.get('file', '')
        # self.filename_data = kwargs.get('data', '')
        self.__data_func = kwargs.get('data_func', lambda data: tuple(data))
        self.names = names
        self.steam = kwargs.get('steam', False)
        # self.sql_script_lines = ''
        if not self.filename:
            print("未设置文件名")
            return
        elif not re.search(r'(\.data$)', self.filename):
            print("文件类型不符合")
            return
        elif re.search(r'(\.line\.data$)', self.filename):
            self.file_line = True

        # print(f"%s %s" % (self.sql_data_list[0], self.data_count))

    def convert(self):
        pass

    def get_insert(self):
        ret = list()
        try:
            if self.steam:
                # ret = list()
                for sql_data in self.files_load_json():
                    if len(sql_data) > 0:
                        # print(self.__file_fd,self.__data_func)
                        self.sql_data_list = self.convert_data(sql_data, self.__data_func)
                        # print(self.sql_data_list)
                        ret.append(self.script_insert(table='goods_data', names=self.names, values=self.sql_data_list))
                self.sql_line_count = len(ret)

            else:
                self.files_load_json().__next__()
                if len(self.source_list) > 0:
                    self.sql_data_list = self.convert_data(self.source_list, self.__data_func)
                    self.data_count = len(self.sql_data_list)
                    ret.append(self.script_insert(table='goods_data', names=self.names, values=self.sql_data_list))

                    # ret = self.script_insert(table='goods_data', names=self.names, values=self.sql_data_list)
                    # self.sql_line_count = 1
        except StopIteration as ex:
            print('StopIteration ', ex)
        except Exception as ex:
            print("insert Exception", ex)
        finally:
            self.sql_line_count = len(ret)
            return ret

    def execute(self, sql_script, **kwargs):
        default_host = {'host': kwargs.get('host', '127.0.0.1'),
                        'user': kwargs.get('user', 'root'),
                        'port': kwargs.get('port', 3306),
                        'password': kwargs.get('password', 123456),
                        'database': kwargs.get('database', ''),
                        'charset': kwargs.get('charset', 'utf8')
                        }

        # 执行sql语句
        conn = pymysql.connect(default_host, **kwargs)
        cursor = conn.cursor()

        count = cursor.execute("select * from students;")

        for i in cursor.fetchall():
            print(i)

        conn.close()


if __name__ == "__main__":
    # demo
    # 处理文件中数据
    def jd_goods(goods):
        href, name, price, commit = goods
        re_price = re.search(r"\d+(\.?\d+)", price)
        if re_price:
            price = float(re_price.group())

        ret = tuple([href, name, price, commit])
        return ret


    st = SqlTools()
    st.json_files(file='mysql/jd_21_2_18.line.data', names=['href_url', 'name', 'price', 'commit'],
                  data_func=jd_goods,
                  # steam=True
                  )
    insert = st.get_insert()
    print(insert)
    print(st.data_count, st.sql_line_count)
    # print(st.get_insert(), st.sql_line_count)
    # print(st.get_insert())
    # print(st.get_insert())
