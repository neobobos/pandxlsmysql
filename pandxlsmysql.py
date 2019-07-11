# encoding=utf-8
import pymysql
import pandas as pd
MYSQL_HOST ='127.0.0.1'
MYSQL_USER = 'root'  #
MYSQL_PASSWD = '****'  #
MYSQL_PORT = 3306
MYSQL_DATA_DB = 'hello_word'
MYSQL_CHARSET = 'utf8mb4'

class excel_to_mysql(object):
    def __init__(self,table):
        self.table = table
    def is_table(self,table_name):
        conn = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, MYSQL_DATA_DB, charset=MYSQL_CHARSET,
                                  port=MYSQL_PORT)
        cur = conn.cursor()
        is_table = ' show tables like "{}";'.format(table_name)
        cur.execute(is_table)
        conn.commit()
        is_table_result = cur.fetchall()
        if len(is_table_result) == 0:
            return False
        else:
            return True

    def delete_table(self,table_name):
        # 连接mysql
        sql_exists = ''' DROP TABLE IF EXISTS {};'''.format(table_name)
        connect = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, MYSQL_DATA_DB, charset=MYSQL_CHARSET,
                                  port=MYSQL_PORT)
        cursor = connect.cursor()
        try:
            cursor.execute(sql_exists)
            connect.commit()
            print('删除成功')
        except Exception as e:
            print(e)
        cursor.close()
        connect.close()


    def create_table(self,cols,table_name):
        cols_txt = ','.join(['`'+x+'`' +' text' for x in cols])
        sql = """CREATE TABLE {} ({}) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8MB4;\
        """.format(table_name,cols_txt)
        # 连接mysql
        connect = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, MYSQL_DATA_DB, charset=MYSQL_CHARSET,
                                  port=MYSQL_PORT)
        cursor = connect.cursor()
        try:
            cursor.execute(sql)
            connect.commit()
            print('创建成功')
        except Exception as e:
            connect.rollback()
            print(e)
        cursor.close()
        connect.close()


    def truncate_table(self, table_name):
        conn = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD,
                               MYSQL_DATA_DB, charset=MYSQL_CHARSET, port=MYSQL_PORT)
        cur = conn.cursor()
        try:
            SQL = 'truncate {tb};'.format(tb=table_name)
            cur.execute(SQL)
            conn.commit()
        except Exception as e:
            print('erros', e)
            conn.rollback()
            return False
        cur.close()
        conn.close()
        return True

    def baker_table(self,table_name):
        conn = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD,
                             MYSQL_DATA_DB, charset=MYSQL_CHARSET, port=MYSQL_PORT)
        cur = conn.cursor()
        table_name_bak = table_name + '_' + 'bak'

        try:
            is_table = ' show tables like "{}";'.format(table_name_bak)
            cur.execute(is_table)
            conn.commit()
            is_table_result = cur.fetchall()

            if len(is_table_result) ==0:
                createSQL = "CREATE TABLE {bak} LIKE {tb}".format(tb=table_name, bak=table_name_bak)
                cur.execute(createSQL)
                conn.commit()
                createSQL = "INSERT INTO {bak} SELECT * FROM {tb};".format(tb=table_name,
                                                                                     bak=table_name_bak)
                cur.execute(createSQL)
                conn.commit()
            else:
                print('备份表已存在，放弃备份')
        except Exception as e:
            print('erros', e)
            conn.rollback()
        cur.close()
        conn.close()
        return table_name_bak

    def get_columns_from_mysql(self,table_name):
        db = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD,
                             MYSQL_DATA_DB, charset=MYSQL_CHARSET, port=MYSQL_PORT)
        cur = db.cursor()
        is_table = ' show tables like "{}";'.format(table_name)
        cur.execute(is_table)
        db.commit()
        is_table_result = cur.fetchall()
        print(is_table_result)
        if len(is_table_result)>=1:
            sql_column = "select *  from {} LIMIT 0,1".format(table_name)
            cur.execute(sql_column)
            cols = cur.description
            mysql_columns = [c[0] for c in cols]
            cur.close()
            db.close()
            return mysql_columns
        else:
            print('此表不存在')
            cur.close()
            db.close()
            return None

    def localdate(self,dates):  # 定义转化日期戳的函数
        import datetime
        delta = datetime.timedelta(days=dates)
        today = datetime.datetime.strptime('1899-12-30', '%Y-%m-%d') + delta
        return datetime.datetime.strftime(today, '%Y-%m-%d')

    def insert_results(self, table, cols, values):
        # 链接数据库
        db = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD,
                             MYSQL_DATA_DB, charset=MYSQL_CHARSET, port=MYSQL_PORT)
        cur = db.cursor()
        sql_insert = "insert into `{}` ({}) values ({});".format(table, ','.join(cols),
                                                                 ','.join(['%s'] * len(values)))
        try:
            cur.execute(sql_insert, values)
            # 提交
            db.commit()
        except Exception as e:
            # 错误回滚
            print("mysql,erro", e)
            db.rollback()
        finally:
            pass
        cur.close()
        db.close()

    def get_excel_path(self,file_path):
        import os,glob
        basepath = os.path.abspath(os.path.realpath(file_path))
        # print(basepath,os.path.realpath(file_path))
        file_paths = [x for x in glob.glob(basepath+'/*.xls*') if '.xls' == x[-4:] or '.xlsx'==x[-5:]]
        return file_paths

    def read_excel_to_mysql(self,start=1,title=True,file_path='',table_name='',add=True):
        if add != True:
            self.truncate_table(table_name)

        print('*'*20,file_path,'*'*20)
        # print('file_title.*',file_title)
        bad_file=False
        try:
            df = pd.read_excel(file_path, converters={1: str}, sheet_name=0, error_bad_lines=False)
        except Exception as e:
            print(e,file_path)
            try:
                df_list = pd.read_html(file_path)
                df = pd.DataFrame(df_list[0])
            except:
                bad_file=True
                print('file is bad')
        if bad_file==False:
            if title:
                titles = df.columns.values
                #start=1
            else:
                titles = self.get_columns_from_mysql(table_name) #EXCEL表里没有字段名，从mysql中读取
                #start = 0
            print(titles)
            idata = iter(df.values[start:])
            # 逐行读取数据，存入数据库
            for index_data,one in enumerate(idata):
                result = []
                for x in one:
                    try:
                        x = str(x).strip()
                    except:
                        x = ''
                    if x == 'nan':
                        x = ''
                    result.append(x.strip().replace('\\n', ' '))

                item = dict(zip(titles, result))
                cols,values = zip(*item.items())
                # print(item)
                try:
                    self.insert_results(self.table, cols, values)
                    print('已插入', index_data)
                except Exception as e:
                    print(e,'插入失败')




if __name__=='__main__':

    table_name = 'hello_world' #目标表名
    p = excel_to_mysql(table_name)
    p.is_table(table_name)  # 判断表是否存在

    # 创建Mysql表
    cols=[chr(x) for x in range(ord('a'), ord('z') + 1)] #字段命名可自定义
    p.create_table(cols=cols,table_name=table_name)
    # 类初始化
    cols= p.get_columns_from_mysql(table_name)  # 获得列名



    #file_paths excle表所在文件夹路径 ，程序将自动选择文件夹里的excel表，若要多个文件合并，sheet表格式与字段必须一致
    # get_excel_path() 获取所有表的路径
    file_paths = p.get_excel_path(file_paths='')

    #title excel首行是否包含标题，默认True，数据从标题下第1行开始读取。若为False,从表格首行0读取
    #start 代表从表格第几行开始读取数据
    #file_path EXCEl表格文件夹目录
    #table_name Mysql表名称
    #add True为追加 False为清空表格写入
    for file_path in file_paths:
        # 读取文件夹下所有表到mysql
        p.read_excel_to_mysql(title=True, file_path=file_path,table_name=table_name,add=True)
    # 判断表是否存在
    t = p.is_table(table_name)
    # 删除表
    p.delete_table(table_name)



