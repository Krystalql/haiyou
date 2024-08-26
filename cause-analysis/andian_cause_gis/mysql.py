# -- coding: utf-8 --
import pandas as pd
import pymysql
from sqlalchemy import create_engine


class Mysql:
    # 初始化函数，初始化连接列表
    def __init__(self, host, user, pwd, dbname, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.dbname = dbname
        self.port = port

    # 获取数据库游标对象cursor
    # 游标对象：用于执行查询和获取结果
    def getCursor(self, sql):

        # 建立数据库连接
        self.db = pymysql.connect(self.host, self.user, self.pwd, self.dbname, port=self.port)

        # 创建游标对象
        cur = self.db.cursor()
        cur.execute(sql)
        des = cur.description

        self.head = [[item[0] for item in des]]
        # 返回
        return cur

    def createtable(self, sql, tablename, DF, isDF=True):
        # 使用cursor()方法获取操作游标 
        cursor = self.getCursor(sql)
        # 如果存在表Sutdent先删除

        cursor.execute(f"DROP TABLE IF EXISTS {tablename}")

        if isDF == True:
            # Create a new table.
            sql = f"""CREATE TABLE {tablename} (
                ID CHAR(10) NOT NULL,
                Name CHAR(8),
                Grade INT )"""
            # Write DataFrame data.
            con_engine = create_engine('mysql+pymysql://root:data@123456@47.108.51.23:3306/electric_monitor')
            sql = "select * from test_subcable"
            DF.to_sql(f'{tablename}', con_engine, index=False)
        else:
            sql1 = f"""CREATE TABLE {tablename}(Id INT PRIMARY KEY AUTO_INCREMENT, Data MEDIUMBLOB)"""
            cursor.execute(sql1)
            sql = "select * from transformer_monitor"
            Mysql.insertdb(self, sql, tablename, DF)

    def insertdb(self, sql, tablename, data_to_write, trans_name):

        # 使用cursor()方法获取操作游标
        cursor = self.getCursor(sql)

        # SQL 插入语句
        sql = f"""INSERT INTO {tablename} (
                `{trans_name}` MediumBlob)
             VALUES {data_to_write}"""

        # try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        self.db.commit()
        # except:
        #     # Rollback in case there is any error
        #     print('插入数据失败!')
        #     self.db.rollback()
        self.db.close()

    # 查询操作
    def queryOperation(self, sql):

        # 建立连接获取游标对象
        cur = self.getCursor(sql)
        try:
            # 执行SQL语句
            cur.execute(sql)

            # 获取数据的行数
            row = cur.rowcount

            # 获取查询数据
            # fetch*
            # all 所有数据,one 取结果的一行，many(size),去size行
            dataList = cur.fetchall()

            # 关闭游标对象
            cur.close()
        except pymysql.Error as e:
            print(e)
            print('操作数据库失败')
        # 关闭连接
        self.db.close()

        # 返回查询的数据
        return dataList, row, self.head

    # 删除操作
    def deleteOperation(self, sql):

        # 获取游标对象
        cur = self.getCursor()
        try:
            # 执行SQL语句
            cur.execute(sql)

            # 正常结束事务
            self.db.commit()

        except Exception as e:
            print(e)

            # 数据库回滚
            self.db.rollback()

        # 关闭游标对象
        cur.close()

        # 关闭数据库连接
        self.db.close()

    # 数据更新
    def updateOperation(self, sql):
        cur = self.getCursor()
        try:
            cur.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

        cur.close()
        self.db.close()

    # 添加数据
    def insertOperation(self, sql):

        cur = self.getCursor()
        try:
            cur.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()
        cur.close()
        self.db.close()

    def readtransformerdata(self):
        db = Mysql(host='47.108.51.23', user='root', pwd='data@123456', dbname='electric_monitor', port='3306')
        sql = "select * from transformer_monitor"
        dataList, row = db.queryOperation(sql)
        data_temp = list(dataList)
        print(data_temp)
        return data_temp

    def readevaluationdata(self):
        db = Mysql(host='47.108.51.23', user='root', pwd='data@123456', dbname='electric_monitor', port='3306')
        sql = "select * from device_comprehensive_evaluation_output"
        dataList, row = db.queryOperation(sql)
        data_temp = list(dataList)
        data_temp = pd.DataFrame(data_temp)
        print(data_temp)
        return data_temp


def writepng(tablename, trans_name):
    trans_name = trans_name.replace('-', '_')

    fin = open("C45tree.png", 'rb')
    img = fin.read()
    img_bin = pymysql.Binary(img)
    # print(pymysql.Binary(img))
    # image =  tf.gfile.FastGFile('image/C45tree.jpg','rb').read()
    # print(img)
    fin.close()

    # 链接mysql，获取对象
    conn = pymysql.connect(host='47.108.51.23',
                           port=3306,
                           user='root',
                           passwd='data@123456',
                           db='electric_monitor',
                           charset='utf8',
                           use_unicode=True)
    # 获取执行cursor
    cursor = conn.cursor()

    # cursor.execute(f"DROP TABLE IF EXISTS {tablename}")
    # sql = f"""CREATE TABLE {tablename} (
    # 		`data-123` MediumBlob) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    # cursor.execute(sql)

    sql = f"ALTER TABLE {tablename} add column {trans_name} MediumBlob;"

    cursor.execute(sql)

    sql = f"INSERT INTO {tablename}({trans_name}) VALUES(%s)"  # 将数据插入到mysql数据库中，指令
    # args = ('data',img)
    cursor.execute(sql, (img_bin))
    # 直接将数据作为字符串，插入数据库
    # cursor.execute("INSERT INTO Images SET Data='%s'" % pymysql.Binary(img))
    # sql="INSERT INTO demo_pic_repo (touxiang_data_blob) VALUES  (%s)"
    # cursor.execute(sql, img)
    # 提交数据
    conn.commit()
    # 提交之后，再关闭cursor和链接
    cursor.close()
    conn.close()


def readpng(tablename, pngname):
    conn = pymysql.connect(host='47.108.51.23',
                           port=3306,
                           user='root',
                           passwd='data@123456',
                           db='electric_monitor',
                           charset='utf8',
                           use_unicode=True)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {tablename} LIMIT 1")
    fout = open(f'image/{pngname}.png', 'wb')
    fout.write(cursor.fetchone()[0])
    fout.close()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    conn = pymysql.connect(host='47.108.51.23',
                           port=3306,
                           user='root',
                           passwd='data@123456',
                           db='electric_monitor',
                           charset='utf8',
                           use_unicode=True)
    # 获取执行cursor
    cursor = conn.cursor()

    tablename = 'test_C45treeplot'
    cursor.execute(f"DROP TABLE IF EXISTS {tablename}")
    sql_temp = f"""CREATE TABLE {tablename} (
                `Transformer Name` double) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    cursor.execute(sql_temp)

    writepng(f'{tablename}', 'ZJ_DF_10-01_BYQ002')
    writepng(f'{tablename}', 'ZJ_DF_10-03_BYQ009')

    conn.commit()
    cursor.close()
    conn.close()
