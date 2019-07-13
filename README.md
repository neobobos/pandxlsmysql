# pandxlsmysql

读取各种EXCEL表到MySQL数据库。using this utils called Pandxlsmysql to transfer excle sheets to mysqldatabase,funny done.

-----------------------------------------------------------------------

*必安装依赖包*
python 2.7+
pandas
pymysql

-----------------------------------------------------------------------

#### Mysql 目标表名：
```
table_name = 'hello_world'
```
#### 实例化：
```
p = excel_to_mysql(table_name)
```
#### 判断表是否存在：
```
p.is_table(table_name)
```
#### 字段命名，可自定义：
```
cols = [chr(x) for x in range(ord('a'), ord('z') + 1)]
```
#### 创建Mysql表：
```
p.create_table(cols=cols,table_name=table_name)
```
#### 从Mysql获得表列名 ：
```
cols= p.get_columns_from_mysql(table_name)
```

#### 获取所有excle表文件的路径；若要多个文件合并，sheet表格式与字段必须一致：
```
p.get_excel_path(file_paths='')
```
#### 将文件夹下所有excel文件xls/xlsx入库到Mysql表：
```
for file_path in file_paths:
p.read_excel_to_mysql(title=True, file_path=file_path,table_name=table_name,add=True)
```
##### read_excel_to_mysql 参数说明：
* title excel首行是否包含标题，默认True，数据从标题下第1行开始读取。若为False,从表格首行0读取
* start 代表从表格第几行开始读取数据
* file_path EXCEl表格文件夹目录
* table_name Mysql表名称
* add True为追加 False为清空表格写入

#### 判断表是否存在：
```
p.is_table(table_name)
```
#### 删除表：
```
p.delete_table(table_name)
```


