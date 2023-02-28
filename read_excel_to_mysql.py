
filename="/Users/qz/Downloads/cxy/company_name.csv"
date_and_price="/Users/qz/Downloads/yfsql/20220216_no_header.csv"
import csv
import db_mysql


def read_company_name():
    with open(filename) as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            return row

def read_data(company_name,conn):
    with open(date_and_price) as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            pk_date=row[0]
            # 2022/1/1  2022-01-01
            split = pk_date.split('/')
            month=''
            day=''
            if len(split[1])==1:
                month='0'+split[1]
            if len(split[2])==1:
                day='0'+split[2]
            pk_date=split[0]+'-'+month+'-'+day
            sql_header=""" INSERT INTO stock_price.history_price (pk_date,company_name,price) VALUES """
            values=''
            for i in range(len(company_name)):
                if i==0:
                    continue
                else:
                    c_name = company_name[i]
                    price = row[i]
                    if price =='':
                        price='null'
                    values=values+"""(\'{0}\', \'{1}\', {2}),""".format(pk_date, c_name,price)
                    # print(values)
                    # print(pk_date+" - "+c_name+" - "+price)
            execute_sql=(sql_header+values)[:-1]+';'
            # print(execute_sql)
            # break
            db_mysql.execute_one(conn,execute_sql)
        conn.close()
if __name__ == '__main__':
    company_name_list = read_company_name()
    conn = db_mysql.get_conn('localhost', 3306, 'root', '', 'stock_price')
    read_data(company_name_list,conn)




