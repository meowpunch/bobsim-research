import sys
import logging
from src.constant.rds_config import *
import pymysql
#rds settings

rds_host  = "production-bobsim-aurora-instance-1.cm9kakdihdlv.ap-northeast-2.rds.amazonaws.com"
name = db_username
password = db_password
database_name = db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

"""
    This function fetches content from MySQL RDS instance
"""

#item_count = 0


#creat_table_name= ''
#creat_table_attributes={}
#테이블 이름을 쓰시오

def create_table() :
    table_name = input('테이블 이름을 쓰시오 : ')
    attribute()
    return table_name

def att_num() :
    num = int(input('attribute의 갯수를 입력해주세요 : '))
    return num

def attribute(num):
        att_name = list(input('{}번째 attribute 의 이름을 쓰세요 (없으면 0입력) :  '.format(num)))
        return att_name
def datatype(a):
    datatype = input('{}번째 attribute의 Data_Type을 입력하세요 : '.format(int(a)))
    return datatype
def datacons(a):
    constraint = input(' 나머지 조건을 입력해주세요 : ')
    return constraint
#def more_data:
  #  a= input()

  #  if attributses[len(atributeses)-1] = -1 :
  #      cur.execute("create ",table_name," ( ",)


class CreatTable :
    table_name= create_table()
    att_number= int(att_num())
    for n in att_number:
        att_name=list(attribute(n))
        att_type=list(datatype(n))
        att_cons=list(datacons(n))

    with conn.crsor() as cur:
        sentence=[]

        for n in att_num() :
            sentence[n-1] = str("creat table ",

            cur.excute("create table ",table_name," ( ",   )


#getData = {}
# cur.execute("create table {}


# with conn.cursor() as cur:
#         cur.execute(create_)

create_table()

#with conn.cursor() as cur:
        # cur.execute("create table Employee ( EmpID  int NOT NULL, Name varchar(255) NOT NULL, PRIMARY KEY (EmpID))")
        # cur.execute('insert into Employee (EmpID, Name) values(1, "Joe")')
        # cur.execute('insert into Employee (EmpID, Name) values(2, "Bob")')
        # cur.execute('insert into Employee (EmpID, Name) values(3, "Mary")')
        # conn.commit()
        #cur.execute("select * from recipe")
        # for row in cur:
        #     item_count += 1
        #     logger.info(row)
        #     #print(row)
#conn.commit()

print ("Added %d items from RDS MySQL table" %(item_count))


    # print("Added %d items from RDS MySQL table" % item_count)
