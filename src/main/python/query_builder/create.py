# create.py
import sys
import logging
#from src.config.rds_config import *
import pymysql

# rds settings




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
    return table_name

def att_num() :
    num = int(input('attribute의 갯수를 입력해주세요 : '))
    return num

def attribute(num):
        att_name = input('{}번째 attribute 의 이름을 쓰세요  :  '.format(num+1))
        return att_name

def datatype(a):
    datatype = input('{}번째 attribute의 Data_Type을 입력하세요 : '.format(int(a)+1))
    return datatype

def datacons(num):
    cons = input('{}번째 attribute의 나머지 조건을 입력해주세요(없을시 0 입력) : '.format(num+1))
    if  cons == "0"  :
        cons = ""
        return cons
    else :
        return cons

def foreign_key() :
    foreign = input('FK키가 되는 attribute를 입력하세요 (2개 이상일 경우 ,로 구분  없을시 0 입력) : ')
    if foreign == "0" :
        foreign = ""
        return foreign
    else :
        references_table = input(" reference table 이름을 입력해주세요 : ")
        references_att= input(" reference attribute 이름을 입력해주세요 : ")
    return [references_table, references_att]



#def more_data:
  #  a= input()

  #  if attributses[len(atributeses)-1] = -1 :
  #      cur.execute("create ",table_name," ( ",)


def CreatTable():
    table_name= create_table()
    attNumber= int(att_num())
    attName =  [0 for i in range(attNumber)]
    attType= [0 for i in range(attNumber)]
    attCons= [0 for i in range(attNumber)]
    full_sentence = [0 for i in range(attNumber)]
    for n in range(0,attNumber):
        attName[n]=attribute(n)
        attType[n]=datatype(n)
        attCons[n]=datacons(n)
        full_sentence[n]= attName[n]+ "  " + attType[n]+ "  " + attCons[n]
        print(full_sentence[n])
    foreignKey=foreign_key()
    i = 0
    full_full_sentence = ""
    while i < len(full_sentence)-1:
        full_full_sentence += str(full_sentence[i])
        full_full_sentence += str(" , ")
        i += 1
        if i == len(full_sentence)-1:
            full_full_sentence += str(full_sentence[i])

    print('create table if not exists '+table_name+' ('+ full_full_sentence + " ) ")
    with conn.cursor() as cur:
          cur.execute('create table if not exists '+table_name+' ('+ full_full_sentence + " ) ")
          conn.commit()



         #cur.execute("select * from recipe")


CreatTable()


#
# with conn.cursor() as cur:
#         sentence=[]
#
#     for n in att_num() :
#             sentence[n-1] = str(
#             cur.excute("create table ",table_name," ( ",   )


#getData = {}
# cur.execute("create table {}


# with conn.cursor() as cur:
#         cur.execute(create_)

# create_table()

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

#print ("Added %d items from RDS MySQL table" %(item_count))


    # print("Added %d items from RDS MySQL table" % item_count)