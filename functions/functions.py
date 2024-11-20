from db import sqlalchemyUtils
from pyetl import DatabaseConnection
import logging
import time
import settings


#dst = pymysqlUtils(host="192.168.66.71", user="root", password="Root@Metis03", db="ecsp")
#database=DatabaseConnection(dst)
dst=sqlalchemyUtils(name_or_url=settings.MYSQL_CHEMY_URL,#"mysql+pymysql://newmetis:NewMetis@Metis03@192.168.66.71:3306/ecsp"
                        echo=False,
                        isolation_level="READ COMMITTED",
                        max_overflow=0,  # 超过连接池大小外最多创建的连接
                        pool_size=500,  # 连接池大小
                        pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
                        pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）)
                    )                   
database=DatabaseConnection(dst)
channels=database.db.get_table('channels').find().get_all()
temp_erp_items=database.db.get_table('temp_erp_item').find().get_all()
exchange_rate=database.db.get_table('exchange_rate').find().get_all()
temp_des_mapping=database.db.get_table('temp_des_mapping').find().get_all()
cost_mapping_table=database.db.get_table('cost_mapping_table').find().get_all()
amazon_b_report_code=database.db.get_table('amazon_b_report_code').find().get_all()
amazon_b_report_collect=database.db.get_table('amazon_b_report_collect').find().get_all()
amazon_b_mapping=database.db.get_table('amazon_b_mapping').find().get_all()
logger = logging.getLogger(f'etl_main')

def categoryFunction(need):
    if '不上' in need:
        return need.replace('不上','')
    else:
        return need

def dateFunction(need):
    need=str(need)
    return need.replace('.','-')+'-01'

def erpAndItemFunciton(need):
    result=database.db.get_table('temp_erp_item').find_one(condition={'asin':need})
    if result:
        return result.get('item_no')
    else:
        #logger.error(f'erp编码找不到对应item_no,erp编码为:{need}')
        database.db.get_table('temp_erp_not_found').upsert({'erp_no':need,'error_date':time.strftime("%Y-%m-%d", time.localtime()) })
        return None


def areaCountryFunction(need):
    channel=list(filter(lambda x:x.get('platform')==need[0] and x.get('country')==need[1],channels))
    return channel[0].get('channel_no')





