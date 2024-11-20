import datetime
import logging
import re
import time
from functools import reduce,wraps
import math
from hashlib import md5
from utils import test_date

import numpy as np
import settings
from pyetl import DatabaseConnection

from .functions import (channels, dateFunction, dst, exchange_rate,

                        temp_erp_items,temp_des_mapping,cost_mapping_table)
logger = logging.getLogger(f'etl_main')

def filterNanFunction(func):
    """
    定义处理时成功，异常捕获接口
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if isinstance(args[0],float) and math.isnan(args[0]):
            res_list=list(args)
            res_list[0]=None
            args=tuple(res_list)
        r = func(*args, **kwargs)
        return r
    return wrapper

def generalFilterFunction(func):
    """
    定义处理时成功，异常捕获接口
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        code=args[1].get('code')
        filter_funciton_names=code.get('filter_funciton')
        if filter_funciton_names!=None:
            for filter_funciton_name in filter_funciton_names.split(','):
                FILTER_FUNCTIONS[filter_funciton_name](args)
        r = func(*args, **kwargs)
        return r
    return wrapper

@filterNanFunction
def intFunction(value):
    if isinstance(value,str):
        value=value.replace(',','')
    return value

@filterNanFunction
def floatFunction(value):
    if isinstance(value,str):
        value=value.replace(',','').replace('%','').replace('$','').replace('€','')
        return float(re.search('-?(\d*\.?\d{0,2})$',value).group(0))
    else:
        return value

@filterNanFunction
def dateTimeFunction(value):
    if isinstance(value,str):
        if value.find('T')>=0:
            value=datetime.datetime.strptime(value[:-6],"%Y-%m-%dT%H:%M:%S")#value.replace('/','-')
        else:
            value=datetime.datetime.strptime(value,"%m/%d/%Y")
    else:
        value=value.to_pydatetime()
        #value=value
    return value

@filterNanFunction
def varcharFunction(value):
    return value

def addDateFunction(args):
    #args[1].get('names')[2][:10]
    if args[0].get('return_date',None)==None:
        args[0]['snapshot_date']=datetime.datetime.strptime(args[1].get('names')[2][:8],"%Y%m%d")
    else:
        args[0]['snapshot_date']=args[0]['return_date'][:10]

def judgeCountryFunction(args):
    if args[1]['code']['country']=='美国':
        args[0]['country']='美国' if args[0]['order_id']!=None and args[0]['order_id'][:1]==1 else '加拿大'
    elif args[1]['code']['country']=='英国':
        if args[0]['order_id'][:3]=='026' or args[0]['order_id'][:1]=='2':
            args[0]['country']='英国'
        elif args[0]['order_id'][:3]=='028' or args[0]['order_id'][:1]=='3':
            args[0]['country']='德国'
        elif args[0]['order_id'][:3]=='171' or args[0]['order_id'][:1]=='4':
            args[0]['country']='法国'



TYPE_fUNCTIONS = {
    'int' : intFunction,
    'float':floatFunction,
    'date': dateTimeFunction,
    'varchar':varcharFunction
}

FILTER_FUNCTIONS={
    'addDateFunction':addDateFunction,
    'judgeCountryFunction':judgeCountryFunction
}

def salesTargetFlatMapFunction(record,conditions):
    source_data=[x for x in record]
    result_list=[]
    #北美
    country_id={'ANA':0.7,'ANC':0.3}
    #欧洲
    #country_id={'AUU':0.478,'AUD':0.45,'AUF':0.06,'AUI':0.005,'AUS':0.007}
    #日本
    #country_id={'AAJ':1}
    #国内
    #country_id={'T00':1}
    sales_num_list=list(filter(lambda x:x.get('类别')=='目标销量' and x.get('category_III') is not None and x.get('category_III').lower()!='new',source_data))
    sales_money_list=list(filter(lambda x:x.get('类别')=='目标销售额',source_data))
    for data in sales_num_list:
        sales_money=list(filter(lambda x:x.get('category_III')==data.get('category_III'),sales_money_list))
        sales_money=sales_money[0]
        for i in range(1,13):
            for k,v in country_id.items():
                result_data={'ym':'2021-'+str(i)+'-01','channel_no':k,'category_III':data.get('category_III')}
                result_data['target_amount']=sales_money.get(str(i)+'月')*v if sales_money.get(str(i)+'月') else 0
                result_data['target_qty']=data.get(str(i)+'月')*v if data.get(str(i)+'月') else 0
                #md5生成主键
                # result_data['sn']=result_data.get('ym')+':'+result_data.get('channel_no')+':'+result_data.get('category_III')
                # result_data['sn']=md5(result_data['sn'].encode('utf8')).hexdigest()
                result_data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                result_list.append(result_data)
    return result_list

def skuDocumentFlatMapFunction(record):
    result_list=[]
    for data in record:
        data['active_start_time']=data['active_start_time'].replace('/','-')
        result_list.append(data)
    return result_list

def salesFactOldFlatMapFunciton(record,conditions):
    result_list={}
    channel_no=None
    database=DatabaseConnection(dst)
    for data in record:
        result=list(filter(lambda x:x.get('asin')==data.get('item_no') and x.get('platform')==conditions.get('platform') and x.get('country')==conditions.get('country'),temp_erp_items))
        #result=database.db.get_table('temp_erp_item').find_one(condition={'asin':data.get('item_no'),'platform':conditions.get('platform'),'country':conditions.get('country')})
        if result:
            data['item_no']=result[0].get('item_no')
        else:
            #logger.error(f'erp编码找不到对应item_no,erp编码为:{need}')
            database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('item_no'),'SKU':data.get('SKU'),'platform':conditions.get('platform'),'country':conditions.get('country'),'error_date':time.strftime("%Y-%m-%d", time.localtime()) })

        ym=dateFunction(conditions['ym'])
        data.pop('SKU')
        #result=database.db.get_table('exchange_rate').find_one(condition={'country':conditions['country'],'ym':ym})
        result=list(filter(lambda x:x.get('country')==conditions['country'] and x.get('ym').strftime('%Y-%m-%d')== ym,exchange_rate))
        data['sales_amount']=data['sales_amount'].replace(',','')
        data['sales_amount']=float(re.search('(\d*\.?\d{0,2})$',data['sales_amount'].strip()).group(0))
        data['sales_amount']=data['sales_amount']*result[0].get('exchange_rate',1)
        #获取渠道
        if channel_no is None:
            channel=list(filter(lambda x:x.get('platform')==conditions['platform'] and x.get('country')==conditions['country'],channels))
            channel_no=channel[0].get('channel_no',None)
        data['ym']=ym
        data['channel_no']=channel_no
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        #判断result_list是否含有相同的货号
        if result_list.get(data.get('item_no')+":"+data.get('channel_no')):
            sale_fact=result_list.get(data.get('item_no')+":"+data.get('channel_no'))
            sale_fact['sales_amount']=sale_fact.get('sales_amount')+data['sales_amount']
            sale_fact['sales_qty']=sale_fact.get('sales_qty')+data['sales_qty']
            continue
        try:
            # data['sn']=data['ym']+':'+data['channel_no']+':'+data.get('item_no')
            # data['sn']=md5(data['sn'].encode('utf8')).hexdigest()
            result_list[data.get('item_no')+":"+data.get('channel_no')]=data
        except Exception as e:
            logger.error(f'生成主键失败,错误数据:{data},错误原因:{e.args}')
    return list(result_list.values())


def salesFactFlatMapFunciton(record,conditions):
    result_list={}
    channel_no=None
    database=DatabaseConnection(dst)
    temp_erp_items=database.db.get_table('temp_erp_item').find().get_all()
    for data in record:
        ym=dateFunction(conditions['ym'])
        if data.get('type')!=settings.COUNTRY_TYPES.get(conditions.get('country')) or data.get('sku')==None:
            continue
        data['order_time_msg']=test_date.run(data['order_time_msg'],country=conditions['country'])
        #result=database.db.get_table('temp_erp_item').find_one(condition={'sku':data.get('sku'),'platform':conditions.get('platform'),'country':conditions.get('country')})
        temp_platform='Amazon' if conditions.get('platform')=='UNITFREE' else conditions.get('platform')
        # result_i=list(filter(lambda x:x.get('sku').lower()==data.get('sku').lower() and x.get('platform')==temp_platform and x.get('country')==conditions.get('country')
        #                     and data['order_time_msg'].split(' ')[0]>=getDateDefualt(x,'active_start_time','1970-01-01') and
        #                     data['order_time_msg'].split(' ')[0]<=getDateDefualt(x,'active_end_time','2099-12-31'),temp_erp_items))
        result_i=list(filter(lambda x:x.get('sku').lower()==data.get('sku').lower() and x.get('platform')==temp_platform and x.get('country')==conditions.get('country')
                            ,temp_erp_items))
        if result_i:
            data['item_no']=result_i[0].get('item_no')
        else:
            #logger.error(f'erp编码找不到对应item_no,erp编码为:{need}')
            database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('sku'),'platform':temp_platform,'ym':ym,'country':conditions.get('country'),'SKU':data.get('sku'),'error_date':time.strftime("%Y-%m-%d", time.localtime()) })
            continue
        data.pop('sku')
        data.pop('type')
        data.pop('order_time_msg')
        #result=database.db.get_table('exchange_rate').find_one(condition={'country':conditions['country'],'ym':ym})
        result=list(filter(lambda x:x.get('country')==conditions['country'] and x.get('ym').strftime('%Y-%m-%d')== ym,exchange_rate))
        if isinstance(data['sales_amount'],(list,tuple)):
            if conditions['country'] in ['德国','法国','西班牙','意大利']:
                #data['sales_amount']=data['sales_amount'].replace(',','.')
                if len(data['sales_amount'])>1:
                    data['sales_amount']=reduce(lambda x, y: float(x.replace(',','.') if isinstance(x,str) else x)+float(y.replace(',','.')), data['sales_amount'])
                elif len(data['sales_amount'])==1:
                    data['sales_amount']=float(data['sales_amount'][0].replace(',','.'))
            elif conditions['country'] in ['日本','加拿大']:
                if len(data['sales_amount'])>1:
                    data['sales_amount']=reduce(lambda x, y: float(x.replace(',','') if isinstance(x,str) else x)+float(y.replace(',','') if isinstance(y,str) else y), data['sales_amount'])
                elif len(data['sales_amount'])==1:
                    data['sales_amount']=float(data['sales_amount'][0].replace(',',''))
            elif conditions['country'] in ['英国','美国']:
                data['sales_amount']=reduce(lambda x, y: float(x)+float(y), data['sales_amount'])
        elif isinstance(data['sales_amount'],str):
            data['sales_amount']=float(re.search('(\d*\.?\d{0,2})$',data['sales_amount']).group(0))
        # if data['shipping credits']:
        #     data['sales_amount']=data['sales_amount']+data['shipping credits']
        data['sales_amount']=data['sales_amount']*result[0].get('exchange_rate',1)
        #获取渠道
        if channel_no is None:
            channel=list(filter(lambda x:x.get('platform')==conditions['platform'] and x.get('country')==conditions['country'],channels))
            channel_no=channel[0].get('channel_no',None)
        data['ym']=ym
        data['channel_no']=channel_no
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

        #数据校对所用
        # test_str=f"{str(data['ym'])}\t{str(result_i[0].get('sku'))}\t{str(data['item_no'])}\t{str(data['sales_amount'])}\t{str(data['sales_qty'])}"
        # fp.write(test_str)
        # fp.write('\r\n')

        #判断result_list是否含有相同的货号
        try:
            if result_list.get(data.get('item_no')+":"+data.get('channel_no')):
                sale_fact=result_list.get(data.get('item_no')+":"+data.get('channel_no'))
                sale_fact['sales_amount']=sale_fact.get('sales_amount')+data['sales_amount']
                sale_fact['sales_qty']=sale_fact.get('sales_qty')+data['sales_qty']
                continue
            # data['sn']=data['ym']+':'+data['channel_no']+':'+data.get('item_no')
            # data['sn']=md5(data['sn'].encode('utf8')).hexdigest()
            result_list[data.get('item_no')+":"+data.get('channel_no')]=data
        except Exception as e:
            logger.error(f'生成主键失败,错误数据:{data},错误原因:{e.args}')
    return list(result_list.values())

def salesFactReturnFlatMapFunciton(record,conditions):
    result_list={}
    channel_no=None
    database=DatabaseConnection(dst)
    temp_erp_items=database.db.get_table('temp_erp_item').find().get_all()
    for data in record:
        ym=dateFunction(conditions['ym'])
        if data.get('type')==None:
            continue
        if data.get('type').lower()!=settings.COUNTRY_SALES_RETURN_TYPES.get(conditions.get('country')).lower() or data.get('sku')==None:
            continue
        data['order_time_msg']=test_date.run(data['order_time_msg'],country=conditions['country'])
        #result=database.db.get_table('temp_erp_item').find_one(condition={'sku':data.get('sku'),'platform':conditions.get('platform'),'country':conditions.get('country')})
        temp_platform='Amazon' if conditions.get('platform')=='UNITFREE' else conditions.get('platform')
        # result=list(filter(lambda x:x.get('sku').lower()==data.get('sku').lower() and x.get('platform')==temp_platform and x.get('country')==conditions.get('country')
        #                     and data['order_time_msg']>=getDateDefualt(x,'active_start_time','1970-01-01') and
        #                     data['order_time_msg']<=getDateDefualt(x,'active_end_time','2099-12-31'),temp_erp_items))
        result=list(filter(lambda x:x.get('sku').lower()==data.get('sku').lower() and x.get('platform')==temp_platform and x.get('country')==conditions.get('country')
                            ,temp_erp_items))
        if result:
            data['item_no']=result[0].get('item_no')
        else:
            #logger.error(f'erp编码找不到对应item_no,erp编码为:{need}')
            database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('sku'),'platform':temp_platform,'ym':ym,'country':conditions.get('country'),'SKU':data.get('sku'),'error_date':time.strftime("%Y-%m-%d", time.localtime()) })
            continue
        data.pop('sku')
        data.pop('type')
        data.pop('order_time_msg')
        #result=database.db.get_table('exchange_rate').find_one(condition={'country':conditions['country'],'ym':ym})
        result=list(filter(lambda x:x.get('country')==conditions['country'] and x.get('ym').strftime('%Y-%m-%d')== ym,exchange_rate))
        if isinstance(data['sales_amount'],(list,tuple)):
            if conditions['country'] in ['德国','法国','西班牙','意大利']:
                #data['sales_amount']=data['sales_amount'].replace(',','.')
                if len(data['sales_amount'])>1:
                    data['sales_amount']=reduce(lambda x, y: float(x.replace(',','.') if isinstance(x,str) else x)+float(y.replace(',','.')), data['sales_amount'])
                elif len(data['sales_amount'])==1:
                    data['sales_amount']=float(data['sales_amount'][0].replace(',','.'))
            elif conditions['country'] in ['日本','加拿大']:
                if len(data['sales_amount'])>1:
                    data['sales_amount']=reduce(lambda x, y: float(x.replace(',','') if isinstance(x,str) else x)+float(y.replace(',','') if isinstance(y,str) else y), data['sales_amount'])
                elif len(data['sales_amount'])==1:
                    data['sales_amount']=float(data['sales_amount'][0].replace(',',''))
            elif conditions['country'] in ['英国','美国']:
                data['sales_amount']=reduce(lambda x, y: float(x)+float(y), data['sales_amount'])
        if isinstance(data['sales_amount'],str):
            data['sales_amount']=float(re.search('(\d*\.?\d{0,2})$',data['sales_amount']).group(0))
        #判断退货量与退货金额
        if data['sales_amount']>0:
            data['sales_amount']=data['sales_amount']*-1
        if data['sales_qty']>0:
            data['sales_qty']=data['sales_qty']*-1
        data['sales_amount']=data['sales_amount']*result[0].get('exchange_rate',1)
        #获取渠道
        if channel_no is None:
            channel=list(filter(lambda x:x.get('platform')==conditions['platform'] and x.get('country')==conditions['country'],channels))
            channel_no=channel[0].get('channel_no',None)
        data['ym']=ym
        data['channel_no']=channel_no
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        #判断result_list是否含有相同的货号
        try:
            if result_list.get(data.get('item_no')+":"+data.get('channel_no')):
                sale_fact=result_list.get(data.get('item_no')+":"+data.get('channel_no'))
                sale_fact['sales_amount']=sale_fact.get('sales_amount')+data['sales_amount']
                sale_fact['sales_qty']=sale_fact.get('sales_qty')+data['sales_qty']
                continue
            # data['sn']=data['ym']+':'+data['channel_no']+':'+data.get('item_no')
            # data['sn']=md5(data['sn'].encode('utf8')).hexdigest()
            result_list[data.get('item_no')+":"+data.get('channel_no')]=data
        except Exception as e:
            logger.error(f'生成主键失败,错误数据:{data},错误原因:{e.args}')
    return list(result_list.values())

def internalSalesFactReturnFlatMapFunciton(record,conditions):
    result_list={}
    database=DatabaseConnection(dst)
    for data in record:
        ym=dateFunction(conditions['ym'])
        if data.get('sales_qty')==0:
            continue
        result=list(filter(lambda x:x.get('erp_no').lower()==data.get('item_no').replace('\t','').lower() and x.get('country')=='中国'
                            and ym>=getDateDefualt(x,'active_start_time','1970-01-01') and
                            ym<=getDateDefualt(x,'active_end_time','2099-12-31'),temp_erp_items))
        if result:
            data['item_no']=result[0].get('item_no')
        else:
            #logger.error(f'erp编码找不到对应item_no,erp编码为:{need}')
            database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('item_no'),'SKU':data.get('item_no'),'platform':settings.STORE_LIST.get(data['platform']),'country':'中国','error_date':time.strftime("%Y-%m-%d", time.localtime()) })
        if isinstance(data['sales_amount'],str):
            data['sales_amount']=float(data['sales_amount'].replace(',',''))
        else:
            data['sales_amount']=float(data.get('sales_amount',0))
        data['sales_amount']=data['sales_amount']/1.13
        #数量，金额正转负
        if data['sales_amount']>0:
            data['sales_amount']=data['sales_amount']*-1
        if data['sales_qty']>0:
            data['sales_qty']=data['sales_qty']*-1
        #获取渠道
        channel=list(filter(lambda x:x.get('platform')==settings.STORE_LIST.get(data['platform']) and x.get('country')=='中国',channels))
        data.pop('platform')
        data['ym']=ym
        data['channel_no']=channel[0].get('channel_no',None)
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        # if data['channel_no']=='500':
        #     print('=')
        #判断result_list是否含有相同的货号
        if result_list.get(data.get('item_no')+":"+data.get('channel_no')):
            sale_fact=result_list.get(data.get('item_no')+":"+data.get('channel_no'))
            sale_fact['sales_amount']=sale_fact.get('sales_amount')+data['sales_amount']
            sale_fact['sales_qty']=sale_fact.get('sales_qty')+data['sales_qty']
            continue
        try:
            # data['sn']=data['ym']+':'+data['channel_no']+':'+data.get('item_no')
            # data['sn']=md5(data['sn'].encode('utf8')).hexdigest()
            result_list[data.get('item_no')+":"+data.get('channel_no')]=data
        except Exception as e:
            logger.error(f'生成主键失败,错误数据:{data},错误原因:{e.args}')
    return list(result_list.values())

def internalSalesFactFlatMapFunciton(record,conditions):
    result_list={}
    database=DatabaseConnection(dst)
    for data in record:
        ym=dateFunction(conditions['ym'])
        data['item_no']=data.get('item_no').replace('\t','')
        data['send_time']=datetime.datetime.strptime(data['send_time'].split(' ')[0],'%Y/%m/%d')
        result=list(filter(lambda x:x.get('erp_no').lower()==data.get('item_no').replace('\t','').lower() and x.get('country')=='中国'
                            and data['send_time']>=getDateDefualt2(x,'active_start_time',datetime.datetime.strptime('1970-01-01','%Y-%m-%d')) and
                            data['send_time']<=getDateDefualt2(x,'active_end_time',datetime.datetime.strptime('2099-12-31','%Y-%m-%d')),temp_erp_items))
        data.pop('send_time')
        if result:
            data['item_no']=result[0].get('item_no').replace('\t','')
        else:
            #logger.error(f'erp编码找不到对应item_no,erp编码为:{need}')
            database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('item_no'),'SKU':data.get('item_no'),'platform':settings.STORE_LIST.get(data['platform']),'country':'中国','error_date':time.strftime("%Y-%m-%d", time.localtime()) })
        if isinstance(data['sales_amount'],str):
            data['sales_amount']=float(data['sales_amount'].replace(',',''))
        else:
            data['sales_amount']=float(data['sales_amount'])
        data['sales_amount']=data['sales_amount']/1.13
        #获取渠道
        channel=list(filter(lambda x:x.get('platform')==settings.STORE_LIST.get(data['platform']) and x.get('country')=='中国',channels))
        data.pop('platform')
        data['ym']=ym
        data['channel_no']=channel[0].get('channel_no',None)
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        # if data['channel_no']=='500':
        #     print('=')
        #判断result_list是否含有相同的货号
        if result_list.get(data.get('item_no')+":"+data.get('channel_no')):
            sale_fact=result_list.get(data.get('item_no')+":"+data.get('channel_no'))
            sale_fact['sales_amount']=sale_fact.get('sales_amount')+data['sales_amount']
            sale_fact['sales_qty']=sale_fact.get('sales_qty')+data['sales_qty']
            continue
        try:
            # data['sn']=data['ym']+':'+data['channel_no']+':'+data.get('item_no')
            # data['sn']=md5(data['sn'].encode('utf8')).hexdigest()
            result_list[data.get('item_no')+":"+data.get('channel_no')]=data
        except Exception as e:
            logger.error(f'生成主键失败,错误数据:{data},错误原因:{e.args}')
    return list(result_list.values())

def profitAndLossFlatMapFunciton(record,conditions):
    result_list={}
    for data in record:
        cost_date=data.get('cost_date')
        data['cost_date']=datetime.date(data['year'], cost_date, 1)
        data.pop('year')
        if data['countrys']!="中国":
            channel=list(filter(lambda x:x.get('platform')==data['platform'] and x.get('country')==data['countrys'],channels))
            data['channels']=channel[0].get('channel_no',None)
        else:
            data['channels']='500,600,J00,O00,T00'
        data.pop('platform')
        countrys=data.get('countrys')
        indexLocate=settings.COUNTRYS.index(countrys)
        countrys=['0','0','0','0','0','0','0','0','0','0']
        countrys[indexLocate]='1'
        data["countrys"]="".join(countrys)
        data["cost_type_c"]="其他"
        result_list[str(data.get('cost_date'))+'_'+data.get('cost_subject')+"_"+data.get('countrys')]=data
    return list(result_list.values())

def purchaseCostFlatMapFunciton(record,conditions):
    result_list=[]
    for data in record:
        #===========================
        status=data.get('status')
        if status=='销售中':
            data.pop('status')
            result_list.append(data)
    return result_list

def skuDocumentFlatMapFunciton(record,conditions):
    result_list=[]
    database=DatabaseConnection(dst)
    for data in record:
        #===========================
        status=data.get('status')
        if status=='销售中':
            data.pop('status')
            channel=list(filter(lambda x:x.get('platform')=='Amazon' and x.get('country')==data.get('country'),channels))
            if len(channel)>0:
                data['channel_no']=channel[0].get('channel_no',None)
                data['cin']=data.get('item_no',None)+"_"+channel[0].get('channel_no',None)
            data.pop('country')
            result_list.append(data)
    return result_list

#2020年销售数据处理函数
def salesFact2020FlatMapFunciton(record,conditions):
    result_list={}
    database=DatabaseConnection(dst)
    for data in record:
        if data.get('country')=='中国':
            continue
        result=database.db.get_table('temp_erp_item').find_one(condition={'asin':data.get('asin'),'country':data.get('country'),'platform':'Amazon'})
        if result:
            data['item_no']=result.get('item_no')
        else:
            logger.error(f'erp编码找不到对应item_no,erp编码为:{data.get("asin")}')
            database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('asin'),'platform':result.get('platform'),'country':data.get('country'),'error_date':time.strftime("%Y-%m-%d", time.localtime()) })
        data['ym']=dateFunction(data['ym'])
        channel=list(filter(lambda x:x.get('platform')=='Amazon' and x.get('country')==data.get('country'),channels))
        data['channel_no']=channel[0].get('channel_no',None)
        data.pop('asin')
        data.pop('country')
        try:
            # data['sn']=data['ym']+':'+data['channel_no']+':'+data.get('item_no')
            # data['sn']=md5(data['sn'].encode('utf8')).hexdigest()
            result_list[data['ym']+':'+data.get('item_no')+':'+data.get('channel_no')]=data
        except Exception as e:
            continue
            logger.error(f'生成主键失败,错误数据:{data},错误原因:{e.args}')
    return list(result_list.values())

def salesTargetJdFlatMapFunciton(record,conditions):
    result_list=[]
    update_result=[]
    for data in record:
        # result=database.db.get_table('sales_target').find_one(condition={'ym':data.get('ym'),'channel_no':'T00','category_III':data.get('category_III')})
        # if result:
        #     try:
        #         if result.get('target_qty',0)!=0:
        #             result['target_amount']=result.get('target_amount')-float(data.get('target_amount',0))
        #             result['target_qty']=result.get('target_qty')-float(data.get('target_qty',0))
        #             update_result.append(result)
        #     except Exception as e:
        #         logger.error(f'错误result数据:{result},错误data数据:{data},错误原因:{e.args}')
        # else:
        #     database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('category_III'),'platform':'天猫','country':data.get('country'),'error_date':time.strftime("%Y-%m-%d", time.localtime())})
        channel=list(filter(lambda x:x.get('platform')==data.get('platform') and x.get('country')==data.get('country'),channels))
        data['channel_no']=channel[0].get('channel_no',None)
        data.pop('country')
        data.pop('platform')
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        try:
            # data['sn']=data['ym']+':'+data['channel_no']+':'+data.get('category_III')
            # data['sn']=md5(data['sn'].encode('utf8')).hexdigest()
            result_list.append(data)
        except Exception as e:
            logger.error(f'生成主键失败,错误数据:{data},错误原因:{e.args}')
            continue
    # for i in update_result:
    #     database.db.get_table('sales_target').upsert(result)
    return result_list

def productCostFunction(record,conditions):
    result_list=[]
    database=DatabaseConnection(dst)
    for data in record:
        try:
            #获取item_no
            result=database.db.get_table('temp_erp_item').find_one(condition={'erp_no':data.get('erp_no'),'country':data.get('country'),'platform':data.get('platform')})
            if result:
                data['item_no']=result.get('item_no')
            else:
                logger.error(f'erp编码找不到对应item_no,erp编码为:{data.get("erp_no")}')
                database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('erp_no'),'platform':data.get('platform'),'country':data.get('country'),'error_date':time.strftime("%Y-%m-%d", time.localtime()) })
            #获得渠道
            channel=list(filter(lambda x:x.get('platform')==data.get('platform') and x.get('country')==data.get('country'),channels))
            data['channel_no']=channel[0].get('channel_no',None)
            data.pop('erp_no')
            data.pop('country')
            data.pop('platform')
            data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            data['cin']=data['item_no']+'_'+data['channel_no']
            result_list.append(data)
        except Exception as e:
            logger.error(f'错误数据:{data},错误原因:{e.args}')
            continue
    return result_list

def apportionFunction(record,conditions):
    result_list=[]
    database=DatabaseConnection(dst)
    result_channels=[c.get('channel_no') for c in channels if c.get('active')==1]
    result_sales=database.db.read(f'select channel_no,sum(sales_amount) money from sales_fact where ym>="2021-01-01" and ym<="2021-12-01" GROUP BY channel_no').get_all()
    #获取所有国家渠道的比例
    # for i,country in enumerate(settings.COUNTRYS):
    #     #该国家的比例
    #     channel_ratios=np.zeros((len(result_channels)))
    #     #获取国家对应的渠道比例
    #     country_channel=list(filter(lambda x:x.get('active')==1 and x.get('country')==country,channels))
    #     country_channel=list(map(lambda x:x.get('channel_no'),country_channel))
    #     country_sales = [x for x in result_sales if x.get('channel_no') in country_channel]
    #     country_total_money=sum(sales.get('money') for sales in country_sales)
    #     #计算对应渠道比例，插入该国家比例列中
    #     for country_sale in country_sales:
    #         country_ratio=country_sale.get('money')/country_total_money
    #         channel_index=result_channels.index(country_sale.get('channel_no'))
    #         channel_ratios[channel_index]=country_ratio
    #     channel_ratio_list[i]=channel_ratios
    #各类别费用分摊到各个国家与渠道
    cnt=np.zeros((len(settings.APPORTION_TYPE),len(settings.COUNTRYS),len(result_channels)))   #(item, country, channel)
    for data in record:
        try:
            channel_ratio_list=np.zeros((len(settings.COUNTRYS),len(result_channels)))
            #获取分摊渠道
            data_channels=data.get('channels').split(',')
            for i,country in enumerate(settings.COUNTRYS):
                #该国家的比例
                channel_ratios=np.zeros((len(result_channels)))
                country_channels=[c.get('channel_no') for c in channels if c.get('active')==1 and country==c.get('country')]
                need_channels=[c for c in data_channels if c in country_channels]
                for country_chan in need_channels:
                    country_ratio=1/len(need_channels)
                    channel_index=result_channels.index(country_chan)
                    channel_ratios[channel_index]=country_ratio
                channel_ratio_list[i]=channel_ratios
            apportion_num=data.get('apportion_num') #(country)
            flg=np.array([list(map(int,str(data.get('countrys'))))]).T   #(item,country,channel)
            #获取cnt中的索引位置
            index=settings.APPORTION_TYPE.index((data.get('fee_type_I'),data.get('fee_type_II')))
            cty = flg.copy()
            cty[flg==1] = apportion_num / flg.sum()
            ads = channel_ratio_list * cty
            cnt[index, :, :] += ads
        except Exception as e:
            logger.error(f'错误数据:{data},错误原因:{e.args}')
            continue
    #获取不同渠道的总收入
    result_sales_dict={x.get('channel_no'):x for x in result_sales}
    need_apportion_itemNos=database.db.read(f'select item_no,channel_no,sum(sales_amount) total_sales from sales_fact where ym>="2021-01-01" and ym<="2021-12-01" GROUP BY item_no,channel_no').get_all()
    channel_country={x.get('channel_no'):x.get('country') for x in channels}

    for need_apportion_itemNo in need_apportion_itemNos:
        channel_no=need_apportion_itemNo.get('channel_no')
        item_no=need_apportion_itemNo.get('item_no')
        country=channel_country.get(channel_no)
        country_index=settings.COUNTRYS.index(country)
        channel_index=result_channels.index(need_apportion_itemNo.get('channel_no'))
        channel_total_sales=result_sales_dict.get(channel_no).get('money')
        result_data={'cin':item_no+'_'+channel_no,'item_no':item_no,'channel_no':channel_no}
        for i,apport in enumerate(settings.APPORTION_TYPE):
            #定位np对应的位置,获取金额
            channel_apporitemtion_money=cnt[i][country_index][channel_index]
            #获取对应渠道，货号分摊到的金额
            item_apporitemtion_money=channel_apporitemtion_money*(need_apportion_itemNo.get('total_sales')/channel_total_sales)
            flied_name=settings.APPORTION_TYPE_NAME.get((apport[0],apport[1]))
            result_data[flied_name]=float(item_apporitemtion_money)
        result_data['appor_version']="20211201-20210101"
        result_data['update_time']=time.strftime("%Y-%m-%d", time.localtime())
        result_list.append(result_data)
    return result_list

def financeFeeFlatMapFunciton(record,conditions):
    result_list=[]
    database=DatabaseConnection(dst)
    exchange_rate=database.db.get_table('exchange_rate').find().get_all()
    ym=dateFunction(conditions['ym'])
    result=list(filter(lambda x:x.get('country')==conditions['country'] and x.get('ym').strftime('%Y-%m-%d')== ym,exchange_rate))

    result_dict={'FBA':0,'广告费':0,'仓储费':0,'其他':0,'退还邮费':0,
                '库存报销(丢件)':0,'库存报销(损坏)':0,'库存报销(服务)':0,'库存报销(调整)':0,
                '库存报销(退货)':0,'退货其他':0,'服务费':0}
    result_list=[]
    
    for data in record:
        #添加fba
        fba=getDefualt(data,'fba',0.00,conditions['country'])*result[0].get('exchange_rate',1)
        result_dict['FBA']=result_dict['FBA']+fba
        #添加佣金
        # sale_fee=getDefualt(data,'sale_fee',0.00,conditions['country'])*result[0].get('exchange_rate',1)
        # result_dict['佣金']=result_dict['佣金']+sale_fee
        #添加广告费
        # commercial=getDefualt(data,'commercial',0.00,conditions['country'])*result[0].get('exchange_rate',1)
        # result_dict['广告费']=result_dict['广告费']+commercial
        #添加仓储费
        stock_name=list(filter(lambda x:x.get('cost_subject')=='仓储费' and x.get('country')==conditions['country'],temp_des_mapping))
        if len(stock_name)>0 and data['type']==stock_name[0].get('name'):
            stock_fee=getDefualt(data,'other',0.00,conditions['country'])*result[0].get('exchange_rate',1)
            result_dict['仓储费']=result_dict['仓储费']+stock_fee
        #添加退货其他
        if data.get('type')!=None and data.get('type').lower()==settings.COUNTRY_SALES_RETURN_TYPES.get(conditions.get('country')).lower():
            return_other=getDefualt(data,'other',0.00,conditions['country'])*result[0].get('exchange_rate',1)
            result_dict['退货其他']=result_dict['退货其他']+return_other
        #添加服务费
        serve_name=list(filter(lambda x:x.get('cost_subject')=='服务费' and x.get('country')==conditions['country'],temp_des_mapping))
        if len(serve_name)>0 and data['type']==serve_name[0].get('name'):
            serve_fee=getDefualt(data,'other',0.00,conditions['country'])*result[0].get('exchange_rate',1)
            result_dict['服务费']=result_dict['服务费']+serve_fee
        #添加其他费用
        for other_fee in data.get('other_fee'):
            if isinstance(other_fee,str):
                if conditions['country']=='日本':
                    other_fee=other_fee.replace(',','')
                else:
                    other_fee=other_fee.replace(',','.')
                    if other_fee.count('.')>1:
                        other_fee=other_fee.replace('.','',1)
            other_fee=float(other_fee)*result[0].get('exchange_rate',1)
            other_fee=0.0 if math.isnan(other_fee) else other_fee
            result_dict['其他']=result_dict['其他']+other_fee
        #添加退还邮费
        return_post_name=list(filter(lambda x:x.get('cost_subject')=='退还邮费' and x.get('country')==conditions['country'],temp_des_mapping))
        if len(return_post_name)>0 and data['type']==return_post_name[0].get('name'):
            return_post_fee=getDefualt(data,'other',0.00,conditions['country'])*result[0].get('exchange_rate',1)
            result_dict['退还邮费']=result_dict['退还邮费']+return_post_fee
        #添加type为adjustment费用
        adjustment_names=list(filter(lambda x:x.get('type')==3 and x.get('country')==conditions['country'],temp_des_mapping))
        if len(adjustment_names)>0 and data['type']==adjustment_names[0].get('name'):
            for adjustment_name in adjustment_names:
                if adjustment_name['description']!=None and adjustment_name['description'] in data['description'].strip():
                    adjustment_fee=getDefualt(data,'other',0.00,conditions['country'])*result[0].get('exchange_rate',1)
                    result_dict[adjustment_name['cost_subject']]=result_dict[adjustment_name['cost_subject']]+adjustment_fee
                    continue
    for k in result_dict:
        if result_dict[k]==0:   continue
        cost_mapping=list(filter(lambda x:x.get('cost_subject')==k,cost_mapping_table))[0]
        country_index=settings.COUNTRYS.index(conditions['country'])
        #国家
        country_result=list('0000000000')
        country_result[country_index]='1'
        country_result = "".join(country_result)
        #渠道
        channel=list(filter(lambda x:x.get('platform')=='Amazon' and x.get('country')==conditions['country'],channels))[0]
        result_list.append({'cost_date':dateFunction(conditions['ym']),'cost_subject':conditions['brand'].lower()+'_'+k,'monetary':result_dict[k]*-1,
                            'cost_type_p':cost_mapping['cost_type_p'],'cost_type_c':cost_mapping['cost_type_c'],'countrys':country_result,'channels':channel['channel_no']})
    return result_list

def metadataFunction(record,conditions):
    result_list=[]
    for data in record:
        ym=dateFunction(conditions['ym'])
        result=list(filter(lambda x:x.get('country')==conditions['country'] and x.get('ym').strftime('%Y-%m-%d')== ym,exchange_rate))
        data_format={"product_sales":"product_sales",
        "product_sales_tax":"product_sales_tax",
        "shipping_credits":"shipping_credits",
        "shipping_credits_tax":"shipping_credits_tax",
        "gift_wrap_credits":"gift_wrap_credits",
        "giftwrap_credits_tax":"giftwrap_credits_tax",
        "promotional_rebates":"promotional_rebates",
        "promotional_rebates_tax":"promotional_rebates_tax",
        "marketplace_withheld_tax":"marketplace_withheld_tax",
        "selling_fees":"selling_fees",
        "fba_fees":"fba_fees",
        "other_transaction_fees":"other_transaction_fees",
        "other":"other","total":"total"}
        for k,v in data.items():
            if k in data_format.keys():
                data[k]=getDefualt(data,k,0.00,conditions['country'])*result[0].get('exchange_rate',1)
        # try:
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        data['order_time']=dateFunction(conditions['ym'])
        data['order_time_msg']=test_date.run(data['order_time_msg'],country=conditions['country'])
        data['country']=conditions['country']
        #1.先判断是否是int或float 2.再进入是否是nan的判断
        data['quantity']=0 if isinstance(data.get('quantity'),(int,float)) and math.isnan(data['quantity']) else data.get('quantity')
        data['order_postal']=None if isinstance(data.get('order_postal'),(int,float)) and math.isnan(data['order_postal']) else data.get('order_postal')
        data['fulfillment']=None if isinstance(data.get('fulfillment'),(int,float)) and math.isnan(data['fulfillment']) else data.get('fulfillment')
        data['order_id']=None if isinstance(data.get('order_id'),(int,float)) and math.isnan(data['order_id']) else data.get('order_id')
        data['sku']=None if isinstance(data.get('sku'),(int,float)) and math.isnan(data['sku']) else data.get('sku')
        data['account_type']=None if isinstance(data.get('account_type'),(int,float)) and math.isnan(data['account_type']) else data.get('account_type')
        data['order_city']=None if isinstance(data.get('order_city'),(int,float)) and math.isnan(data['order_city']) else data.get('order_city')
        data['order_state']=None if isinstance(data.get('order_state'),(int,float)) and math.isnan(data['order_state']) else data.get('order_state')
        data['tax_collection_model']=None if isinstance(data.get('tax_collection_model'),(int,float)) and math.isnan(data['tax_collection_model']) else data.get('tax_collection_model')
        result_list.append(data)
        # except Exception as e:
        #     logger.error(f'错误数据:{data},错误原因:{e.args}')
        #     continue
    return result_list


def salesStatisticsCategoryFunction(record,conditions):
    result_list=[]
    for data in record:
        if data.get('category_I')==None or data.get('category_II')==None or data.get('category_III')==None or data.get('category_III')=='0':
            continue
        data['brand']=data['brand']
        if data['brand'] is None:
            data.pop('brand')
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        result_list.append(data)
    return result_list

def salesStatisticsProductsFunction(record,conditions):
    result_list=[]
    for data in record:
        result_list.append(data)
    return result_list

def salesStatisticsMappingFunction(record,conditions):
    result_list=[]
    for data in record:
        if data['item_no']==None:
            continue
        #store_result=['Petsfit','US']
        data['erp_no']=data['erp_no'].replace('\t','')
        data['item_no']=data['item_no'].replace('\t','')
        data['asin']=data['asin'].replace('\t','')
        store_result=data['store'].split('_')
        data['store']=store_result[0].upper()
        country_chs=settings.COUNTRY_MAPPING[store_result[1]]
        data['platform']='Amazon'
        data['country']=country_chs
        data['is_new']=1 if data['is_new']=='新品' else 0
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        result_list.append(data)
    return result_list

def innerSalesStatisticsMappingFunction(record,conditions):
    result_list=[]
    for data in record:
        if data.get('category_III')==None or data.get('category_III')=='0':
            continue        
        if data['item_no']==None:
            continue
        #store_result=['Petsfit','US']
        data['erp_no']=data['erp_no'].replace('\t','')
        data['item_no']=data['item_no'].replace('\t','')
        data['sku']=data['sku'].replace('\t','')
        data['update_time']=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        data['active_end_time']='2099-12-31 00:00:00'
        result_list.append(data)
    return result_list

def productMassageFunction(record,conditions):
    result_list=[]
    for data in record:
        #if '/' in data.get('product_high',None):data['product_high']=0
        data.pop("category_III")
        data['update_time']=time.strftime("%Y-%m-%d", time.localtime())
        result_list.append(data)
    return result_list


def generalFunction(record,conditions):
    result_list=[]
    for data in record:
        generalDateFunction(data,conditions)
        result_list.append(data)
    return result_list

def saleAppraisalFunction(record,conditions):
    result_list=[]
    database=DatabaseConnection(dst)
    for data in record:
        if data.get('item_no',None)==None:
            continue
        channel=list(filter(lambda x:x.get('platform').lower()==data.get('platform').lower() and x.get('area')==data.get('area') and x.get('country')==data.get('country'),channels))[0]
        data['channel_no']=channel.get('channel_no')
        data.pop('platform')
        data.pop('area')
        data.pop('country')
        result={}
        try:
            result=database.db.get_table('products').find(condition={'item_no':data.get('item_no')}).get_all()[0]
        except Exception as e:
            logger.error(f'未找到相应货号{data.get("item_no")}')
        data['category_III']=result.get('category_iii',None)
        if data['target_qty']==None:
            data.pop('target_qty')
        if data['shipped_qty']==None:
            data.pop('shipped_qty')
        data['ym']=data['ym'].to_pydatetime()# "2024/01/01"
        result_list.append(data)
    return result_list


@generalFilterFunction
def generalDateFunction(data,conditions):
    mappings=conditions.get('mappings')
    for key, value in data.items():
        mapping_name=mappings.get(key,None)
        if mapping_name!=None:
            #print(key,value)
            data[key]=TYPE_fUNCTIONS[mapping_name.get('type')](value)
        else:
            data[key]=value
    data['country']=data['country'].replace("\r","") if data.get('country',None)!=None else conditions.get('country')
    data['brand']=conditions['names'][0].lower()


def getDefualt(x,key,defualt,country):
    if isinstance(x.get(key),str):
        temp=None
        if country=='日本':
            temp=x.get(key).replace(',','')
        else:
            temp=x.get(key).replace(',','.')
            if temp.count('.')>1:
                temp=temp.replace('.','',1)
        return float(re.search('-?(\d*\.?\d{0,2})$',temp).group(0))
    else:
        result=float(x.get(key,defualt)) if x.get(key,defualt) else defualt
        return defualt if math.isnan(result) else result

def getDateDefualt(x,key_name,default_value):
    if x.get(key_name)==None:
        return str(default_value)
    else:
        return str(x.get(key_name))

def getDateDefualt2(x,key_name,default_value):
    if x.get(key_name)==None:
        return default_value
    else:
        return x.get(key_name)
