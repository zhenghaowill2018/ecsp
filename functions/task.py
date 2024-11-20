#-*- coding : utf-8 -*-
# coding: utf-8
import datetime
import functools
import logging
import os
import pymysql
import shutil
from hashlib import md5
from time import sleep
import requests

import pandas as pd
import settings
from pyetl import (DatabaseWriter, ExcelReader, FileReader,DatabaseConnection,
                   Task)
from utils import LogUtils

from .flatMapFunctions import *
from .functions import (categoryFunction,database,
                        dateFunction, dst,amazon_b_report_code,amazon_b_mapping,amazon_b_report_collect)


LogUtils.log_config(f'ecsp','0.0.0.0','0000')
logger = logging.getLogger(f'etl_main')

def create_datebase():
    dst = pymysql.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD, db=settings.MYSQL_DB)
    database=DatabaseConnection(dst)
    return database

def file_processing_log(func):
    """
    定义处理时成功，异常捕获接口
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        dst = pymysql.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD, db=settings.MYSQL_DB)
        database=DatabaseConnection(dst)
        try:
            r = func(*args, **kwargs)
            database.db.get_table('file_processing_log').upsert({'file_name':args[0],'pro_result':'success','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
        except Exception as e:
            if len(e.args)>1 and e.args[1]=='No such file or directory':
                logger.error(f'处理文件:{args[0]} 错误,错误原因:{e.args}')
            else:
                database.db.get_table('file_processing_log').upsert({'file_name':args[0],'pro_result':'fail','messge':f'错误类型:{type(e)} 错误原因:{e.args}','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
                logger.error(f'处理文件:{args[0]} 错误,错误原因:{e.args}')
        finally:
            dst.close()
        return r
    return wrapper

#处理ATR相关文件处理日志
def file_processing_log2(func):
    """
    定义处理时成功，异常捕获接口
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        strs=args[0].split('.'+args[1])
        names=strs[0].split('_')
        dst = pymysql.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD, db=settings.MYSQL_DB)
        database=DatabaseConnection(dst)
        try:
            r = func(*args, **kwargs)
            #database.db.get_table('file_processing_log').upsert({'file_name':args[0],'brand':names[0],'file_type':names[1],'date_range':names[2],'country':names[3],'pro_result':'success','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
            database.db.get_table('file_handle_process_log').upsert({'id':names[4],'file_process_code':4,'etl_process_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
        except Exception as e:
            if len(e.args)>1 and e.args[1]=='No such file or directory':
                logger.error(f'处理文件:{args[0]} 错误,错误原因:{e.args}')
            else:
                #database.db.get_table('file_processing_log').upsert({'file_name':args[0],'brand':names[0],'file_type':names[1],'date_range':names[2],'country':names[3],'pro_result':'fail','messge':f'错误类型:{type(e)} 错误原因:{e.args}','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
                database.db.get_table('file_handle_process_log').upsert({'id':names[4],'file_process_code':3,'etl_msg':f'错误类型:{type(e)} 错误原因:{e.args}','etl_process_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
                logger.error(f'处理文件:{args[0]} 错误,错误原因:{e.args}')
                logger.error(f'错误信息e:{e}')
        finally:
            dst.close()
        return r
    return wrapper

#处理日期范围报告文件
def file_processing_log3(func):
    """
    定义处理时成功，异常捕获接口
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        strs=args[0].split('.csv')
        names=strs[0].split('_')
        dst = pymysql.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD, db=settings.MYSQL_DB)
        database=DatabaseConnection(dst)
        try:
            r = func(*args, **kwargs)
            #database.db.get_table('file_processing_log').upsert({'file_name':args[0],'brand':names[0],'file_type':names[1],'date_range':names[2],'country':names[3],'pro_result':'success','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
            database.db.get_table('file_handle_process_log').upsert({'id':names[4],'file_process_code':4,'etl_process_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
        except Exception as e:
            if len(e.args)>1 and e.args[1]=='No such file or directory':
                logger.error(f'处理文件:{args[0]} 错误,错误原因:{e.args}')
            else:
                #database.db.get_table('file_processing_log').upsert({'file_name':args[0],'brand':names[0],'file_type':names[1],'date_range':names[2],'country':names[3],'pro_result':'fail','messge':f'错误类型:{type(e)} 错误原因:{e.args}','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
                database.db.get_table('file_handle_process_log').upsert({'id':names[4],'file_process_code':3,'etl_msg':f'错误类型:{type(e)} 错误原因:{e.args}','etl_process_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
                logger.error(f'处理文件:{args[0]} 错误,错误原因:{e.args}')
                logger.error(f'错误信息e:{e}')
        finally:
            dst.close()
        return r
    return wrapper

def salesTargetTask(file):
    sheet_name='2021欧洲户外用每月目标'
    #任务一
    reader = ExcelReader(os.path.join('./source_data/',file),sheet_name=sheet_name)
    writer = DatabaseWriter(dst, table_name="categories_p",batch_size=1)
    columns = {"category_III": "小类","category_II": "中类","category_I":"大类"}
    Task(reader, writer,columns=columns).start(time_diff_message='excel 销售目标')
    #任务二
    reader_2 = ExcelReader(os.path.join('./source_data/',file),sheet_name=sheet_name)
    writer_2 = DatabaseWriter(dst, table_name="sales_target",batch_size=1)
    columns_2 = {"category_III": "小类",'类别':'类别','1月':'1月','2月':'2月','3月':'3月','4月':'4月','5月':'5月','6月':'6月','7月':'7月','8月':'8月','9月':'9月','10月':'10月','11月':'11月','12月':'12月'}
    functions_2={"category_III":categoryFunction}
    Task(reader_2, writer_2,columns=columns_2,functions=functions_2,flatMapFunciton=salesTargetFlatMapFunction).start(time_diff_message='excel 销售目标')

@file_processing_log
def salesFactOldTask(file):
    """
    旧的销售实际处理函数
    """
    strs=file.split('.csv')
    target_path=os.path.join('./target_data',file.replace('.c',datetime.datetime.now().strftime("_%Y%m%d %H%M%S.c")))
    shutil.move(os.path.join('./source_data/',file),target_path)
    conditions=strs[0].split(' ')
    # df=pd.read_csv(target_path, encoding=settings.COUNTRY_ENCODING.get(conditions[2],'utf-8'))
    # print(df.iloc[0,3])
    #处理销售实际(订单的数据)
    pd_params={'encoding':settings.COUNTRY_ENCODING_OLD.get(conditions[2],'utf-8')}
    reader = FileReader(target_path,pd_params)
    writer = DatabaseWriter(dst, table_name="sales_fact_sale",batch_size=1)
    Task(reader, writer,columns=settings.COUNTRY_COLUMNS_OLD.get(conditions[2]),flatMapFunciton=salesFactOldFlatMapFunciton,conditions={'ym':conditions[0],'platform':conditions[1],'country':conditions[2]}).start(time_diff_message='excel 销售实绩')
    shutil.move(target_path,'./target_data/finish/')

@file_processing_log3
def  salesFactTask(file):
    """
    新的销售实际处理函数
    """
    strs=file.split('.csv')
    conditions=strs[0].split('_')

    target_path=os.path.join('../ecsp_file/target_data',file)
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    df=pd.read_csv(target_path, encoding=settings.COUNTRY_ENCODING.get(conditions[2],'utf-8'),skiprows=7)
    platform='UNITFREE' if (conditions[0]=='UNITFREE') else 'Amazon'
    #处理销售实际(订单的数据)
    conditions[3]=settings.COUNTRY_MAPPING.get(conditions[3])
    pd_params={'encoding':settings.COUNTRY_ENCODING.get(conditions[3],'utf-8'),"skiprows":7}
    reader = FileReader(target_path,pd_params)
    writer = DatabaseWriter(dst, table_name="sales_fact",batch_size=1,names=conditions)
    Task(reader, writer,columns=settings.COUNTRY_COLUMNS.get(conditions[3]),flatMapFunciton=salesFactFlatMapFunciton,conditions={'ym':conditions[2],'platform':platform,'country':conditions[3]}).start(time_diff_message='excel 销售实绩')
    #处理销售实际(退货的数据)
    pd_params={'encoding':settings.COUNTRY_ENCODING.get(conditions[3],'utf-8'),"skiprows":7}
    reader = FileReader(target_path,pd_params)
    writer = DatabaseWriter(dst, table_name="sales_fact_return",batch_size=1)
    Task(reader, writer,columns=settings.COUNTRY_COLUMNS.get(conditions[3]),flatMapFunciton=salesFactReturnFlatMapFunciton,conditions={'ym':conditions[2],'platform':platform,'country':conditions[3]}).start(time_diff_message='excel 销售退货实绩')
    #销售费用
    # pd_params={'encoding':settings.COUNTRY_ENCODING.get(conditions[3],'utf-8'),"skiprows":7}
    # reader = FileReader(target_path,pd_params)
    # writer = DatabaseWriter(dst, table_name="cost_records",batch_size=1)
    # Task(reader, writer,columns=settings.COUNTRY_FEE_COLUMNS.get(conditions[3]),flatMapFunciton=financeFeeFlatMapFunciton,conditions={'ym':conditions[2],'platform':platform,'country':conditions[3],'brand':conditions[0].upper()}).start(time_diff_message='excel 销售退货实绩')
    #原始数据
    pd_params={'encoding':settings.COUNTRY_ENCODING.get(conditions[3],'utf-8'),"skiprows":7}
    reader = FileReader(target_path,pd_params)
    writer = DatabaseWriter(dst, table_name="amazon_business_data",batch_size=1,names=conditions)
    Task(reader, writer,columns=settings.COUNTRY_ALL_COLUMNS.get(conditions[3]),flatMapFunciton=metadataFunction,conditions={'ym':conditions[2],'platform':platform,'country':conditions[3],'brand':conditions[0].upper()}).start(time_diff_message='excel 销售退货实绩')
    if os.path.exists("../ecsp_file/target_data/finish/"+file):
        os.remove("../ecsp_file/target_data/finish/"+file)
    shutil.move(target_path,'../ecsp_file/target_data/finish/')


@file_processing_log3
def  salesStatisticsTask(file):
    """
    易仓基础数据处理函数
    """
    target_path=os.path.join('../ecsp_file/target_data',file)
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    pd_params={"skiprows":2}
    #任务一
    reader = FileReader(target_path,pd_params)
    writer = DatabaseWriter(dst, table_name="categories_t",batch_size=1)
    columns = {"category_I":"一级品类","category_II":"二级品类","category_III":"三级品类","brand":"品牌"}
    Task(reader, writer,columns=columns,flatMapFunciton=salesStatisticsCategoryFunction).start(time_diff_message='excel categories_t')
    #任务二
    reader = FileReader(target_path,pd_params)
    writer = DatabaseWriter(dst, table_name="products",batch_size=1)
    columns = {"item_no":"产品SKU","purchase_cost":"默认采购单价","category_I":"一级品类","category_II":"二级品类","category_III":"三级品类","brand":"品牌"}
    Task(reader, writer,columns=columns,flatMapFunciton=salesStatisticsProductsFunction).start(time_diff_message='excel products')

    if os.path.exists("../ecsp_file/target_data/finish/"+file):
        os.remove("../ecsp_file/target_data/finish/"+file)
    shutil.move(target_path,'../ecsp_file/target_data/finish/')

@file_processing_log3
def innerSalesListingTask(file):
    target_path=os.path.join('../ecsp_file/target_data',file)
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    #任务一
    reader = FileReader(target_path,pd_params={'encoding':'gbk'})
    writer = DatabaseWriter(dst, table_name="categories_t",batch_size=1)
    columns = {"category_I":"大类","category_II":"中类","category_III":"小类","brand":"品牌"}
    Task(reader, writer,columns=columns,flatMapFunciton=salesStatisticsCategoryFunction).start(time_diff_message='excel categories_t')
    #任务二
    reader = FileReader(target_path,pd_params={'encoding':'gbk'})
    writer = DatabaseWriter(dst, table_name="products",batch_size=1)
    columns = {"item_no":"商品简称","category_I":"大类","category_II":"中类","category_III":"小类","brand":"品牌"}
    Task(reader, writer,columns=columns,flatMapFunciton=salesStatisticsProductsFunction).start(time_diff_message='excel products')
    if os.path.exists("../ecsp_file/target_data/finish/"+file):
        os.remove("../ecsp_file/target_data/finish/"+file)
    shutil.move(target_path,"../ecsp_file/target_data/finish/")

@file_processing_log3
def salesListingTask(file):
    target_path=os.path.join('../ecsp_file/target_data',file)
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    reader = FileReader(target_path)
    writer = DatabaseWriter(dst, table_name="temp_erp_item",batch_size=1)
    #缺少开始售卖时间
    columns = {"erp_no":"仓库SKU","item_no":"仓库SKU","sku":"Seller SKU","asin":"ASIN","store":"账号","duty_p":"运营负责人","visible_p":"可见人员","is_new":"标签"}
    Task(reader, writer,columns=columns,flatMapFunciton=salesStatisticsMappingFunction).start(time_diff_message='excel temp_erp_item')
    if os.path.exists("../ecsp_file/target_data/finish/"+file):
        os.remove("../ecsp_file/target_data/finish/"+file)
    shutil.move(target_path,"../ecsp_file/target_data/finish/")    

@file_processing_log3
def saleFBATask(file):
    target_path=os.path.join('../ecsp_file/target_data',file)
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    reader = FileReader(target_path)
    writer = DatabaseWriter(dst, table_name="temp_erp_item",batch_size=1)
    #缺少开始售卖时间
    columns = {"erp_no":"仓库sku","item_no":"仓库sku","sku":"Seller SKU","asin":"ASIN","store":"账号","duty_p":"运营负责人","visible_p":"可见人员","is_new":"标签"}
    Task(reader, writer,columns=columns,flatMapFunciton=salesStatisticsMappingFunction).start(time_diff_message='excel temp_erp_item')
    if os.path.exists("../ecsp_file/target_data/finish/"+file):
        os.remove("../ecsp_file/target_data/finish/"+file)
    shutil.move(target_path,"../ecsp_file/target_data/finish/")
    

@file_processing_log
def internalSalesFactReturnTask(file,suffix):
    strs=file.split('.'+suffix)
    target_path=os.path.join('../ecsp_file/target_data',file.replace('.'+suffix,datetime.datetime.now().strftime("_%Y%m%d %H%M%S."+suffix)))
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    conditions=strs[0].split(' ')
    reader=None
    if suffix=='csv':
        reader = FileReader(target_path,pd_params={'encoding':'gbk'})
    if suffix=='xlsx':
        reader = ExcelReader(target_path,pd_params={'encoding':'gbk'},sheet_name=0)
    #reader = ExcelReader(os.path.join('./source_data/',file),sheet_name=0)
    writer = DatabaseWriter(dst, table_name="sales_fact_return",batch_size=1)
    columns = {"platform":"店铺名称","item_no":"商品代码","sales_qty":"入库数量","sales_amount":"实际退款金额"}
    Task(reader, writer,columns=columns,flatMapFunciton=internalSalesFactReturnFlatMapFunciton,conditions={'ym':conditions[0]}).start(time_diff_message='excel 国内退货实绩')
    shutil.move(target_path,'../ecsp_file/target_data/finish/')


@file_processing_log
def internalSalesFactTask(file,suffix):
    strs=file.split('.'+suffix)
    target_path=os.path.join('../ecsp_file/target_data',file.replace('.'+suffix,datetime.datetime.now().strftime("_%Y%m%d %H%M%S."+suffix)))
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    conditions=strs[0].split(' ')
    reader=None
    if suffix=='csv':
        reader = FileReader(target_path,pd_params={'encoding':'gbk'})
    if suffix=='xlsx':
        reader = ExcelReader(target_path,pd_params={'encoding':'gbk'},sheet_name=0)
    #reader = ExcelReader(os.path.join('./source_data/',file),sheet_name=0)
    writer = DatabaseWriter(dst, table_name="sales_fact",batch_size=1)
    columns = {"platform":"店铺名称","item_no":"商品代码","send_time":"发货时间","sales_qty":"数量","sales_amount":"让利后金额"}
    Task(reader, writer,columns=columns,flatMapFunciton=internalSalesFactFlatMapFunciton,conditions={'ym':conditions[0]}).start(time_diff_message='excel 销售实绩')
    shutil.move(target_path,'../ecsp_file/target_data/finish/')

@file_processing_log
def profitAndLossTask(file,suffix):
    strs=file.split('.'+suffix)
    target_path=os.path.join('./target_data',file.replace('.'+suffix,datetime.datetime.now().strftime("_%Y%m%d %H%M%S."+suffix)))
    shutil.move(os.path.join('./source_data/',file),target_path)
    reader=None
    if suffix=='csv':
        reader = FileReader(target_path,pd_params={'encoding':'utf-8'})
    if suffix=='xlsx':
        reader = ExcelReader(target_path,pd_params={'encoding':'utf-8'},sheet_name=0)
    writer = DatabaseWriter(dst, table_name="cost_records",batch_size=1)
    columns = {"year":"年份","cost_date":"月份","cost_subject":"摘要","cost_type_p":"费用类别","countrys":"国家","monetary":"金额","platform":"渠道"}
    Task(reader, writer,columns=columns,flatMapFunciton=profitAndLossFlatMapFunciton).start(time_diff_message='excel 销售实绩')

@file_processing_log
def skuDocumentTask(file,suffix):
    strs=file.split('.'+suffix)
    target_path=os.path.join('../ecsp_file/target_data',file.replace('.'+suffix,datetime.datetime.now().strftime("_%Y%m%d %H%M%S."+suffix)))
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    reader=None
    if suffix=='csv':
        reader = FileReader(target_path,pd_params={'encoding':'utf-8'})
    if suffix=='xlsx':
        reader = ExcelReader(target_path,pd_params={'encoding':'utf-8'},sheet_name=0)
    writer = DatabaseWriter(dst, table_name="products",batch_size=1)
    columns = {"item_no":"货号","purchase_cost":"采购成本（含税）","product_cost":"单个成本","status":"状态"}
    Task(reader, writer,columns=columns,flatMapFunciton=purchaseCostFlatMapFunciton).start(time_diff_message='excel 销售实绩')
    writer1 = DatabaseWriter(dst, table_name="price_table",batch_size=1)
    columns1 = {"country":"国家","item_no":"货号","status":"状态","sale_fee":"佣金","ocean_freight":"运费","storage_fee":"仓储","tariff":"关税","VAT":"VAT","express_fee":"快递","advertise_fee_percent":'广告%',"advertise_fee":"广告","manage_other_fee":"管理费用","financial_other_fee":"财务费用","min_price":"最低售价","platform_price":"平台在售价格"}
    Task(reader, writer1,columns=columns1,flatMapFunciton=skuDocumentFlatMapFunciton).start(time_diff_message='excel 销售实绩')
    shutil.move(target_path,'../ecsp_file/target_data/finish/')

def salesFact2020Task(file):
    reader = ExcelReader(os.path.join('./source_data/',file),sheet_name=0)
    writer = DatabaseWriter(dst, table_name="sales_fact",batch_size=1)
    columns = {"ym":"日期","country":"国家",'asin':'子ASIN','sales_qty':'2020销量','sales_amount':'2020销售额'}
    Task(reader,writer,columns=columns,flatMapFunciton=salesFact2020FlatMapFunciton).start(time_diff_message='excel 销售实绩')

def salesTargetJdTask(file):
    reader = ExcelReader(os.path.join('./source_data/',file),sheet_name=0)
    writer = DatabaseWriter(dst, table_name="sales_target",batch_size=1)
    columns = {"ym":"日期","country":"国家",'category_III':'小类','platform':'平台/渠道','target_qty':'目标数量','target_amount':'目标销售额'}
    functions={'ym':dateFunction}
    Task(reader,writer,columns=columns,functions=functions,flatMapFunciton=salesTargetJdFlatMapFunciton).start(time_diff_message='excel 销售目标')

def productCostTask(file):
    # reader = ExcelReader(os.path.join('./source_data/',file),sheet_name='商品成本')
    # writer = DatabaseWriter(dst, table_name="price_table_copy",batch_size=1)
    # columns = {"country":"国家",'platform':'平台','erp_no':'商品代码','ocean_freight':'运费','tariff':'关税','vat':'VAT','storage_fee':'仓储','express_fee':'快递'}
    # Task(reader,writer,columns=columns,flatMapFunciton=productCostFunction).start(time_diff_message='商品成本')
    reader2 = ExcelReader(os.path.join('./source_data/',file),sheet_name='财务费用')
    writer2 = DatabaseWriter(dst, table_name="contrast_table",batch_size=1)
    columns = {"apportion_num":"金额","fee_type_I":"费用类别","fee_type_II":"费用子类","countrys":"国别","channels":"渠道"}
    Task(reader2,writer2,columns=columns,flatMapFunciton=apportionFunction).start(time_diff_message='财务费用')

@file_processing_log2
def generalTask(file,suffix):
    strs=file.split('.'+suffix)
    target_path=os.path.join('../ecsp_file/target_data',file)
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    names=strs[0].split('_')
    collect=list(filter(lambda x:x.get('report_name') in names[1],amazon_b_report_collect))[0]
    #国家
    country=settings.COUNTRY_MAPPING.get(names[3])
    code=list(filter(lambda x:x.get('code_id')==collect.get('id') and x.get('country')==country and x.get('brand').lower()==names[0].lower(),amazon_b_report_code))
    if len(code)==0:
        raise Exception('错误类型:未找到对应code')
    reader=None
    df=None
    try:
        if suffix=='csv':
            df=pd.read_csv(target_path, encoding='utf-8')
            reader = FileReader(target_path,pd_params={'encoding':'utf-8'})
        if suffix=='xlsx':
            df=pd.read_excel(target_path)
            reader = ExcelReader(target_path,pd_params={'encoding':'utf-8'},sheet_name=0)
    except Exception as e:
        # database.db.get_table('file_processing_log').upsert({'file_name':file,'brand':names[0],'file_type':names[1],'date_range':names[2],'country':names[3],
        # 'pro_result':'fail','messge':f'错误类型:编解码问题','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
        database.db.get_table('file_handle_process_log').upsert({'id':names[4],'file_process_code':3,'etl_msg':f'错误类型:编解码问题','etl_process_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
    writer = DatabaseWriter(dst, table_name=code[0].get('insert_table'),batch_size=1,names=names)
    #database.db.get_table('temp_erp_not_found').upsert({'erp_no':data.get('sku'),'error_date':time.strftime("%Y-%m-%d", time.localtime()) })
    #模板编号
    template=code[0].get('template')
    original_names=df.columns.values
    mappings=list(filter(lambda x:x.get('template')==template,amazon_b_mapping))
    columns={}
    condition_mapping={}
    for i,tag in enumerate(mappings):
        #判断按顺序还是字段对应
        if code[0].get('is_queue')==0:
            if tag.get('original_name').strip().lower()!=original_names[i].strip().lower():
                logger.warning(f"code_id：{collect.get('id')}--国家：{country}--映射字段不一致--列字段：{original_names[i]}----数据库字段：{tag.get('original_name')}")
                # database.db.get_table('file_processing_log').upsert({'file_name':file,'brand':names[0],'file_type':names[1],'date_range':names[2],'country':names[3],
                #     'pro_result':'fail','messge':f'错误类型:字段存在不对应','update_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))})
                r=requests.post('http://192.168.66.37:5000/dwt/notice?to=ROBOT', json = {"text": {"content": f"file_name:{file}========>date_range:{names[2]}=========>不一致字段:{original_names[i]}========>{tag.get('original_name')}"},"msgtype": "text"})
                print(r.url)
                raise Exception('错误类型:字段存在不对应')
            columns[tag.get('target_name').strip()]=original_names[i]
        else:
            columns[tag.get('target_name').strip()]=tag.get('original_name')
        condition_mapping[tag.get('target_name').strip()]=tag
    Task(reader, writer,columns=columns,flatMapFunciton=generalFunction,conditions={'country':country,'names':names,"code":code[0],"mappings":condition_mapping}).start(time_diff_message='excel 销售实绩')
    if os.path.exists("../ecsp_file/target_data/finish/"+file):
        os.remove("../ecsp_file/target_data/finish/"+file)
    shutil.move(target_path,'../ecsp_file/target_data/finish/')


@file_processing_log
def saleAppraisalTask(file):
    logger.info("任务启动:处理文件 {}".format(file))
    target_path=os.path.join('../ecsp_file/target_data',file.replace('.',datetime.datetime.now().strftime("_%Y%m%d %H%M%S.")))
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    reader = ExcelReader(target_path,sheet_name=0)
    writer = DatabaseWriter(dst, table_name="sales_target_item",batch_size=1)
    columns = {"ym": "年月", "platform": '平台','area':'区域','country':'国别','item_no':'货号','target_qty':'采购计划','shipped_qty':'销量预估'}
    Task(reader, writer,columns=columns,flatMapFunciton=saleAppraisalFunction).start(time_diff_message='saleAppraisalTask')
    shutil.move(target_path,'../ecsp_file/target_data/finish/')
