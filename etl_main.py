import datetime
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
import time
import pymysql
import pandas

from pyetl import DatabaseWriter, ExcelReader, Task, DatabaseConnection
from utils import LogUtils
import functions
import logging
import settings

dst = pymysql.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD, db=settings.MYSQL_DB)
database=DatabaseConnection(dst)
countryDirt=None
LogUtils.log_config(f'ecsp','0.0.0.0','0000') 
logger = logging.getLogger(f'etl_main')

def erpAndItemFunciton(need):
    result=database.db.get_table('temp_erp_item').find_one(condition={'erp_no':need})
    return result.get('item_no') if result else result

def platformFunction(need):
    return 'Amazon' if need=='亚马逊' else need

def isNullFunction(need):
    if need is None:
        need='FOCUS'
    return need


def countryFunction(need):
    return need

def dateFunciton(need):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

def priceTableFlatMapFunction(record,conditions):
    result_list=[]
    record=list(filter(lambda x:x.get('item_no') is not None,record))
    channels=database.db.get_table('channels').find().get_all()
    for data in record:
        try:
            new_data={}
            channel=list(filter(lambda x:x.get('platform')==data.get('platform') and x.get('country')==data.get('country'),channels))
            new_data['cin']=data.get('item_no')+"_"+channel[0].get('channel_no')
            new_data['channel_no']=channel[0].get('channel_no')
            new_data['item_no']=data.get('item_no')
            result_list.append(new_data)
        except :
            logger.error(f'priceTable flatmap错误,错误数据为:{data}!!!')
    return result_list

def itemDutyFlatMapFunction(record,conditions):
    result_list=[]
    record=list(filter(lambda x:x.get('item_no') is not None,record))
    channels=database.db.get_table('channels').find().get_all()
    for data in record:
        try:
            new_data={}
            if data.get('item_no')=='/':
                continue
            if data.get('platform')=='亚马逊VC':
                data['platform']='Amazon VC'
            if data.get('duty_p')==None:
                continue
            channel=list(filter(lambda x:x.get('platform')==data.get('platform') and x.get('country')==data.get('country'),channels))
            new_data['item_no']=data.get('item_no')
            new_data['sku']=data.get('sku')
            new_data['duty_p']=data.get('duty_p')
            new_data['channel_no']=channel[0].get('channel_no')
            new_data['product_type']=data.get('product_type')
            result_list.append(new_data)
        except :
            logger.error(f'priceTable flatmap错误,错误数据为:{data}!!!')
    return result_list

def skuDocumentFlatMapFunction(record,conditions):
    result_list=[]
    for data in record:
        if isinstance(data['active_start_time'],(pandas._libs.tslibs.timestamps.Timestamp,datetime.datetime)) and not isinstance(data['active_start_time'],pandas._libs.tslibs.nattype.NaTType):
            data['active_start_time']=data['active_start_time'].strftime("%Y-%m-%d %H:%M:%S")
        else:
            data['active_start_time']="2000-01-01 00:00:00"
        if isinstance(data['active_end_time'],(pandas._libs.tslibs.timestamps.Timestamp,datetime.datetime)) and not isinstance(data['active_end_time'],pandas._libs.tslibs.nattype.NaTType):
            data['active_end_time']=data['active_end_time'].strftime("%Y-%m-%d %H:%M:%S")
        else:
            data['active_end_time']="2099-12-31 00:00:00"
        result_list.append(data)
    return result_list

def productsFlatMapFunciton(record,conditions):
    result_list=[]
    for data in record:
        if data.get('item_no') is not None:
            item_no=data.get('item_no')
            material_str=item_no[2]
            data['customs_material_type']=settings.MATERIAL_MAPPING.get(material_str)
        result_list.append(data)
    return result_list

def salesStatisticsCategoryFunction(record,conditions):
    for data in record:
        data['category']
    return None

@functions.file_processing_log
def atrMassageTask(file):
    # target_path=os.path.join('../ecsp_file/target_data')
    # shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    reader = ExcelReader(file,sheet_name='每日可售库存数量')
    writer = DatabaseWriter(dst, table_name="amazon_s_invt_report",batch_size=1)
    mappings=list(filter(lambda x:x.get('template')=='temp_RSR_en',functions.amazon_b_mapping))
    columns={}
    condition_mapping={}
    for i,tag in enumerate(mappings):
        columns[tag.get('target_name').strip()]=tag.get('original_name')
        condition_mapping[tag.get('target_name').strip()]=tag
    Task(reader, writer,columns=columns,flatMapFunciton=functions.generalFunction,conditions={'country':"德国",'names':['PETSFIT'],"code":{},"mappings":condition_mapping}).start(time_diff_message='excel sheet_name=0')
    #shutil.move(target_path,'../ecsp_file/target_data/finish/')


@functions.file_processing_log
def productMassageTask(file):
    target_path=os.path.join('../ecsp_file/target_data',file.replace('.',datetime.datetime.now().strftime("_%Y%m%d %H%M%S.")))
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    reader = ExcelReader(target_path,sheet_name=0)
    writer = DatabaseWriter(dst, table_name="products",batch_size=1)
    columns = {"item_no":"item_no",
                "brand":"brand",
                "purchase_cost":"price_RMB",
                "category_I":"category_I",
                "category_II":"category_II",
                "category_III":"category_III",
                "product_length":"product_length",
                "product_wide":"product_wide",
                "product_high":"product_high",
                "product_weight":"product_weight",
                "product_fold_length":"product_fold_length",
                "product_fold_wide":"product_fold_wide",
                "product_fold_high":"product_fold_high",
                "internal_packet_length":"internal_packet_length",
                "internal_packet_wide":"internal_packet_wide",
                "internal_packet_high":"internal_packet_high",
                "internal_box_num":"internal_box_num",
                "internal_rough_weight":"internal_rough_weight",
                "external_packet_length":"external_packet_length",
                "external_packet_wide":"external_packet_wide",
                "external_packet_high":"external_packet_high",
                "external_packet_num":"external_packet_num",
                "external_rough_weight":"external_rough_weight",
                "update_time":"update_time"}
    Task(reader, writer,columns=columns,flatMapFunciton=functions.productMassageFunction).start(time_diff_message='excel sheet_name=0')
    shutil.move(target_path,'../ecsp_file/target_data/finish/')

@functions.file_processing_log
def procutBaseTask(file):
    logger.info("任务启动:处理文件 {}".format(file))
    target_path=os.path.join('../ecsp_file/target_data',file.replace('.',datetime.datetime.now().strftime("_%Y%m%d %H%M%S.")))
    shutil.move(os.path.join('../ecsp_file/source_data/',file),target_path)
    #任务一
    reader = ExcelReader(target_path,sheet_name=0)
    writer = DatabaseWriter(dst, table_name="categories_t",batch_size=1)
    columns = {"category_I":"大类","category_II":"中类","category_III":"小类","brand":"品牌",'update_time':"供应商料号"}
    function_me={'update_time':dateFunciton}
    Task(reader, writer,columns=columns,functions=function_me).start(time_diff_message='excel sheet_name=0')
    #任务二
    reader = ExcelReader(target_path,sheet_name=0)
    writer = DatabaseWriter(dst, table_name="products",batch_size=1)
    columns = {"category_III":"小类","item_no":"货号（不可以重复)","brand":"品牌","purchase_cost":"采购成本（含税）","update_time":"小类","category_I":"大类","category_II":"中类"}
    function_me={'update_time':dateFunciton}
    Task(reader, writer,columns=columns,functions=function_me,flatMapFunciton=productsFlatMapFunciton).start(time_diff_message='excel sheet_name=0')
    #=========================================
    # #任务二
    # reader_sheet2= ExcelReader(target_path,sheet_name=2)
    # writer_sheet2 = DatabaseWriter(dst, table_name="temp_erp_item",batch_size=1)
    # columns = {"erp_no": "产品编号（ERP产品编号）", "item_no": "货号"}
    # Task(reader_sheet2, writer_sheet2,columns=columns).start(time_diff_message='excel sheet_name=2')
    #=========================================
    #任务三
    reader_sheet2= ExcelReader(target_path,sheet_name=1)
    writer_sheet2 = DatabaseWriter(dst, table_name="temp_erp_item",batch_size=1)
    columns = {"erp_no": "产品编号（ERP产品编号）", "item_no": "货号","sku":"SKU(不可重复)","asin":"子ASIN","platform":"平台","country":"国家","active_start_time":"起","active_end_time":"讫"}
    function_me={'platform':platformFunction,'item_no':isNullFunction,'erp_no':isNullFunction}
    Task(reader_sheet2, writer_sheet2,functions=function_me,flatMapFunciton=skuDocumentFlatMapFunction,columns=columns).start(time_diff_message='excel sheet_name=2')
    functions.temp_erp_items=database.db.get_table('temp_erp_item').find().get_all()
    # #任务四
    reader_sheet1= ExcelReader(target_path,sheet_name=1)
    writer_sheet1 = DatabaseWriter(dst, table_name="price_table",batch_size=1)
    columns = {"item_no": "产品编号（ERP产品编号）", "platform": '平台','country':'国家','update_time':'国家'}
    function_me={"item_no":erpAndItemFunciton,"platform":platformFunction,'update_time':dateFunciton}
    Task(reader_sheet1, writer_sheet1,columns=columns,functions=function_me,flatMapFunciton=priceTableFlatMapFunction).start(time_diff_message='excel sheet_name=1')
    #任务五
    # reader_sheet1= ExcelReader(target_path,sheet_name=1)
    # writer_sheet1 = DatabaseWriter(dst, table_name="temp_item_duty_p",batch_size=1)
    # columns = {"item_no": "产品编号（ERP产品编号）", "duty_p": '负责人','sku':'SKU(不可重复)','product_type':'新品',"platform": '平台','country':'国家','update_time':'国家'}
    # function_me={"platform":platformFunction,'update_time':dateFunciton}
    # Task(reader_sheet1, writer_sheet1,columns=columns,functions=function_me,flatMapFunciton=itemDutyFlatMapFunction).start(time_diff_message='excel sheet_name=1')
    shutil.move(target_path,'../ecsp_file/target_data/finish/')

def salesTargetTask(file):
    logger.info("任务启动:处理文件 {}".format(file))
    # target_path=os.path.join('./target_data',file.replace('.',datetime.datetime.now().strftime("_%Y%m%d %H%M%S.")))
    # shutil.move(os.path.join('./source_data/',file),target_path)
    reader = ExcelReader(os.path.join('../ecsp_file/source_data/',file),sheet_name='2021北美室内用每月目标')
    writer = DatabaseWriter(dst, table_name="sales_target",batch_size=1)
    columns = {"category_III": "小类",'类别':'类别','1月':'1月','2月':'2月','3月':'3月','4月':'4月','5月':'5月','6月':'6月','7月':'7月','8月':'8月','9月':'9月','10月':'10月','11月':'11月','12月':'12月'}
    Task(reader, writer,columns=columns,flatMapFunciton=functions.salesTargetFlatMapFunction).start(time_diff_message='excel 销售目标')


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=settings.THEADPOOL_MAX_WORKER) as t:
        while True:
            logger.info("扫描文件=============")
            for f in os.listdir('../ecsp_file/source_data'):
                if f.split('.')[-1]=='xlsx':
                    if '内外销全' in f:
                        t.submit(procutBaseTask,f)
                    elif 'SKU主档' in f:
                        t.submit(functions.skuDocumentTask,f,'xlsx')
                    elif '国内退货' in f:
                        t.submit(functions.internalSalesFactReturnTask,f,'xlsx')
                    elif '国内' in f and '退货' not in f:
                        t.submit(functions.internalSalesFactTask,f,'xlsx')
                    elif '损益表' in f:
                        t.submit(functions.profitAndLossTask,f,'xlsx')
                    elif '产品信息汇总表' in f:
                        t.submit(productMassageTask,f)
                    elif '商品推广-推广的商品' in f:
                        t.submit(functions.generalTask,f,'xlsx')
                    elif '展示型推广-推广的商品' in f:
                        t.submit(functions.
                        generalTask,f,'xlsx')
                    elif '品牌推广-广告活动广告位' in f:
                        t.submit(functions.generalTask,f,'xlsx') 
                    elif '品牌推广视频-广告活动广告位' in f:
                        t.submit(functions.generalTask,f,'xlsx')
                    elif '销售目标' in f:
                        t.submit(salesTargetTask,f)
                    elif '货号销量预估' in f:
                         t.submit(functions.saleAppraisalTask,f)
                if f.split('.')[-1]=='csv':
                    if '日期范围报告' in f:
                        #if '业务' in f:
                        t.submit(functions.salesFactTask,f)
                    elif '国内退货' in f:
                        t.submit(functions.internalSalesFactReturnTask,f,'csv')
                    elif '国内' in f and '退货' not in f:
                        t.submit(functions.internalSalesFactTask,f,'csv')
                    elif '库存和销售报告-库存分类账' in f:
                        t.submit(functions.generalTask,f,'csv')
                    elif '买家退货' in f:
                        t.submit(functions.generalTask,f,'csv')
                    elif '子商品详情页的销售量与访问量' in f:
                        t.submit(functions.generalTask,f,'csv')
                    elif 'nt87xk4PRODUCT' in f:
                        t.submit(functions.salesStatisticsTask,f)
                    elif 'fba_fulfillment_current_inventory_goods_export' in f:
                        t.submit(functions.saleFBATask,f)
                    elif 'sales_statistics_sub_asin' in f:
                        t.submit(functions.salesListingTask,f)
                    elif '内销商品信息' in f:
                        t.submit(functions.innerSalesListingTask,f)                                              
            time.sleep(2)
    # productMassageTask('C:/Users/Administrator/Desktop/ecsp/target_data/excel/亚马逊运营日常销售跟踪表--德P 日数据汇总表【模板】.xlsx')
    # atrMassageTask('C:/Users/Administrator/Desktop/ecsp/target_data/excel/亚马逊运营日常销售跟踪表--德P 日数据汇总表【模板】.xlsx')
    # atrMassageTask('C:/Users/zhengh/Desktop/ecsp/source_data/产品信息汇总表-电商.xlsx')
    # #functions.transExcel('2021.04 Amazon 意大利 PETSFIT.csv')
    # salesTargetTask('2021年产品布局销售目标-海外亚马逊(3).xlsx')
    # functions.productCostTask('202002损益表 底稿 test.xlsx')