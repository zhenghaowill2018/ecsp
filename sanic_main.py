import asyncio
import datetime
import decimal
import json
import logging
import os

from sanic import Sanic, response

import sanic_service
from utils import LogUtils
#import winrm

loop = asyncio.get_event_loop()
app = Sanic()
app.config.REQUEST_TIMEOUT=500
app.config.RESPONSE_TIMEOUT=500
app.config.KEEP_ALIVE_TIMEOUT=15
app.static('/static', os.path.abspath('.')+'/static')
LogUtils.log_config(f'ecsp_interface','0.0.0.0','0000')
logger = logging.getLogger(f'interface')


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        if isinstance(o,datetime.date):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        super(DecimalEncoder, self).default(o)

@app.route('api/normal', methods=['POST'])
async def normal(request):
    table=request.json.get('table')
    fields=request.json.get('fields')
    condition=request.json.get('condition')
    logger.info(f'调用/normal接口,table:{table},fields:{fields},condition:{condition}')
    result=await sanic_service.normalService(table,fields,condition)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/category/list', methods=['POST'])
async def categoryList(request):
    fields=request.json.get('fields')
    platforms=request.json.get('platforms')
    areas=request.json.get('areas')
    countrys=request.json.get('countrys')
    logger.info(f'调用api/category/list接口,fields:{fields},platforms:{platforms},areas:{areas},countrys:{countrys}')
    result=await sanic_service.categoryList(fields,platforms,areas,countrys)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/delete/normal', methods=['POST'])
async def updateNormal(request):
    table=request.json.get('table')
    condition=request.json.get('condition')
    logger.info(f'调用/normal接口,table:{table},condition:{condition}')
    result=await sanic_service.deleteNormalService(table,condition)
    return response.json(result)

@app.route('api/update/normal', methods=['POST'])
async def updateNormal(request):
    table=request.json.get('table')
    update=request.json.get('update')
    condition=request.json.get('condition')
    logger.info(f'调用/update/normal接口,table:{table},update:{update},condition:{condition}')
    result=await sanic_service.updateNormalService(table,update,condition)
    return response.json(result)

@app.route('api/insert/normal', methods=['POST'])
async def insertNormal(request):
    table=request.json.get('table')
    records=request.json.get('records')
    logger.info(f'调用/insert/normal接口,table:{table},records:{records}')
    result=await sanic_service.insertNormalService(table,records)
    return response.json(result)

@app.route('api/file/upload', methods=['POST'])
async def insertNormal(request):
    f=request.files.get('file')
    with open('./source_data/'+f.name, 'wb') as fileUp:  # 这里必须为读写二进制模式的 wb
        fileUp.write(f.body)
        fileUp.close()
    return response.json({'response':'success'})

@app.route('api/upsert/normal', methods=['POST'])
async def insertNormal(request):
    table=request.json.get('table')
    record=request.json.get('record')
    logger.info(f'调用api/upsert/normal接口,table:{table},records:{record}')
    result=await sanic_service.upsertNormalService(table,record)
    return response.json(result)

@app.route('api/upsert/new/category', methods=['POST'])
async def insertNormal(request):
    table=request.json.get('table')
    record=request.json.get('record')
    logger.info(f'调用api/upsert/normal接口,table:{table},records:{record}')
    result=await sanic_service.upsertNewCategory(table,record)
    return response.json(result)

@app.route('api/sales/sum', methods=['POST'])
async def salesSum(request):
    need=request.json.get('need',['fact','target'])
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    channels=request.json.get('channels')
    duty_p=request.json.get('duty_p')
    category_I=request.json.get('category_I')
    category_II=request.json.get('category_II')
    category_III=request.json.get('category_III')
    logger.info(f'调用/sales/sum接口,start_time:{start_time},end_time:{end_time},channels:{channels},category_I:{category_I},category_II:{category_II},category_III:{category_III}')
    result=await sanic_service.salesSumService(start_time,end_time,channels,category_I,category_II,category_III,duty_p,need)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/category/target', methods=['POST'])
async def categoryTarget(request):
    channels=request.json.get('channels')
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    category_I=request.json.get('category_I')
    category_II=request.json.get('category_II')
    category_III=request.json.get('category_III')
    logger.info(f'调用/category/target接口,{start_time},end_time:{end_time},channels:{channels},category_I:{category_I},category_II:{category_II},category_III:{category_III}')
    result=await sanic_service.categoryTargetService(start_time,end_time,channels,category_I,category_II,category_III)
    return response.json(result,cls=DecimalEncoder)


#年度销售计划
@app.route('api/category/next/target', methods=['POST'])
async def nextYearCategoryTarget(request):
    channels=request.json.get('channels')
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    category_I=request.json.get('category_I')
    category_II=request.json.get('category_II')
    category_III=request.json.get('category_III')
    logger.info(f'调用api/category/next/target接口,{start_time},end_time:{end_time},channels:{channels},category_I:{category_I},category_II:{category_II},category_III:{category_III}')
    result=await sanic_service.nextYearCategoryTargetService(start_time,end_time,channels,category_I,category_II,category_III)
    return response.json(result,cls=DecimalEncoder)

#采购计划
@app.route('api/category/sales/next/target', methods=['POST'])
async def nextYearCategoryTargetSales(request):
    channels=request.json.get('channels')
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    category_I=request.json.get('category_I')
    category_II=request.json.get('category_II')
    category_III=request.json.get('category_III')
    logger.info(f'调用api/category/sales/next/target接口,{start_time},end_time:{end_time},channels:{channels},category_I:{category_I},category_II:{category_II},category_III:{category_III}')
    result=await sanic_service.nextYearCategoryTargetSalesService(start_time,end_time,channels,category_I,category_II,category_III)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/category/remove/rate', methods=['POST'])
async def removeRate(request):
    ym=request.json.get('ym')
    channels=request.json.get('channels')
    category_III=request.json.get('category_III')
    logger.info(f'调用api/category/remove/rate接口,ym:{ym},channel_no:{channels},category_III:{category_III}')
    result=await sanic_service.removeGrowRateService(ym,channels,category_III)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/category/mid/rate', methods=['POST'])
async def removeRate(request):
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    channels=request.json.get('channels')
    category_II=request.json.get('category_II')
    category_III=request.json.get('category_III')
    flag=request.json.get('flag')
    logger.info(f'调用api/category/mid/rate接口,start_time:{start_time},end_time:{end_time},channel_no:{channels},category_II:{category_II},category_III:{category_III},flag:{flag}')
    result=None
    try:
        if flag==1:
            result=await sanic_service.factMidCategoryRateService(start_time,end_time,channels,category_II,category_III)
        elif flag==2:
            result=await sanic_service.targetMidCategoryRateService(start_time,end_time,channels,category_II,category_III)
    except Exception as e:
        logger.error(f'调用api/category/mid/rate接口失败,start_time:{start_time},end_time:{end_time},channel_no:{channels},category_II:{category_II},category_III:{category_III},flag:{flag}')
    return response.json(result,cls=DecimalEncoder)


@app.route('api/category/itemNo', methods=['POST'])
async def categoryTarget(request):
    channels=request.json.get('channels')
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    item_no=request.json.get('item_no')
    logger.info(f'调用/category/target接口,{start_time},end_time:{end_time},channels:{channels},item_no:{item_no}')
    result=await sanic_service.itemNoTargetService(start_time,end_time,channels,item_no)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/export/excel', methods=['POST'])
async def exportExcel(request):
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    channels=request.json.get('channels')
    logger.info(f'调用/api/export/excelt接口,start_time:{start_time},end_time:{end_time},channels:{channels}')
    fileName=datetime.datetime.now().strftime("%Y%m")
    await sanic_service.exportExcelService(start_time,end_time,channels,fileName,loop)
    return await response.file(
        os.getcwd()+'/target_data/excel/'+fileName+'.xlsx',
        filename=fileName+'.xlsx'
    )

@app.route('api/costrecords/export/excel', methods=['POST'])
async def costRecordsExportExcel(request):
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    logger.info(f'调用api/costrecords/export/excel接口,start_time:{start_time},end_time:{end_time}')
    fileName='costrecords'+datetime.datetime.now().strftime("%Y%m")
    await sanic_service.costRecordsExportExcelService(start_time,end_time,fileName,loop)
    return await response.file(
        os.getcwd()+'/target_data/excel/'+fileName+'.xlsx',
        filename=fileName+'.xlsx'
    )

@app.route('api/salesfact/export/excel', methods=['POST'])
async def exportExcel(request):
    channels=request.json.get('channels')
    logger.info(f'调用/api/export/excelt接口,channels:{channels}')
    fileName='salesfact'+datetime.datetime.now().strftime("%Y%m")
    await sanic_service.salesFactExportExcelService(channels,fileName,loop)
    return await response.file(
        os.getcwd()+'/target_data/excel/'+fileName+'.xlsx',
        filename=fileName+'.xlsx'
    )

@app.route('api/salesfact/export/product', methods=['POST'])
async def exportExcel(request):
    fileName='product'+datetime.datetime.now().strftime("%Y%m")
    await sanic_service.productExportExcelService(fileName,loop)
    return await response.file(
        os.getcwd()+'/target_data/excel/'+fileName+'.xlsx',
        filename=fileName+'.xlsx'
    )

@app.route('api/export/excel/salesTarget', methods=['POST'])
async def exportExcel(request):
    fileName='salesTarget'+datetime.datetime.now().strftime("%Y%m")
    await sanic_service.ExportExcelTargetService(fileName,loop)
    return await response.file(
        os.getcwd()+'/target_data/excel/'+fileName+'.xlsx',
        filename=fileName+'.xlsx'
    )

@app.route('api/salestarget/item/export/excel', methods=['POST'])
async def exportExcel(request):
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    channels=request.json.get('channels')
    logger.info(f'调用/api/export/excelt接口,channels:{channels}')
    fileName='salestargetitem'+datetime.datetime.now().strftime("%Y%m")
    await sanic_service.salesTargetItemExportExcelService(channels,start_time,end_time,fileName,loop)
    return await response.file(
        os.getcwd()+'/target_data/excel/'+fileName+'.xlsx',
        filename=fileName+'.xlsx'
    )

@app.route('api/salestarget/new', methods=['POST'])
async def salesTargetNew(request):
    channels=request.json.get('channels')
    category_III=request.json.get('category_III')
    year=request.json.get('year')
    result=await sanic_service.salesTargetNew(channels,category_III,year)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/salestarget/new/category', methods=['POST'])
async def salesTargetNewCategory(request):
    channels=request.json.get('channels')
    category_III=request.json.get('category_III')
    year=request.json.get('year')
    result=await sanic_service.salesTargetNewCategory(channels,category_III,year)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/price/apportion', methods=['POST'])
async def priceApportion(request):
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    result=await sanic_service.priceApportionService(start_time,end_time)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/price/table', methods=['POST'])
async def priceTable(request):
    channel=request.json.get('channel')
    category_III=request.json.get('category_III')
    result=await sanic_service.priceTableService(channel,category_III)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/price/compare', methods=['POST'])
async def priceCompare(request):
    channel=request.json.get('channel')
    category_III=request.json.get('category_III')
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    country=request.json.get('country')
    result=await sanic_service.priceCompareService(channel,category_III,start_time,end_time,country)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/price/average',methods=['POST'])
async def averagePrice(request):
    item_no=request.json.get('item_no')
    country=request.json.get('country')
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    result=await sanic_service.averagePriceService(item_no,country,start_time,end_time)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/category/targetItem', methods=['POST'])
async def categoryTargetItem(request):
    ym=request.json.get('ym')
    channel=request.json.get('channels')
    category_III=request.json.get('category_III')
    result=await sanic_service.categoryTargetItem(ym,channel,category_III)
    return response.json(result,cls=DecimalEncoder)

@app.route('api/excel/normal', methods=['POST'])
async def excelNormal(request):
    data=request.json.get('data')
    sql=request.json.get("sql")
    titles=request.json.get('titles')
    sheet_name=request.json.get('sheet_name')
    fileName='excelNormal'+datetime.datetime.now().strftime("%Y%m")
    await sanic_service.excelNormalService(data,sql,titles,sheet_name,fileName,loop)
    return await response.file(
        os.getcwd()+'/target_data/excel/'+fileName+'.xlsx',
        filename=fileName+'.xlsx'
    )

@app.route('api/temporary/updateTarget', methods=['POST'])
async def updateTarget(request):
    start_time=request.json.get('start_time')
    end_time=request.json.get('end_time')
    channel_no=request.json.get("channel_no")
    nickname=request.json.get("nickname")
    logger.info(f'调用下单按钮,nickname:{nickname},fields:{channel_no},start_time:{start_time},end_time:{end_time}')
    #result=await sanic_service.temporaryUpdateTarget(start_time,end_time,channel_no)
    result=None
    return response.json(result,cls=DecimalEncoder)


# @app.route('api/button/reload', methods=['POST'])
# async def buttonReload(request):
#     session = winrm.Session('192.168.66.91', auth=('yuzhu', 'yuzhu'))

#     #cmd = session.run_cmd(r'dir')
#     cmd = session.run_ps(r"python 'C:\Users\yuzhu\Desktop\Super Browser\upload_report.py'")
#     print(cmd.std_out.decode('GBK'))  # 打印获取到的信息
#     print(cmd.std_err.decode('GBK')) # 打印错误信息
#     return response.json({'response':'success'})

@app.route('api/test', methods=['POST'])
async def getAsin(request):
    #table=request.json.get('table')
    return response.json({'response':'success'})


@app.middleware('response')
async def prevent_xss(request, response):
  if response:
    response.headers["Access-Control-Allow-Origin"] = "*" #'http://localhost:8080'
    #response.headers["Access-Control-Allow-Headers"] = "content-type,user-agent"
    response.headers["Access-Control-Allow-Headers"] = "X-Custom-Header,content-type"
    response.headers["Access-Control-Allow-Methods"] = "PUT,POST,GET,DELETE,OPTIONS"

@app.middleware('request')
async def prevent_xsr(request):
    if request.json is not None:
        for k,v in request.json.items():
            if isinstance(k,str) and isinstance(v,str):
                for err_char in ['alter','update','delete','truncate','drop']:
                    if err_char in k or err_char in v:
                        return response.json({'err_message':'非法字符'})
    print('连接===================')
    if request.method == 'OPTIONS':
        return response.json(None)
 
#test2
if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=8000,debug=True)
    server = app.create_server(host="0.0.0.0", port=8050, return_asyncio_server=True)
    task = asyncio.ensure_future(server)
    loop.run_forever()
    # app.run(host="0.0.0.0", port=8010, return_asyncio_server=True)