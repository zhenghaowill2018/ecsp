import datetime
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5
from time import sleep

import pymysql

import settings
from pyetl import DatabaseConnection, DatabaseWriter, ExcelReader, Task
from utils import LogUtils
import functions


if __name__ == "__main__":
    #functions.salesFactTask('2021.02 Amazon 英国 PETSFIT.csv')
    #functions.internalSalesFactTask('2020.12 国内 PETSFIT.xlsx')
    functions.salesFact2020Task('2020月销售数据.xlsx')
    #functions.salesTargetJdTask('2021年产品布局销售目标-petsfit-京东.xlsx')