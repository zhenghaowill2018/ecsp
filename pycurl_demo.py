import pycurl

# 创建 Curl 对象
c = pycurl.Curl()

# 设置 URL 和其他选项
c.setopt(c.URL, 'https://app-amz.eccang.com/erp/fba-inventory/listing-fba-list')
c.setopt(c.HTTPHEADER, ['Content-Type: application/json'])
c.setopt(c.POSTFIELDS, '{"page": 1,"pageSize": 20,"select_type": "sku","sku_type": "SELLERSKU","user_account": [],"operator": "=","sites": []}')
c.setopt(c.VERBOSE, True)

# 执行请求并获取响应
response = b''
c.setopt(c.WRITEFUNCTION, lambda x: response.extend(x))
c.perform()

# 关闭 Curl 对象
c.close()

# 打印响应
print(response.decode('utf-8'))