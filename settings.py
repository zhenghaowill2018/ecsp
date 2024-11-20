MYSQL_URL="mysql://ecsp_metis:6NjubR7arWcZ@192.168.66.71:3306/ecsp" #newmetis/NewMetis@Metis03
MYSQL_CHEMY_URL="mysql+pymysql://ecsp_metis:6NjubR7arWcZ@192.168.66.71:3306/ecsp"
MYSQL_HOST="192.168.66.71"
MYSQL_USER="ecsp_metis"
MYSQL_PASSWORD="6NjubR7arWcZ"
MYSQL_DB="ecsp"
#72
# MYSQL_CHEMY_URL="mysql+pymysql://root:metis@2020@192.168.66.72:3306/ecsp"
# MYSQL_URL="mysql://root:metis@2020@192.168.66.72:3306/ecsp"
# MYSQL_HOST="192.168.66.72"
# MYSQL_USER="root"
# MYSQL_PASSWORD="metis@2020"
# MYSQL_DB="ecsp"
#ETL处理线程数
THEADPOOL_MAX_WORKER=20
COUNTRY_ENCODING={
    '美国':'utf-8',
    '加拿大':'utf-8',
    '墨西哥':'utf-8',
    '英国':'utf-8',#utf-8 gbk
    '日本':'utf-8',#utf-8,Shift_JIS
    '德国':'utf-8',#'utf-8','iso-8859-1','gbk'
    '法国':'utf-8',#gbk utf-8
    '意大利':'utf-8',#utf-8 gbk
    '西班牙':'utf-8'#iso-8859-1 utf-8 gbk
}
COUNTRY_ENCODING_OLD={
    '美国':'utf-8',
    '加拿大':'gbk',#gbk utf-8
    '英国':'gbk',#utf-8 gbk
    '日本':'utf-8',#Shift_JIS gbk iso-8859-1 utf-8
    '德国':'utf-8',#'utf-8',iso-8859-1 gbk
    '法国':'iso-8859-1',#gbk utf-8 iso-8859-1
    '意大利':'iso-8859-1',#utf-8 gbk iso-8859-1
    '西班牙':'utf-8'#iso-8859-1 utf-8
}
COUNTRY_COLUMNS_OLD={
     #旧的列名规范
    #'美国':{"item_no":"(Child) ASIN","sales_amount":"Ordered Product Sales","sales_qty":"Units Ordered","SKU":"SKU"},
    '美国':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"},
    '加拿大':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"},
    #'英国':{"item_no":"(Child) ASIN","sales_amount":"Ordered Product Sales","sales_qty":"Units Ordered","SKU":"SKU"},
    '英国':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"},
    #'日本':{"item_no":"(子)ASIN","sales_amount":"注文商品売上","sales_qty":"注文された商品点数","SKU":"SKU"},
    '日本':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"},
    #'德国':{"item_no":"(Child) ASIN","sales_amount":"Ordered Product Sales","sales_qty":"Units Ordered","SKU":"SKU"},
    #'德国':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"},
    '德国':{"item_no":'£¨×Ó£©ASIN',"sales_amount":"ÒÑ¶©¹ºÉÌÆ·ÏúÊÛ¶î","sales_qty":"ÒÑ¶©¹ºÉÌÆ·ÊýÁ¿","SKU":"SKU"},
    #'法国':{"item_no":"(Child) ASIN","sales_amount":"Ordered Product Sales","sales_qty":"Units Ordered","SKU":"SKU"},
    #'法国':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"},
    '法国':{"item_no":'£¨×Ó£©ASIN',"sales_amount":"ÒÑ¶©¹ºÉÌÆ·ÏúÊÛ¶î","sales_qty":"ÒÑ¶©¹ºÉÌÆ·ÊýÁ¿","SKU":"SKU"},
    '意大利':{'item_no': '£¨×Ó£©ASIN', 'sales_amount': 'ÒÑ¶©¹ºÉÌÆ·ÏúÊÛ¶î', 'sales_qty': 'ÒÑ¶©¹ºÉÌÆ·ÊýÁ¿',"SKU":"SKU"},
    #'意大利':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"},
    '西班牙':{'item_no': '£¨×Ó£©ASIN', 'sales_amount': 'ÒÑ¶©¹ºÉÌÆ·ÏúÊÛ¶î', 'sales_qty': 'ÒÑ¶©¹ºÉÌÆ·ÊýÁ¿',"SKU":"SKU"}
    #'西班牙':{"item_no":"（子）ASIN","sales_amount":"已订购商品销售额","sales_qty":"已订购商品数量","SKU":"SKU"}
}

COUNTRY_ALL_COLUMNS={
    '美国':{"order_time_msg":"date/time","settlement_id":"settlement id","type":"type","order_id":"order id","sku":"sku","description":"description","quantity":"quantity",
            "marketplace":"marketplace","account_type":"account type","fulfillment":"fulfillment","order_city":"order city","order_state":"order state",
            "order_postal":"order postal","tax_collection_model":"tax collection model","product_sales":"product sales","product_sales_tax":"product sales tax",
            "shipping_credits":"shipping credits","shipping_credits_tax":"shipping credits tax","gift_wrap_credits":"gift wrap credits",
            "giftwrap_credits_tax":"giftwrap credits tax","promotional_rebates":"promotional rebates","promotional_rebates_tax":"promotional rebates tax",
            "marketplace_withheld_tax":"marketplace withheld tax","selling_fees":"selling fees","fba_fees":"fba fees","other_transaction_fees":"other transaction fees",
            "other":"other","total":"total"},
    '墨西哥':{"order_time_msg":"fecha/hora","settlement_id":"Id. de liquidación","type":"tipo","order_id":"Id. del pedido","sku":"sku","description":"descripción","quantity":"cantidad",
            "marketplace":"marketplace","fulfillment":"cumplimiento","order_city":"ciudad del pedido","order_state":"estado del pedido",
            "order_postal":"código postal del pedido","tax_collection_model":"modelo de recaudación de impuestos","product_sales":"ventas de productos","product_sales_tax":"impuesto de ventas de productos",
            "shipping_credits":"créditos de envío","shipping_credits_tax":"impuesto de abono de envío","gift_wrap_credits":"créditos por envoltorio de regalo",
            "giftwrap_credits_tax":"impuesto de créditos de envoltura","promotional_rebates":"descuentos promocionales","promotional_rebates_tax":"impuesto de reembolsos promocionales",
            "marketplace_withheld_tax":"impuesto de retenciones en la plataforma","selling_fees":"tarifas de venta","fba_fees":"tarifas fba","other_transaction_fees":"tarifas de otra transacción",
            "other":"otro","total":"total"},
    "加拿大":{"order_time_msg":"date/time","settlement_id":"settlement id","type":"type","order_id":"order id","sku":"sku","description":"description","quantity":"quantity",
            "marketplace":"marketplace","fulfillment":"fulfillment","order_city":"order city","order_state":"order state",
            "order_postal":"order postal","tax_collection_model":"tax collection model","product_sales":"product sales","product_sales_tax":"product sales tax",
            "shipping_credits":"shipping credits","shipping_credits_tax":"shipping credits tax","gift_wrap_credits":"gift wrap credits",
            "giftwrap_credits_tax":"giftwrap credits tax","promotional_rebates":"promotional rebates","promotional_rebates_tax":"promotional rebates tax",
            "selling_fees":"selling fees","fba_fees":"fba fees","other_transaction_fees":"other transaction fees","other":"other","total":"total"},
    "英国":{"order_time_msg":"date/time","settlement_id":"settlement id","type":"type","order_id":"order id","sku":"sku","description":"description","quantity":"quantity",
            "marketplace":"marketplace","fulfillment":"fulfilment","order_city":"order city","order_state":"order state",
            "order_postal":"order postal","tax_collection_model":"tax collection model","product_sales":"product sales","product_sales_tax":"product sales tax",
            "shipping_credits":"postage credits","shipping_credits_tax":"shipping credits tax","gift_wrap_credits":"gift wrap credits",
            "giftwrap_credits_tax":"giftwrap credits tax","promotional_rebates":"promotional rebates","promotional_rebates_tax":"promotional rebates tax",
            "marketplace_withheld_tax":"marketplace withheld tax","selling_fees":"selling fees","fba_fees":"fba fees","other_transaction_fees":"other transaction fees",
            "other":"other","total":"total"},
    "德国":{"order_time_msg":"Datum/Uhrzeit","settlement_id":"Abrechnungsnummer","type":"Typ","order_id":"Bestellnummer","sku":"SKU","description":"Beschreibung","quantity":"Menge",
            "marketplace":"Marketplace","fulfillment":"Versand","order_city":"Ort der Bestellung","order_state":"Bundesland","order_postal":"Postleitzahl",
            "tax_collection_model":"Steuererhebungsmodell","product_sales":"Ums?tze","product_sales_tax":"Produktumsatzsteuer",
            "shipping_credits":"Gutschrift für Versandkosten","shipping_credits_tax":"Steuer auf Versandgutschrift","gift_wrap_credits":"Gutschrift für Geschenkverpackung",
            "giftwrap_credits_tax":"Steuer auf Geschenkverpackungsgutschriften","promotional_rebates":"Rabatte aus Werbeaktionen","promotional_rebates_tax":"Steuer auf Aktionsrabatte",
            "marketplace_withheld_tax":"Einbehaltene Steuer auf Marketplace","selling_fees":"Verkaufsgebühren","fba_fees":"Gebühren zu Versand durch Amazon","other_transaction_fees":"Andere Transaktionsgebühren",
            "other":"Andere","total":"Gesamt"},
    "法国":{"order_time_msg":"date/heure","settlement_id":"numéro de versement","type":"type","order_id":"numéro de la commande","sku":"sku","description":"description","quantity":"quantité",
            "marketplace":"Marketplace","fulfillment":"traitement","order_city":"ville d'où provient la commande","order_state":"Région d'où provient la commande","order_postal":"code postal de la commande",
            "tax_collection_model":"Modèle de perception des taxes","product_sales":"ventes de produits","product_sales_tax":"Taxes sur la vente des produits",
            "shipping_credits":"crédits d'expédition","shipping_credits_tax":"taxe sur les crédits d’expédition","gift_wrap_credits":"crédits sur l'emballage cadeau",
            "giftwrap_credits_tax":"Taxes sur les crédits cadeaux","promotional_rebates":"Rabais promotionnels","promotional_rebates_tax":"Taxes sur les remises promotionnelles",
            "marketplace_withheld_tax":"Taxes retenues sur le site de vente","selling_fees":"frais de vente","fba_fees":"Frais Expédié par Amazon","other_transaction_fees":"autres frais de transaction",
            "other":"autre","total":"total"},
    "日本":{"order_time_msg":"日付/時間","settlement_id":"決済番号","type":"トランザクションの種類","order_id":"注文番号","sku":"SKU","description":"説明","quantity":"数量",
            "marketplace":"Amazon 出品サービス","account_type":"フルフィルメント","fulfillment":"市町村","order_city":"都道府県","order_state":"郵便番号","order_postal":"税金徴収型",
            "tax_collection_model":"商品売上","product_sales":"商品の売上税","product_sales_tax":"配送料",
            "shipping_credits":"配送料の税金","shipping_credits_tax":"ギフト包装手数料","gift_wrap_credits":"ギフト包装クレジットの税金",
            "giftwrap_credits_tax":"Amazonポイントの費用","promotional_rebates":"プロモーション割引額","promotional_rebates_tax":"プロモーション割引の税金",
            "marketplace_withheld_tax":"源泉徴収税を伴うマーケットプレイス","selling_fees":"手数料","fba_fees":"FBA 手数料","other_transaction_fees":"トランザクションに関するその他の手数料",
            "other":"その他","total":"合計"},
    "西班牙":{"order_time_msg":"fecha y hora","settlement_id":"identificador de pago","type":"tipo","order_id":"número de pedido","sku":"sku","description":"descripción","quantity":"cantidad",
            "marketplace":"web de Amazon","fulfillment":"gestión logística","order_city":"ciudad de procedencia del pedido","order_state":"comunidad autónoma de procedencia del pedido","order_postal":"código postal de procedencia del pedido",
            "tax_collection_model":"Formulario de recaudación de impuestos","product_sales":"ventas de productos","product_sales_tax":"product sales tax",
            "shipping_credits":"abonos de envío","shipping_credits_tax":"impuestos por abonos de envío","gift_wrap_credits":"abonos de envoltorio para regalo",
            "giftwrap_credits_tax":"giftwrap credits tax","promotional_rebates":"devoluciones promocionales","promotional_rebates_tax":"impuestos de descuentos por promociones",
            "marketplace_withheld_tax":"impuesto retenido en el sitio web","selling_fees":"tarifas de venta","fba_fees":"tarifas de Logística de Amazon","other_transaction_fees":"tarifas de otras transacciones",
            "other":"otro","total":"total"},
    "意大利":{"order_time_msg":"Data/Ora:","settlement_id":"Numero pagamento","type":"Tipo","order_id":"Numero ordine","sku":"SKU","description":"Descrizione","quantity":"Quantità",
            "marketplace":"Marketplace","fulfillment":"Gestione","order_city":"Città di provenienza dell'ordine","order_state":"Provincia di provenienza dell'ordine","order_postal":"CAP dell'ordine",
            "tax_collection_model":"modello di riscossione delle imposte","product_sales":"Vendite","product_sales_tax":"imposta sulle vendite dei prodotti",
            "shipping_credits":"Accrediti per le spedizioni","shipping_credits_tax":"imposta accrediti per le spedizioni","gift_wrap_credits":"Accrediti per confezioni regalo",
            "giftwrap_credits_tax":"imposta sui crediti confezione regalo","promotional_rebates":"Sconti promozionali","promotional_rebates_tax":"imposta sugli sconti promozionali",
            "marketplace_withheld_tax":"trattenuta IVA del marketplace","selling_fees":"Commissioni di vendita","fba_fees":"Costi del servizio Logistica di Amazon","other_transaction_fees":"Altri costi relativi alle transazioni",
            "other":"Altro","total":"totale"}
}

COUNTRY_COLUMNS={
    '美国':{"order_time_msg":"date/time","type":"type","sku":"sku","sales_qty":"quantity","sales_amount":["product sales","shipping credits","promotional rebates"]},
    '加拿大':{"order_time_msg":"date/time","type":"type","sku":"sku","sales_qty":"quantity","sales_amount":["product sales","shipping credits","promotional rebates"]},
    '墨西哥':{"order_time_msg":"fecha/hora","type":"tipo","sku":"sku","sales_qty":"cantidad","sales_amount":["ventas de productos","créditos de envío","descuentos promocionales"]},
    '英国':{"order_time_msg":"date/time","type":"type","sku":"sku","sales_qty":"quantity","sales_amount":["product sales","product sales tax","postage credits","shipping credits tax","promotional rebates","promotional rebates tax"]},
    '德国':{"order_time_msg":"Datum/Uhrzeit","type":"Typ","sku":"SKU","sales_qty":"Menge","sales_amount":["Umsätze","Produktumsatzsteuer","Gutschrift für Versandkosten","Steuer auf Versandgutschrift","Rabatte aus Werbeaktionen","Steuer auf Aktionsrabatte"]},#Ums?tze
    '法国':{"order_time_msg":"date/heure","type":"type","sku":"sku","sales_qty":"quantité","sales_amount":["ventes de produits","Taxes sur la vente des produits","crédits d'expédition","taxe sur les crédits d’expédition","Rabais promotionnels","Taxes sur les remises promotionnelles"]},
    '日本':{"order_time_msg":"日付/時間","type":"トランザクションの種類","sku":"SKU","sales_qty":"数量","sales_amount":["商品売上","商品の売上税","配送料","配送料の税金","Amazonポイントの費用","プロモーション割引額","プロモーション割引の税金"]},
    '西班牙':{"order_time_msg":"fecha y hora","type":"tipo","sku":"sku","sales_qty":"cantidad","sales_amount":["ventas de productos","impuesto de ventas de productos","impuesto de ventas de productos","abonos de envío","impuestos por abonos de envío","devoluciones promocionales","impuestos de descuentos por promociones"]},#impuesto de ventas de productos/product sales tax
    '意大利':{"order_time_msg":"Data/Ora:","type":"Tipo","sku":"SKU","sales_qty":"Quantità","sales_amount":["Vendite","imposta sulle vendite dei prodotti","Accrediti per le spedizioni","imposta accrediti per le spedizioni","Sconti promozionali","imposta sugli sconti promozionali"]}
}
COUNTRY_FEE_COLUMNS={
    '美国':{'type':'type','fba':'fba fees','sale_fee':'selling fees','commercial':'other transaction fees','other':'other','description':'description',
    'other_fee':['product sales tax','shipping credits','shipping credits tax','gift wrap credits','giftwrap credits tax','promotional rebates','promotional rebates tax','marketplace withheld tax']},
    '加拿大':{'type':'type','fba':'fba fees','sale_fee':'selling fees','commercial':'other transaction fees','other':'other','description':'description',
    'other_fee':['product sales tax','shipping credits','shipping credits tax','gift wrap credits','giftwrap credits tax','promotional rebates','promotional rebates tax','marketplace withheld tax']},
    '墨西哥':{'type':'tipo','fba':'fba_fees','sale_fee':'selling_fees','commercial':'other transaction fees','other':'otro','description':'descripción',
    'other_fee':['impuesto de ventas de productos','créditos de envío','impuesto de abono de envío','créditos por envoltorio de regalo','impuesto de créditos de envoltura','descuentos promocionales','impuesto de reembolsos promocionales','impuesto de retenciones en la plataforma']},
    '英国':{'type':'type','vat':'product sales tax','fba':'fba fees','sale_fee':'selling fees','commercial':'other transaction fees','other':'other','description':'description',
    'other_fee':['shipping credits','shipping credits tax','gift wrap credits','giftwrap credits tax','promotional rebates','promotional rebates tax','marketplace withheld tax']},
    '德国':{"type":"Typ",'vat':'Produktumsatzsteuer','fba':'Gebühren zu Versand durch Amazon','sale_fee':'Verkaufsgebühren','commercial':'Andere Transaktionsgebühren','other':'Andere','description':'Beschreibung',
    'other_fee':['Gutschrift für Versandkosten','Steuer auf Versandgutschrift','Gutschrift für Geschenkverpackung','Steuer auf Geschenkverpackungsgutschriften','Rabatte aus Werbeaktionen','Steuer auf Aktionsrabatte','Einbehaltene Steuer auf Marketplace']},
    '法国':{"type":"type",'vat':'Taxes sur la vente des produits','fba':'Frais Expédié par Amazon','sale_fee':'frais de vente','commercial':'autres frais de transaction','other':'autre','description':'description',
    'other_fee':["crédits d'expédition",'taxe sur les crédits d’expédition',"crédits sur l'emballage cadeau",'Taxes sur les crédits cadeaux','Rabais promotionnels','Taxes sur les remises promotionnelles','Taxes retenues sur le site de vente']},
    '日本':{"type":"トランザクションの種類",'vat':'商品の売上税','fba':'FBA 手数料','sale_fee':'手数料','commercial':'トランザクションに関するその他の手数料','other':'その他','description':'説明',
    'other_fee':['配送料','配送料の税金','ギフト包装手数料','ギフト包装クレジットの税金','Amazonポイントの費用','プロモーション割引額','プロモーション割引の税金','源泉徴収税を伴うマーケットプレイス']},
    '西班牙':{"type":"tipo",'vat':'product sales tax','fba':'tarifas de Logística de Amazon','sale_fee':'tarifas de venta','commercial':'tarifas de otras transacciones','other':'otro','description':'descripción',
    'other_fee':['abonos de envío','impuestos por abonos de envío','abonos de envoltorio para regalo','giftwrap credits tax','devoluciones promocionales','impuestos de descuentos por promociones','impuesto retenido en el sitio web']},
    '意大利':{"type":"Tipo",'vat':'imposta sulle vendite dei prodotti','fba':'Costi del servizio Logistica di Amazon','sale_fee':'Commissioni di vendita','commercial':'Costi del servizio Logistica di Amazon','other':'Altro','description':'Descrizione',
    'other_fee':['Accrediti per le spedizioni','imposta accrediti per le spedizioni','Accrediti per confezioni regalo','imposta sui crediti confezione regalo','Sconti promozionali','imposta sugli sconti promozionali','trattenuta IVA del marketplace']}
    }
# COUNTRY_COLUMNS={
#     '美国':{"type":"type","sku":"sku","sales_qty":"quantity","sales_amount":"product sales"},
#     '德国':{"type":"Typ","sku":"SKU","sales_qty":"Menge","sales_amount":["Ums?tze"]},
#     '加拿大':{"type":"type","sku":"sku","sales_qty":"quantity","sales_amount":"product sales"},
#     '法国':{"type":"type","sku":"sku","sales_qty":"quantité","sales_amount":["ventes de produits"]},
#     '日本':{"type":"トランザクションの種類","sku":"SKU","sales_qty":"数量","sales_amount":["商品売上"]},
#     '西班牙':{"type":"tipo","sku":"sku","sales_qty":"cantidad","sales_amount":["ventas de productos"]},
#     '意大利':{"type":"Tipo","sku":"SKU","sales_qty":"Quantità","sales_amount":["Vendite"]},
#     '英国':{"type":"type","sku":"sku","sales_qty":"quantity","sales_amount":["product sales"]}
# }
COUNTRY_TYPES={
    '美国':'Order',
    '加拿大':'Order',
    '墨西哥':'Pedido',
    '英国':'Order',
    '德国':'Bestellung',
    '法国':'Commande',
    '意大利':'Ordine',
    '西班牙':'Pedido',
    '日本':'注文'
}
COUNTRY_SALES_RETURN_TYPES={
    '美国':'refund',
    '加拿大':'refund',
    '墨西哥':'Reembolso',
    '英国':'refund',
    '德国':'Erstattung',
    '法国':'Remboursement',
    '意大利':'Rimborso',
    '西班牙':'Reembolso',
    '日本':'返金'
}
STORE_LIST={
    'A4pet宠物用品店':'淘宝',
    'A4Pet旗舰店':'天猫',
    'petsfit宠物生活官方旗舰店':'京东',
    'petsfit旗舰店':'天猫',
    'petsfit旗舰店供应商':'供销平台',
    '贝芬菲特宠物用品店':'淘宝',
    '唯宠线下':'唯宠线下'
}
export_sql="select n.ym,n.category_I,n.category_II,n.category_III,n.area,n.country,n.platform,n.channel_no,n.sales_amount,n.sales_qty,b.sales_amount before_sales_amount,b.sales_qty before_sales_qty,n.target_amount,n.target_qty from (select f.sales_amount,f.sales_qty,t.* from (SELECT sf.ym,SUM(sf.sales_amount) sales_amount,SUM(sf.sales_qty) sales_qty,sf.channel_no,c.platform,c.area,c.country,cp.category_I,cp.category_II,cp.category_III FROM sales_fact sf,products p,channels c,categories_p cp WHERE sf.ym>='2021-01-01' and sf.ym<='2021-12-01' and sf.item_no = p.item_no AND sf.channel_no = c.channel_no AND p.category_III = cp.category_III GROUP BY sf.channel_no,cp.category_III,sf.ym) f right join (select st.*,cp.category_II,cp.category_I,c.platform,c.area,c.country from sales_target st,categories_p cp,channels c where cp.category_III=st.category_III and c.channel_no=st.channel_no) t on f.category_III=t.category_III and f.channel_no=t.channel_no and f.ym=t.ym) n LEFT join (select sf.ym,sf.channel_no,sf.item_no,sf.sales_amount,sf.sales_qty,p.category_III from sales_fact sf,products p where sf.item_no=p.item_no GROUP BY sf.channel_no,p.category_III,sf.ym) b on b.ym=DATE_ADD(n.ym,INTERVAL -1 YEAR) and n.channel_no=b.channel_no and n.category_III=b.category_III"

COUNTRYS=['美国','加拿大','英国','德国','法国','西班牙','意大利','日本','中国','韩国']
APPORTION_TYPE=[('销售','佣金'),('销售','仓储'),('销售','广告'),('销售','快递'),('销售','税费'),('销售','运费'),('销售','其他'),
                ('管理','其他'),
                ('财务','其他')]
APPORTION_TYPE_NAME={('销售','佣金'):'sale_fee',('销售','运费'):'ocean_freight',('销售','税费'):'tariff',('销售','广告'):'advertise_fee',('销售','仓储'):'storage_fee',('销售','快递'):'express_fee',('销售','其他'):'sale_other_fee',
                     ('管理','其他'):'manage_other_fee',
                     ('财务','其他'):'financial_other_fee'}
#渠道
CHANNELS=["401","402","403","491","492","493","500","600","AAJ","ANA","ANC","AUD","AUF","AUI","AUS","AUU","BUD","BUF","BUI","BUS","BUU","CNA","DUF","ENA","EUD","EUU","FNA","J00","L99","NUD","NUF","NUI","NUS","NUU","O00","PNA","T00","VNA","WNA"]
MATERIAL_MAPPING={
    'W':'Wooden',
    'C':'Cloth',
    'M':'Metal',
    'P':'Plastic',
    'A':'Association',
    'B':'Bamboo'
}
COUNTRY_MAPPING={
    "CA":"加拿大",
    "US":"美国",
    "MX":"墨西哥",
    "UK":"英国",
    "DE":"德国",
    "FR":"法国",
    "IT":"意大利",
    "PL":"波兰",
    "ES":"西班牙",
    "CZ":"捷克",
    "SK":"斯洛伐克",
    "JP":"日本"
}