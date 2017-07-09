from bs4 import BeautifulSoup
import requests,pymysql
##获取城市页数
basepath = ''
##域名
domain = 'http://tudi.leju.com'
def getRange():
    req = requests.get(basepath)
    html = req.text
    #获取并解析html
    bsObj = BeautifulSoup(html, "lxml")
    #找到...
    breakPoint = bsObj.find("span",{"class":"omit"});
    maxPageNum = int(breakPoint.next_sibling.string) + 1
    return maxPageNum

##获取每页土地信息链接
def getLandLink(maxPageNum):
    LandlinkList = []
    for i in range(1,maxPageNum+1):
        path = basepath + "/?page=" + str(i)
        req = requests.get(path)
        html = req.text
        # 获取并解析html
        bsObj = BeautifulSoup(html, "html5lib")
        aTags = bsObj.find_all("a",{"class":"l_tit js_title"})
        for aTag in aTags:
            LandlinkList.append(domain+aTag["href"])
            print(aTag["href"])
    return LandlinkList

##获取每项土地信息
def getInfo(path):
    req = requests.get(path)
    html = req.text
    ##解析器换成html5lib
    bsObj = BeautifulSoup(html,"html5lib")
    #所在位置
    h2Tags = bsObj.find_all("h2")
    ##为h2标签添加属性
    for h2Tag in h2Tags:
        if h2Tag.string == "所在区域：":
            h2Tag['id'] = "location"
        elif h2Tag.string == "土地地址：":
            h2Tag['id'] = "address"
        elif h2Tag.string == "总 用 地：":
            h2Tag['id'] = "total_square"
        elif h2Tag.string == "规划（预计）总建面积：":
            h2Tag['id'] = "building_square"
        elif h2Tag.string == "土地属性：":
            h2Tag['id'] = "land_property"
        elif h2Tag.string == "成交价格：":
            h2Tag['id'] = "closing_price"
        elif h2Tag.string == "成交日期：":
            h2Tag['id'] = "closing_time"
        elif h2Tag.string == "公告日期：":
            h2Tag['id'] = "announce_time"
        elif h2Tag.string == "楼板价（楼面价）：":
            h2Tag['id'] = "floor_price"
        elif h2Tag.string == "每亩地价（元/亩）：":
            h2Tag['id'] = "price_per_mu"
    ##打包数据项
    location = bsObj.find("h2",{"id":"location"}).next_sibling.next_sibling.string.strip()
    address = bsObj.find("h2",{"id":"address"}).next_sibling.next_sibling.string.strip()
    total_square = bsObj.find("h2", {"id": "total_square"}).next_sibling.next_sibling.string.strip()
    building_square = bsObj.find("h2", {"id": "building_square"}).next_sibling.next_sibling.string.strip()
    land_property = bsObj.find("h2", {"id": "land_property"}).next_sibling.next_sibling.string.strip()
    closing_price = bsObj.find("h2", {"id": "closing_price"}).next_sibling.next_sibling.string.strip()
    closing_time = bsObj.find("h2", {"id": "closing_time"}).next_sibling.next_sibling.string.strip()
    announce_time = bsObj.find("h2", {"id": "announce_time"}).next_sibling.next_sibling.string.strip()
    floor_price = bsObj.find("h2", {"id": "floor_price"}).next_sibling.next_sibling.string.strip()
    price_per_mu = bsObj.find("h2", {"id": "price_per_mu"}).next_sibling.next_sibling.string.strip()
    data = {"location":location,"address":address,"total_square":total_square,"building_square":building_square,
            "land_property":land_property,"closing_price":closing_price,"closing_time":closing_time,
            "announce_time":announce_time,"floor_price":floor_price,"price_per_mu":price_per_mu}
    return data

##生成插入语句,十条数据执行一次插入操作，data类型：list(dict())
def generate_sql(datas):
    sql = ''
    #字段值
    field_str = ''
    #数据值
    value_str = ''
    #初始化field_value
    field_str += '('
    for field in datas[0].keys():
        field_str += field+','
    field_value = field_str[:-1]
    field_value += ')'
    #初始化value_str
    for data in datas:
        value_str += '('
        for value in data.values():
            value_str += "'" + value + "'" + ','
        value_str = value_str[:-1]
        value_str += '),'
    value_str = value_str[:-1]
    sql = 'INSERT INTO sh_land_info' + field_value + ' VALUES' + value_str
    print(sql)
    return sql

if __name__ == "__main__":
    locationCode = input("请输入LocationCode：")
    basepath = 'http://tudi.leju.com/zpg/' + locationCode
    counter = 0
    sql = ''
    data = []
    ###########################################
    # 打开数据库连接
    db = pymysql.connect("localhost", "root", "", "land",charset = 'utf8')
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    cursor.execute("SELECT MAX(sh_land_info.id) AS maxid FROM sh_land_info")
    results = cursor.fetchall()
    for row in results:
        if row[0] == None:
            last_id = 0
        else:
            last_id = row[0]
    ###########################################
    #for link in getLandLink(getRange()):
    land_link_list = getLandLink(getRange())
    for i in range(last_id,len(land_link_list)):
        counter = counter + 1
        data.append(getInfo(land_link_list[i]))
        if counter % 10 == 0:
            sql = generate_sql(data)
            sta = cursor.execute(sql)
            db.commit()
            #日志
            print("###################已添加第"+str(counter//10)+"批数据##################")
            data.clear()
            sql=''
    db.close()
    print("###################   采 集 完 成  ###################")
    #print(requests.get("http://tudi.leju.com/1018220.html#wt_source=pc_land_dlist_title").text)
    '''
    generate_sql([{'location': '南京六合', 'address': '六合区程桥街道桂花河以南、东大河以东地块', 'total_square': '14378.3㎡',
                   'building_square': '27173.12㎡', 'land_property': '纯住宅', 'closing_price': '10600',
                   'closing_time': '2016-12-16', 'announce_time': '2016-11-12', 'floor_price': '3900元/㎡',
                   'price_per_mu': '4914838元/亩'},{'location': '南京六合', 'address': '六合区程桥街道桂花河以南、东大河以东地块', 'total_square': '14378.3㎡',
                   'building_square': '27173.12㎡', 'land_property': '纯住宅', 'closing_price': '10600',
                   'closing_time': '2016-12-16', 'announce_time': '2016-11-12', 'floor_price': '3900元/㎡',
                   'price_per_mu': '4914838元/亩'}])
    '''