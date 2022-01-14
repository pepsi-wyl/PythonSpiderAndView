# -*- coding: utf-8 -*-
# @Time: 2022/1/12 14:26
# @Author: pepsi-wyl
# @File: spider.py
# @Software: PyCharm

# 爬取疫情数据
import re
import sqlite3
import threading
import requests
import xlwt as xlwt
from bs4 import BeautifulSoup  # 网页解析,获取数据

# 初始化数据库
def initDatabase(dbPathYingQing, dbPathDouBan):
    sql = "drop table if exists province"
    dbUtil(sql, dbPathYingQing)
    sql = "create table province(id integer primary key autoincrement, year numeric,date varchar,country varchar,province varchar, confirm numeric,dead numeric,heal numeric,newConfirm numeric,newHeal numeric,newDead numeric)"
    dbUtil(sql, dbPathYingQing)
    sql = "drop table if exists provinceNow"
    dbUtil(sql, dbPathYingQing)
    sql = "create table provinceNow(id integer primary key autoincrement, year numeric,date varchar,country varchar,province varchar, confirm numeric,dead numeric,heal numeric,newConfirm numeric,newHeal numeric,newDead numeric)"
    dbUtil(sql, dbPathYingQing)
    sql = "drop table if exists address"
    dbUtil(sql, dbPathYingQing)
    sql = "create table address(id integer primary key autoincrement, area varchar ,type numeric )"
    dbUtil(sql, dbPathYingQing)
    sql = "drop table if exists province31"
    dbUtil(sql, dbPathYingQing)
    sql = "create table province31(id integer primary key autoincrement, province varchar ,city varchar ,syear numeric ,date varchar ,confirm numeric ,nowConfirm numeric ,confirmAdd numeric ,dead numeric ,grade varchar )"
    dbUtil(sql, dbPathYingQing)
    sql = "drop table if exists movieTop250"
    dbUtil(sql, dbPathDouBan)
    sql = "create table movieTop250(id integer primary key autoincrement, title1 varchar , title2 varchar , otherTitle varchar , imgLink text, infoLink text, score numeric , rated numeric , instroduction text, info text )"
    dbUtil(sql, dbPathDouBan)

# 模拟浏览器去访问指定URL
def askUrl(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36",
            "Referer": url,
            "Connection": "keep-alive"
        }
        return requests.get(url=url, headers=headers, timeout=3)
    except Exception as result:
        print("异常为:", result)

#  处理数据得到dataListProvince
def getDataProvince(urlProvince, province, savePath, dbPath):
    print("......正在爬取全国各个省份疫情的历史数据......")
    dataList = []
    for item in province:
        item = (str(item.encode('utf-8')).lstrip("b'").rstrip("'").replace(r'\x', '%').upper())
        data = askUrl(urlProvince + item).json()['data']
        dataList.extend(data)
    t1 = threading.Thread(target=saveDateToFileProvince, args=(dataList, savePath))
    t2 = threading.Thread(target=saveDateToDatabaseProvince, args=(dataList, dbPath))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


# 保存数据到文件Province
def saveDateToFileProvince(dataList, savePath):
    print("......正在保存全国各个省份疫情的历史数据到文件......")
    workbook = xlwt.Workbook(encoding="utf-8", style_compression=0)
    worksheet = workbook.add_sheet(savePath, cell_overwrite_ok=True)
    column = ['年', '日期', '国家', '省份', '累计确诊', ' 累计死亡', '累计治愈', '新确诊', '新确诊', '新死亡']
    for i in range(len(column)):
        worksheet.write(0, i, column[i])
    i = 1
    for item in dataList:
        param = [item['year'], item['date'], item['country'], item['province'], item['confirm'], item['dead'],
                 item['heal'], item['newConfirm'], item['newHeal'], item['newDead']]
        for j in range(len(param)):
            worksheet.write(i, j, param[j])
        i = i + 1
    workbook.save(savePath)  # 保存数据表


# 保存数据到数据库Province
def saveDateToDatabaseProvince(dataList, dbPath):
    print("......正在保存全国各个省份疫情的历史数据到数据库......")
    for item in dataList:
        #             年            日期            国家              省份             累计确诊         累计死亡      累计治愈          新确诊             新确诊            新死亡
        param = [item['year'], item['date'], item['country'], item['province'], item['confirm'], item['dead'],
                 item['heal'], item['newConfirm'], item['newHeal'], item['newDead']]
        for index in range(len(param)):
            param[index] = '"' + str(param[index]) + '"'
        sql = "insert into province(year,date,country,province,confirm,dead,heal,newConfirm,newHeal,newDead) values (%s)" % ",".join(
            param)
        print(sql)
        dbUtil(sql, dbPath)


#  处理数据得到dataListProvinceNow
def getDataProvinceNow(province, savePath, dbPath):
    print("......正在查询全国各个省份疫情的最新数据......")
    dataList = []
    # 解决省份更新时间不同问题
    for item in province:
        item = '"' + item + '"'
        sql = "select * from province where province like %s order by year DESC ,date DESC ,province limit 1" % item
        print(sql)
        conn = sqlite3.connect(dbPath)
        result = conn.cursor().execute(sql)
        for data in result:
            dataList.append(data)
        conn.close()
    t1 = threading.Thread(target=saveDateToFileProvinceNow, args=(dataList, savePath))
    t2 = threading.Thread(target=saveDateToDatabaseProvinceNow, args=(dataList, dbPath))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


# 保存数据到文件ProvinceNow
def saveDateToFileProvinceNow(dataList, savePath):
    print("......正在保存全国各个省份疫情的历史数据到文件......")
    workbook = xlwt.Workbook(encoding="utf-8", style_compression=0)
    worksheet = workbook.add_sheet(savePath, cell_overwrite_ok=True)
    column = ['年', '日期', '国家', '省份', '累计确诊', ' 累计死亡', '累计治愈', '新确诊', '新确诊', '新死亡']
    for i in range(len(column)):
        worksheet.write(0, i, column[i])
    for i in range(len(dataList)):
        data = dataList[i]
        for j in range(len(data) - 1):
            worksheet.write(i + 1, j, data[j + 1])
    workbook.save(savePath)  # 保存数据表


# 保存数据到数据库ProvinceNow
def saveDateToDatabaseProvinceNow(dataList, dbPath):
    print("......正在保存全国各个省份疫情的历史数据到数据库......")
    for item in dataList:
        param = [item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10]]
        for index in range(len(param)):
            param[index] = '"' + str(param[index]) + '"'
        sql = "insert into provinceNow(year,date,country,province,confirm,dead,heal,newConfirm,newHeal,newDead) values(%s)" % ",".join(
            param)
        print(sql)
        dbUtil(sql, dbPath)


# 处理数据得到dataListAddress
def getDataAddress(urlAddress, savePath, dbPath):
    print("......正在爬取全国各个省份的风险地区数据......")
    dataList = askUrl(urlAddress).json()['data']
    t1 = threading.Thread(target=saveDateToFileAddress, args=(dataList, savePath))
    t2 = threading.Thread(target=saveDateToDatabaseAddress, args=(dataList, dbPath))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


# 保存数据到文件Address
def saveDateToFileAddress(dataList, savePath):
    print("......正在保存全国各个省份的风险地区数据到文件......")
    workbook = xlwt.Workbook(encoding="utf-8", style_compression=0)
    worksheet = workbook.add_sheet(savePath, cell_overwrite_ok=True)
    column = ['地区', '风险类型']
    for i in range(len(column)):
        worksheet.write(0, i, column[i])
    i = 1
    for item in dataList:
        param = [item['area'], "中风险地区" if ['type'] == '1' else "高风险地区"]
        for j in range(len(param)):
            worksheet.write(i, j, param[j])
        i = i + 1
    workbook.save(savePath)  # 保存数据表


# 保存数据到数据库Address
def saveDateToDatabaseAddress(dataList, dbPath):
    print("......正在保存全国各个省份的风险地区数据到数据库......")
    for item in dataList:
        param = [item['area'], "中风险地区" if item['type'] == '1' else "高风险地区"]
        for index in range(len(param)):
            param[index] = '"' + str(param[index]) + '"'
        sql = "insert into address (area,type) values(%s)" % ",".join(param)
        print(sql)
        dbUtil(sql, dbPath)


# 处理数据得到dataList31Province
def getData31Province(url31Province, savePath, dbPath):
    print("......正在爬取近期31省区市本土病例数据......")
    dataList = askUrl(url31Province).json()['data']['statisGradeCityDetail']
    t1 = threading.Thread(target=saveDateToFile31Province, args=(dataList, savePath))
    t2 = threading.Thread(target=saveDateToDatabase31Province, args=(dataList, dbPath))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


# 保存数据到文件31Province
def saveDateToFile31Province(dataList, savePath):
    print("......正在保存近期31省区市本土病例数据到文件......")
    workbook = xlwt.Workbook(encoding="utf-8", style_compression=0)
    worksheet = workbook.add_sheet(savePath, cell_overwrite_ok=True)
    column = ['省份', '城市', '年', '日期', '累计确诊', ' 现确诊', '确诊增加', '死亡', '风险等级']
    for i in range(len(column)):
        worksheet.write(0, i, column[i])
    i = 1
    for item in dataList:
        param = [item['province'], item['city'], item['syear'], item['date'], item['confirm'], item['nowConfirm'],
                 item['confirmAdd'], item['dead'], item['grade']]
        for j in range(len(param)):
            worksheet.write(i, j, param[j])
        i = i + 1
    workbook.save(savePath)  # 保存数据表


# 保存数据到数据库31Province
def saveDateToDatabase31Province(dataList, dbPath):
    print("......正在保存近期31省区市本土病例数据到数据库......")
    for item in dataList:
        #               省份              城市            年           日期           累计确诊           现确诊               确诊增加           死亡        风险等级
        param = [item['province'], item['city'], item['syear'], item['date'], item['confirm'], item['nowConfirm'],
                 item['confirmAdd'], item['dead'], item['grade']]
        for index in range(len(param)):
            param[index] = '"' + str(param[index]) + '"'
        sql = "insert into province31 (province,city,syear,date,confirm,nowConfirm,confirmAdd,dead,grade) values ( %s )" % ",".join(param)
        print(sql)
        dbUtil(sql, dbPath)


# 爬取网页和数据解析DouBanTop
def getDateDouBanTop(url, savePath, dbPath):
    dataList = []
    for i in range(0, 10):
        # 循环爬取数据   0-10
        print("正在爬取豆瓣电影Top250的数据......")
        html = askUrl(url + str(i * 25)).text
        # 数据解析 HTML
        bs = BeautifulSoup(html, "html.parser")
        for item in bs.find_all("div", class_="item"):  # 获取class为item的div
            data = []  # 存储一部电影的信息
            # 电影名称
            title = re.findall(
                re.compile(r'<span class="title">(.*)</span>'),
                str(item)
            )
            otherTitle = re.findall(
                re.compile(r'<span class="other">(.*)</span>'),
                str(item)
            )
            data.append(title[0])
            if len(title) == 2:
                data.append(title[1].replace("/", " ").replace("\xa0", ""))
            else:
                data.append(' ')
            data.append(otherTitle[0].replace("/", " ").replace("\xa0", ""))
            # 照片链接
            img = re.findall(
                re.compile(re.compile(r'<img.*src="(.*?)"', re.S)),  # re.s 让换行符号包含在字符串中
                str(item)
            )[0]
            data.append(img)
            # 电影详情链接
            link = re.findall(
                re.compile(r'<a href="(.*?)">'),
                str(item)
            )[0]
            data.append(link)
            # 评价分数
            ratingNumber = re.findall(
                re.compile(r'<span class="rating_num" property="v:average">(.*)</span>'),
                str(item)
            )[0]
            data.append(ratingNumber)
            # 评价人数
            JudgeNumber = re.findall(
                re.compile(r'<span>(\d*)人评价</span>'),
                str(item)
            )[0]
            data.append(JudgeNumber)
            # 电影评价
            inq = re.findall(
                re.compile(r'<span class="inq">(.*)</span>'),
                str(item)
            )
            if len(inq) != 0:
                data.append(inq[0].replace("。", ""))
            else:
                data.append(' ')
            # 相关内容
            related = re.findall(
                re.compile(r'<p class="">(.*?)</p>', re.S),
                str(item)
            )
            data.append(related[0].replace("<br/>", "    ").replace("/", " ").replace("\xa0", " ").
                        replace("\n                            ", "").replace("\n                        ", "")
                        )
            dataList.append(data)  # 数据集合
    t1 = threading.Thread(target=saveDateToFileDouBanTop, args=(dataList, savePath))
    t2 = threading.Thread(target=saveDataToDatabaseDouBanTop, args=(dataList, dbPath))
    t1.start()
    t2.start()
    t1.join()
    t2.join()


# 保存数据到文件
def saveDateToFileDouBanTop(dataList, savePath):
    workbook = xlwt.Workbook(encoding="utf-8", style_compression=0)  # 创建workbook对象
    worksheet = workbook.add_sheet("doubanTop250", cell_overwrite_ok=True)  # 创建工作表
    column = ['影片中文名', '影片外文名', '影片别名', '图片链接', '电影详情链接', '评分', '评价数', '概况', '相关信息']
    for i in range(len(column)):
        worksheet.write(0, i, column[i])
    for i in range(len(dataList)):
        data = dataList[i]
        for j in range(len(data)):
            worksheet.write(i + 1, j, data[j])
    workbook.save(savePath)  # 保存数据表


# 保存数据到数据库
def saveDataToDatabaseDouBanTop(dataList, dbPath):
    for data in dataList:
        for index in range(len(data)):
            data[index] = '"' + data[index] + '"'
        sql = "insert into movieTop250 (title1,title2,otherTitle,imgLink,infoLink,score,rated,instroduction,info)values (%s)" % ",".join(
            data)
        dbUtil(sql, dbPath)


def dbUtil(sql, dbPath):
    conn = sqlite3.connect(dbPath)  # 打开或者创建数据库文件
    conn.cursor().execute(sql)  # 获取游标  执行sql语句
    conn.commit()  # 提交数据库操作
    conn.close()  # 关闭数据库连接


# 主函数
def main():
    dbPathYiQing = "chinaYiQing.db"
    dbPathDouBan = "doubanMovie.db"
    savePathDouBan = "doubanTop250.xls"
    savaPathProvince = "province.xls"
    savaPathAddress = "address.xls"
    savaPath31Province = "province31.xls"
    savaPathProvinceNow = "provinceNow.xls"
    province = ['河南', '河北', '山西', '辽宁', '吉林', '黑龙江', '江苏',
                '浙江', '安徽', '福建', '江西', '山东', '湖北', '湖南',
                '广东', '海南', '四川', '贵州', '云南', '陕西', '甘肃',
                '青海', '台湾', '内蒙古', '广西', '西藏', '宁夏', '新疆',
                '北京', '天津', '上海', '重庆', '香港', '澳门']
    urlDouBan = "https://movie.douban.com/top250?start="
    urlProvince = "https://api.inews.qq.com/newsqa/v1/query/pubished/daily/list?province="
    urlAddress = "https://eyesight.news.qq.com/sars/riskarea"
    url31Province = "https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=statisGradeCityDetail"

    threading.Thread(target=initDatabase, args=(dbPathYiQing, dbPathDouBan)).start()  # 初始化数据库
    thread1 = threading.Thread(target=getDataProvince, args=(urlProvince, province, savaPathProvince, dbPathYiQing))
    thread2 = threading.Thread(target=getData31Province, args=(url31Province, savaPath31Province, dbPathYiQing))
    thread3 = threading.Thread(target=getDataAddress, args=(urlAddress, savaPathAddress, dbPathYiQing))
    thread4 = threading.Thread(target=getDateDouBanTop, args=(urlDouBan, savePathDouBan, dbPathDouBan))
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5 = threading.Thread(target=getDataProvinceNow, args=(province, savaPathProvinceNow, dbPathYiQing))
    thread5.start()
    thread5.join()

# 调用主函数
if __name__ == "__main__":
    main()
