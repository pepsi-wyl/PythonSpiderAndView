
from flask import Flask, render_template, redirect, request
import jieba  # 分词
import matplotlib.pyplot as plt
from matplotlib import pyplot  # 绘图
from wordcloud import WordCloud  # 词云
from PIL import Image  # 图片处理
import numpy    # 矩阵运算
import sqlite3  # sqlite数据库
import spider

app = Flask(__name__)

# 首页
@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

# 电影
@app.route('/movie')
def movie():
    dataList = []
    conn = sqlite3.connect("doubanMovie.db")
    result = conn.cursor().execute("select * from movieTop250")
    for item in result:
        dataList.append(item)
    conn.close()
    return render_template("movie.html", dataList=dataList)

# 评分
@app.route('/score')
def score():
    date = []; dataName = []; dataValue= []
    conn = sqlite3.connect("doubanMovie.db")
    result = conn.cursor().execute("select score,count(score) from movieTop250 group by score")
    for name, value in result:
        dataDict = {'name': name, 'value': value}
        dataName.append(name); dataValue.append(value); date.append(dataDict)
    conn.close()
    return render_template("score.html",
                           date=date,
                           dataName=dataName,
                           dataValue=dataValue
                           )

# 词云
@app.route('/word')
def word():
    str = ""
    conn = sqlite3.connect("doubanMovie.db")
    sql = "select instroduction from movieTop250"
    result = conn.cursor().execute(sql)
    for item in result:
        str = str + item[0]
    conn.close()
    cut = jieba.cut(str)
    str = ' '.join(cut)
    print(len(str))
    wc = WordCloud(
        background_color='white',
        mask=numpy.array(Image.open("static/assets/img/tree.jpg")),
        font_path="msyh.ttc"
    ).generate_from_text(str)
    pyplot.figure(1)
    plt.imshow(wc)
    plt.axis('off')
    plt.savefig("static/assets/img/treeWord.jpg")
    return render_template("word.html")

# province31
@app.route('/province31')
def province31():
    cityList=[]; conformList=[]; nowConfirmList=[]; confirmAddList=[]; deadList=[]
    conn = sqlite3.connect("chinaYiQing.db")
    result = conn.cursor().execute("select * from province31 order by confirmAdd DESC limit 19")
    for item in result:
        cityList.append(item[2])
        conformList.append(item[5])
        nowConfirmList.append(item[6])
        confirmAddList.append(item[7])
        deadList.append(item[8])
    conn.close()
    return render_template("province31.html",
                           cityList=cityList,
                           conformList=conformList,
                           nowConfirmList=nowConfirmList,
                           confirmAddList=confirmAddList,
                           deadList=deadList
                           )

# address
@app.route('/address')
def address():
    areaListMiddle = []; areaListHigh = []
    conn = sqlite3.connect("chinaYiQing.db")
    resultMiddle = conn.cursor().execute("select * from address where type like '中风险地区' order by area ")
    resultHigh = conn.cursor().execute("select * from address where type like '高风险地区' order by area ")
    for item in resultMiddle:
        areaListMiddle.append(item[1])
    for item in resultHigh:
        areaListHigh.append(item[1])
    conn.close()
    return render_template("address.html",
                           areaListMiddle=areaListMiddle,
                           areaListHigh=areaListHigh
                           )

# provinceNow
@app.route('/provinceNow')
def provinceNow():
    cityList = []; confirmList = []; deadList = []; healList = []; newConfirmList = []; newDeadList = []; newHealList = []; dataList=[]
    conn = sqlite3.connect("chinaYiQing.db")
    result = conn.cursor().execute("select * from provinceNow  order by confirm")
    for item in result:
        cityList.append(item[4])
        confirmList.append(item[5])
        deadList.append(item[6])
        healList.append(item[7])
        newConfirmList.append(-item[8])
        newDeadList.append(-item[10])
        newHealList.append(-item[9])
        data = {"name": item[4], "value": item[5]}
        dataList.append(data)
    conn.close()
    return render_template("provinceNow.html",
                           cityList=cityList,
                           confirmList=confirmList,
                           deadList=deadList,
                           healList=healList,
                           newConfirmList=newConfirmList,
                           newDeadList=newDeadList,
                           newHealList=newHealList,
                           dataList=dataList
                           )

# provinceHistory
@app.route('/provinceHistory')
def provinceHistory():
    conn = sqlite3.connect("chinaYiQing.db")
    timeList = []; confirmList = []; deadList = []; healList = []; newConfirmList = []; newDeadList = []; newHealList = []
    result = conn.cursor().execute("select * from province  where province like '河南' order by year ,date")
    for item in result:
        timeList.append(str(item[1]) + "-" + item[2])
        confirmList.append(item[5])
        deadList.append(item[6])
        healList.append(item[7])
        newConfirmList.append(item[8])
        newDeadList.append(item[10])
        newHealList.append(item[9])
    conn.close()
    dataList = []
    conn = sqlite3.connect("chinaYiQing.db")
    provinces = ['河南', '河北', '山西', '辽宁', '吉林', '黑龙江', '江苏',
                 '浙江', '安徽', '福建', '江西', '山东', '湖北', '湖南',
                 '广东', '海南', '四川', '贵州', '云南', '陕西', '甘肃',
                 '青海', '台湾', '内蒙古', '广西', '西藏', '宁夏', '新疆',
                 '北京', '天津', '上海', '重庆', '香港', '澳门']
    for province in provinces:
        result = conn.cursor().execute(
            "select * from province  where province like %s order by year ,date " % ("'" + province + "'"))
        oneProvinceData = []
        for item in result:
            oneProvinceData.append(item[8])
        dataList.append(oneProvinceData)
    conn.close()
    return render_template('provinceHistory.html',
                           timeList=timeList,
                           confirmList=confirmList,
                           deadList=deadList,
                           healList=healList,
                           newConfirmList=newConfirmList,
                           newDeadList=newDeadList,
                           newHealList=newHealList,
                           henan=dataList[0],
                           hebie=dataList[1],
                           shanxi=dataList[2],
                           liaoning=dataList[3],
                           jilin=dataList[4],
                           helongjiang=dataList[5],
                           jiangsu=dataList[6],
                           zhejiang=dataList[7],
                           anhui=dataList[8],
                           fujian=dataList[9],
                           jiangxi=dataList[10],
                           shandong=dataList[11],
                           hubei=dataList[12],
                           hunan=dataList[13],
                           guandong=dataList[14],
                           hainan=dataList[15],
                           sichuan=dataList[16],
                           guizhou=dataList[17],
                           yunnan=dataList[18],
                           shaixi=dataList[19],
                           gansu=dataList[20],
                           qinghai=dataList[21],
                           taiwan=dataList[22],
                           neinonggu=dataList[23],
                           guangxi=dataList[24],
                           xizhang=dataList[25],
                           ningxia=dataList[26],
                           xinjiang=dataList[27],
                           biejing=dataList[28],
                           tianjing=dataList[29],
                           shanghai=dataList[30],
                           chongqing=dataList[31],
                           xianggang=dataList[32],
                           aomen=dataList[33]
                           )

@app.route('/admin')
def admin():
    return render_template('login.html')

@app.route('/toLogin', methods=['POST', 'GET'])
def toLogin():
    result = request.form
    if result['username']=='pepsi-wyl' and result['password']=='888888':
        spider.main()
    return redirect('/index', code=302, Response=None)

if __name__ == '__main__':
    app.run()
