import re
import threading
from queue import Queue

import pymysql
import requests
from lxml import etree


def get_one_page(url):
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        text = response.content.decode('utf-8')
        return text
    return None


# 获取姓氏
def parse_xpath(html):
    # 连接数据库
    db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, database='a_hundred_family_name',
                         charset='utf8')
    # print(db)
    cursor = db.cursor()
    family_name = etree.HTML(html).xpath('.//div[@class="col-xs-12"]/a')
    for name in family_name:
        i = {}
        i['first_names'] = name.xpath('text()')
        i['href'] = name.xpath('@href')
        i['title'] = name.xpath('@title')
        first_names = i['first_names'][0]
        first_name = first_names.replace('姓名字大全', '')
        # print(first_name)
        href1 = i['href'][0]
        href = 'http:' + href1
        title = i['title'][0]
        sql = "insert into first_name_all(first_name,first_names,href,title) values('%s','%s','%s','%s')" \
              % (first_name, first_names, href, title)
        # print(sql)
        cursor.execute(sql)
        db.commit()
    return None


# 创建姓氏表
# create table first_name_all(
#    -> id int auto_increment primary key,
#    -> first_name varchar(10) not null,
#    -> first_names varchar(50) not null,
#    -> href varchar(100) not null,
#    -> title varchar(120) not null);

def get_names(html1):
    # 连接数据库
    db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, database='a_hundred_family_name',
                         charset='utf8')
    # print(db)
    cursor = db.cursor()
    first_name1 = etree.HTML(html1).xpath('//*[@id="head_"]/div/div/div[1]/a/div[1]/text()')
    first_name2 = first_name1[0]
    first_name = first_name2[:-3]
    # first_name = first_name2.replace('姓之家', '')
    names = etree.HTML(html1).xpath('//div[@class="col-xs-12"]/a/text()')
    for name in names:
        sql = "insert into names(first_name,name) values('%s','%s')" \
              % (first_name, name)
        print('===', sql)
        cursor.execute(sql)
        db.commit()


def get_name_info(name_url):
    # 连接数据库
    db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, database='a_hundred_family_name',
                         charset='utf8')
    cursor = db.cursor()
    html2=get_one_page(name_url)
    # 姓氏
    first_name1 = etree.HTML(html2).xpath('//div[@id="head_"]/div/div/div[1]/a/div[1]/text()')
    first_name2 = first_name1[0]
    first_name = first_name2[:-3]

    # 名字
    name = etree.HTML(html2).xpath('/html/body/div[2]/div/div[1]/div/nav/ul/li/a/text()')
    name2 = name[0]
    name1 = name2[3:]

    # 这个名字的总人数
    num1 = etree.HTML(html2).xpath('//div[@class="navbar-brand"]/text()')
    num = re.findall('\d+', str(num1))

    # 男上占比
    boy1 = etree.HTML(html2).xpath('/html/body/div[2]/div/div[2]/div/div/div[1]/text()')
    boy = re.findall('([0-9.]+)[ ]*%', str(boy1))

    # 女生占比
    girl1 = etree.HTML(html2).xpath('/html/body/div[2]/div/div[2]/div/div/div[2]/text()')
    girl = re.findall('([0-9.]+)[ ]*%', str(girl1))

    # 名字五行
    name_wuxing = etree.HTML(html2).xpath('/html/body/div[2]/div/div[4]/div[2]/div[1]/div[2]/div[1]/blockquote/text()')

    # 三才配置
    name_cancai = etree.HTML(html2).xpath('/html/body/div[2]/div/div[4]/div[2]/div[1]/div[2]/div[2]/blockquote/text()')

    # 保存数据到mysql表中
    sql = "insert into name_infoes(first_name,name1,num,boy,girl,name_wuxing,name_cancai) values('%s','%s','%s','%s','%s','%s','%s')" \
          % (first_name, name1, num[0], boy[0], girl[0], name_wuxing[0], name_cancai[0])
    print('---', sql)
    cursor.execute(sql)
    db.commit()
    db.close()

    # return 'ok'

#获取名字的url
def get_name_url(url4):
    # for url4 in url3:
    for url5 in url4:
        for i in range(10):
            url = url5.replace('.html', '_%d.html') % (i + 1)
            html1 = get_one_page(url)
            # get_names(html1)
            href = etree.HTML(html1).xpath('//div[@class="col-xs-12"]/a/@href')
            for j in href:
                name_url = url.replace('/name_list_%d.html'%(i+1), j)
                get_name_info(name_url)



#按姓氏分组
def url_list():
    url = "http://www.resgain.net/xmdq.html?tdsourcetag=s_pcqq_aiomsg"
    html = get_one_page(url)
    # parse_xpath(html)
    href = etree.HTML(html).xpath('.//div[@class="col-xs-12"]/a/@href')
    urls = []
    for url1 in href:
        url2 = 'http:' + url1
        urls.append(url2)
    url3=[]
    n=20
    for x in range(0, len(urls),n):
        url4=urls[x:x+n]
        url3.append(url4)
        # print(len(url3),url3)
    # get_name_url(url3)
    return url3



def main():
    url3 = url_list()
    # print(len(url3),url3)
    # url3   [[], [], []....]
    thread_list = []
    for url in url3:
        print(url[0])
        th = threading.Thread(target=get_name_url, args=(url,))
        th.start()
        thread_list.append(th)
        print('************************')
    for th in thread_list:
        th.join()



if __name__ == '__main__':
    main()
