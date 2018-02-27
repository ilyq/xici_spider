import time
from concurrent.futures import ThreadPoolExecutor

import requests
import requests_html
import pymongo

stop_flog = 0
page_num = 1
month_ago = int(time.time()) - 60*60*24*30*1
pool = ThreadPoolExecutor()


def to_timestamp(time_data):
    """
    格式化时间
    :param time_data:
    :return:
    """
    tmp_time = time_data.split(" ")[0]
    time_array = time.strptime("20{}".format(tmp_time), "%Y-%m-%d")
    return int(time.mktime(time_array))


def adapt(data):
    """
    适配内容
    :param data:
    :return:
    """
    verify_data = []
    global month_ago
    global stop_flog

    for item in data:
        content = item.text.split("\n")
        content_len = len(content)
        if content_len == 6 or content_len == 7:
            # 修改没有服务器地址的IP
            if content_len == 6:
                content.insert(2, "")
            ip, port, local, is_anonymous, http_type, save_time, verify_time = content
            data = {
                "ip": ip,
                "port": port,
                "local": local,
                "is_anonymous": is_anonymous,
                "http_type": http_type,
                "save_time": save_time,
                "verify_time": verify_time
            }

            # 只获取最近一个月的IP
            timestamp = to_timestamp(verify_time)
            if timestamp > month_ago:
                verify_data.append(data)
            else:
                stop_flog = 1

    pool.map(verify_ip, verify_data)


def verify_ip(data):
    """
    验证ip
    :param data:
    :return:
    """
    proxies = {data.get("http_type").lower(): "{}://{}:{}".format(
        data.get("http_type").lower(),
        data.get("ip"),
        data.get("port"))}
    try:
        if requests.head("https://www.baidu.com", proxies=proxies, timeout=5).status_code == 200:
            add_to_mongo(data)
    except Exception as e:
        print(e)


def add_to_mongo(data):
    """
    入库
    :param data:
    :return:
    """
    connection = pymongo.MongoClient()
    tdb = connection.Spider
    db = tdb.ip_pool
    if isinstance(data, list):
        db.insert_many(data)
    else:
        db.insert(data)


def xici_spider():
    """
    爬取内容
    :return:
    """
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch",
        "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Host": "www.xicidaili.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
    }

    global stop_flog
    global page_num

    while 1:
        # 退出
        if stop_flog:
            return

        session = requests_html.Session()
        try:
            r = session.get("http://www.xicidaili.com/nt/{}".format(page_num), headers=headers, timeout=30)
            page_num += 1
            if r.status_code == 200:
                data = r.html.find('tr', _encoding="utf-8")
                adapt(data)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    xici_spider()
