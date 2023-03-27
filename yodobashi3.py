import requests
from requests.exceptions import Timeout
from bs4 import BeautifulSoup
import datetime
import sqlite3
import os
from time import sleep
import numpy as np
import pandas as pd
import re


hyou = ''
fl = open('ua.txt', 'r')
ua = fl.read()
fl.close()

url = pd.read_table('urls.txt', header=None)
url = url[0][:]

header = {"User-Agent": ua}
dt = datetime.datetime.now()
key = 'yodobashibase.sqlite3'
conn = sqlite3.connect(key)
c = conn.cursor()


def huta(q):
    if q < 10:
        return '0'+str(q)
    else:
        return str(q)


def shutoku(a):
    r = requests.get(a, timeout=10, headers=header)
    r.encoding = r.apparent_encoding
    bs = BeautifulSoup(r.text, 'html.parser')
    bs.br.replace_with('\n')
    namae = bs.select('#products_maintitle')[0].get_text()
    zaiko = bs.select('.stockInfo')[0].get_text()
    nedan = bs.select('#js_scl_unitPrice')[0].get_text()
    return namae, zaiko, nedan, huta(dt.year)+huta(dt.month)+huta(dt.day)+huta(dt.hour)+huta(dt.minute)+huta(dt.second), huta(dt.month), huta(dt.day), huta(dt.hour), huta(dt.minute)


def kakikomidb(b):
    try:
        result = shutoku(b)
        sql = 'insert into hyou(url, name, stock, price, year, month, day, hour, min) values(?,?,?,?,?,?,?,?,?)'
        sqll = (b, result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7])
        c.execute(sql, sqll)
        conn.commit()
    except:
        c.execute('create table if not exists hyou(url, name, stock, price, year, month, day, hour, min)')
        conn.commit()
        sql = 'insert into hyou(url, name, stock, price, year, month, day, hour, min) values(?,?,?,?,?,?,?,?,?)'
        result = shutoku(b)
        sqll = (b, result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7])
        c.execute(sql, sqll)
        conn.commit()


def keshidb(d):
    sql = "select * from hyou WHERE url = '"+d+"'"
    c.execute(sql)
    conn.commit()
    res = c.fetchall()
    if len(res) > 5:
        jik = []
        for i in res:
            jik.append(i[4])
        jik.sort()
        huru = jik[0]
        ds = "DELETE FROM hyou WHERE url = '"+d+"' AND year = '"+huru+"'"
        c.execute(ds)
        conn.commit()


def hikaku(e):
    jik = []
    sql = "select * from hyou WHERE url = '"+e+"'"
    c.execute(sql)
    conn.commit()
    res = c.fetchall()
    for i in res:
        jik.append(i[4])
    jik.sort()
    if len(jik) > 1:
        fst = jik[-1]
        sql = "select * from hyou WHERE url = '"+e+"' AND year = '"+fst+"'"
        c.execute(sql)
        conn.commit()
        qq = c.fetchall()
        fst = qq[0][3][1:]
        fst = int(fst.replace(',', ''))
        fz = qq[0][2]
        sec = jik[-2]
        sql = "select * from hyou WHERE url = '"+e+"' AND year = '"+sec+"'"
        c.execute(sql)
        conn.commit()
        pp = c.fetchall()
        sec = pp[0][3][1:]
        sec = int(sec.replace(',', ''))
        sz = pp[0][2]
        if fz == sz:
            if fst == sec:
                return '', ''
            else:
                return '', str(fst-sec)
        else:
            if fst == sec:
                return '!!!!!!!', ''
            else:
                return '!!!!!!!', str(fst-sec)
    else:
        sql = "select * from hyou WHERE url = '"+e+"'"
        c.execute(sql)
        conn.commit()
        return '', ''


def flame(f):
    sql = "select * from hyou WHERE url = '"+f+"' ORDER BY year"
    c.execute(sql)
    conn.commit()
    you = list(c.fetchall())
    df = []
    koushin = []
    for i in you:
        df.append(list(i[2:4]))
        koushin.append(i[4][:8])
    df.append(hikaku(f))
    koushin.append('変化')
    DF = pd.DataFrame(df, columns=[you[0][1], 'price'], index=koushin)
    return DF.T


fl = open('kekka.html', 'w', encoding='utf-8')
mo = input('all->enter\nhenkanomi->nannkaoshite enter\n->')
for i in url:
    kakikomidb(i)
    keshidb(i)
    html = flame(i).to_html()
    html = re.sub('<th>', '<th width="12.5%">', html)
    html = re.sub('<table', '<table height="150"', html)
    if mo == '':
        fl.write(html)

    hyou = flame(i)
    sleep(1)
fl.close()

conn.close()
