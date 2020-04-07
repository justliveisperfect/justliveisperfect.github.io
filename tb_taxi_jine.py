# -*- coding: utf-8 -*-
# @Time    : 2019/12/13 9:34
# @Author  : @tongwoo.cn
# @简介    : 
# @File    : tb_taxi_jine.py
from __future__ import division
import re
import json
import datetime
import numpy as np
from urllib import unquote
import time
from geo import bl2xy, calc_dist
from apscheduler.schedulers.background import BackgroundScheduler
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler  # 导入HTTP处理相关的模块
import logging
import pandas as pd
from scipy.optimize import curve_fit
'''
s1 = "http://192.168.0.98:6072/taxi/?dep=123.456791,30.234535&dest=123.456712,30.234504&dt=2019-11-25 13:24:23&if_cj=1"
s2 = 'http://192.168.0.98:6072/taxi/?dist=123.45&dt=2019-11-25 13:24:23&if_cj=1&wait=35'
'''


def func_exp(x, a, b, c):
    return a * np.exp(b * x) + c


def func_linear(x, a, b):
    return a * (x - 20) + b   # b = f(20)


def fit(table_name):
    """
    分段拟合
    wait.csv记录下里程-等候时长的分布情况 dist里程 wait等候时长
    """
    df = pd.read_csv('./data/{0}.csv'.format(table_name))
    # 20公里内的数据用指数函数拟合
    train = df.loc[df.dist < 15]
    x, y = train.dist, train.wait
    popt, pcov = curve_fit(func_exp, x, y, maxfev=1000, p0=[-600, -0.5, 600])
    a, b, c = popt[0:3]

    train2 = df.loc[(df.dist > 20) & (df.dist < 40)]
    x, y = train2.dist, train2.wait
    popt, pcov = curve_fit(func_linear, x, y, maxfev=1000)
    a1, b1 = popt[0], func_exp(20, a, b, c)
    return a, b, c, a1, b1


def morn_peak(dist):
    if dist <= 20:
        wait = func_exp(dist, pa, pb, pc)
        return (wait / 32) * 0.5, wait
    else:
        wait = func_linear(dist, pa1, pb1)
        return (wait / 32) * 0.5, wait


def not_morn_peak(dist):
    if dist <= 20:
        wait = func_exp(dist, na, nb, nc)
        return (wait / 48) * 0.5, wait
    else:
        wait = func_linear(dist, na1, nb1)
        return (wait / 48) * 0.5, wait


class HTTPHandler(BaseHTTPRequestHandler):
    # 处理GET请求
    def do_GET(self):
        # 获取URL
        print 'URL=', self.path
        global na, nb, nc, na1, nb1
        global pa, pb, pc, pa1, pb1
        if re.match('^/taxi/\?.*$', self.path):
            param = self.path.split('?')[1]
            item = param.split('&')
            if item[0].split('=')[0] == 'dep':
                dep_longi, dep_lati = item[0].split('=')[1].split(',')
                dest_longi, dest_lati = item[1].split('=')[1].split(',')
                dt = time.strptime(unquote(item[2]).split('=')[1], '%Y-%m-%d %H:%M:%S')
                cj = int(item[3].split('=')[1])
                bt = time.clock()
                x, y = bl2xy(dep_lati, dep_longi)
                x1, y1 = bl2xy(dest_lati, dest_longi)
                dist = calc_dist([x, y], [x1, y1])/1000
                if 23 <= dt.tm_hour < 24 or 0 <= dt.tm_hour < 5:  # 23-5 夜间
                    if dist <= 3:
                        je = 13
                        wait_time = 0
                    elif 3 < dist <= 10:
                        je = 13 + (dist - 3) * (2.5 + 0.75)
                        res, wait_time = not_morn_peak(dist)
                        je += res
                    else:
                        je = 13 + 7*(2.5 + 0.75) + (dist-10) * (2.5 + 0.75 + 1.25)
                        res, wait_time = not_morn_peak(dist)
                        je += res
                else:
                    if dist <= 3:
                        je = 13
                        wait_time = 0
                    elif 3 < dist <= 10:
                        je = 13 + (dist - 3) * 2.5
                        if 7 <= dt.tm_hour < 9:  # 7点到9点
                            res, wait_time = morn_peak(dist)
                            je += res
                        else:
                            res, wait_time = not_morn_peak(dist)
                            je += res
                    else:
                        je = 13 + 7*2.5 + (dist-10) * (2.5 + 1.25)
                        if 7 <= dt.tm_hour < 9:  # 7点到9点
                            res, wait_time = morn_peak(dist)
                            je += res
                        else:
                            res, wait_time = not_morn_peak(dist)
                            je += res
                if cj == 1:
                    je += 10
                et = time.clock()
                print 'iterate cost ', et - bt
                if int(je) < je:
                    in_json = json.dumps({'je': int(je) + 1, 'lc': round(dist, 3), 'wait': wait_time})
                else:
                    in_json = json.dumps({'je': int(je), 'lc': round(dist, 3), 'wait': wait_time})
            elif item[0].split('=')[0] == 'dist':
                dist = float(item[0].split('=')[1])
                dt = time.strptime(unquote(item[1]).split('=')[1], '%Y-%m-%d %H:%M:%S')
                cj = int(item[2].split('=')[1])
                wt = int(item[3].split('=')[1])
                bt = time.clock()
                if 23 <= dt.tm_hour < 24 or 0 <= dt.tm_hour < 5:  # 23-5 夜间
                    if dist <= 3:
                        je = 13
                    elif 3 < dist <= 10:
                        je = 13 + (dist - 3) * (2.5 + 0.75)
                        je += (wt / 48) * 0.5
                    else:
                        je = 13 + 7 * (2.5 + 0.75) + (dist-10) * (2.5 + 0.75 + 1.25)
                        je += (wt / 48) * 0.5
                else:
                    if dist <= 3:
                        je = 13
                    elif 3 < dist <= 10:
                        je = 13 + (dist - 3) * 2.5
                        if 7 <= dt.tm_hour < 9:  # 7点到9点
                            je += (wt / 32) * 0.5
                        else:
                            je += (wt / 48) * 0.5
                    else:
                        je = 13 + 7 * 2.5 + (dist-10) * (2.5 + 1.25)
                        if 7 <= dt.tm_hour < 9:  # 7点到9点
                            je += (wt / 32) * 0.5
                        else:
                            je += (wt / 48) * 0.5
                if cj == 1:
                    je += 10
                et = time.clock()
                print 'iterate cost ', et - bt
                if int(je) < je:
                    in_json = json.dumps({'je': int(je) + 1})
                else:
                    in_json = json.dumps({'je': int(je)})
            else:
                in_json = json.dumps({'je': 'error'})

            self.protocal_version = 'HTTP/1.1'  # 设置协议版本
            self.send_response(200)  # 设置响应状态码
            self.send_header("Welcome", "Contact")  # 设置响应头
            self.end_headers()
            self.wfile.write(in_json)  # 输出响应内容


def start_server(port):
    http_server = HTTPServer(('', int(port)), HTTPHandler)
    http_server.serve_forever()  # 设置一直监听并接收请求


if __name__ == '__main__':
    logging.basicConfig()
    print "$$$$$   NOW READY to LISTEN   $$$$$"
    scheduler = BackgroundScheduler()
    pa, pb, pc, pa1, pb1 = fit('wait')
    na, nb, nc, na1, nb1 = fit('wait_notp')
    try:
        scheduler.start()
    except SystemExit:
        pass
    start_server(6072)  # 启动服务，监听6069端口

