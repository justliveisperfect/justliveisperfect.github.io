# -*- coding: utf-8 -*-
# @Time    : 2020/3/26 11:17
# @Author  : @tongwoo.cn
# @简介    : 
# @File    : tb_vehicle_over_10.py
import cx_Oracle
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
'''
杭州新汇客运有限公司、杭州新港客运有限公司、杭州新晨客运有限公司      xhky、xgky、xckyxms
输出当天速度值大于10的车牌号码，车牌号，第一次速度大于10时间，第一次速度大于10时速度，速度大于10次数
每半小时计算一次
'''


def get_vehicle():
    conn_113 = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur_113 = conn_113.cursor()
    sql = "select vehi_no,comp_name from vw_vehicle where ba_name='杭州新汇客运有限公司' " \
          "or ba_name='杭州新港客运有限公司' or ba_name='杭州新晨客运有限公司'"
    record = {}
    cur_113.execute(sql)
    for i in cur_113:
        record[i[0]] = i[1]
    cur_113.close()
    conn_113.close()
    return record


def get_gps_data(t0, t1, st):
    conn_113 = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur_113 = conn_113.cursor()
    sql = "select vehicle_num,speed,speed_time from tb_gps_{0} where speed_time>=:1 and speed_time<:2 " \
          "and carstate='1' order by speed_time".format(st)
    cur_113.execute(sql, (t0, t1))
    record = defaultdict(list)
    for item in cur_113:
        record[item[0]].append(item[1:])

    record1 = {}
    sql = "select vehi_num,speed,stime,times from TB_VEHICLE_OVER_10 where dbtime=:1"
    dt = datetime(t0.year, t0.month, t0.day)
    cur_113.execute(sql, [(dt)])
    for item1 in cur_113:
        record1[item1[0]] = list(item1[1:])
    cur_113.close()
    conn_113.close()
    return record, record1


def insert_in(up_list, in_list):
    conn_113 = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur_113 = conn_113.cursor()
    sql = "update TB_VEHICLE_OVER_10 set times=:1 where vehi_num=:2 and dbtime=:3"
    try:
        cur_113.executemany(sql, up_list)
        conn_113.commit()
        print 'update', len(up_list)
    except Exception, e:
        print 'update error', e
    sql = "insert into TB_VEHICLE_OVER_10(comp_name,vehi_num,speed,stime,times,dbtime) values(:1,:2,:3,:4,:5,:6)"
    try:
        cur_113.executemany(sql, in_list)
        conn_113.commit()
        print 'insert', len(in_list)
    except Exception, e:
        print 'update error', e
    cur_113.close()
    conn_113.close()


def process_gps_data(t0, t1, st):
    dbtime = datetime(t0.year, t0.month, t0.day)
    veh_dict = get_vehicle()
    gps_data, veh_over_10 = get_gps_data(t0, t1, st)
    up_list = []
    ins_list = []
    for veh, comp_name in veh_dict.items():
        try:
            value = gps_data[veh]
        except KeyError:
            continue
        obj = pd.DataFrame(value, columns=['speed', 's_time'])
        new_obj = obj[obj['speed'] > 10]
        try:
            ind_list = new_obj.index
            first_speed, first_stime, num = new_obj['speed'][ind_list[0]], new_obj['s_time'][ind_list[0]], new_obj.shape[0]
        except IndexError:
            continue
        except KeyError:
            print veh, 'keyerror'
            continue
        try:
            speed, stime, times = veh_over_10[veh]
            up_list.append([times + num, veh, dbtime])
        except KeyError:
            ins_list.append([comp_name, veh, first_speed, first_stime, num, dbtime])
    insert_in(up_list, ins_list)


def main():
    now = datetime.now()
    t0 = datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)
    t1 = t0 + timedelta(minutes=-30)
    st = t0.strftime("%y%m")
    print t1
    process_gps_data(t1, t0, st)


# t1 = datetime(2020, 3, 31, 0, 0, 0)
# t0 = datetime(2020, 3, 31, 10, 0, 0)
# process_gps_data(t1, t0, '2003')
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'cron', minute='0,30', max_instances=5)
    try:
        scheduler.start()
    except SystemExit:
        pass

