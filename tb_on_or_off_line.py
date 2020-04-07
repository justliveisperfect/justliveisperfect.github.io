# -*- coding: utf-8 -*-
# @Time    : 2020/3/31 14:17
# @Author  : @tongwoo.cn
# @简介    : 
# @File    : tb_on_or_off_line.py
import cx_Oracle
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timedelta
import time
import threading
from dateutil.relativedelta import relativedelta
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
lock = threading.Lock()


def insert_on_or_not(on_list, not_list, latest_time, st, dbtime):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    insert_sql = "insert into tb_vehicle_online_{0}(DB_TIME, vehi_no) values(:1,:2)".format(st)
    tup_list = []
    for veno in on_list:
        tup_list.append((dbtime, veno))
    try:
        cursor.executemany(insert_sql, tup_list)
        db.commit()
        print 'insert online {0}'.format(len(tup_list))
    except Exception, e:
        print e

    insert_sql = "insert into tb_vehicle_not_{0}(VEHI_NO, ONLINE_TIME, DB_TIME)" \
                 "values(:1,:2,:3)".format(st)
    tup_list = []
    for i in not_list:
        try:
            l_time = latest_time[i]
        except KeyError:
            l_time = None
        tup_list.append((i, l_time, dbtime))
    try:
        cursor.executemany(insert_sql, tup_list)
        db.commit()
        print 'insert not online {0}'.format(len(tup_list))
    except Exception, e:
        print e
    cursor.close()
    db.close()


def get_veno():
    conn_69 = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = conn_69.cursor()
    sql = "select vehi_no from vw_vehicle"
    cursor.execute(sql)
    record = set()
    for i in cursor:
        if i[0] is None:
            continue
        record.add(i[0])
    cursor.close()
    conn_69.close()
    return list(record)


def get_gps(st, t0, t1, vehs):
    conn_113 = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = conn_113.cursor()
    on_line = []
    not_line = []
    for veno in vehs:
        sql = "select vehicle_num,speed_time from tb_gps_{0} where vehicle_num='{1}' and " \
              "speed_time>=:1 and speed_time<:2 order by speed_time desc".format(st, veno)
        cursor.execute(sql, (t0, t1))
        cnt = 0
        for item in cursor:
            cnt += 1
            break
        if cnt == 0:
            not_line.append(veno)
        else:
            on_line.append(veno)
    cursor.close()
    conn_113.close()
    return on_line, not_line


def get_real_latest():
    conn = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur = conn.cursor()
    sql = "select vehi_num,stime from tb_mdt_status order by stime"
    cur.execute(sql)
    rec_time = {}
    for i in cur:
        if i[1] is None:
            continue
        rec_time[i[0]] = i[1]
    cur.close()
    conn.close()
    return rec_time


def get_latest_3_months(veno, yst, now, cursor):
    real_late = get_real_latest()
    try:
        rel_time = real_late[veno]
        if rel_time >= now:
            for i in range(1, 4):
                last_month = (yst - relativedelta(months=i)).strftime("%y%m")
                sql = "select speed_time from tb_gps_{0} where vehicle_num='{1}' order by speed_time desc".format(last_month, veno)
                cursor.execute(sql)
                for item in cursor:
                    return item[0]
            return None
        elif yst <= rel_time < now:
            print veno, 'today'
            return None
        else:
            return rel_time
    except KeyError:
        for i in range(1, 4):
            last_month = (yst - relativedelta(months=i)).strftime("%y%m")
            sql = "select speed_time from tb_gps_{0} where vehicle_num='{1}' order by speed_time desc".format(
                last_month, veno)
            cursor.execute(sql)
            for item in cursor:
                return item[0]
        return None


def get_latest_time(ve_list, yst, now, st):
    conn_113 = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = conn_113.cursor()
    record = {}
    new_ve_list = []
    for veno in ve_list:
        sql = "select speed_time from tb_gps_{0} where vehicle_num='{1}' and speed_time<:1 " \
              "order by speed_time desc".format(st, veno)
        cursor.execute(sql, [(yst)])
        cnt = 0
        for i in cursor:
            cnt += 1
            record[veno] = i[0]
            break
        if cnt == 0:
            new_ve_list.append(veno)
    for veno in new_ve_list:
        res = get_latest_3_months(veno, yst, now, cursor)
        record[veno] = res
    cursor.close()
    return record


def get_on_or_off(st, t0, t1, vehs):
    bt = time.time()
    on_line, not_line = get_gps(st, t0, t1, vehs)
    not_time = get_latest_time(not_line, t0, t1, st)
    # print len(on_line), len(not_line)
    year = t0.strftime("%Y")
    insert_on_or_not(on_line, not_line, not_time, year, t0)
    lock.acquire()
    lock.release()
    et = time.time()
    print et - bt


def main():
    veh_list = get_veno()      # 获取车辆
    now = datetime.now()
    now = datetime(now.year, now.month, now.day, 0, 0, 0)
    yst = now + timedelta(days=-1)
    print yst, now
    st = yst.strftime("%y%m")
    trds = []
    num = 10       # 10个线程一起查，每个线程查其中一批车辆
    for i in range(num):
        t = threading.Thread(target=get_on_or_off, args=(st, yst, now, veh_list[i::num]))
        trds.append(t)
    for t in trds:
        t.start()
    for t in trds:
        t.join()


if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(main, 'cron', hour='6', minute='30', max_instances=10)
    try:
        scheduler.start()
    except SystemExit:
        pass

