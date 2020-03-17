# coding= gbk
# @Time    : 2019/4/15 10:25
# @Author  : @tongwoo.cn
# @简介    :
# @File    : pl_night_alarm.py
import cx_Oracle
from datetime import datetime, timedelta
import time
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
import os
import redis
import json
import csv
import re
import sys
reload(sys)
type = sys.getfilesystemencoding()
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
comp_phone = {}
ve_type = {}   # veno:[dur_value, yj_value]
ve_type_night = {}
ve_comp = {}
last_state = {}
ve_send_msg = {}  # 宏辰发送短信标记
ve_send_20 = {}   # 休息满20分钟的时候 发送一次短信
message = '[疲劳驾驶]即将疲劳驾驶，请停车休息'.decode('gbk').encode('utf8')
message1 = '[疲劳驾驶]您已疲劳驾驶，请停车休息'.decode('gbk').encode('utf8')


def write_csv(rec, cl):
    fn = 'E:\wangfei\word\{0}.csv'.format(cl.decode('utf8').encode('gbk'))
    try:
        with open(fn, 'ab') as f:
            writer = csv.writer(f)
            writer.writerow(rec)
        f.close()
    except IOError:
        print cl


def get_new_during(dur):
    rec = re.findall(r"\d+", dur)
    h, m, s = rec
    new_dur = 0
    if int(h) > 0:
        new_dur += int(h)*60*60
    if int(m) > 0:
        new_dur += int(m)*60
    if int(s) > 0:
        new_dur += int(s)
    return new_dur


def get_veh_type_cycle():
    global ve_type, ve_comp
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    sql = "select comp_id,comp_name,phone,day_yj,day_alarm,night_yj,night_alarm from tb_comp_pl"
    cursor.execute(sql)
    record1 = cursor.fetchall()
    for item in record1:
        comp_id, comp_name, phone, day_yj, day_alarm, night_yj, night_alarm = item
        day_yj = get_new_during(day_yj)
        day_alarm = get_new_during(day_alarm)
        yj_dur = day_alarm - day_yj   # 预警时长
        if comp_id in ['17028001', '17028002', '17028004']:
            sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
            cursor.execute(sql1)
            for vehi_no, mdt_no in cursor.fetchall():
                if vehi_no == '浙A8J137'.decode('gbk').encode('utf8') or vehi_no == '浙A8J136'.decode('gbk').encode('utf8'):
                    continue
                ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5个小时
                ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
        else:
            sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
            cursor.execute(sql1)
            for vehi_no, mdt_no in cursor.fetchall():
                ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5个小时
                ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
    cursor.close()
    db.close()


def get_veh_type_night():
    global ve_type_night
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    sql = "select comp_id,comp_name,phone,day_yj,day_alarm,night_yj,night_alarm from tb_comp_pl"
    cursor.execute(sql)
    record1 = cursor.fetchall()
    for item in record1:
        comp_id, comp_name, phone, day_yj, day_alarm, night_yj, night_alarm = item
        night_yj = get_new_during(night_yj)
        night_alarm = get_new_during(night_alarm)
        yj_dur_night = night_alarm - night_yj
        if comp_id in ['17028001', '17028002', '17028004']:
            sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
            cursor.execute(sql1)
            for vehi_no, mdt_no in cursor.fetchall():
                if vehi_no == '浙A8J137'.decode('gbk').encode('utf8') or vehi_no == '浙A8J136'.decode('gbk').encode('utf8'):
                    continue
                ve_type_night[vehi_no] = [night_yj, yj_dur_night]  # 3.5个小时
        else:
            sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
            cursor.execute(sql1)
            for vehi_no, mdt_no in cursor.fetchall():
                ve_type_night[vehi_no] = [night_yj, yj_dur_night]  # 3.5个小时
    cursor.close()
    db.close()


def get_veh_type():
    global ve_type, last_state, ve_send_msg, comp_phone, ve_send_20, ve_comp
    comp_phone = {}
    ve_type = {}  # veno:dur_value
    now = datetime.now()

    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    sql = "select comp_id,comp_name,phone,day_yj,day_alarm,night_yj,night_alarm from tb_comp_pl"
    cursor.execute(sql)
    record1 = cursor.fetchall()
    if 6 < now.hour < 22:
        temp_comp_phone = {}  # 去重
        for item in record1:
            comp_id, comp_name, phone, day_yj, day_alarm, night_yj, night_alarm = item
            day_yj = get_new_during(day_yj)
            day_alarm = get_new_during(day_alarm)
            yj_dur = day_alarm - day_yj   # 预警时长
            if comp_id in ['17028001', '17028002', '17028004']:
                comp_phone[comp_name] = '13606527102'
                sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
                cursor.execute(sql1)
                for vehi_no, mdt_no in cursor.fetchall():
                    if vehi_no == '浙A8J137'.decode('gbk').encode('utf8') or vehi_no == '浙A8J136'.decode('gbk').encode('utf8'):
                        continue
                    ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5个小时
                    ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                    last_state[vehi_no] = None
                    ve_send_msg[vehi_no] = [0, 0, None]
                    ve_send_20[vehi_no] = 0
            else:
                try:
                    if phone in temp_comp_phone[comp_name]:
                        continue
                    comp_phone[comp_name] += ',' + phone
                    temp_comp_phone[comp_name].append(phone)
                except KeyError:
                    comp_phone[comp_name] = phone
                    temp_comp_phone[comp_name] = [phone]
                except TypeError:
                    comp_phone[comp_name] = phone
                    temp_comp_phone[comp_name] = [phone]
                sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
                cursor.execute(sql1)
                for vehi_no, mdt_no in cursor.fetchall():
                    ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5个小时
                    ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                    last_state[vehi_no] = None
                    ve_send_msg[vehi_no] = [0, 0, None]
                    ve_send_20[vehi_no] = 0
    elif now.hour == 6:
        temp_comp_phone = {}  # 去重
        for item in record1:
            comp_id, comp_name, phone, day_yj, day_alarm, night_yj, night_alarm = item
            day_yj = get_new_during(day_yj)
            day_alarm = get_new_during(day_alarm)
            yj_dur = day_alarm - day_yj  # 预警时长
            if comp_id in ['17028001', '17028002', '17028004']:
                comp_phone[comp_name] = '13606527102'
                sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
                cursor.execute(sql1)
                for vehi_no, mdt_no in cursor.fetchall():
                    if vehi_no == '浙A8J137'.decode('gbk').encode('utf8') or vehi_no == '浙A8J136'.decode('gbk').encode('utf8'):
                        continue
                    ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5个小时
                    ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
            else:
                try:
                    if phone in temp_comp_phone[comp_name]:
                        continue
                    comp_phone[comp_name] += ',' + phone
                    temp_comp_phone[comp_name].append(phone)
                except KeyError:
                    comp_phone[comp_name] = phone
                    temp_comp_phone[comp_name] = [phone]
                except TypeError:
                    comp_phone[comp_name] = phone
                    temp_comp_phone[comp_name] = [phone]
                sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
                cursor.execute(sql1)
                for vehi_no, mdt_no in cursor.fetchall():
                    ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5个小时
                    ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
    elif now.hour == 22:  # 夜间
        temp_comp_phone = {}  # 去重
        for item in record1:
            comp_id, comp_name, phone, day_yj, day_alarm, night_yj, night_alarm = item
            day_yj = get_new_during(day_yj)
            day_alarm = get_new_during(day_alarm)
            yj_dur = day_alarm - day_yj  # 预警时长
            night_yj = get_new_during(night_yj)
            night_alarm = get_new_during(night_alarm)
            yj_dur_night = night_alarm - night_yj
            if comp_id in ['17028001', '17028002', '17028004']:
                comp_phone[comp_name] = '13606527102'
                sql1 = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
                cursor.execute(sql1)
                for vehi_no, mdt_no in cursor.fetchall():
                    if vehi_no == '浙A8J137'.decode('gbk').encode('utf8') or vehi_no == '浙A8J136'.decode('gbk').encode(
                            'utf8'):
                        continue
                    try:
                        recs = last_state[vehi_no]
                        last_time, last_spd, last_mcspeed, dur_0, dur = recs[0:5]
                        if dur + dur_0 < night_yj:
                            ve_type[vehi_no] = [night_yj, yj_dur_night]  # 1.5个小时
                            ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                        else:
                            write_csv(['22 over night_yj'], vehi_no)
                            ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5
                            ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                    except TypeError:
                        ve_type[vehi_no] = [night_yj, yj_dur_night]  # 1.5个小时
                        ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                        # phone:stime,speed,mcspeed,dur_0,dur  此处during_0是用来存储spd=0的持续时间 如果<20*60则记持续行驶
                    except KeyError:
                        ve_type[vehi_no] = [night_yj, yj_dur_night]  # 1.5个小时
                        ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
            else:
                try:
                    if phone in temp_comp_phone[comp_name]:
                        continue
                    comp_phone[comp_name] += ',' + phone
                    temp_comp_phone[comp_name].append(phone)
                except KeyError:
                    comp_phone[comp_name] = phone
                    temp_comp_phone[comp_name] = [phone]
                except TypeError:
                    comp_phone[comp_name] = phone
                    temp_comp_phone[comp_name] = [phone]
                sql = "select vehi_no,mdt_no from vw_vehicle t where comp_id='{0}'".format(comp_id)
                cursor.execute(sql)
                for vehi_no, mdt_no in cursor.fetchall():
                    try:
                        recs = last_state[vehi_no]
                        last_time, last_spd, last_mcspeed, dur_0, dur = recs[0:5]
                        if dur + dur_0 < night_yj:
                            ve_type[vehi_no] = [night_yj, yj_dur_night]  # 1.5个小时
                            ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                        else:
                            write_csv(['22 over night_yj'], vehi_no)
                            ve_type[vehi_no] = [day_yj, yj_dur]  # 3.5
                            ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                    except TypeError:
                        ve_type[vehi_no] = [night_yj, yj_dur_night]  # 1.5个小时
                        ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
                        # phone:stime,speed,mcspeed,dur_0,dur  此处dur_0是用来存储spd=0的持续时间 如果<20*60则记持续行驶
                    except KeyError:
                        ve_type[vehi_no] = [night_yj, yj_dur_night]  # 1.5个小时
                        ve_comp[vehi_no] = [comp_name, comp_id, mdt_no]
    cursor.close()
    db.close()


def insert_msg_alarm(rec_dic):
    global comp_phone
    st = datetime.now().strftime("%Y%m")[2:]
    tup_list = []
    tup_list_113 = []
    for key, records in rec_dic.items():
        for record in records:
            stime, comp_name, speed = record[2:5]
            veno = record[6]
            message = record[-2]
            send_status = record[-1]
            # longi,lati,stime,comp_name,speed,mcspeed,veno,comp_id,dur_value,yj_value,begin_time,end_time,during,message,send/not
            if send_status == 2:
                try:
                    phone = comp_phone[comp_name]
                    tup_list.append((record[7], comp_name, '', '', phone, veno, '', speed, stime,
                                     '短信待发送'.decode('gbk').encode('utf8'), '', '', message,
                                     '[疲劳驾驶]'.decode('gbk').encode('utf8'), '疲劳驾驶警告'.decode('gbk').encode('utf8'), '', round(record[12], 2)))
                except KeyError:
                    tup_list.append((record[7], comp_name, '', '', '', veno, '', speed, stime,
                                     '待发送'.decode('gbk').encode('utf8'), '', '', message,
                                     '[疲劳驾驶]'.decode('gbk').encode('utf8'), '疲劳驾驶警告'.decode('gbk').encode('utf8'), '', round(record[12], 2)))
                    print comp_name, 'KeyError'
                tup_list_113.append([veno, 3, record[10], record[11], round(record[12], 2), record[0], record[1], '',
                                     '', message, speed])
            else:
                tup_list.append((record[7], comp_name, '', '', '', veno, '', speed, stime,
                                 '待发送'.decode('gbk').encode('utf8'), '', '', message,
                                 '[疲劳驾驶]'.decode('gbk').encode('utf8'), '疲劳驾驶警告'.decode('gbk').encode('utf8'), '', round(record[12], 2)))

    if len(tup_list) > 0 or len(tup_list_113) > 0:
        conn113 = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
        cursor = conn113.cursor()
        sql = "insert into tb_msg_alarm_{0} values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,sysdate,:13,:14,:15,:16,:17)".format(st)
        try:
            cursor.executemany(sql, tup_list)
            conn113.commit()
            print 'insert night alarm pl {0}'.format(len(tup_list))
        except Exception, e:
            print tup_list[0]
            print e

        try:
            insert_sql = "insert into alarm_static_{0} (vehi_no,alarmtype,sta_time,end_time,during,longi,lati," \
                         "contactor,phone,msg_send_time,msginfo,speed,type)" \
                         "values(:1,:2,to_date(:3,'yyyy-mm-dd hh24:mi:ss'),:4,:5,:6,:7,:8,:9,sysdate,:10,:11,2)".format(st)
            try:
                cursor.executemany(insert_sql, tup_list_113)
                conn113.commit()
                print 'insert alarm_static {0} '.format(len(tup_list_113))
            except Exception, e:
                print 'insert', e
        except Exception, e:
            print '113 error', e
        cursor.close()
        conn113.close()


def get_veh_data():
    global ve_type, ve_comp
    print len(ve_type),
    pool = redis.ConnectionPool(host='192.168.0.96', port=6389, db=1)
    r = redis.Redis(connection_pool=pool)
    keys = r.keys()
    result = r.mget(keys)
    record = {}
    for item in result:
        dic = json.loads(item)
        vehi_no = dic['vehi_no'].encode('utf8')
        longi = dic['px']
        lati = dic['py']
        speed = dic['speed']
        stime = datetime.strptime(dic['positionTime'], "%Y-%m-%d %H:%M:%S")
        rspeed = dic['rspeed']
        isu = dic['isu']
        try:
            dur_value, yj_value = ve_type[vehi_no]
        except KeyError:
            continue
        try:
            comp_name, comp_id, mdt_no = ve_comp[vehi_no]
            if mdt_no != isu:
                continue
        except KeyError:
            continue
        record[vehi_no] = [longi, lati, stime, comp_name, speed, rspeed, vehi_no, comp_id, dur_value, yj_value]
    return record


def get_veh_data_old():
    global ve_type
    print len(ve_type),
    veh_gps = {}
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    sql = "select VW_VEHICLE.COMP_ID,TB_COMPANY.COMP_NAME,VW_VEHICLE.VEHI_NO,TB_MDT_STATUS.LONGI," \
          "TB_MDT_STATUS.LATI, TB_MDT_STATUS.SPEED,TB_MDT_STATUS.STIME,TB_MDT_STATUS.MCSPEED from VW_VEHICLE,TB_MDT_STATUS,TB_COMPANY " \
          "where TB_MDT_STATUS.VEHI_ID=VW_VEHICLE.VEHI_ID AND TB_COMPANY.COMP_ID=VW_VEHICLE.COMP_ID and TB_MDT_STATUS.CARSTATE!='0'"
    cursor.execute(sql)
    rec = cursor.fetchall()
    for i in rec:
        comp_id, comp_name, veno = i[0:3]
        if i[5] > 200:  # 浙A7L993  出现有的车辆速度上千（23846.0）的情况
            continue
        try:
            dur_value, yj_value = ve_type[veno]
        except KeyError:
            continue
        veh_gps[veno] = [i[3], i[4], i[6], comp_name, i[5], i[7], veno, comp_id, dur_value, yj_value]
    # veno:longi,lati,stime,comp_name,speed,mcspeed,veno,comp_id,dur_value
    cursor.close()
    db.close()
    return veh_gps


def if_first_msg(veno, pl_tm, k_type):
    global ve_send_msg
    if k_type == 1:
        try:
            try:
                cnt, cnt1, send_time = ve_send_msg[veno]
            except KeyError:
                ve_send_msg[veno] = [0, 0, None]
                cnt, cnt1 = 0, 0
                send_time = None
                print veno, 'KeyError'
            if cnt == 0:
                ve_send_msg[veno] = [1, cnt1, pl_tm]
                print 1, cnt1, pl_tm, veno, 'will'
                return True
            else:
                cnt += 1
                ve_send_msg[veno] = [cnt, cnt1, pl_tm]
                print cnt, cnt1, pl_tm, veno, 'will'
                return False
        except KeyError:
            print veno, 'KeyError1'
    elif k_type == 2:
        try:
            try:
                cnt, cnt1, send_time = ve_send_msg[veno]
            except KeyError:
                ve_send_msg[veno] = [0, 0, None]
                cnt, cnt1 = 0, 0
                send_time = None
                print veno, 'KeyError'
            if cnt1 == 0:
                ve_send_msg[veno] = [cnt, 1, pl_tm]
                print cnt, 1, pl_tm, veno, 'already'
                return True
            else:
                cnt1 += 1
                ve_send_msg[veno] = [cnt, cnt1, pl_tm]
                print cnt, cnt1, pl_tm, veno, 'already'
                return False
        except KeyError:
            print veno, 'KeyError1'
    return False


def get_hour_min_sec(dur):
    m, s = divmod(dur, 60)
    h, m = divmod(m, 60)
    str = ""
    if h > 0:
        str += "{0}小时".format(int(h))
    if m > 0:
        str += "{0}分钟".format(int(m))
    if s > 0:
        str += "{0}秒".format(int(s))
    return str


def tick_day():
    global last_state, ve_send_20, ve_send_msg
    bt = time.time()
    ve_dic = get_veh_data()
    print 'veh_num', len(ve_dic)
    # veno:longi,lati,stime,comp_name,speed,mcspeed,veno,comp_id,dur_value
    # dur_value就是车辆的监控阀值 连续驾驶不能超过这个时间
    pl_ve_dic = {}
    for key, value in ve_dic.items():
        pl_ve_dic[key] = []
        now_time, comp_name, now_spd, now_mcspeed = value[2:6]
        dur_value, yj_value = value[-2:]
        try:
            recs = last_state[key]
            last_time, last_spd, last_mcspeed, dur_0, dur = recs[0:5]
        except TypeError:
            if now_spd > 0 and now_mcspeed > 0:  # 都大于0 算开始
                last_state[key] = [value[2], value[4], value[5], 0, 0]
            # phone:stime,speed,mcspeed,during_0,during  此处during_0是用来存储spd=0的持续时间 如果<20*60则记持续行驶
            continue
        except KeyError:
            write_csv(['KeyError'], key)
            if now_spd > 0 and now_mcspeed > 0:
                last_state[key] = [now_time, now_spd, now_mcspeed, 0, 0]
            else:
                last_state[key] = None
            continue

        if now_time <= last_time:
            continue
        write_csv(['{0}'.format(now_time), '{0}'.format(now_spd), '{0}'.format(now_mcspeed), '{0}'.format(dur_0),
                   '{0}'.format(dur)], key)
        try:
            ct = (now_time - last_time).total_seconds()
            if ct > 1200:  # 20分钟没有上传数据
                write_csv(['20minutes'], key)
                last_state[key] = None
                continue
        except UnboundLocalError:
            pass

        if last_spd > 0 and last_mcspeed > 0:       # 都大于0算行驶 #
            if now_spd == 0 or now_mcspeed == 0:    # 开始计算休息时间 如果之前累计运行超出 报警
                ve_send_20[key] = 0
                during = (now_time - last_time).total_seconds()
                sum_during = dur
                last_state[key] = [now_time, now_spd, now_mcspeed, during, sum_during]
                if dur_value <= sum_during < dur_value + yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 1):
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                            'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，即将疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message, 1])
                    # print now_time, last_time, begin_time
                elif sum_during >= dur_value + yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 2):
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                            'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，已经疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt1 % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message1, 1])
            else:
                during = (now_time - last_time).total_seconds()
                sum_during = during + dur
                last_state[key] = [now_time, now_spd, now_mcspeed, 0, sum_during]
                if dur_value <= sum_during < dur_value + yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 1):
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                            'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，即将疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message, 1])
                    # print now_time, last_time, begin_time
                elif sum_during >= dur_value+yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 2):
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                            'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，已经疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt1 % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message1, 1])
        else:
            if now_spd == 0 or now_mcspeed == 0:  # 继续计时dur_0
                during = (now_time - last_time).total_seconds()
                sum_during = during + dur_0
                last_state[key] = [now_time, now_spd, now_mcspeed, sum_during, dur]
                try:
                    cnt, cnt1, send_time = ve_send_msg[key]
                except KeyError:
                    ve_send_msg[key] = [0, 0, None]
                    cnt = 0
                if sum_during >= 20 * 60 and ve_send_20[key] == 0 and cnt > 0:
                    ve_send_msg[key] = [0, 0, None]
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '您已休息满20分钟'.decode(
                        'gbk').encode('utf8')
                    pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 1])  # 2发短信 1发终端
                    ve_send_20[key] = 1  # 确保只发一次
            else:                                 # 开始计时dur
                during = (now_time - last_time).total_seconds()
                if dur_0 + during < 20 * 60:  # 休息时长小于20分钟 算作持续行驶 加入dur   #########去掉该判断
                    sum_during = dur + dur_0 + during
                    if dur_value <= sum_during < dur_value + yj_value:
                        nt = time.mktime(now_time.timetuple())
                        begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                        if if_first_msg(key, now_time, 1):
                            sc = get_hour_min_sec(sum_during)
                            mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，即将疲劳驾驶，请停车休息'.decode(
                             'gbk').encode('utf8')
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                        else:
                            cnt, cnt1, send_time = ve_send_msg[key]
                            if cnt % 9 == 0:
                                pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message, 1])
                        # print now_time, last_time, begin_time
                    elif sum_during >= dur_value + yj_value:
                        nt = time.mktime(now_time.timetuple())
                        begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                        if if_first_msg(key, now_time, 2):
                            sc = get_hour_min_sec(sum_during)
                            mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，已经疲劳驾驶，请停车休息'.decode(
                                'gbk').encode('utf8')
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                        else:
                            cnt, cnt1, send_time = ve_send_msg[key]
                            if cnt1 % 9 == 0:
                                pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message1, 1])
                    last_state[key] = [now_time, now_spd, now_mcspeed, 0, sum_during]
                else:  # 休息时长超过20分钟 dur重新开始计算
                    sum_during = dur_0 + during
                    try:
                        cnt, cnt1, send_time = ve_send_msg[key]
                    except KeyError:
                        ve_send_msg[key] = [0, 0, None]
                        cnt = 0
                    if ve_send_20[key] == 0 and cnt > 0:
                        nt = time.mktime(now_time.timetuple())
                        begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '您已休息满20分钟'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 1])  # 2发短信 1发终端
                        ve_send_20[key] = 1  # 确保只发一次
                    last_state[key] = None
                    ve_send_msg[key] = [0, 0, None]
    if len(pl_ve_dic) > 0:
        # print 'pl_ve_dic', len(pl_ve_dic)
        insert_msg_alarm(pl_ve_dic)
    et = time.time()
    print et - bt


def tick_night():
    global last_state, ve_type_night, ve_type, ve_send_20, ve_send_msg
    bt = time.time()
    ve_dic = get_veh_data()
    print 'veh_num', len(ve_dic)
    # veno:longi,lati,stime,comp_name,speed,mcspeed,veno,comp_id,dur_value
    # dur_value就是车辆的监控阀值 连续驾驶不能超过这个时间
    pl_ve_dic = {}
    for key, value in ve_dic.items():
        pl_ve_dic[key] = []
        now_time, comp_name, now_spd, now_mcspeed,  = value[2:6]
        dur_vlue, yj_value = value[-2:]
        try:
            recs = last_state[key]
            last_time, last_spd, last_mcspeed, dur_0, dur = recs[0:5]
        except TypeError:
            if now_spd > 0 and now_mcspeed > 0:
                last_state[key] = [now_time, now_spd, now_mcspeed, 0, 0]
            # phone:stime,speed,mcspeed,during_0,during  此处during_0是用来存储spd=0的持续时间 如果<20*60则记持续行驶
            continue
        except KeyError:
            write_csv(['KeyError'], key)
            if now_spd > 0 and now_mcspeed > 0:
                last_state[key] = [now_time, now_spd, now_mcspeed, 0, 0]
            else:
                last_state[key] = None
            continue

        if now_time <= last_time:
            continue
        write_csv(['{0}'.format(now_time), '{0}'.format(now_spd), '{0}'.format(now_mcspeed), '{0}'.format(dur_0),
                   '{0}'.format(dur)], key)
        try:
            ct = (now_time - last_time).total_seconds()
            if ct > 1200:  # 20分钟没有上传数据
                write_csv(['20minutes'], key)
                last_state[key] = None
                continue
        except UnboundLocalError:
            pass

        if last_spd > 0 and last_mcspeed > 0:
            if now_spd == 0 or now_mcspeed == 0:  # 开始计算休息时间 如果之前累计运行超出 报警
                ve_send_20[key] = 0
                during = (now_time - last_time).total_seconds()
                sum_during = dur
                last_state[key] = [now_time, now_spd, now_mcspeed, during, sum_during]
                if dur_vlue <= sum_during < dur_vlue + yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 1):
                        try:
                            res = ve_type_night[key]
                            ve_type[key] = res
                        except KeyError:
                            print 'night', key
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，即将疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message, 1])
                elif sum_during >= dur_vlue + yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 2):
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，已经疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt1 % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message1, 1])
            else:
                during = (now_time - last_time).total_seconds()
                sum_during = during + dur
                last_state[key] = [now_time, now_spd, now_mcspeed, 0, sum_during]
                if dur_vlue <= sum_during < dur_vlue + yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 1):
                        try:
                            res = ve_type_night[key]
                            ve_type[key] = res
                        except KeyError:
                            print 'night', key
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，即将疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message, 1])
                elif sum_during >= dur_vlue + yj_value:
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    if if_first_msg(key, now_time, 2):
                        sc = get_hour_min_sec(sum_during)
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，已经疲劳驾驶，请停车休息'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                    else:
                        cnt, cnt1, send_time = ve_send_msg[key]
                        if cnt1 % 9 == 0:
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message1, 1])
        else:
            if now_spd == 0 or now_mcspeed == 0:  # 继续计时dur_0
                during = (now_time - last_time).total_seconds()
                sum_during = during + dur_0
                last_state[key] = [now_time, now_spd, now_mcspeed, sum_during, dur]
                try:
                    cnt, cnt1, send_time = ve_send_msg[key]
                except KeyError:
                    ve_send_msg[key] = [0, 0, None]
                    cnt = 0
                if sum_during >= 20 * 60 and ve_send_20[key] == 0 and cnt > 0:
                    ve_send_msg[key] = [0, 0, None]
                    nt = time.mktime(now_time.timetuple())
                    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                    mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '您已休息满20分钟'.decode(
                        'gbk').encode('utf8')
                    pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 1])  # 2发短信 1发终端
                    ve_send_20[key] = 1  # 确保只发一次
            else:  # 开始计时dur
                during = (now_time - last_time).total_seconds()
                if dur_0 + during < 20 * 60:  # 算作持续行驶 加入dur
                    sum_during = dur + dur_0 + during
                    if dur_vlue <= sum_during < dur_vlue + yj_value:
                        nt = time.mktime(now_time.timetuple())
                        begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                        if if_first_msg(key, now_time, 1):
                            try:
                                res = ve_type_night[key]
                                ve_type[key] = res
                            except KeyError:
                                print 'night', key
                            sc = get_hour_min_sec(sum_during)
                            mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，即将疲劳驾驶，请停车休息'.decode(
                                'gbk').encode('utf8')
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                        else:
                            cnt, cnt1, send_time = ve_send_msg[key]
                            if cnt % 9 == 0:
                                pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message, 1])
                    elif sum_during >= dur_vlue + yj_value:
                        nt = time.mktime(now_time.timetuple())
                        begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                        if if_first_msg(key, now_time, 2):
                            sc = get_hour_min_sec(sum_during)
                            mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '已累计行驶'.decode(
                             'gbk').encode('utf8') + sc.decode('gbk').encode('utf8') + '，已经疲劳驾驶，请停车休息'.decode(
                                'gbk').encode('utf8')
                            pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 2])  # ########
                        else:
                            cnt, cnt1, send_time = ve_send_msg[key]
                            if cnt1 % 9 == 0:
                                pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, message1, 1])
                    last_state[key] = [now_time, now_spd, now_mcspeed, 0, sum_during]
                else:  # dur重新开始计算
                    sum_during = dur_0 + during
                    try:
                        cnt, cnt1, send_time = ve_send_msg[key]
                    except KeyError:
                        ve_send_msg[key] = [0, 0, None]
                        cnt = 0
                    if ve_send_20[key] == 0 and cnt > 0:
                        nt = time.mktime(now_time.timetuple())
                        begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nt - sum_during))
                        mes = '[疲劳驾驶]'.decode('gbk').encode('utf8') + comp_name + key + '您已休息满20分钟'.decode(
                            'gbk').encode('utf8')
                        pl_ve_dic[key].append(value + [begin_time, now_time, sum_during, mes, 1])  # 2发短信 1发终端
                        ve_send_20[key] = 1  # 确保只发一次
                    try:
                        res = ve_type_night[key]
                        ve_type[key] = res
                    except KeyError:
                        print 'night', key
                    last_state[key] = None
                    ve_send_msg[key] = [0, 0, None]
    if len(pl_ve_dic) > 0:
        insert_msg_alarm(pl_ve_dic)
    et = time.time()
    print et - bt


get_veh_type()
tick_day()
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    scheduler.add_job(tick_day, 'cron', hour='6-21', second='*/19', misfire_grace_time=30, max_instances=10)
    scheduler.add_job(tick_night, 'cron', hour='22-23', second='*/19', misfire_grace_time=30, max_instances=10)
    scheduler.add_job(tick_night, 'cron', hour='0-5', second='*/19', misfire_grace_time=30, max_instances=10)
    # scheduler.add_job(send, 'cron', hour='*', minute='*/1', misfire_grace_time=30)  # 直接发送短信
    scheduler.add_job(get_veh_type, 'cron', hour='22')  # 夜间
    scheduler.add_job(get_veh_type_night, 'cron', hour='22')  # 夜间
    scheduler.add_job(get_veh_type, 'cron', hour='6')   # 白天
    scheduler.add_job(get_veh_type_cycle, 'cron', hour='6-21', minute='30')  # 一小时更新一次

    try:
        scheduler.start()
    except SystemExit:
        pass

