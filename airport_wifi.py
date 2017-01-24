# coding:utf-8

'''
所有参赛数据均已提前导入到mysql中
对数据经行清理，将2016-09-10 19：00：00之前的数据全部删除
'''


import MySQLdb
import numpy as np
import time
import matplotlib.pyplot as plt
import csv
import re
from sklearn.ensemble import RandomForestRegressor
import csv



gate_area_list = ['WC', 'F', 'Ec', 'T1', 'A2', 'W2', 'EC', 'W1', 'W3', 'JF', 'E1', 'E3', 'E2']
focus_area = ['E1','E2','E3','EC','T1','W1','W2','W3','WC']



##对航空排班表中的数据进行清理
def clean_flight_table():
    fr = open('D:/flight.csv','r')
    content = fr.readlines()
    flight_info = []
    for line in content:
        curline = line.strip().split(',')
        if curline[3] != '':
            if curline[3][0] == '"':
                curline[3] = curline[3][1:]
                del curline[4]
            if curline[2] =='':
                curline[2] = curline[1]
            if len(curline[1]) != 18:
                curline[1] = curline[2]
            if len(curline[2]) != 18:
                curline[2] = curline[1]
            flight_info.append(curline)

    fr = open('D:/flight2.csv','w')
    for i in flight_info:
        for j in i:
            fr.write(j+',')
        fr.write('\n')
    fr.close()


##删除departure表中有安检但没登机的乘客
##共30万乘客，约2万没有登机
def clean_departure_table():
    fr = open('D:/departure.csv','r')
    content = fr.readlines()
    fr.close()
    info = []
    for line in content:
        curline = line.strip().split(',')
        if curline[2]!= '':
            info.append(curline)
    fr = open('D:/departure2.csv','w')
    for i in info:
        for j in i:
            fr.write(j+',')
        fr.write('\n')
    fr.close()


##对航班排班数据中UTC时间改为北京时间
def add_time():
    conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '040516', db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('''update flt_stime_atime_gate set scheduled_time = addtime(scheduled_time,'08:00:00'),actual_time = addtime(actual_time,'08:00:00');''')
    conn.commit()
    conn.close()



##根据乘客安检时间表整理出乘客的候机区域
##注意有部分乘客（check_time<2016-9-10-13-00-00）的航班在排班表中找不到
def create_psg_detail_info():
    conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '040516', db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('create table psg_detail_info(passenger varchar(30), check_time varchar(20),scheduled_time varchar(20),actual_time varchar(20),flight_ID varchar(10),gate_ID varchar(4),gate_area varchar(2));')
    conn.commit()
    print 'create table psg_detail_info finished'
    cursor.execute('insert into psg_detail_info select passenger_ID,check_time,scheduled_time,actual_time,psg_chk_flt.flight_ID,flt_stime_atime_gate.gate_ID,gate_area from psg_chk_flt,gate,flt_stime_atime_gate where psg_chk_flt.flight_ID = flt_stime_atime_gate.flight_ID and flt_stime_atime_gate.gate_ID = gate.gate_ID and day(flt_stime_atime_gate.scheduled_time) = substring(psg_chk_flt.passenger_ID,-7,2);')
    conn.commit()
    print 'insert into table finished'
    cursor.execute('create index gate_area on psg_detail_info(gate_area)')
    conn.commit()
    print 'create index finished'
    conn.close()

##根据乘客departure表整理出乘客的候机区域。这个数据比用安检时间整理出来的更全一点点
def create_psg_detail_info2():
    conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '040516', db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('create table psg_detail_info2(passenger_ID2 varchar(20),flight_ID varchar(10),leaf_time varchar(20),check_time varchar(20),gate_ID varchar(10),gate_area varchar(2));')
    conn.commit()
    print 'create table psg_detail_info2 finished'
    cursor.execute('insert into psg_detail_info2 select passenger_ID2,departure.flight_ID,leaf_time,check_time,flt_stime_atime_gate.gate_ID,gate_area from departure,flt_stime_atime_gate,gate where departure.flight_ID = flt_stime_atime_gate.flight_ID and flt_stime_atime_gate.gate_ID = gate.gate_ID and unix_timestamp(flt_stime_atime_gate.actual_time)-unix_timestamp(departure.leaf_time)>0 and unix_timestamp(flt_stime_atime_gate.actual_time)-unix_timestamp(departure.leaf_time)<7200;')
    conn.commit()
    print 'insert into table finished'
    cursor.execute('create index gate_area on psg_detail_info2(gate_area)')
    conn.commit()
    print 'create index finished'
    conn.close()

##按候机区域进行整理
def sub_table(table_name = 'psg_detail_info2',colum_name = 'gate_area',title_and_type = 'passenger_ID2 varchar(20),check_time varchar(20),leaf_time varchar(20),gate_area char(2)',title = 'passenger_ID2,check_time,leaf_time,gate_area'):
    conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '040516', db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('select distinct %s from %s;' % (colum_name,table_name))
    ca = cursor.fetchall()
    distinct_list = []
    for i in ca:
        for j in i:
            distinct_list.append(j)
    for i in distinct_list:
        cursor.execute('create table %s (%s);' % (i,title_and_type))
        conn.commit()
        cursor.execute('''insert into %s select %s from %s where %s = '%s' order by check_time;'''% (i,title,table_name,colum_name,i))
        conn.commit()
        print 'create and insert into table %s finished!'%i
    conn.close()

##创建时间列表
def add_unix_time(unix_time,add_time):
    import re
    d = re.findall(r'(\d{,4}).(\d{,2}).(\d{,2}).(\d{,2}).(\d{,2}).(\d{,2})',add_time)
    d = map(float,list(d[0]))
    if d[0]!=0 or d[1]!=0:
        print 'this funtion cannot add time in month or year'
    else:
        add_unix_time = d[2]*86400+d[3]*3600+d[4]*60+d[5]
        new_unix_time = unix_time+add_unix_time
        return new_unix_time

def create_time_list(start_time,end_time,margin,time_format):
    import time
    time_list = []
    time_list.append(start_time)
    format_start_time = time.strptime(start_time,time_format)
    unix_start_time = time.mktime(format_start_time)
    format_end_time = time.strptime(end_time,time_format)
    unix_end_time = time.mktime(format_end_time)
    while unix_start_time<unix_end_time:
        unix_start_time = add_unix_time(unix_start_time,margin)
        new_time = time.localtime(unix_start_time)
        dt = time.strftime(time_format,new_time)
        time_list.append(dt)
    return time_list

def write_time_list_to_file(time_list,filename):
    fr = open(filename,'w')
    for i in time_list:
        fr.write(i)
        fr.write('\n')
    fr.close()
    print 'time list has writen in file'


##计算各个时间段进入与离开候机区域的人数并导入表中
def count_in_out(gate_area):
    conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '040516', db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('create table %s_count_in(in_num varchar(6),time varchar(20));' % gate_area)
    conn.commit()
    print 'create table %s_count_in finished '% gate_area

    cursor.execute('insert into %s_count_in select count(time_list),time_list from (select passenger_ID2,check_time,time_list from %s,time_list where unix_timestamp(time_list)-unix_timestamp(check_time)<600 and unix_timestamp(time_list)-unix_timestamp(check_time)>=0) as time group by time_list;' % (gate_area,gate_area))
    conn.commit()
    print 'insert into table %s_count_in finished' % gate_area

    cursor.execute('create table %s_count_out(out_num varchar(6),time varchar(20));' % gate_area)
    conn.commit()
    print 'create table %s_count_out finished '% gate_area

    cursor.execute('insert into %s_count_out select count(time_list),time_list from (select passenger_ID2,leaf_time,time_list from %s,time_list where unix_timestamp(time_list)-unix_timestamp(leaf_time)<600 and unix_timestamp(time_list)-unix_timestamp(leaf_time)>=0) as time group by time_list;' % (gate_area,gate_area))
    conn.commit()
    print 'insert into table %s_count_out finished' % gate_area

    conn.close()

##将乘客进出数据读入内存，并填补空缺（人数为0的没显示）
def get_in_num(gate_area):
    conn = MySQLdb.connect(host = 'localhost',user = 'root',passwd = '040516',db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('select * from %s_count_in' % gate_area)
    ca = cursor.fetchall()
    conn.close()
    ca = [[i.strip() for i in j] for j in ca]
    time_list = create_time_list('2016-09-10-19-00-00', '2016-09-14-14-50-00', '0-0-0-0-10-0', '%Y-%m-%d-%H-%M-%S')
    in_num_list = [0]*len(time_list)
    for i in ca:
        in_num_list[time_list.index(i[1])] = i[0]
    in_num_list = map(float,in_num_list)
    return in_num_list

def get_out_num(gate_area):
    conn = MySQLdb.connect(host = 'localhost',user = 'root',passwd = '040516',db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('select * from %s_count_out' % gate_area)
    ca = cursor.fetchall()
    conn.close()
    ca = [[i.strip() for i in j] for j in ca]
    time_list = create_time_list('2016-09-10-19-00-00', '2016-09-14-14-50-00', '0-0-0-0-10-0', '%Y-%m-%d-%H-%M-%S')
    out_num_list = [0]*len(time_list)
    for i in ca:
        out_num_list[time_list.index(i[1])] = i[0]
    out_num_list = map(float,out_num_list)
    return out_num_list

##将时间中不是十分钟的筛选出来
def load_wifi_data():
    fr = open('D:/wifi.csv','r')
    content = fr.readlines()
    fr.close()
    wifi_ap = []
    for i in content:
        i = i.strip().split(',')
        if float(i[2][8:10])>=20 and float(i[2][-5:-3])%10 == 0:
        # if float(i[2][-5:-3])%10 == 0:
            wifi_ap.append(i)
    csvfile = file('D:/sub_wifi_ap.csv', 'wb')
    writer = csv.writer(csvfile)
    writer.writerows(wifi_ap)
    csvfile.close()

##获取区域列表
def get_gate_area_list():
    area = []
    fr = open('D:/sub_wifi_ap.csv')
    content = fr.readlines()
    fr.close()
    for i in content:
        i = i.strip().split(',')
        res = re.findall(r'(\w{1,2})-(\d{1}).*?',i[0])#提取位置特征
        area.append(res[0][0])
    area_list = list(set(area))
    return area_list

##特征提取/区域+楼层
def get_charactor():
    all_chara = []
    fr = open('D:/sub_wifi_ap.csv')
    content = fr.readlines()
    fr.close()
    for i in content:
        i = i.strip().split(',')
        chara = []
        res = re.findall(r'(\w{1,2})-(\d{1}).*?',i[0])#提取位置特征
        time = re.findall(r'(\d{4}).(\d{2}).(\d{2}).(\d{2}).(\d{2}).(\d{2})',i[2])#提取时间特征
        chara.append(str(gate_area_list.index(res[0][0])))
        chara.append(res[0][1])
        chara.append(time[0][3])
        chara.append(time[0][4])
        # chara.append(i[2])
        chara.append(i[1])
        all_chara.append(chara)
    fr = open('D:/charactor.csv','w')
    for i in all_chara:
        for j in i:
            fr.write(j+',')
        fr.write('\n')
    fr.close()

##得到wifi_ap列表
def get_wifi_ap():
    wifi_ap = []
    conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '040516', db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('select distinct wifi_ap from wifi_psg;')
    ca = cursor.fetchall()
    for i in ca:
        for j in i:
            wifi_ap.append(j)
    conn.close()
    return wifi_ap

##查找某一wifi_ap的具体数据
def get_wifi_info(wifi_ap):
    conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '040516', db = 'wifi')
    cursor = conn.cursor()
    cursor.execute('''select wifi_ap, passenger, time from wifi_psg where wifi_ap = '%s' order by time;''' % wifi_ap)
    ca = cursor.fetchall()
    return ca

def get_wifi_info2(wifi_ap):
    fr = open('D:/sub_wifi_ap.csv','r')
    content = fr.readlines()
    fr.close()
    wifi_info = []
    for i in content:
        i = i.strip().split(',')
        if i[0] == wifi_ap:
            wifi_info.append(i[1])
    return wifi_info

##创建答案时间列表
def create_answer_list():
    wifi_ap_list = get_wifi_ap()
    time_list = create_time_list('2016-09-25-15-00-00', '2016-09-25-17-59-59', '0-0-0-0-10-0', '%Y-%m-%d-%H-%M-%S')
    answer_list = []
    for i in wifi_ap_list:
        for j in time_list:
            row = []
            row.append(i)
            row.append(j)
            answer_list.append(row)
    fr = open('D:/answer_list.csv','w')
    for i in answer_list:
        for j in i:
            fr.write(j+',')
        fr.write('\n')
    fr.close()

##用随机森林预测
def random_forest_solve():
    fr = open('D:/charactor.csv','r')
    content = fr.readlines()
    content = [i.strip().split(',') for i in content]
    X = [i[0:4] for i in content]
    Y = [[i[-2]] for i in content]
    list_to_pred = answer_list_to_charactor()
    rf = RandomForestRegressor()  # 随机森林
    rf.fit(X, Y)
    pred = rf.predict(list_to_pred)
    return pred

def combine_pred_and_answer(pred):
    fr = open('D:/answer_list.csv')
    content = fr.readlines()
    fr.close()
    ans = []
    for i in range(len(content)):
        curans = []
        line = content[i].strip().split(',')
        curans.append(str(pred[i]))
        curans.append(line[0])
        curans.append(line[1][:-4])
        ans.append(curans)
    csvfile = file('D:/answer.csv', 'wb')
    writer = csv.writer(csvfile)
    writer.writerows(ans)
    csvfile.close()

def delete_ans_not_focus():
    real_ans = []
    fr = open('D:/answer.csv')
    content = fr.readlines()
    for i in content:
        i = i.strip().split(',')
        area = re.findall(r'(\w{1,2})-(\d{1}).*?',i[1])[0][0]
        if area in focus_area and i[2]!='2016-09-25-18-0':
            real_ans.append(i)
    csvfile = file('D:/new_answer.csv', 'wb')
    writer = csv.writer(csvfile)
    writer.writerows(real_ans)
    csvfile.close()


if __name__ == "__main__":
    
    clean_flight_table()
    clean_departure_table()
    add_time()
    create_psg_detail_info2()
    sub_table()

    time_list = create_time_list('2016-09-10-19-00-00','2016-09-14-14-50-00','0-0-0-0-10-0','%Y-%m-%d-%H-%M-%S')
    write_time_list_to_file(time_list,'D:/timelist.txt')
    #把time_list写入本地，并取名为time_list
    gate_area_list = ['e1','e2','e3','w1','w2','w3']
    for i in gate_area_list:
        count_in_out(i)
        time.sleep(5)

    load_wifi_data()
    get_charactor()
    create_answer_list()
    pred = random_forest_solve()
    combine_pred_and_answer(pred)
    delete_ans_not_focus()


