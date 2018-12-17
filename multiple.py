#!/usr/bin/python3
#-*-coding:utf-8-*-


from threading import Thread
import pymysql
from Sqlcore import Sqlcore


'''
入口功能
'''

'''vmode打印显示模式full/standard/simple'''

#本机的photo相册
conn = {"hostname":"localhost","username":"website","password":"website123","database":"photodb","hostport":3306}
cfgs = {"table":"sdb_photo", "column":"corver,gallery", "offset":0, "vmode":"simple", "limit":5, "sleep": 0}




'''开始执行核心工作'''
def runcore(uniq, conn, cfgs):
    sc = Sqlcore(uniq, conn, cfgs)
    sc.run()

'''结果集总数'''
def totalcount(conn, table):
    db = pymysql.connect(conn['hostname'], conn['username'], conn['password'], conn['database'], conn['hostport'])
    cursor = db.cursor()
    cursor.execute("SELECT count(*) FROM `{table}`".format(table=table))
    #cursor.fetchone()
    data = cursor.fetchone()
    #print(data[0])
    return int(data[0])#type(data[0])

    

if __name__ == "__main__":
    print("welcome")
    
    total = totalcount(conn, cfgs["table"])
    print("发现数据库表"+cfgs["table"]+"存在总数："+str(total)+"条记录")
    
    while True:
        tnum = input("开启线程数，最少1个，最多10个，exit退出：")
        if tnum == "exit":
            exit()
        if tnum.isdigit():
            tnum = int(tnum)
            if tnum <1:
                raise ValueError("输入的必须是正整数啊")
            elif tnum > 50:
                print("线程数不能够超过50啊~")
            elif total < tnum:
                print("结果集还没有线程多~")
            else:
                break
        else:
            print("输入的必须是1以上的数字啊~")

    if tnum == 1:
        #单线程
        runcore("ONETHREAD", conn, cfgs)
    else:
        #多线程
        one = total//tnum #每个线程的开始游标
        tlist = []
        for i in range(tnum):
            cfg = cfgs.copy()
            cfg["offset"] = one * i
            #print(cfgs)
            t = Thread(target=runcore, args=("#t"+str(i), conn, cfg))
            tlist.append(t)
            t.start()
            #print("\n" + t.getName())#获取线程名
            
        for i in tlist:
            i.join()#阻塞主线程，当前执行完再执行下一步

    
    print("allDownloaded")
