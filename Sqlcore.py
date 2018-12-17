#!/usr/bin/python3
#-*-coding:utf-8-*-

#可缺省，为了指明python脚本解析器的路径
#可缺省，为了告知python脚本文件解析器此脚本的字符集

import pickle
import hashlib
import urllib.request
import urllib.parse
import urllib.error
import os
import pymysql
import re
import time
from tools import Util

'''
根据数据库字段进行多线程下载核心
有拓展名：严格按照目录存储在domain文件夹下
无拓展名：直接存储在domain目录下的保留下划线英文数字的文件
'''
class Sqlcore:

    #_conn连接串
    #_cfgs拓展配置
    #_apppath当前文件绝对目录
    #_cfgfile配置文件地址
    #_cfginfo配置键值对
    #_cfghash当前配置哈希串键

    '''初始化配置'''
    def __init__(self, uniq, conn, cfgs):
        self._uniq = uniq
        self._conn = conn
        self._cfgs = cfgs

        self._apppath = os.getcwd() + os.sep
        
        self._cfginfo = {}
        self._cfghash = str(self._conn) + str(self._cfgs["table"]) + str(self._cfgs["column"]) + str(self._cfgs["offset"])
        self._cfghash = hashlib.md5(self._cfghash.encode("UTF-8")).hexdigest()
        self._cfgfile = "cfg_" + self._cfghash + ".pkl"
        
        #self.output("UNIQ=" + str(uniq))
        pass

    
    '''开始运行'''
    def run(self):
        self.output("Download run...")

         #读取断点
        if os.path.exists(self._cfgfile):
            with open(self._cfgfile, "rb") as f:
                self._cfginfo = pickle.load(f)
                if self._cfginfo.get("offset"):
                    self.output("找到上次断点:" + str(self._cfginfo))
                    self._cfgs["offset"] = self._cfginfo["offset"]
                    self._cfgs["limit"] = self._cfginfo["limit"]
                else:
                    self.output("断点文件configError")

    
    
        data = ["default"]
        while len(data)>0:
            self.output("游标从" + str(self._cfgs["offset"]) + "开始，每次"+ str(self._cfgs["limit"]) + "条")
                
            start = time.time()
            data = self.files(self._conn, self._cfgs["table"], self._cfgs["column"], self._cfgs["offset"], self._cfgs["limit"])
            end = time.time()
            
            if self._cfgs["vmode"] != "simple":
                self.output("组合数据耗时：" + str(end-start))

            try:
                self.load(data)
            except:
                self.output("下载load异常")
            finally:
                #写入断点
                with open(self._cfgfile, "wb") as f:
                    self._cfginfo = {"offset":self._cfgs["offset"],"limit":self._cfgs["limit"]}
                    pickle.dump(self._cfginfo, f)
            
            self._cfgs["offset"] += self._cfgs["limit"]
            if self._cfgs["sleep"] > 0:
                if self._cfgs["vmode"] != "simple":
                    self.output("即将休息时间：" + str(self._cfgs["sleep"]) + "秒...")
                time.sleep(self._cfgs["sleep"])
        
        self.output("OK，全部下载完毕！")
        return False


    '''获取数据库指定字段的URL文件'''
    def files(self, conn, table, cols, offset, limit):
        rg1 = re.compile(r"^http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+$") #全匹配
        rg2 = re.compile(r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+") #仅URL匹配
        
        db = pymysql.connect(conn['hostname'], conn['username'], conn['password'], conn['database'], conn['hostport'])
        cursor = db.cursor()
        
        cursor.execute("SELECT {cols} FROM `{table}` LIMIT {offset}, {limit}".format(table=table, cols=cols, offset=offset, limit=limit))
        result = cursor.fetchall()
        urls = ""
        for item in result:
            for url in item:
                if url and len(url) > 0 :
                    urls += " || " + url
            
        db.close()
        #exit()
        
        return rg2.findall(urls)
    

    '''传入URL数组循环下载核心方法'''
    def load(self, data):
        total = 0
        jump = 0
        down = 0
        fail = 0
        for i in data:
            total+=1
            if self.found(i):
                jump+=1
                if self._cfgs["vmode"] == "full":
                    self.output(str(total) + ".跳过：" + urllib.parse.urlparse(i).path)
                
                #continue
            else:
                if self._cfgs["vmode"] == "full":
                    self.output(str(total) + ".[正在下载]:" + urllib.parse.urlparse(i).path)
                    
                try:
                    ua = Util.randUA()
                    req = urllib.request.Request(i, headers={"User-Agent": ua})
                    response = urllib.request.urlopen(req)
                except urllib.error.URLError as reason:
                    fail+=1
                    self.output("URLError:" + str(reason))
                except urllib.error.HTTPError as reason:
                    fail+=1
                    self.output("HTTPError:" + str(reason))
                else:
                    down+=1
                    img = response.read()
                    with open(self.fullpath(i), "wb") as f:
                        f.write(img)
                        
        self.output("实时总数%s，跳过%s，下载%s，失败%s"%(total,jump,down,fail))
        return True
    


    '''传入url返回符合规则的指定文件元组'''
    def found(self, url):
        urls = urllib.parse.urlparse(url)
        '''urls.netloc.replace(".", "_").replace("-", "_")'''
        base = self._apppath + urls.netloc.replace(".", "_").replace("-", "_")
        if not os.path.exists(base):
            os.mkdir(base)

        path = os.path.dirname(urls.path.replace("/", os.sep))
        file = os.path.basename(urls.path)
        
        if not os.path.isdir(base + path):
            os.makedirs(base + path)

        
        if os.path.exists(base + path + os.sep + file):
            return True
            #return {"base":base, "path":path, "file":file}

        return False

    '''返回完整目录文件名'''
    def fullpath(self, url):
        urls = urllib.parse.urlparse(url)
        base = self._apppath + urls.netloc.replace(".", "_").replace("-", "_")
        return base + urls.path.replace("/", os.sep)

    def output(self, info):
        print("\n["+ time.strftime("%Y-%m-%d %H:%M:%S") + "]线程"+ self._uniq + ":" + str(info))


if __name__ == "__main__":
    print("this is Sqlcore")
    #dp = Downloadphotos()
    #dp.run()
