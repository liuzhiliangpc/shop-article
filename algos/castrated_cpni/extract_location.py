#!/usr/bin/env python3
# encoding: utf-8

import os
import json
import jieba
import re

# 地名有歧义情形下，自动关联为热点地区
myumap = {'南关区': '长春市',
    '南山区': '深圳市',
    '宝山区': '上海市',
    '普陀区': '上海市',
    '朝阳区': '北京市',
    '河东区': '天津市',
    '白云区': '广州市',
    '西湖区': '杭州市',
    '铁西区': '沈阳市',
    '鼓楼区': '南京市'}

class ExtractLocation(object):

    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        loc_set_path = os.path.join(dir_path, 'dicts/loc.txt')
        loc_tree_path = os.path.join(dir_path, 'dicts/loc_tree.txt')

        jieba.load_userdict(loc_set_path)
        self._jieba = jieba
        self.loc_dict = self.loadLocSet(loc_set_path)
        self.id2loc, self.name2ids, self.pre2ids = self.loadLocTree(loc_tree_path)

    def loadLocSet(self, path):
        loc_dict = set()
        with open(path, 'r', encoding='utf-8') as fin:
            for line_id,line in enumerate(fin):
                w = line.strip()
                if w in ['社区','街道','新区','市区','通道','公安','延长','开发区',
                           '高明','会同','平安','比如','资源','凤凰','安康','延寿','城口',
                         '中方','合作','同心','紫阳','和平','互助','西区','正安','合计','经济开发区']:
                    continue
                loc_dict.add(w)
        return loc_dict

    def loadLocTree(self, path):
        id2loc = {}
        name2ids = {}
        pre2ids = {}
        with open(path, 'r', encoding='utf-8') as fin:
            for line_id, line in enumerate(fin):
                line = line.strip().split('\t')
                line = [l for l in line if l]
                if line:
                    lid, lname, lparent, llevel, lstatus = line
                    if lid == lparent:
                        continue
                    id2loc[lid] = {'name':lname, 'parent':lparent, 'level':llevel}

                    if lname not in name2ids:
                        name2ids[lname] = []
                    name2ids[lname].append(lid)

                    if lname[0] not in pre2ids:
                        pre2ids[lname[0]] = []
                    pre2ids[lname[0]].append(lid)
        return id2loc, name2ids, pre2ids

    def getLocWord(self, text):
        text = text.replace('政府', ' ')
        text = text.replace('当局', ' ')
        ws = self._jieba.lcut(text)
        locs = {}
        for w in ws:
            if w in self.loc_dict:
                if w not in locs:
                    locs[w]=0
                locs[w]+=1
        locs = sorted(locs.items(), key=lambda kv:kv[1], reverse=True)[:4]
        locs = [l for l in locs if l[1] > 0]
        return locs

    def find_loc(self, text):
        locs = self.getLocWord(text)
        if locs:
            temp_locs = [i[0] for i in locs]
        else:
            temp_locs = locs
        geo_location = temp_locs
        return geo_location

if __name__ == '__main__':
    ET = ExtractLocation()
    # title = "上海市宝山区开发区黄金回收哪家好"
    title = "上海南通市公司注册多少钱？"
    title = "上海闵行区教育研究院转让什么价格？医学研究院注册流程是什么？？"
    # title = "朝阳区北苑华贸城"
    title = "南京市鼓楼北苑华贸城"

    import time
    time1 = time.time()
    # for i in range(1000):
    ret = ET.find_loc(title)
    time2 = time.time() - time1
    print("spend time is {} s".format(time2))
    print(ret)

