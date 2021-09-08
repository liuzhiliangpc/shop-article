#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: upload_article.py
@time: 2021/9/7 18:18
@desc: 上传人工文章素材，主要是校验
"""

import os
import pandas as pd
from init import es, red_lock_factory
from retrying import retry
from pydantic import validate_arguments, ValidationError  # 支持泛型参数校验
from typing import Any, Tuple, List, Dict  # 泛型类型支持
import json
from core.core import logger
import datetime


def validate_request(
    language_s: str,
    url: str,
    crawler_time: str,
    title: str,
    content_s: str,
    site_name: str,
    category_s: str,
    level_s: str,
    industry_l1: str,
    industry_l2: str,
    industry_l3: str,
    crawler_batch_tag: str,
    business_category: str,
    crawler_industry_l1: str,
    crawler_industry_l2: str,
    crawler_industry_l3: str,
):
    # 必须字段
    if (
        not language_s.strip()
        or not url.strip()
        or not crawler_time.strip()
        or not title.strip()
        or not content_s.strip()
        or not site_name.strip()
        or not category_s.strip()
        or not business_category.strip()
    ):
        return False
    # 枚举字段
    if level_s:
        if level_s.strip() not in ("A", "B", "C", "D"):
            return False
    if business_category:
        if business_category.strip() not in ("B2B", "B2C"):
            return False
    if category_s:
        if category_s.strip() not in ("综合资讯", "专业资讯", "问答"):
            return False
    if language_s:
        if language_s.strip() != "zh":
            return False
    return True


def run(request):
    r = {}
    # 获取请求数据
    logger.info("[新增素材] [接收数据:{}]".format(request))
    data = request.get("data", [])
    # 补全缺失值
    updated_data = []  # 对缺省字段补全后的数据
    for index, value in enumerate(data):
        language_s = value.get("language_s", "zh")
        url = value.get("url")
        crawler_time = value.get("crawler_time")
        if not crawler_time:
            crawler_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        title = value.get("title")
        content_s = value.get("content_s")
        site_name = value.get("site_name")
        category_s = value.get("category_s", "")
        level_s = value.get("level_s", "")
        industry_l1 = value.get("industry_l1", "")
        industry_l2 = value.get("industry_l2", "")
        industry_l3 = value.get("industry_l3", "")
        crawler_batch_tag = value.get("crawler_batch_tag", "")
        business_category = value.get("business_category", "")
        crawler_industry_l1 = value.get("crawler_industry_l1", "")
        crawler_industry_l2 = value.get("crawler_industry_l2", "")
        crawler_industry_l3 = value.get("crawler_industry_l3", "")
        updated_data.append(
            {
                "language_s": language_s,
                "url": url,
                "crawler_time": crawler_time,
                "title": title,
                "content_s": content_s,
                "site_name": site_name,
                "category_s": category_s,
                "level_s": level_s,
                "industry_l1": industry_l1,
                "industry_l2": industry_l2,
                "industry_l3": industry_l3,
                "crawler_batch_tag": crawler_batch_tag,
                "business_category": business_category,
                "crawler_industry_l1": crawler_industry_l1,
                "crawler_industry_l2": crawler_industry_l2,
                "crawler_industry_l3": crawler_industry_l3,
            }
        )

    for index, value in enumerate(updated_data):
        is_validate = False
        try:
            is_validate = validate_request(
                language_s=value.get("language_s"),
                url=value.get("url"),
                crawler_time=value.get("crawler_time"),
                title=value.get("title"),
                content_s=value.get("content_s"),
                site_name=value.get("site_name"),
                category_s=value.get("category_s"),
                level_s=value.get("level_s"),
                industry_l1=value.get("industry_l1"),
                industry_l2=value.get("industry_l2"),
                industry_l3=value.get("industry_l3"),
                crawler_batch_tag=value.get("crawler_batch_tag"),
                business_category=value.get("business_category"),
                crawler_industry_l1=value.get("crawler_industry_l1"),
                crawler_industry_l2=value.get("crawler_industry_l2"),
                crawler_industry_l3=value.get("crawler_industry_l3"),
            )
        except Exception as e:
            pass
        if not is_validate:
            logger.error("required fields are '' or unlawful ")
            r["retcode"] = 1
            r["msg"] = "required fields are '' or unlawful "
            return r
    r["retcode"] = 0
    r["msg"] = "success" # 执行成功
    logger.info("[新增素材] [结果:{}]".format(str(r)))
    return r


if __name__ == "__main__":
    request = {
        "data": [
            {
                "language_s": "zh",
                "url": "https://www.bbaqw.com/cs/354061.html",
                "crawler_time": "2021-08-25 11:24:00",
                "title": "开锁,方向盘锁死功能原理",
                "content_s": """汽车成为了家家户户都有的基本交通工具，而科技的先进也使得车锁渐渐的演变成了电子锁，一键启动汽车方向盘锁死无法启动是经常会出现的现象。那么，方向盘锁死功能原理呢？下面就让小编来介绍一下吧！


	方向盘锁死功能原理如下： 


	方向盘锁死的原因是汽车非常基本的一个防盗功能。汽车方向盘防盗锁属于机械锁的一种，方向盘锁一般由锁柄、锁座和锁杆构成，锁座和锁壳直接安装在锁柄上，锁柄的一端直接设有锁杆，锁杆上有系列平台侧部是垂直边，另一边是斜边。原理是通过钥匙的旋转，由弹簧来控制一个钢销子，当钥匙拔出后，方向盘只要有转动，那个钢销子就会弹进预先留好的孔里，然后卡住方向盘确保你转不动。在方向盘锁死的情况下，方向盘会转不动，钥匙也拧不动，汽车无法启动，从而实现防盗。


	随着我国汽车的增多，对安全防盗意识的提高，在我国所买到的小汽车，都增设有方向盘锁止机构。也就是说熄火后只要拔出钥匙，方向盘转动锁止机构就会将方向盘锁住。启动时，先插入钥匙，一手来回转动方向盘一手拧动钥匙才可打开。


	汽车方向盘自动锁止机构和门栓原理相似，如果每次来回扭动方向盘打开，容易使锁止机构产生磨损甚，影响行车安全。


	实际上方向盘完全没有必要人为每次锁死，每次熄火拔出钥匙后，方向盘即处于待锁止状态，只要转动方向盘随即锁死，盗贼无法将车开走。也就说盗贼进入车内只要转一下方向盘，即处于方向盘锁死状态，即便发动车，也就只能原地转圈，有可能放弃偷车的念头。


	以上便是小编为大家介绍的关于方向盘锁死功能原理的一些内容，希望对大家有所帮助哦！如果想要了解更多关于交通安全的知识。请您多多关注吧！

																	陌陌安全网""",
                "site_name": "陌陌安全网",
                "category_s": "专业资讯",
                "level_s": "A",
                "industry_l1": "服务",
                "industry_l2": "换锁修锁",
                "industry_l3": "开锁",
                "crawler_batch_tag": "V20210825",
                "business_category": "B2C",
                "crawler_industry_l1": "服务",
                "crawler_industry_l2": "换锁修锁",
                "crawler_industry_l3": "开锁",
            }
        ]
    }
    print(run(request=request))
