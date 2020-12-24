#基础镜像
FROM python:3.6
#FROM centos_with_python:3.6

#制作者信息
MAINTAINER liuzhiliang@baixing.com
 
##设置环境变量
ENV PATH /usr/local/bin:$PATH

#将文件复制到容器的/code文件夹里
ADD . /
WORKDIR /
#安装sasl编译依赖

#apt-get源 使用163的源
#RUN mv /etc/apt/sources.list /etc/apt/sources.list.bak && \
#    echo "deb http://mirrors.aliyun.com/debian jessie main" >> /etc/apt/sources.list && \
#    echo "deb http://mirrors.aliyun.com/debian jessie-updates main" >> /etc/apt/sources.list
#错误
#    echo "deb http://mirrors.aliyun.com/debian/ jessie main non-free contrib" >>/etc/apt/sources.list && \
#    echo "deb http://mirrors.aliyun.com/debian/ jessie-proposed-updates main non-free contrib" >>/etc/apt/sources.list && \
#    echo "deb-src http://mirrors.aliyun.com/debian/ jessie main non-free contrib" >>/etc/apt/sources.list && \
#    echo "deb-src http://mirrors.aliyun.com/debian/ jessie-proposed-updates main non-free contrib" >>/etc/apt/sources.list
#RUN sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list
#RUN apt-get clean
#RUN apt-get update && apt-get install -y --no-install-recommends python-dev libsasl2-dev gcc && rm -rf /var/lib/apt/lists/*
#RUN apt-get update && apt-get install -y --no-install-recommends python-dev libsasl2-dev gcc && rm -rf /var/lib/apt/lists/*

#安装yum
#RUN yum -y update
#RUN yum -y install gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64
# centos 中pip install psycopg2会缺少pg_config，pg_config在postgresql-devel

#更新pip镜像源
RUN pip3 install pip -U
RUN pip3 config set global.index-url http://mirrors.aliyun.com/pypi/simple
RUN pip3 config set install.trusted-host mirrors.aliyun.com
#安装pip依赖
RUN pip3 install -r requirements.txt

# 设置本地时区
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' > /etc/timezone
RUN mkdir -p /tmp/

#容器启动命令
#CMD python3 bootstrap.py
CMD gunicorn -c gunicorn_config.py 'server.web:app'
HEALTHCHECK --start-period=1m --interval=2m --timeout=20s --retries=3 CMD /bin/bash init.sh

