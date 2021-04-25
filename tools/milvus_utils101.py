#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: milvus_utils.py
@time: 2021/4/2 15:59
@desc: pymilvus更新到1.0.1
       milvus更新到1.0.0
       与0.10.4下部分函数与差异，最近更新时间2021年4月20日
       TODO delete_by_id 需要完善 返回项
"""
from typing import Any, Tuple, List, Dict  # 类型支持
import numpy as np
import time
from milvus import Milvus, IndexType, MetricType, Status
from config import conf
from pydantic import validate_arguments, ValidationError # 支持泛型参数校验


class MyMilvus(object):
    def __init__(self):
        self._host = conf.getConf('milvus_server', 'HOST')
        self._port = conf.getConf('milvus_server', 'PORT')
        print(self._host)
        print(self._port)
        self.milvus_device = Milvus(uri='tcp://localhost:19530')
        # self.milvus_device = Milvus(host=self._host, port=self._port)

    @validate_arguments
    def create_collection(self, collection_name: str, dimension: int, index_file_size: int = 1024, metric_type: MetricType = MetricType.L2, index_type: IndexType = IndexType.FLAT, is_rebuild: bool = False, timeout: int = 30, index_param: Any = None) -> None:
        """
        创建collection
        :param collection_name: 数据集名称，缺省为test，其他要和线上的兼容
        :param dimension: dimension of vector
        :param index_file_size: max file size of stored index，单位:MB，为避免查询时未建立索引的数据文件过大,此处为1024。
        :param metric_type: 缺省默认是 MetricType.L2 欧式距离
        :param index_type: 缺省默认是 IndexType.FLAT，索引类型有：FLAT / IVFLAT / IVF_SQ8 / IVF_SQ8H，其中FLAT是精确索引，速度慢，但是有100%的召回率
        :param is_rebuild: 是否重新创建同名数据集，True/False，主要针对测试阶段
        :param timeout: 超时设置，单位秒
        :param index_param: 其他索引参数，至少为param={'nlist': 16384}`
        :return:
        """
        status, ok = self.milvus_device.has_collection(collection_name)

        param = {
            "collection_name": collection_name,
            "dimension": dimension,
            "index_file_size": index_file_size,  # optional
            "metric_type": metric_type,  # optional
        }
        # 没有该集合名，则创建集合
        if not ok:
            self.milvus_device.create_collection(param)
        else:
            # 若需重建该集合，则删除后重建
            if is_rebuild:
                status = self.milvus_device.drop_collection(collection_name)
                time.sleep(1)  # 这里有个睡眠,删除越大的数据库耗时越久
                self.milvus_device.create_collection(param)

        # Show collections in Milvus server
        _, collections = self.milvus_device.list_collections()
        print("当前所有集合:{}".format(collections))
        _, collection = self.milvus_device.get_collection_info(collection_name)
        print("新建集合信息:{}".format(collection))

        # 官方建议在插入向量之前，建议先使用 milvus.create_index 以便系统自动增量创建索引
        # You can search vectors without creating index. however, Creating index help to search faster
        status = self.milvus_device.create_index(
            collection_name, index_type, index_param, timeout=timeout
        )

    @validate_arguments
    def drop_collection(self, collection_name: str, timeout: int = 30) -> None:
        """
        删除分区
        :param collection_name: 集合名
        :param timeout: 超时设置，单位秒
        :return:
        """
        self.milvus_device.drop_index(collection_name=collection_name, timeout=timeout) # 先卸载索引
        self.milvus_device.drop_collection(collection_name=collection_name, timeout=timeout) # 再删除集合
        # self.milvus_device.close() # 断开连接

    @validate_arguments
    def create_partition(self, tag_name: str, collection_name: str, timeout: int = 30) -> None:
        """
        创建分区，存在不新创建
        :param tag_name: 分区名 str or None
        :param collection_name: 集合名
        :return:
        """
        status, ok = self.milvus_device.has_partition(
            collection_name=collection_name, partition_tag=tag_name
        )
        if not ok:
            self.milvus_device.create_partition(
                collection_name=collection_name, partition_tag=tag_name, timeout=timeout
            )

    @validate_arguments
    def drop_partition(self, tag_name: str, collection_name: str) -> None:
        """
        删除分区,不存在不删除
        :param tag_name: 分区名 str or None
        :param collection_name: 集合名
        :return:
        """
        status, ok = self.milvus_device.has_partition(
            collection_name=collection_name, partition_tag=tag_name
        ) #
        if ok:
            self.milvus_device.drop_partition(
                collection_name=collection_name, partition_tag=tag_name
            )

    @validate_arguments
    def insert(self, collection_name: str, vectors: List, vector_ids: Any = None, tag_name: Any=None, timeout: int = 30) -> Tuple:
        """
        插入向量，允许插入同名向量
        :param vectors: 向量列表
        :param collection_name: 集合名
        :param vector_ids: 向量id列表
        :param tag_name: 分区名 str or None
        :param timeout: 超时设置，单位:秒
        :return:
        """
        # partition_tag为None或字符串
        status, ids = self.milvus_device.insert(
            collection_name=collection_name,
            records=vectors,
            ids=vector_ids,
            partition_tag=tag_name,
            timeout=timeout
        )  # ids允许为空，内部随机生成
        if not status.OK():
            raise OSError("Insert failed: {}".format(vectors))
        # 返回插入的向量内部id列表
        return status.OK(), ids

    @validate_arguments
    def delete_entity_by_id(self, collection_name: str, vector_ids: List[int], timeout: int = 30) -> bool:
        """
        根据id删除向量（同名向量会都删除，不存在亦可执行，但id列表不能为空）
        :param collection_name: 集合名
        :param vector_ids: 向量id列表
        :param timeout: 超时设置，单位：秒
        :return:
        """
        status = self.milvus_device.delete_entity_by_id(
            collection_name=collection_name, id_array=vector_ids, timeout=timeout
        )
        if status.OK():
            print("Delete result is correct")
        else:
            print("Delete result isn't correct")
        # 1s后会自动落盘，先不手动落盘，减少性能损失。
        # self.milvus.flush(collection_name_array=[collection_name])
        return status.OK()

    @validate_arguments
    def get_entity_by_id(self, collection_name: str, vector_ids: List[int], timeout: int = 30) -> Tuple:
        """
        根据id查询向量，不存在返回空列表，但id列表不能为空
        :param collection_name: 集合名
        :param vector_ids: 向量列表
        :param timeout: 超时设置，单位：秒
        :return:
        """
        status, result_vectors = self.milvus_device.get_entity_by_id(collection_name=collection_name, ids=vector_ids, timeout=timeout)
        # print(status, result_vectors)
        return status, result_vectors

    @validate_arguments
    def create_index(self, collection_name: str, index_param: Dict, index_type: IndexType = IndexType.FLAT, timeout: int = 30) -> None:
        """
        手动创建索引
        :param collection_name: 集合名
        :param index_param: 索引参数
        :param index_type: 索引类型
        :param timeout: 超时设置，单位：秒
        :return:
        """
        # 一个集合只支持一种索引类型，切换索引类型会自动删除旧的索引文件。在创建其它索引前，FLAT 作为集合的默认索引类型。index_param是很重要的设置参数。
        status = self.milvus_device.create_index(collection_name=collection_name, index_type=index_type, params=index_param, timeout=timeout)

    @validate_arguments
    def drop_index(self, collection_name: str, timeout: int = 30) -> None:
        """
        手动删除索引
        :param collection_name: 集合名
        :param timeout: 超时设置，单位：秒
        :return:
        """
        # 删除索引后，集合再次使用默认索引类型 FLAT。
        status = self.milvus_device.drop_index(collection_name=collection_name, timeout=timeout)

    @validate_arguments
    def get_index_info(self, collection_name: str, timeout: int = 30) -> Tuple:
        """
        检查索引信息
        :param collection_name: 集合
        :param timeout: 超时设置，单位：秒
        :return:
        """
        status, index = self.milvus_device.get_index_info(collection_name=collection_name, timeout=timeout)
        return status, index

    @validate_arguments
    def search(self, query_vectors: List, collection_name: str, search_param: Dict, tag_names: Any = None, top_k: int = 3, timeout: int = 30):
        """
        查询最相似的向量
        :param query_vectors: 查询向量，可能是16进制，不一定是浮点型
        :param collection_name: 集合名
        :param search_param: 查询参数，如 search_param = {"nprobe": 32}
        :param tag_names: 分区列表，缺省为空 List[str] or None
        :param top_k: 前K个最相似的向量
        :param timeout: 超时设置，单位：秒
        :return: 为了和之前的兼容，有部分取舍
        """
        print("Searching ... ")
        param = {
            "collection_name": collection_name,
            "query_records": query_vectors,
            "partition_tags": tag_names,
            "top_k": top_k,
            "params": search_param,
            "timeout": timeout
        }

        try:
            status, results = self.milvus_device.search(**param)
            if status.OK():
                return status.OK(), results
            else:
                print("Search failed. ", status)
                return status.OK(), []
        except Exception as e:
            print(e)
            return False, []

    @validate_arguments
    def flush(self, collection_names: List[str]) -> None:
        """
        数据落盘，集合列表。1s后会自动落盘，手动落盘，性能会有不少的损失。
        :param collection_names: 集合列表
        :return:
        """
        self.milvus_device.flush(collection_name_array=collection_names)

    @validate_arguments
    def close(self) -> None:
        # 断开数据连接
        self.milvus_device.close() # 断开连接


def to_binary(sims):
    """
    转化为Milvus支持的二进制格式
    :param sims: 列表或字符串,多种类型，未校验限制
    :return: 适用于Milvus的二进制编码
    """
    if not isinstance(sims, list):
        if isinstance(sims, str):
            try:
                sims = [sims]
            except Exception as e:
                raise ValueError(
                    "Param type incorrect, expect {} but get {} instead".format(
                        type(dict), type(sims)
                    )
                )
        else:
            raise ValueError(
                "Param type incorrect, expect {} but get {} instead".format(
                    type(dict), type(sims)
                )
            )

    vectors = []
    for sim_str in sims:
        sim_bin = ("0" * 64 + bin(int(sim_str, 16)).replace("0b", ""))[-64:]
        vector = []
        i = 0
        while i < len(sim_bin):
            if i == 0:
                bin_cut8 = sim_bin[-8:]
            else:
                bin_cut8 = sim_bin[-8 - i : -i]
            i += 8
            vector.append(int(bin_cut8, 2))
        vector = vector[::-1]
        vectors.append(vector)
    # print(vectors)
    data = np.array(vectors, dtype="uint8").astype(
        "uint8"
    )  # 转化到10进制8位0-255片段，如[67, 248, 41, 25, 149, 231, 48, 47]

    out_list = [bytes(i) for i in data]  # bytes编码
    return out_list


if __name__ == "__main__":

    "--------------------------------------------------"
    mymilvus = MyMilvus()
    # mymilvus.create_collection(
    #     collection_name="qianci_baixing_content_b2c_dev",
    #     dimension=64,
    #     index_file_size=1024,
    #     metric_type=MetricType.HAMMING,
    #     index_type=IndexType.IVF_FLAT,
    #     index_param={"nlist": 16384},
    #     is_rebuild=False,
    # )  # 站内千词的线上排重库
    # mymilvus.create_collection(
    #     collection_name="qianci_baixing_content_b2b_dev",
    #     dimension=64,
    #     index_file_size=1024,
    #     metric_type=MetricType.HAMMING,
    #     index_type=IndexType.IVF_FLAT,
    #     index_param={"nlist": 16384},
    #     is_rebuild=False,
    # )  # 站内千词的线上排重库
    # mymilvus.create_collection(
    #     collection_name="qianci_baixing_content_other_dev",
    #     dimension=64,
    #     index_file_size=1024,
    #     metric_type=MetricType.HAMMING,
    #     index_type=IndexType.IVF_FLAT,
    #     index_param={"nlist": 16384},
    #     is_rebuild=False,
    # )  # 站内千词的线上排重库
    _, collections = mymilvus.milvus_device.list_collections()
    print(collections)
    # status, result_vectors = mymilvus.get_entity_by_id("qianci_baixing_content_b2b", vector_ids=[1, 2, 3, 4, 5])
    # status, result_vectors = mymilvus.get_entity_by_id("qianci_baixing_content_b2b", vector_ids=list(range(1000)))
    # status, result_vectors = mymilvus.get_entity_by_id("qianci_baixing_content_b2c", vector_ids=list(range(1000)))
    "-------------------------------------------------------------------------------------------------"
    status, result_vectors = mymilvus.get_entity_by_id("qianci_baixing_content_b2c", vector_ids=[395153, 459730])
    print(status)
    print(result_vectors)
    # status = mymilvus.delete_entity_by_id(collection_name="qianci_baixing_content_b2b", vector_ids=[1])
    # print(status)

    "-------------------------------------------------------------------------------------------------"

    # 官方接口
    # status, oth = mymilvus.milvus_device.list_partitions(collection_name="qianci_baixing_content_b2b")
    # print(status, oth)
    # 先删除原集合collection，在重新迁移
    # mymilvus.drop_collection(collection_name="qianci_baixing_content_b2b")
    # mymilvus.drop_collection(collection_name="qianci_baixing_content_b2c")
