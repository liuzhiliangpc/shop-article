#!/usr/bin/env python3
# encoding: utf-8
'''
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: milvus_utils.py
@time: 2020/8/27 15:59
@desc: 当前版本更新到2020年9月22日
'''

import numpy as np
import time
from milvus import Milvus, IndexType, MetricType, Status
from config import conf
# from algos.sim_bert import SimilarModel

# b_sim = SimilarModel()

def to_binary(sims):
    """
    转化为Milvus支持的二进制格式
    :param sims: 列表或字符串
    :return: 适用于Milvus的二进制编码
    """
    if not isinstance(sims, list):
        if isinstance(sims, str):
            try:
                sims = [sims]
            except Exception as e:
                raise ValueError('Param type incorrect, expect {} but get {} instead'
                             .format(type(dict), type(sims)))
        else:
            raise ValueError('Param type incorrect, expect {} but get {} instead'
                         .format(type(dict), type(sims)))

    vectors = []
    for sim_str in sims:
        sim_bin = ("0"*64+bin(int(sim_str, 16)).replace('0b', ''))[-64:]
        vector = []
        i = 0
        while i < len(sim_bin):
            if i == 0:
                bin_cut8 = sim_bin[-8:]
            else:
                bin_cut8 = sim_bin[-8 - i:-i]
            i += 8
            vector.append(int(bin_cut8, 2))
        vector = vector[::-1]
        vectors.append(vector)
    # print(vectors)
    data = np.array(vectors, dtype='uint8').astype('uint8')  # 转化到10进制8位0-255片段，如[67, 248, 41, 25, 149, 231, 48, 47]

    out_list = [bytes(i) for i in data] # bytes编码
    return out_list

class MyMilvus():
    def __init__(self):
        # Milvus server IP address and port.
        self._host = conf.getConf('milvus_server', 'HOST')
        self._port = conf.getConf('milvus_server', 'PORT')
        # 实例化milvus数据库
        self.milvus = Milvus(self._host, self._port)
        # create index of vectors, search more rapidly
        self.index_param = {
            'nlist': 16384
        }

    def create_milvus(self, collection_name='simhash_collection_', dimension=64, index_file_size = 1024, \
                      metric_type=MetricType.HAMMING, indextype=IndexType.IVF_FLAT, index_param=None, is_rebuild=False):
        """
        创建collection
        :param collection_name: 数据集名称
        :param dimension: dimension of vector
        :param index_file_size: max file size of stored index,为了避免查询时未建立索引的数据文件过大,此处为1024,默认单位：MB
        :param metric_type: 度量单位
        :param rebuild: 是否重新创建同名数据集
        :return:
        """
        status, ok = self.milvus.has_collection(collection_name)

        param = {
            'collection_name': collection_name,
            'dimension': dimension,
            'index_file_size': index_file_size,  # optional
            # 'metric_type': MetricType.L2  # optional
            'metric_type': metric_type  # optional
        }
        # 没有该集合名，则创建集合
        if not ok:
            self.milvus.create_collection(param)
        else:
        # 若需重建该集合，则删除后重建
            if is_rebuild:
                status = self.milvus.drop_collection(collection_name)
                time.sleep(3)
                self.milvus.create_collection(param)

        # Show collections in Milvus server
        _, collections = self.milvus.list_collections()
        _, collection = self.milvus.get_collection_info(collection_name)
        print(collection)

        # 官方建议在插入向量之前，建议先使用 milvus.create_index 以便系统自动增量创建索引
        # 索引类型有：FLAT / IVFLAT / IVF_SQ8 / IVF_SQ8H，其中FLAT是精确索引，速度慢，但是有100%的召回率
        # You can search vectors without creating index. however, Creating index help to search faster
        status = self.milvus.create_index(collection_name, indextype, index_param)

    def create_partition(self, tag_name='test',collection_name='simhash_collection_'):
        status, ok = self.milvus.has_partition(collection_name=collection_name, partition_tag=tag_name)
        if not ok:
            self.milvus.create_partition(collection_name=collection_name, partition_tag=tag_name)
        # _, partition_list = self.milvus.list_partitions(collection_name=collection_name)
        # print(partition_list)
        # print(ok)
        # return ok

    def insert(self, vectors, ids=None, partition_tag=None, indextype=IndexType.IVF_FLAT, index_param=None, collection_name='simhash_collection_'):
        # partition_tag为None或字符串
        status, ids = self.milvus.insert(collection_name=collection_name, records=vectors, ids=ids, partition_tag=partition_tag) # ids允许为空，内部随机生成
        if not status.OK():
            print("Insert failed: {}".format(status))
        # print(status.OK())
        # 官方建议 向量插入结束后，相同的索引需要手动再创建一次.
        # status_index = self.milvus.create_index(collection_name, indextype, index_param)
        return status.OK()

    def check_partition(self, partition_tags, collection_name='simhash_collection_'):
        # 校验partitions是否存在
        out_partition_tags = []
        for part in partition_tags:
            status, ok = self.milvus.has_partition(collection_name=collection_name, partition_tag=part)
            if ok:
               out_partition_tags.append(part)
        return out_partition_tags

    def search(self, vectors, partition_tags=None, collection_name='simhash_collection_'):
        # execute vector similarity search
        # partition_tags为None或列表（分区列表）
        query_vectors = vectors
        # print(query_vectors)

        search_param = {
            "nprobe": 32
        }

        print("Searching ... ")

        param = {
            'collection_name': collection_name,
            'query_records': query_vectors,
            'partition_tags': partition_tags,
            'top_k': 3,
            'params': search_param,
        }

        try:
            status, results = self.milvus.search(**param)
            # print(status)
            # print(results)
            if status.OK():
                # print(results)
                # if results[0][0].distance == 0.0:
                #     print('Query result is correct')
                # else:
                #     print('Query result isn\'t correct')
                # print(results)
                return results
            else:
                print("Search failed. ", status)
                return None
        except Exception as e:
            print(e)
            return None

    def delete(self, id_array, collection_name='simhash_collection_'):
        status = self.milvus.delete_entity_by_id(collection_name=collection_name, id_array=id_array)
        if status.OK():
            print('Delete result is correct')
        else:
            print('Delete result isn\'t correct')
        # 1s后会自动落盘，先不手动落盘，减少性能损失。
        # self.milvus.flush(collection_name_array=[collection_name])
        return status.OK()

    def delete_partition(self, tag_name=None,collection_name='simhash_collection_'):
        status, ok = self.milvus.has_partition(collection_name=collection_name, partition_tag=tag_name)
        if ok:
            self.milvus.drop_partition(collection_name=collection_name, partition_tag=tag_name)
        _, partition_list = self.milvus.list_partitions(collection_name=collection_name)
        print(partition_list)

    def delete_collection(self, collection_name='simhash_collection_'):
        self.milvus.drop_index(collection_name=collection_name)
        self.milvus.drop_collection(collection_name=collection_name)

        # 断开连接
        self.milvus.close()

if __name__ == '__main__':
    "--------------------------------------------------"
    mymilvus = MyMilvus()
    # status, ok = mymilvus.milvus.has_collection("simhash_collection_")
    # if ok:
    #     status = mymilvus.milvus.drop_collection('simhash_collection_')
    #     print(mymilvus.milvus.list_collections())
    # mymilvus.create_milvus(collection_name='simhash_collection_', dimension=64)
    # mymilvus.create_partition(tag_name='new', collection_name='simhash_collection_')
    "---------------------------------------------------------"
    # vectors_str = ["d8cd7a9eed87d59b", "d8cd7a9eed87d69b", "d8cd7a9eed97d59b"]
    # vectors_str = ['2780af3aefade02e', '206f25d8a5a01c6b', '61153bd602a9a8ee', '244d23b4bb96ec20', '23829f28090aa043', '240ae499106cb111', '23710c04d4e929fe', '37a93d78dc04bbfd', '2a7368fd0c4cc645', '206725c8a5b0b46b', '7c0a287e854a48f8', '204725d885a01c7b', '2044cc81ad1063', '31aeebf20a3c809d', '2eb60bbc2584cacd', '38cd297c2dd9c248', '204f25d8a5a01c6b', '40244c48f2804a0', '39bdc6a284001ab1', '2daa29d31fb6c079', '2387dffb41da7fbf', '23009b5aedb6022e', '2e8499c746d55c74', '3dcb4a828e21becc', '2af978a14c279237', '2c4d23f69b02ec20', '23a00f2eeaefa828', '29b10120c500b8b2', '4180deb663bd222e', '444d23f4ab07e420', '2044cd81ad8067', '240244c49fa814a0', '2402267a4470cd6e', '2becf95d0e041ccf', '240245c48ae814a8', '2fcc6af9c344e6bf', '240204c48a4814a8', '4483dbd667cd2226', '43e7dd162e0d6220', '2712fef86eb9620e', '29d76d8a76382a16', '238633a62d7445bc', '2a00253402828e46', '2e2066254af37643', '2380fa5f6b892214', '2780bb7b4d95e2a6', '240244c48a4814a8', '2afbdec905e83a20', '3321720b8eb22e28', '704da848804a0', '2e377ebdc2b008ba', '403044dd01a5a0e3', '23ac8aca5976603c', '25301bfa84d9073b', '23829b3a0d3d2029', '2580a32bcca5882e', '21b09d4b07914857', '244724d085b0b6fb', '206684c08cf016fb']
    vectors_str = ['2088eb355f04b0ff', '2504b34d0f71a876', '299d9d4c0c602e20', '284ac50e56629c0f', '2580a32bcca5882e', '2780af2b4ef7422d', '37b23be06504f6ef', '26ae56a2e290556d', '3f9ae3aaf9a89854', '2f75d7896780ca8b', '288622a6abb2a063', '7399a3839cbb4861', '2780db1a6dbd222c', '2543664b5e25d6f6', '9941e312fb9ba271', '25a0a9d30c0880b1', '2cd12a09626d5cee', '2dbcc3c2e9a99d11', '23909ff24dbd2224', '1c46a2b6b830ac62', '3d90e3a2f98b9931', '2b331ae1cc873a31', '2b2813d2e058f869', '288222a6abb2a063', '230683b69a902422', '3b434df95cba9413', '3e22e680d8328450', '28c228d86f6163b4', '243d23f0aa02e420', '2e82be7742a47493', '2b02eb7e4d0662f5', '3fb39388b83b8002', '3d3130e3e505e023', '296827867242f4ba', '393160e38400b031', '608266b626745434', '238ecc45704c5baa', '2816bef71e80d0db', '24cc40f33257bf5d', '263609dee325b9e3', '2e1286fc4733aa0a', '2dab96dd2933c473', '2f65df9d6f80c62a', '2029f1d816a7b028', '255962fc02ee146e', '399643a2fba8222a', '449071871e6cf300', '31420ab959876457', '37cee8a346053400', '27b0bb9f6fbd2384', '2304eb2ec62dc22f', '2b7f99c1024eefb8', '2ea63ee66aa24219', '38282624d0d2daf1', '312c62a382132c20', '2832e0c60c704b0a', '344a409b6a65260c', '217d9983024cafb8', '238116fb1db25cc4', '309c10dee32ccb9a', '2f65df196f80c22a', '2f75d7916780caab', '2f75d7916780cebb']
    vectors_list = to_binary(vectors_str)

    print("待输入数据")
    # print(vectors_list)
    "-----------------------------------------------"
    # ids = [9, 7, 0]
    ids = list(range(len(vectors_str))) # 不支持非整型变量作为id
    # ids = [1]*len(vectors_str)
    print(ids)
    # print("插入Milvus数据库数据")
    # mymilvus.insert(vectors=vectors_list, ids=ids, partition_tag='new')
    "--------------------------------------"
    time.sleep(5)  # 睡眠5秒
    print("查询数据")
    vectors_str2 = "2f75d7896780ca8b"
    #
    vectors_search = to_binary(vectors_str2)
    time1 = time.time()
    print(mymilvus.search(vectors=vectors_search, partition_tags=['new']))
    print('查询耗时:{}'.format(time.time()-time1))
    time2 = time.time()
    status = mymilvus.delete(id_array=[9])
    print('删除耗时:{}'.format(time.time()-time2))
    print(status)
    status, result_vectors = mymilvus.milvus.get_entity_by_id('zhidao_baixing_question', ids=[123436])
    # mymilvus.milvus.flush(collection_name_array=['simhash_collection_'])
    # print(mymilvus.search(vectors=vectors_search, partition_tags=['new']))

    ## 实际场景
    # mymilvus.create_milvus(collection_name='zhidao_baixing_question', dimension=768, metric_type=MetricType.IP, \
    #                        indextype=IndexType.IVF_PQ, index_param={'nlist': 16384, 'm': 8}, is_rebuild=True) # 回答的集合
    # # mymilvus.milvus.drop_collection('zhidao_baixing_answer')
    # mymilvus.create_milvus(collection_name='zhidao_baixing_answer', dimension=64, metric_type=MetricType.HAMMING, \
    #                        indextype=IndexType.IVF_FLAT, index_param={'nlist': 16384}, is_rebuild=True)  # 问题的集合
    # mymilvus.create_partition(tag_name='new', collection_name='zhidao_baixing_question')
    # mymilvus.create_partition(tag_name='new', collection_name='zhidao_baixing_answer')
    # vectors_str = ['2088eb355f04b0ff', '2504b34d0f71a876', '299d9d4c0c602e20', '284ac50e56629c0f', '2580a32bcca5882e', '2780af2b4ef7422d', '37b23be06504f6ef', '26ae56a2e290556d', '3f9ae3aaf9a89854', '2f75d7896780ca8b', '288622a6abb2a063', '7399a3839cbb4861', '2780db1a6dbd222c', '2543664b5e25d6f6', '9941e312fb9ba271', '25a0a9d30c0880b1', '2cd12a09626d5cee', '2dbcc3c2e9a99d11', '23909ff24dbd2224', '1c46a2b6b830ac62', '3d90e3a2f98b9931', '2b331ae1cc873a31', '2b2813d2e058f869', '288222a6abb2a063', '230683b69a902422', '3b434df95cba9413', '3e22e680d8328450', '28c228d86f6163b4', '243d23f0aa02e420', '2e82be7742a47493', '2b02eb7e4d0662f5', '3fb39388b83b8002', '3d3130e3e505e023', '296827867242f4ba', '393160e38400b031', '608266b626745434', '238ecc45704c5baa', '2816bef71e80d0db', '24cc40f33257bf5d', '263609dee325b9e3', '2e1286fc4733aa0a', '2dab96dd2933c473', '2f65df9d6f80c62a', '2029f1d816a7b028', '255962fc02ee146e', '399643a2fba8222a', '449071871e6cf300', '31420ab959876457', '37cee8a346053400', '27b0bb9f6fbd2384', '2304eb2ec62dc22f', '2b7f99c1024eefb8', '2ea63ee66aa24219', '38282624d0d2daf1', '312c62a382132c20', '2832e0c60c704b0a', '344a409b6a65260c', '217d9983024cafb8', '238116fb1db25cc4', '309c10dee32ccb9a', '2f65df196f80c22a', '2f75d7916780caab', '2f75d7916780cebb']
    # vectors_list = to_binary(vectors_str)
    #
    # print("待输入数据")
    # print(vectors_list)
    # "-----------------------------------------------"
    # ids = list(range(len(vectors_str))) # 不支持非整型变量作为id
    # print(ids)
    # print("插入Milvus数据库数据")
    # status = mymilvus.insert(vectors=vectors_list, ids=ids, partition_tag='new', \
    #                              indextype=IndexType.IVF_FLAT, index_param={'nlist': 16384}, \
    #                              collection_name='zhidao_baixing_answer')
    # "--------------------------------------"
    # time.sleep(5)  # 睡眠5秒
    # print("查询数据")
    # vectors_str2 = "2f75d7896780ca8b"
    #
    # vectors_search = to_binary(vectors_str2)
    # print(mymilvus.search(vectors=vectors_search, partition_tags=['new'], collection_name='zhidao_baixing_answer'))

    "------------------------------------------------------------------------"
    # import random
    # import numpy as np
    # vectors_1 = [[random.random() for _ in range(10)] for _ in range(2)]
    # vectors = []
    # for vector in vectors_1:
    #     y = np.linalg.norm(vector, keepdims=True)
    #     z = vector/y
    #     # print(z)
    #     vectors.append(z.tolist())
    # print(vectors)
    # # print(vectors)
    # # You can also use numpy to generate random vectors:
    # #   vectors = np.random.rand(10000, _DIM).astype(np.float32)
    #
    # # Insert vectors into demo_collection, return status and vectors id list
    # ids = list(range(len(vectors)))
    # print(ids)
    #
    # status = mymilvus.insert(vectors=vectors, ids=ids, partition_tag='new', \
    #                          indextype=IndexType.IVF_PQ, \
    #                          index_param={'nlist': 16384, 'm': 16}, collection_name='zhidao_baixing_question')
    #
    # if not status:
    #     print("Insert failed: {}".format(status))
    # time.sleep(5)  # 睡眠5秒
    # # Flush collection  inserted data to disk.
    # # mymilvus.flush(['zhidao_baixing_question'])
    # # Get demo_collection row count
    # status, result = mymilvus.milvus.count_entities('zhidao_baixing_question')
    # # Obtain raw vectors by providing vector ids
    # status, result_vectors = mymilvus.milvus.get_entity_by_id('zhidao_baixing_question', ids[:2])
    # print(status)
    # # Use the top 10 vectors for similarity search
    # query_vectors = vectors[0:2]
    # print(query_vectors == vectors)
    # print(len(query_vectors))
    # # execute vector similarity search
    # print("Searching ... ")
    # print(mymilvus.search(vectors=query_vectors, partition_tags=['new'], collection_name='zhidao_baixing_question'))
    # print(mymilvus.search(vectors=query_vectors, collection_name='zhidao_baixing_question'))
    # print(np.dot(vectors[0],vectors[0]))

    mymilvus.create_milvus(collection_name='zhidao_baixing_question', dimension=768, metric_type=MetricType.IP, \
                           indextype=IndexType.IVF_PQ, index_param={'nlist': 16384, 'm': 16}, is_rebuild=False)  # 问题的集合
    # mymilvus.milvus.drop_collection('zhidao_baixing_answer')
    mymilvus.create_milvus(collection_name='zhidao_baixing_answer', dimension=64, metric_type=MetricType.HAMMING, \
                           indextype=IndexType.IVF_FLAT, index_param={'nlist': 16384}, is_rebuild=False)  # 回答的集合

    mymilvus.create_milvus(collection_name='zhidao_baixing_question_dev', dimension=768, metric_type=MetricType.IP, \
                           indextype=IndexType.IVF_PQ, index_param={'nlist': 16384, 'm': 16}, is_rebuild=False)  # 问题的集合
    # mymilvus.milvus.drop_collection('zhidao_baixing_answer')
    mymilvus.create_milvus(collection_name='zhidao_baixing_answer_dev', dimension=64, metric_type=MetricType.HAMMING, \
                           indextype=IndexType.IVF_FLAT, index_param={'nlist': 16384}, is_rebuild=False)  # 回答的集合
    print(mymilvus.milvus.list_collections())



