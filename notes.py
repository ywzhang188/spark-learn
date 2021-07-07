#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='zhangyuwei37'

VectorIndexer: 自动将一些特征判定为离散化特征，并标注， 连续型特征不作处理

由于业务需求，需要对多列进行LabelEncoder编码，SparkML中是通过StringIndexer来实现LabelEncoder的，而StringIndexer是对单列操作的，
而如果循环对每列进行编码，不符合Spark的设计，效率是十分低下的，对于这样的需求，我们使用Pipeline来解决这个问题。

通过persist()或cache()将需要反复使用的数据加载在内存或硬盘当中，以备后用
cache()与.persist(storageLevel=StorageLevel(True, True, False, False, 1))效
