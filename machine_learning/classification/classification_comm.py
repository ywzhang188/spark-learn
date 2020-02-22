#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from machine_learning.regression.regression_comm import *

ds = spark.createDataFrame([[5.1, 3.3, 'A', 1, 1],
                            [4.9, 4.3, 'A', 0.2, 0],
                            [4.7, 2.2, 'C', 2, 2],
                            [4.6, 3.1, 'B', 1.2, 1],
                            [5., 3.5, 'A', 1.6, 1],
                            [5.4, 3.9, 'B', 0.4, 0],
                            [4.6, 4.2, "C", 0.8, 0],
                            [5., 3.4, "C", 3, 2],
                            [4.4, 3.9, "A", 0.7, 0],
                            [4.9, 2.1, "A", 2.5, 2]], ['x1', 'x2', 'x3', 'x4', 'y'])
