#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 11 14:43 2018

@author: phongdk
"""

from property import Property


class Age(Property):
    def __init__(self, name):
        super().__init__()
        self.__name = name
        self.__score = {-1: 0,  # unknown
                        0: 1,  # 0-14
                        1: 1,  # 15-17
                        2: 2,  # 18-25
                        3: 4.8,  # 26-35
                        4: 4.9,  # 36-45
                        5: 5.0,  # 46-55
                        6: 1}  # 55+
        self.__threshold = None

    def get_score(self, label):
        return self.__score[label]

    def get_name(self):
        return self.__name

    def get_threshold(self):
        return self.__threshold

    def set_threshold(self, threshold):
        self.__threshold = threshold
