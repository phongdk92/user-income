#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 10 15:52 2018

@author: phongdk
"""

from property import Property


class OS_name(Property):
    def __init__(self, name):
        super().__init__()
        self.__name = name
        self.__score = {-1: 0,
                        'unknown': 0,
                        'ios': 4.5,
                        'android': 3.5,
                        'winphone': 3,
                        'macos': 5,
                        'windows': 3.2,
                        'linux': 4}

        self.__threshold = None

    def get_score(self, label):
        return self.__score[label]

    def get_name(self):
        return self.__name

    def get_threshold(self):
        return self.threshold

    def set_threshold(self, threshold):
        self.__threshold = threshold

    def get_map_score(self):
        return self.__score
