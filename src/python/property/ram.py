#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 11 14:30 2018

@author: phongdk
"""

from property import Property


class RamSys(Property):
    def __init__(self, name):
        super().__init__(name)
        self.___score = {0: 0,
                         4000: 1,
                         8000: 2,
                         16000: 2.5}

        self.__threshold = None

    def get_score(self, label):
        for key in sorted(self.___score.keys(), reverse=True):
            if label > key:
                return self.__score[label]
        return self.___score[0]

    def get_name(self):
        return self.__name

    def get_threshold(self):
        return self.threshold

    def set_threshold(self, threshold):
        self.__threshold = threshold
