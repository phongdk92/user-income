#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 10 16:05 2018

@author: phongdk
"""

from property import Property


class Luxury(Property):
    def __init__(self, name):
        super().__init__()
        self.__name = name
        self.__score = {-1: 0.0,
                        0: 0.0,  # no-luxury
                        1: 5.0}  # luxury

        self.__threshold = 10

    def get_score(self, label):
        return self.__score[int(label)]

    def get_name(self):
        return self.__name

    def get_threshold(self):
        return self.__threshold

    def set_threshold(self, threshold):
        self.__threshold = threshold
