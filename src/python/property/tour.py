#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 10 16:00 2018

@author: phongdk
"""
from property import Property


class Tour(Property):
    def __init__(self, name):
        super().__init__()
        self.__name = name
        self.__score = {-1: 0.0,
                        0: 0.0,
                        1: 4.0}

        self.__threshold = 8

    def get_score(self, label):
        return self.__score[label]

    def get_name(self):
        return self.__name

    def get_threshold(self):
        return self.__threshold

    def set_threshold(self, threshold):
        self.__threshold = threshold