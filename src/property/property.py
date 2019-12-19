#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 10 15:51 2018

@author: phongdk
"""

import abc


class Property(metaclass=abc.ABCMeta):
    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def get_score(self, label):
        pass

    @abc.abstractmethod
    def get_name(self):
        pass

    @abc.abstractmethod
    def get_threshold(self):
        pass

    @abc.abstractmethod
    def set_threshold(self, threshold):
        pass
