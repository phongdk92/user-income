#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 14 15:59 2018

@author: phongdk
"""

from property import Property
import pandas as pd
import os
import re


class Device(Property):
    def __init__(self, name):
        super().__init__()
        self.__name = name
        self.__data_path = os.path.join(os.getcwd(), "external_data", "device")
        print(self.__data_path)
        if not os.path.exists(self.__data_path):
            print(self.__data_path)
            self.__data_path = os.path.join("../../", "external_data", "device")
            print(self.__data_path)
        self.__score_os = {-1: 0,
                           # 'unknown': 0,
                           'ios': 4,
                           'android': 3.5,
                           'winphone': 3,
                           'macos': 5}
        # 'windows': 3.2,
        # 'linux': 4}

        # Processor: G < (J=N=i3) < (m = i3*U = i5) < (E =i7 = i9 )  : Only for Intel processor
        # For AMD processor, design another dict
        self.__score_intel_processor = {'i9': 5,
                                        'i7': 5,
                                        'E': 5,
                                        'i5': 3.5,
                                        'i3U': 3.5,
                                        'm': 3.5,
                                        'i3': 2,
                                        'J': 2,
                                        'N': 2,
                                        'G': 1}

        self.__intel_processor_map = {'i9': 'i9[0-9-]+',
                                      'i7': 'i7[0-9-]+',
                                      'E': 'E[0-9-]+',
                                      'i5': 'i5[0-9-]+',
                                      'i3U': 'i3[0-9-]+U',  # check key i3U before i3
                                      'm': 'm[0-9-]+',
                                      'i3': 'i3[0-9-]+',
                                      'J': 'J[0-9-]+',
                                      'N': 'N[0-9-]+',
                                      'G': 'G[0-9-]+'}

        self.__score_hardware = {14115: 2,  # this is raw_cate from Bachan's file, try to map to score
                                 14118: 3.5,
                                 14119: 4.5}

        self.__price_ram = {512: 100000,
                            1024: 200000,
                            2048: 410000,
                            3072: 500000,
                            4096: 790000,
                            8192: 1450000,
                            'other': 3250000}

        self.__price_screen = {'1024_768': 10000,
                               '1280_1024': 1460000,
                               '1366_768': 1750000,
                               '1600_900': 1990000,
                               'other': 2650000}

        self.__price_cpu = self.__load_cpu_price()

        self.__hardware_cate = self.__load_hardware_category()

        self.__threshold = None

    def get_score(self, label):
        (os_name, hardware, CPU_model, ram, screen_height, screen_width) = label
        # print(os_name, hardware, '\n', CPU_model, ram, screen_height, screen_width)
        if os_name in ['ios', 'android', 'winphone', 'macos']:  # mobile + MacOs
            return self.__score_os[os_name]

        elif os_name in ['windows', 'linux']:
            if str(CPU_model) != '-1' and 'intel' in CPU_model.lower():  # check processor
                score = self.get_intel_processor_score(CPU_model.lower())
                if score is not None:
                    return score
            if hardware in self.__hardware_cate.index:  # check hardware
                return self.__score_hardware[self.__hardware_cate.loc[hardware].values[0]]  # map raw cate to score
            # cpu_price = self.__price_cpu.loc[CPU_model].values[0] if CPU_model in self.__price_cpu.index else 0
            # ram_price = self.get_ram_price(ram) if ram else 0
            # screen_price = self.get_screen_price(screen_height, screen_width) \
            #     if screen_height and screen_width else 0
            # if cpu_price and ram_price and screen_price:  # get all price of cpu, ram and screen
            #     return cpu_price + ram_price + screen_price
            # need a map from price to score
            return self.__score_os[-1]
        # check device information
        return self.__score_os[-1]

    def get_intel_processor_score(self, CPU_model):
        for processor in list(self.__score_intel_processor.keys()):
            if len(re.findall(pattern=self.__intel_processor_map[processor], string=CPU_model,
                              flags=re.IGNORECASE)) > 0:
                return self.__score_intel_processor[processor]
        return None

    def get_name(self):
        return self.__name

    def get_threshold(self):
        return self.threshold

    def set_threshold(self, threshold):
        self.__threshold = threshold

    def get_ram_price(self, label):
        for key in list(self.__price_ram.keys())[:-1]:
            if label <= key:
                return self.__price_ram[label]
        return self.__price_ram['other']

    def get_screen_price(self, height, width):
        if height <= 1024 and width <= 768:
            return self.__price_screen['1024_768']
        if height <= 1280 and width <= 1024:
            return self.__price_screen['1280_1024']
        if height <= 1366 and width <= 768:
            return self.__price_screen['1366_768']
        if height <= 1600 and width <= 900:
            return self.__price_screen['1600_900']
        return self.__price_screen['other']

    def __load_cpu_price(self):
        df_cpu = pd.read_csv(os.path.join(self.__data_path, 'CPU_prices.csv'), sep='\t',
                             usecols=['CPU model', 'Price, in VND'], index_col='CPU model')
        # print(df_cpu.head())
        return df_cpu

    def __load_hardware_category(self):
        df_hardware = pd.read_csv(os.path.join(self.__data_path, 'model_name_to_class.csv'), sep='\t', header=None)
        df_hardware.columns = ['model', 'raw_cate']
        df_hardware.drop_duplicates(subset=['model', 'raw_cate'], inplace=True)
        df_hardware.set_index('model', inplace=True)
        return df_hardware


if __name__ == '__main__':
    device = Device('device')
    score = device.get_score(label=('windows', None, 'intel i3-8130U (4M cache, 2 Cores, 4 Threads, 2.20 GHz)',
                                    8192, 1366, 768))
    assert (score == 3.5)
    score = device.get_score(label=('windows', None, 'intel G5500 (4M cache, 2 Cores, 4 Threads, 3.80 GHz)',
                                    8192, 1366, 768))
    assert (score == 1)
    score = device.get_score(label=('windows', None, 'intel i9-9960X (22M cache, 16 Cores, 32 Threads, 3.10 GHz)',
                                    8192, 1366, 768))
    assert (score == 5)
    score = device.get_score(label=('windows', None, 'intel i7-9700K (12M cache, 8 Cores, 8 Threads, 3.60 GHz)',
                                    8192, 1366, 768))
    assert (score == 5)
    score = device.get_score(label=('windows', None, 'intel i5-8500 (9M cache, 6 Cores, 6 Threads, 3.00 GHz)',
                                    8192, 1366, 768))
    assert (score == 3.5)
    score = device.get_score(label=('windows', None, 'intel i3-7350K (4M cache, 2 Cores, 4 Threads, 4.20 GHz)',
                                    8192, 1366, 768))
    assert (score == 2)
    score = device.get_score(label=('windows', None, 'intel J3060 (2M cache, 2 Cores, 2 Threads, up to 2.48 GHz)',
                                    8192, 1366, 768))
    assert (score == 2)
    score = device.get_score(label=('windows', None, 'intel M3-8100Y, (4M cache, 2 Cores, 4 Threads, 1.10 GHz)',
                                    8192, 1366, 768))
    assert (score == 3.5)
    score = device.get_score(label=('windows', None, 'intel N5000, 4M cache, 4 Cores, 4 Threads, 2.70 GHz, uLV)',
                                    8192, 1366, 768))
    assert (score == 2)
    score = device.get_score(label=('windows', None, 'intel E-2176M (12M cache, 6 Cores, 12 Threads, 2.70 GHz)',
                                    8192, 1366, 768))
    assert (score == 5)
    print("All tests are correct")
