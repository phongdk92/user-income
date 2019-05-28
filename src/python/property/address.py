#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 04 10:07 2019

@author: phongdk
"""

import numpy as np
from property import Property


class Address(Property):
    def __init__(self, name):
        super().__init__()
        self.__name = name
        self.__score = {"aboard": 1.5,
                        "hà nội": 5,
                        "hồ chí minh": 5,
                        "đà nẵng": 5,
                        "hải phòng": 5,
                        "cần thơ": 5,
                        "hội an, quảng nam": 3.5,
                        "tam kỳ, quảng nam": 3.5,
                        "vũng tàu, bà rịa-vũng tàu": 3.5,
                        "bà rịa, bà rịa-vũng tàu": 3.5,
                        "huế, thừa thiên huế": 3.5,
                        "biên hòa, đồng nai": 3.5,
                        "quy nhơn, bình định": 3.5,
                        "hải dương, hải dương": 3.5,
                        "chí linh, hải dương": 3.5,
                        "nha trang, khánh hòa": 3.5,
                        "cam ranh, khánh hòa": 3.5,
                        "dĩ an, bình dương": 3.5,
                        "thủ dầu một, bình dương": 3.5,
                        "bắc giang, bắc giang": 3.5,
                        "tuy hòa, phú yên": 3.5,
                        "bắc ninh, bắc ninh": 3.5,
                        "nam định, nam định": 3.5,
                        "pleiku, gia lai": 3.5,
                        "thanh hóa, thanh hóa": 3.5,
                        "sầm sơn, thanh hóa": 3.5,
                        "thái bình, thái bình": 3.5,
                        "phan thiết, bình thuận": 3.5,
                        "vinh, nghệ an": 3.5,
                        "cửa lò, nghệ an": 3.5,
                        "ninh bình, ninh bình": 3.5,
                        "tam điệp, ninh bình": 3.5,
                        "mỹ tho, tiền giang": 3.5,
                        "bến tre, bến tre": 3.5,
                        "hà tĩnh, hà tĩnh": 3.5,
                        "vĩnh long, vĩnh long": 3.5,
                        "quảng ninh, quảng bình": 3.5,
                        "hạ long, quảng ninh": 3.5,
                        "móng cái, quảng ninh": 3.5,
                        "uông bí, quảng ninh": 3.5,
                        "tân an, long an": 3.5,
                        "sa đéc, đồng tháp": 3.5,
                        "cao lãnh, đồng tháp": 3.5,
                        "đồng hới, quảng bình": 3.5,
                        "phủ lý, hà nam": 3.5,
                        "quảng trị, quảng trị": 3.5,
                        "hưng yên, hưng yên": 3.5,
                        "bảo lộc, lâm đồng": 3.5,
                        "đà lạt, lâm đồng": 3.5,
                        "long xuyên, an giang": 3.5,
                        "châu đốc, an giang": 3.5,
                        "vĩnh yên, vĩnh phúc": 3.5,
                        "phúc yên, vĩnh phúc": 3.5,
                        "yên bái, yên bái": 3.5,
                        "rạch giá, kiên giang": 3.5,
                        "hà tiên, kiên giang": 3.5,
                        "phú quốc, kiên giang": 3.5,
                        "cà mau, cà mau": 3.5,
                        "tuyên quang, tuyên quang": 3.5,
                        "lào cai, lào cai": 3.5,
                        "trà vinh, trà vinh": 3.5,
                        "hà giang, hà giang": 3.5,
                        "phú thọ, phú thọ": 3.5,
                        "việt trì, phú thọ": 3.5,
                        "tây ninh, tây ninh": 3.5,
                        "cao bằng, cao bằng": 3.5,
                        "sóc trăng, sóc trăng": 3.5,
                        "buôn ma thuột, đắk lắk": 3.5,
                        "bạc liêu, bạc liêu": 3.5,
                        "bắc kạn, bắc kạn": 3.5,
                        "lạng sơn, lạng sơn": 3.5,
                        "lai châu, lai châu": 3.5,
                        "điện biên phủ, điện biên": 3.5,
                        "phan rang-tháp chàm, ninh thuận": 3.5,
                        "quảng ngãi, quảng ngãi": 3.5,
                        "thái nguyên, thái nguyên": 3.5,
                        "sông công, thái nguyên": 3.5,
                        "sơn la, sơn la": 3.5,
                        "đồng xoài, bình phước": 3.5,
                        "kon tum, kon tum": 3.5,
                        "gia nghĩa, đắk nông": 3.5,
                        "vị thanh, hậu giang": 3.5,
                        'others': 1.0}

        self.__threshold = 2  # threshold for is_traveller (THRESHOLD_TRAVELLER)
        # self.__score_traveller = {"traveller": 5.0,
        #                           "non-traveller": 0}
        self.__score_traveller = {1: 0,         # no-travel, one place
                                  2: 4,         # travels, two places
                                  "others": 5   # travel a lot
                                  }

    def get_score(self, list_places):
        '''
        :param list_places: list places a user visited or lived
        :return:   max score if those places
        '''
        list_scores = []
        for place in list_places:
            try:
                list_scores.append(self.__score[place])
            except:
                list_scores.append(self.__score['others'])
        score = np.max(list_scores)
        return score

    def get_score_traveller_by_locations(self, list_places):
        provinces = list(set([place.split(",")[-1] for place in list_places]))      # get set of list provinces
        return self.__score_traveller['traveller'] if len(provinces) > self.__threshold else \
            self.__score_traveller['non-traveller']

    def get_name(self):
        return self.__name

    def get_threshold(self):
        return self.__threshold

    def get_address_map_score(self):
        return self.__score

    def get_travel_map_score(self):
        return self.__score_traveller

    def set_threshold(self, threshold):
        self.__threshold = threshold



if __name__ == '__main__':
    import pandas as pd
    CITY = ["hà nội", "hồ chí minh", "đà nẵng", "hải phòng", "cần thơ"]
    df = pd.read_excel("external_data/location/Vietnam regions.xlsx", sheet_name='Districts')
    df.columns = ["district_id", "district_name", "province_id", "province_name"]
    df.set_index("district_id", inplace=True)
    df["district_name"] = df["district_name"].apply(lambda x: x.lower().strip())
    df["province_name"] = df["province_name"].apply(lambda x: x.lower().strip())

    df["address"] = df.apply(lambda x: x["province_name"] if x["province_name"] in CITY else x["district_name"], axis=1)
    new_dict = df["address"].to_dict()
    # {2, 1041000000, 1041005000, 1041005007}
    # print(df.head())
    # print(len(df))
    # print(new_dict)
    address = Address("address")
    print(address.get_score([new_dict[1043005000]]))