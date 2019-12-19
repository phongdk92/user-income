#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 06 17:49 2019

@author: phongdk
"""

correct_dict_place = {
    "long xuyen township, an giang": "long xuyên, an giang",
    "chau doc, an giang": "châu đốc, an giang",
    "ba ria, bà rịa-vũng tàu": "bà rịa, bà rịa-vũng tàu",
    "di an, bình dương": "dĩ an, bình dương",
    "dau tieng, bình dương": "dầu tiếng, bình dương",
    "dong xoai, bình phước": "đồng xoài, bình phước",
    "bu dop, bình phước": "bù đốp, bình phước",
    "đồng phù, bình phước": "đồng phú, bình phước",
    "tanh linh, bình thuận": "tánh linh, bình thuận",
    "qui nhơn, bình định": "quy nhơn, bình định",
    "van canh, bình định": "vân canh, bình định",  # not exist in old database
    "bac kan, bắc kạn": "bắc kạn, bắc kạn",
    "na ri, bắc kạn": "na rì, bắc kạn",
    "pác nặm, bắc kạn": "pắc nặm, bắc kạn",
    "hoà an, cao bằng": "hòa an, cao bằng",
    "quảng yên, cao bằng": "quảng uyên, cao bằng",
    "k'bang, gia lai": "k\'bang, gia lai",
    "đắk pơ, gia lai": "đắk pơ, gia lai",  # not exist in old database
    "lạc thuỷ, hòa bình": "lạc thủy, hòa bình",
    "yên thuỷ, hòa bình": "yên thủy, hòa bình",
    "vị thuỷ, hậu giang": "vị thủy, hậu giang",
    "châu thành, hậu giang": "châu thành a, hậu giang",
    "van ninh, khánh hòa": "vạn ninh, khánh hòa",
    "thanh uyen, lai châu": "than uyên, lai châu",
    "tân thành, long an": "tân thạnh, long an",
    "thanh hóa, long an": "thạnh hóa, long an",
    "vãn lãng, lạng sơn": "văn lãng, lạng sơn",
    "thanh thuỷ, phú thọ": "thanh thủy, phú thọ",
    "tuy hoa, phú yên": "tuy hòa, phú yên",
    "tuyen hoa, quảng bình": "tuyên hóa, quảng bình",
    "tay giang, quảng nam": "tây giang, quảng nam",
    "thanh trì, sóc trăng": "thạnh trị, sóc trăng",
    "thanh hóa city, thanh hóa": "thanh hóa, thanh hóa",
    "ngọc lạc, thanh hóa": "ngọc lặc, thanh hóa",
    "bim son, thanh hóa": "bỉm sơn, thanh hóa",
    "định hoá, thái nguyên": "định hóa, thái nguyên",
    "go cong, tiền giang": "gò công, tiền giang",
    "nà hang, tuyên quang": "na hang, tuyên quang",
    "chiêm hoá, tuyên quang": "chiêm hóa, tuyên quang",
    "tam đường, vĩnh phúc": "tam dương, vĩnh phúc",
    "tam dao, vĩnh phúc": "tam đảo, vĩnh phúc",
    "mù căng trai, yên bái": "mù cang chải, yên bái",
    "krông nô, đăk nông": "krông nô, đắk nông",
    "đăk glong, đăk nông": "đắk glong, đắk nông",
    "dak song, đăk nông": "đắk song, đắk nông",
    "đăk r'lấp, đăk nông": "đắk r\'lấp, đắk nông",
    "đăk mil, đăk nông": "đắk mil, đắk nông",
    "tuy đức, đăk nông": "tuy đức, đắk nông",
    "cư jút, đăk nông": "cư jút, đắk nông",
    "krông pắk, đắk lắk": "krông pắc, đắk lắk",
    "buon ma thuot, đắk lắk": "buôn ma thuột, đắk lắk",
    "cư m'gar, đắk lắk": "cư m\'gar, đắk lắk",
    "krông buk, đắk lắk": "krông búk, đắk lắk",
    "ea h'leo, đắk lắk": "ea h\'leo, đắk lắk",
    "m'đrăk, đắk lắk": "m\'đrắk, đắk lắk",
    "lăk, đắk lắk": "lắk, đắk lắk",
    "bien hoa, đồng nai": "biên hòa, đồng nai",
    "cao lanh, đồng tháp": "cao lãnh, đồng tháp",
    "điên biên phủ, điện biên": "điện biên phủ, điện biên"
}


def normalize_name(x):
    x = x.lower().replace(' - ', '-')  # bà rịa - vũng tàu  --> bà rịa-vũng tàu
    if 'thừa thiên-huế' in x:
        x = x.replace('-', ' ')
    # for city in ['hà nội', 'hồ chí minh', 'đà nẵng', 'hải phòng', 'cần thơ']:
    #     if city in x:
    #         return city
    try:
        x = correct_dict_place[x]
    except:
        pass
    return x
