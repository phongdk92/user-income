#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Dec 06 16:24 2018

@author: phongdk
"""

LOCATION = {'Hanoi': 5.0,
            'Danang': 5.0,
            'HCM': 5.0,
            'city': 3.0,
            'others': 1.0}

# AGE = {0: 1,
#        25: 2,
#        55: 5,
#        100: 2}

AGE = {0: 1,    # 0-14
       1: 1,    # 15-17
       2: 2,    # 18-25
       3: 5,    # 26-35
       4: 5,    # 36-45
       5: 5,    # 46-55
       6: 1,    # 55+
       -1: 0}

GENDER = {0: 5.0,
          1: 4.0,
          -1: 0.0}

OS_NAME = {'macos': 5,
           'ios': 4.5,
           'android': 4.0,
           'windows': 3.5}

HARDWARE = {}

AIRLINE = {-1: 0.0,
           0: 0.0,
           1: 5.0}

LUXURY = {-1: 0.0,
          0: 0.0,     # non-luxury
          1: 5.0}     # luxury


BOOKING_RESORT = {-1: 0.0,
                    0: 0.0,      # no-booking
                    1: 5.0}      # booking

BOOKING_HOTEL = {-1: 0.0,
                0: 0.0,      # no-booking
                1: 4.0}      # booking

TOUR = {-1: 0.0,
        0: 0.0,
        1: 4.0}

SHOPPING = {-1: 0.0,
            0: 0.0,     # no-shopping
            1: 3.0}     # shopping

map_score = {'age': AGE,
             'gender': GENDER,
             'os': OS_NAME,
             'airline': AIRLINE,
             'luxury': LUXURY,
             'booking_resort': BOOKING_RESORT,
             'booking_hotel': BOOKING_HOTEL,
             'tour': TOUR,
             'shopping': SHOPPING}

THRESHOLD = {'airline': 15,
             'luxury': 10,
             'booking_resort': 8,
             'booking_hotel': 10,
             'tour': 8,
             'shopping': 20}

ram_price = {512: 1,
             1024: 1.5,
             2048: 2,
             3072: 2.5,
             4096: 3,
             8192: 4,
             'other': 5}

# return 100_000 if ($ram <= 512);
# return 200_000 if ($ram <= 1024);
# return 410_000 if ($ram <= 2048);
# return 500_000 if ($ram <= 3072);
# return 790_000 if ($ram <= 4096);
# return 1_450_000 if ($ram <= 8192);
# return 3_250_000;

screen_price = {'1024_768': 1,
                '1280_1024': 2,
                '1366_768': 3,
                '1600_900': 3.5,
                'other': 4.5}

# return 10_000 if ($w <= 1024 and $h <= 768);
# return 1_460_000 if ($w <= 1280 and $h <= 1024);
# return 1_750_000 if ($w <= 1366 and $h <= 768);
# return 1_990_000 if ($w <= 1600 and $h <= 900);
# return 2_650_000;
