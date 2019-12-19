#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Jun 18 10:03 2019

@author: phongdk
"""

import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta

import logging
LOGGER = logging.getLogger(__name__)


def get_milestone(arr, luxury, high, middle, upper_outlier=None):
    luxury_score = np.percentile(arr, luxury)
    high_score = np.percentile(arr, high)
    medium_score = np.percentile(arr, middle)
    if upper_outlier is not None:
        outlier_score = np.percentile(arr, upper_outlier)
        return [medium_score, high_score, luxury_score], outlier_score
    return medium_score, high_score, luxury_score


def classify(x, milestones):
    '''
    :param x: a value
    :param milestones: a list [middle, high, luxury]
    :return: 0: low, 1: middle, 2: high, 3: luxury
    '''
    for (i, value) in enumerate(milestones):
        if x <= value:
            return i
    return len(milestones)


def load_data(path, filename, nrows=None):
    # be carefull with data, since it could have duplicate user_id in different day
    filepath = os.path.join(path, filename)
    LOGGER.info("Load data from file : {}".format(filepath))

    try:
        df = pd.read_csv(filepath, dtype={'user_id': str}, nrows=nrows)
    except:
        LOGGER.info("---------- Cannot load data from file : {} ---------------".format(filepath))
        df = pd.DataFrame()
    return df


def load_multiple_data(path, filename, END_DATE, num_day_back):
    dfs = []
    for day in range(num_day_back):
        date = (datetime.strptime(END_DATE, "%Y-%m-%d") - timedelta(days=day)).strftime("%Y-%m-%d")
        dfs.append(load_data(os.path.join(path, date), filename))
    dfs = pd.concat(dfs, axis=0)
    return dfs


def to_hour_distribution_df(dfs):
    '''
    :param dfs: user_id, hour, count
    :return: user_id, h_0, h_1, ... h_23 (distribution)
    '''
    dfs_by_uid = dfs.groupby(['user_id', 'hour']).sum()#.compute()
    dfs_unstack = dfs_by_uid.unstack(fill_value=0)
    dfs_results = dfs_unstack.reset_index()
    hour_columns = ['h_{}'.format(i) for i in range(24)] #list(np.arange(0,24))
    dfs_results.columns = ['user_id'] + hour_columns
    dfs_results[hour_columns] = dfs_results[hour_columns].div(dfs_results[hour_columns].sum(axis=1),
                                                              axis=0).astype(np.float16)
    dfs_results.set_index("user_id", inplace=True)
    return dfs_results


def get_code2address(filename="external_data/location/Vietnam regions.xlsx"):
    CITY = ["hà nội", "hồ chí minh", "đà nẵng", "hải phòng", "cần thơ"]
    df = pd.read_excel(filename, sheet_name='Districts')
    df.columns = ["district_id", "district_name", "province_id", "province_name"]
    df.set_index("district_id", inplace=True)
    df["district_name"] = df["district_name"].apply(lambda x: x.lower().strip())
    df["province_name"] = df["province_name"].apply(lambda x: x.lower().strip())
    df["address"] = df.apply(lambda x: x["province_name"] if x["province_name"] in CITY else x["district_name"], axis=1)
    new_dict = df["address"].to_dict()
    return new_dict
