
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 18 10:56 2019

@author: phongdk
"""


import os
import gc
import time
import numpy as np
import pandas as pd
import sys

sys.path.append('src/python/property')
sys.path.append('src/python/administrativeArea')

import warnings

warnings.filterwarnings("ignore")

# from score import map_score
from airline import Airline
from hotel import Hotel
from luxury import Luxury
from tour import Tour
from shopping import Shopping
from resort import Resort
from gender import Gender
from age import Age
# from os_name import OS_name
from device import Device
from address import Address
from vietnamADM import AdministrativeArea_KDTree

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


def load_data(path, filename, nrows=None):
    # be carefull with data, since it could have duplicate user_id in different day
    print("Load data from file : {}".format(filename))
    df = pd.read_csv(os.path.join(path, filename), dtype={'user_id': str}, nrows=nrows)
    return df


def process_location(path, filename, property, nrows=None):
    print("---------------------------PROCESS LOCATION------------------")
    df = load_data(path, filename, nrows=nrows)
    vn_adm = AdministrativeArea_KDTree(VNM_ADM_PATH, level=0)

    #address = df.apply(lambda x: vn_adm.find_address(x['lat'], x['lon']), axis=1)
    print('FINDING : Address')
    df_dask = dd.from_pandas(df, npartitions=N_JOBS)    # convert to DASK Dataframe
    address = df_dask.apply(lambda x: vn_adm.find_address(x['lat'], x['lon']), axis=1).compute()

    print('Done : Address')
    df['address'] = address #df.apply(lambda x: vn_adm.find_address(x['lat'], x['lon']), axis=1)
    df = df[['user_id', 'lat', 'lon', 'address']].drop_duplicates(subset=['user_id', 'address'])
    df.to_csv(os.path.join(path, 'location_evaluation.csv.gz'), compression='gzip', index=False)
    return df


def process():
    start = time.time()
    df_location = process_location(PATH, filename_location, Address('address'), nrows=None)  # dask
    print("Time ------------------------------------------- {:.2f} (s)".format(time.time() - start))


if __name__ == '__main__':
    from_date = '2018-11-14'
    end_date = '2018-11-27'
    PATH = '/home/phongdk/data_user_income_targeting'
    VNM_ADM_PATH = '/home/phongdk/VNM_adm/'

    #vn_adm = AdministrativeArea(VNM_ADM_PATH)

    # have to put cluster, client inside main function
    cluster = LocalCluster() # ip="0.0.0.0")
    client = Client(cluster)
    print(client)
    N_JOBS = 32

    if not os.path.exists(PATH):
        os.makedirs(PATH)

    filename_location = "location_from_{}_to_{}.csv.gz".format(from_date, end_date)
    process()
