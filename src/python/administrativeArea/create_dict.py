#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 06 17:22 2019

@author: phongdk
"""

import os
import time
import gc
import numpy as np
import pandas as pd

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from vietnamADM import AdministrativeArea

import warnings
warnings.filterwarnings("ignore")

NUM_ROUND = 5   # this have to be the same else self.NUM_ROUND in vietnamADM.py

if __name__ == '__main__':
    cluster = LocalCluster(ip="0.0.0.0")
    client = Client(cluster)
    print(client)

    N_JOBS = 36
    VNM_ADM_PATH = '/home/phongdk/VNM_adm/'
    vn_adm = AdministrativeArea(VNM_ADM_PATH)
    location_filename = '/home/phongdk/data_user_income_targeting/location_from_2018-11-14_to_2018-11-27.csv.gz'
    df = pd.read_csv(location_filename, usecols=['lat', 'lon'], dtype={'lat': float, 'lon': float})

    df['lat'] = df['lat'].apply(lambda x: round(x, NUM_ROUND))
    df['lon'] = df['lon'].apply(lambda x: round(x, NUM_ROUND))
    print(f'Before removing duplicates lat lon (rounded {NUM_ROUND}) : {df.shape}')
    df = df.drop_duplicates(subset=['lat', 'lon'])
    print(f'After removing duplicates lat lon (rounded {NUM_ROUND}) : {df.shape}')

    print(df.head())

    df_dask = dd.from_pandas(df, npartitions=N_JOBS)
    address = df_dask.apply(lambda x: vn_adm.find_address(x['lat'], x['lon']), axis=1).compute()
    df_dask = df_dask.assign(**{'address': address})

    df_vn = df_dask[df_dask['address'] != 'aboard'].compute()
    df_vn.to_csv('external_data/location/lat_lon_to_location.csv.gz', compression='gzip', index=False)
