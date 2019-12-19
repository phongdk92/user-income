#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Jun 18 11:22 2019

@author: phongdk
"""

import pandas as pd
import argparse
from datetime import date, datetime, timedelta
import os
import json


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=True, help="path to income file")
    ap.add_argument("-t", "--target", required=True, help="which type of target (1: middle, 2: high, 3: luxury)",
                    type=int)
    ap.add_argument("-c", "--cate_id", required=True, help="cate ID to add to QC", type=int)
    ap.add_argument("-o", "--output", required=True, help="path to output file")
    ap.add_argument("-j", "--json_file", required=True, help="path to output json file storing campaign's info")
    ap.add_argument("-ed", "--end_date", required=True, help="end date of a campaign")

    args = vars(ap.parse_args())
    input_filename = args['input']
    target = args['target']
    cate_id = args['cate_id']
    output_filename = args['output']
    outJson = args['json_file']
    end_date = args['end_date']
    start_date = date.today().strftime("%Y-%m-%d")

    if datetime.strptime(end_date, "%Y-%m-%d").date() >= date.today() - timedelta(days=2):
        df = pd.read_csv(input_filename, header=None, names=["browser_id", 'target'], nrows=None)
        print(df.shape)
        df = df[df['target'] == target]
        df['cateID'] = cate_id
        print(df.head())
        print('numer of users for target {} :{}'.format(target, df.shape))
        df[['browser_id', 'cateID']].to_csv(output_filename, sep=' ', compression='gzip', index=False, header=None)

        config = {"name": os.path.basename(output_filename),
                  "category_id": cate_id,
                  "start_date": start_date,
                  "end_date": end_date,
                  "is_runnable": False
                  }

        print(outJson)
        with open(outJson, 'w') as fp:
            json.dump(config, fp)

    else:
        print('CateID {}, End date {} is over. Today is {}'.format(cate_id, end_date, start_date))

