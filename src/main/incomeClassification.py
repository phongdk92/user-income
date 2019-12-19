#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Jun 04 11:28 2019

@author: phongdk
"""

import pandas as pd
import numpy as np
from utils import classify, get_milestone
import logging
LOGGER = logging.getLogger(__name__)


def load_score_data(filename):
    df = pd.read_csv(filename, dtype={'user_id': str, 'work_station': int, 'payment_score': int}, index_col='user_id',
                     nrows=None)
    return df


class IncomeClassification():
    def __init__(self, luxury=92, high=80, middle=35):
        # [26.1, 47.8, 6.7, 7.0] -> [0.29794521, 0.5456621 , 0.07648402, 0.07990868]
        self.LUXURY_PROP = luxury
        self.HIGH_PROP = high
        self.MIDDLE_PROP = middle

    def classify_income(self, df):
        LOGGER.info('-------Start classify by income -----------')
        score_columns = [x for x in df.columns if "score" in x]
        booking_columns = ['airline_score', 'resort_score', 'hotel_score', 'tour_score']

        df['booking'] = (df[booking_columns].sum(axis=1)) > 0
        df['total_score'] = df[score_columns].sum(axis=1)

        # filter users who is work station, booking (airline || resort || hotel || tour) but not shopping online
        condition = ~((df['work_station'] > 0) & (df['booking']) & (df['payment_score'] == 0))
        condition = condition & (df['total_score'] > 0)     # remove users with total_score equals 0

        income_milestones = get_milestone(df[condition]['total_score'], luxury=self.LUXURY_PROP,
                                          high=self.HIGH_PROP, middle=self.MIDDLE_PROP)
        print("MEDIUM_INCOME_SCORE : {} \nHIGH_INCOME_SOCRE: {} \nLUXURY_SCORE: {}".format(*income_milestones))
        LOGGER.info("MEDIUM_INCOME_SCORE : {} \nHIGH_INCOME_SOCRE: {} \nLUXURY_SCORE: {}".format(*income_milestones))
        df['income'] = df['total_score'].apply(lambda x: classify(x, income_milestones))
        df.loc[~condition, 'income'] = 0    # those users don't satisfy condition is set to 0 though their score is high
        print('------------ Proportion Income -----------')
        print(df['income'].value_counts(normalize=True).sort_index())
        return df


if __name__ == '__main__':
    filename = '/home/phongdk/data_user_income_targeting/score/2019-06-02.gz'
    output = '/home/phongdk/data_user_income_targeting/prediction/2019-06-02.gz'
    df = load_score_data(filename)
    income_classification = IncomeClassification()
    df = income_classification.classify(df)
    # df['income'].to_csv(output, header=True, index=True)
