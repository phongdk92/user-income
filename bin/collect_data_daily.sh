#!/usr/bin/env bash

#WORKING_DIR="/home/phongdk/workspace/user-income/"
#the_day=`date -d "$date - 2 days" +"%Y-%m-%d"`
#data_path='/home/phongdk/data_user_income_targeting/data/'
#mkdir -p ${data_path}
#cd ${WORKING_DIR}
#echo ${the_day}

the_day=$1
data_path=$2

python src/db/collect_data_daily.py  --date ${the_day} \
                                     --data_path ${data_path}

