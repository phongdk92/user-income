#!/usr/bin/env bash

#WORKING_DIR="/home/phongdk/workspace/user-income/"
## the_day=`date -d "$date -2 days" +"%Y-%m-%d"`
#the_day='2019-06-02'
#data_path='/home/phongdk/data_user_income_targeting/data/'
#output_file='/home/phongdk/data_user_income_targeting/prediction/'${the_day}'.gz'
#back_date=1
#echo ${output_file}
#cd ${WORKING_DIR}

the_day=$1
data_path=$2
back_date=$3
output_file=$4

python src/main/classifier_daily.py  --date ${the_day} \
                                    --data_path ${data_path} \
                                    --back_date ${back_date} \
                                    --output ${output_file}
