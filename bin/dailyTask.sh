#!/usr/bin/env bash

export PATH="/home/phongdk/anaconda3/envs/demographic-ml/bin:$PATH"
WORKING_DIR="/home/phongdk/workspace/user-income/"

the_day=`date -d "$date -1 days" +"%Y-%m-%d"`
#the_day=$1
data_path='/home/phongdk/data_user_income_targeting/data/'
output_file='/home/phongdk/data_user_income_targeting/prediction/'${the_day}'.gz'
back_date=13

mkdir -p ${data_path}

cd ${WORKING_DIR}
echo "Collect data ${the_day}"
bash bin/collect_data_daily.sh ${the_day} ${data_path}

echo "Classify income for ${the_day}"
bash bin/classify_income.sh ${the_day} ${data_path} ${back_date} ${output_file}

bash bin/create_campaigns.sh ${output_file}


#for the_day in 2019-12-{04..11}; do bash bin/dailyTask.sh ${the_day}; done
