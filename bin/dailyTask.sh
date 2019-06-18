#!/usr/bin/env bash

export PATH="/home/phongdk/anaconda3/envs/demographic-ml/bin:$PATH"
WORKING_DIR="/home/phongdk/workspace/user-income/"

the_day=`date -d "$date -2 days" +"%Y-%m-%d"`
data_path='/home/phongdk/data_user_income_targeting/data/'
output_file='/home/phongdk/data_user_income_targeting/prediction/'${the_day}'.gz'
back_date=14

mkdir -p ${data_path}

cd ${WORKING_DIR}
echo "Collect data ${the_day}"
bash bin/collect_data_daily.sh ${the_day} ${data_path}

echo "Classify income for ${the_day}"
bash bin/classify_income.sh ${the_day} ${data_path} ${back_date} ${output_file}

# -------------------------------Start campaigns-------------------------------------
echo "------------------Create campaigns ----------------------"
low=1
high=2
luxury=3
cate_tmp_path='/home/phongdk/data_custom_targeting/cate_tmp'
campaigns_path='/home/phongdk/data_custom_targeting/campaigns'


cateID=16266
end_date="2019-06-30"
filename="${cate_tmp_path}/luxury_#603776_${cateID}.gz"
jsonfile="${campaigns_path}/luxury_#603776_${cateID}.json"
python src/python/main/create_campaign.py -i ${output_file} -t ${luxury} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}

cateID=16264
end_date="2019-06-30"
filename="${cate_tmp_path}/high_income_#603776_${cateID}.gz"
jsonfile="${cate_tmp_path}/high_income_#603776_${cateID}.json"
python src/python/main/create_campaign.py -i ${output_file} -t ${luxury} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}
