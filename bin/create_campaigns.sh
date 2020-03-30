#!/bin/bash

# -------------------------------Start campaigns-------------------------------------

output_file=$1

echo "------------------Create campaigns ----------------------"
low=0
medium=1
high=2
luxury=3

cate_tmp_path='/home/phongdk/data_custom_targeting/cate_tmp'
campaigns_path='/home/phongdk/data_custom_targeting/campaigns'

cateID=14115
end_date="2020-12-31"
filename="${cate_tmp_path}/Permanent#${cateID}.gz"
jsonfile="${campaigns_path}/Permanent#${cateID}.json"
python src/main/create_campaign.py -i ${output_file} -t ${low} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}

cateID=14118
end_date="2020-12-31"
filename="${cate_tmp_path}/Permanent#${cateID}.gz"
jsonfile="${campaigns_path}/Permanent#${cateID}.json"
python src/main/create_campaign.py -i ${output_file} -t ${medium} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}

cateID=14119
end_date="2020-12-31"
filename="${cate_tmp_path}/Permanent#${cateID}.gz"
jsonfile="${campaigns_path}/Permanent#${cateID}.json"
python src/main/create_campaign.py -i ${output_file} -t ${high} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}

cateID=13993
end_date="2020-12-31"
filename="${cate_tmp_path}/Permanent#${cateID}.gz"
jsonfile="${campaigns_path}/Permanent#${cateID}.json"
python src/main/create_campaign.py -i ${output_file} -t ${luxury} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}
