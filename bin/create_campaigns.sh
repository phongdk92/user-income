#!/bin/bash

# -------------------------------Start campaigns-------------------------------------

output_file=$1

echo "------------------Create campaigns ----------------------"
medium=1
high=2
luxury=3

cate_tmp_path='/home/phongdk/data_custom_targeting/cate_tmp'
campaigns_path='/home/phongdk/data_custom_targeting/campaigns'


cateID=16397
end_date="2019-12-31"
filename="${cate_tmp_path}/USEG-241#${cateID}.gz"
jsonfile="${campaigns_path}/USEG-241#${cateID}.json"
python src/main/create_campaign.py -i ${output_file} -t ${medium} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}

#cateID=16421
#end_date="2019-12-15"
#filename="${cate_tmp_path}/BI-452#${cateID}.gz"
#jsonfile="${campaigns_path}/BI-452#${cateID}.json"
#python src/main/create_campaign.py -i ${output_file} -t ${high} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}
#
#cateID=16422
#end_date="2019-12-15"
#filename="${cate_tmp_path}/BI-452#${cateID}.gz"
#jsonfile="${campaigns_path}/BI-452#${cateID}.json"
#python src/main/create_campaign.py -i ${output_file} -t ${luxury} -c ${cateID} -o ${filename} -j ${jsonfile} -ed ${end_date}
