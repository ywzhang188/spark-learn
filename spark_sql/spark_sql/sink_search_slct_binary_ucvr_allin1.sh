#!/usr/bin/env bash

# the last day of last month
#v_lm=`date +%Y%m01`
#month_end_date=`date -d "${v_lm} last day" +%Y-%m-%d`

predict_date="2021-05-02"  # 2021-05-02 2021-05-09
echo "-- predict_date:""$predict_date"

end_date="2021-04-25"  # 2021-04-25 2021-05-02
echo "-- end_date:""$end_date"

end_date_1w="2021-04-18" # 2021-04-18 2021-04-25
echo "-- end_date_1w:""$end_date_1w"

end_date_2w="2021-04-11" # 2021-04-11 2021-04-18
echo "-- end_date_2w:""$end_date_2w"

input_train_data_table="tmp.tmp_qzl_sink_search_07_all_train_20210502"  # tmp_qzl_sink_search_07_all_train_20210509
input_predict_data_table="tmp.tmp_qzl_sink_search_07_all_predict_20210502" # tmp_qzl_sink_search_07_all_predict_20210509
output_predict_result_table="app.app_vdp_ai_sink_search_binary_ucvr_predict_result"
result_type_binary="binary_prob"
result_type_ucvr="ucvr"
sku_type_old="old"
sku_type_new="new"
sku_type_all="all"

# binary old 
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 16 \
    --queue root.bdp_jmart_scr_union.bdp_jmart_vdp_union.bdp_jmart_vdp_formal \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    sink_search_slct_binary_ucvr_predict.py \
    $result_type_binary $sku_type_old \
    $end_date $end_date_1w $end_date_2w \
    $input_train_data_table $input_predict_data_table $output_predict_result_table $predict_date &
pid[1]=$!

# # binary new 
# spark-submit \
    # --master yarn \
    # --deploy-mode cluster \
    # --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 16 \
    # --queue root.bdp_jmart_scr_union.bdp_jmart_vdp_union.bdp_jmart_vdp_formal \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # sink_search_slct_binary_ucvr_predict.py \
    # $result_type_binary $sku_type_new \
    # $end_date $end_date_1w $end_date_2w \
    # $input_train_data_table $input_predict_data_table $output_predict_result_table $predict_date &
# pid[2]=$!

# # binary all
# spark-submit \
    # --master yarn \
    # --deploy-mode cluster \
    # --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 16 \
    # --queue root.bdp_jmart_scr_union.bdp_jmart_vdp_union.bdp_jmart_vdp_formal \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # sink_search_slct_binary_ucvr_predict.py \
    # $result_type_binary $sku_type_all \
    # $end_date $end_date_1w $end_date_2w \
    # $input_train_data_table $input_predict_data_table $output_predict_result_table $predict_date &
# pid[3]=$!

# ucvr old 
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 16 \
    --queue root.bdp_jmart_scr_union.bdp_jmart_vdp_union.bdp_jmart_vdp_formal \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    sink_search_slct_binary_ucvr_predict.py \
    $result_type_ucvr $sku_type_old \
    $end_date $end_date_1w $end_date_2w \
    $input_train_data_table $input_predict_data_table $output_predict_result_table $predict_date &
pid[4]=$!

# # ucvr new 
# spark-submit \
    # --master yarn \
    # --deploy-mode cluster \
    # --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 16 \
    # --queue root.bdp_jmart_scr_union.bdp_jmart_vdp_union.bdp_jmart_vdp_formal \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # sink_search_slct_binary_ucvr_predict.py \
    # $result_type_ucvr $sku_type_new \
    # $end_date $end_date_1w $end_date_2w \
    # $input_train_data_table $input_predict_data_table $output_predict_result_table $predict_date &
# pid[5]=$!

# # ucvr all 
# spark-submit \
    # --master yarn \
    # --deploy-mode cluster \
    # --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 16 \
    # --queue root.bdp_jmart_scr_union.bdp_jmart_vdp_union.bdp_jmart_vdp_formal \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    # --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    # sink_search_slct_binary_ucvr_predict.py \
    # $result_type_ucvr $sku_type_all \
    # $end_date $end_date_1w $end_date_2w \
    # $input_train_data_table $input_predict_data_table $output_predict_result_table $predict_date &
# pid[6]=$!

# for p in ${pid[@]};
  # do
    # wait ${p}
  # done
