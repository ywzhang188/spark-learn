#!/usr/bin/env bash

# the last day of last month
#v_lm=`date +%Y%m01`
#month_end_date=`date -d "${v_lm} last day" +%Y-%m-%d`

predict_date="2021-02-28" 
echo "-- predict_date:""$predict_date"

end_date="2021-02-21" 
echo "-- end_date:""$end_date"

end_date_1w="2021-02-14" 
echo "-- end_date_1w:""$end_date_1w"

end_date_2w="2021-02-07" 
echo "-- end_date_2w:""$end_date_2w"

input_train_data_table="tmp.tmp_qzl_sink_search_07_all_train" 
input_predict_data_table="tmp.tmp_qzl_sink_search_07_all_predict_20210228"
output_cd3_score_table="app.app_vdp_ai_sink_search_old_model_cd3_score"
output_train_result_table="app.app_vdp_ai_sink_search_old_model_train_result"
output_predict_result_table="app.app_vdp_ai_sink_search_old_model_predict_result"

# 获取需要运行总数 
# num=$(hive -e "select count(distinct item_third_cate_cd) from app.app_vdp_ai_sink_dept3_cate3_scope_mid_da where dt = '2021-01-10' and app_id = 4 and scene_id = 1 and status = 3;")
# num=$(hive -e "select count(distinct item_third_cate_cd) from $input_predict_data_table;")    
num=13
echo "num:""$num"

# # 一个子任务中计算多少个品类
batch=1

#获取迭代次数(向上取整)
c=`echo "scale=2;$num/$batch" | bc`
d=`echo $c|awk '{print int($1)==$1?$1:int(int($1*10/10+1))}'`
task_id=`echo ${d%.*}`
echo "task_id:""$task_id"

pid=()

for ((i=0;i<=task_id-1;i++));
do
   spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 16 \
    --queue root.bdp_jmart_scr_union.bdp_jmart_vdp_union.bdp_jmart_vdp_formal \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_mart_bag:latest \
    sink_search_slct_old_sku_model_train_predict.py \
    $i $batch \
    $end_date $end_date_1w $end_date_2w \
    $input_train_data_table $input_predict_data_table $output_cd3_score_table $output_train_result_table $output_predict_result_table $predict_date &
   pid[i]=$!
done

for p in ${pid[@]};
  do
    wait ${p}
  done
