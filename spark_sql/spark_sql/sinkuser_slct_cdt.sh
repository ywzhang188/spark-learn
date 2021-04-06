#!/bin/bash

#shang yue yue mo
v_lm=`date +%Y%m01`
lastdt=`date -d "${v_lm} last day" +%Y-%m-%d`
#lastdt="2020-07-31"                  ##########！！！！！！！！！##########
echo "lastdt:""$lastdt"

# 品类计算范围
all_code_table="app.app_vdp_sink_rule_range_da" # 本月全部待计算品类
code_table="app.app_vdp_sink_cdt_to_run"  #  目前还没有计算的品类

# 结果输出表
output_table="app.app_vdp_sink_cdt_v2"

# 需要计算的品类并写入表，便于重跑
hive -e "drop table if exists $code_table;"
pid=()
hive -e """
          create table $code_table as
          select
             scope_id
          from
             (
             select a.scope_id,label
              from
              (
             select
                concat_ws('_',item_third_cate_cd,'both',app_id) as scope_id
             from
                $all_code_table
             where
                dt = '$lastdt'
             group by
                item_third_cate_cd,app_id
              )a
              left JOIN
              (select scope_id, 1 as label from $output_table where dt='$lastdt' group by scope_id)b
             on a.scope_id=b.scope_id
              )t
          where
             label is null;
"""
# (select scope_id from app.app_vdp_sink_switching_prob where dt='$lastdt' group by scope_id)a
pid=()

#获取需要运行总三级品类数 ##########！！！！！！！！！##########
num=$(hive -e "select count(*) from $code_table;")
#num=$(hive -e "select count(distinct concat_ws('_',item_third_cate_cd,app_id)) from $all_code_table where dt = '$lastdt';")    ##########！！！！！！！！！##########
#num=161
echo "num:""$num"

# 最大并发任务数
max_task_id=$1
echo "max_task_id:""$max_task_id"

#每个任务计算的品类数(向上取整)
c=`echo "scale=2;$num/$max_task_id" | bc`
d=`echo $c|awk '{print int($1)==$1?$1:int(int($1*10/10+1))}'`
task_num=`echo ${d%.*}`
echo "task_num:""$task_num"

# 实际任务数
c=`echo "scale=2;$num/$task_num" | bc`
d=`echo $c|awk '{print int($1)==$1?$1:int(int($1*10/10+1))}'`
task_id=`echo ${d%.*}`
echo "task_id:""$task_id"

pid=()
for ((i=0;i<=task_id-1;i++));
do
   spark-submit --class sinkuser_slct.CDT4Batch_sinkuser --master yarn --deploy-mode cluster --num-executors 50 --driver-memory 20g  --executor-memory 48g --executor-cores 8 --conf yarn.nodemanager.vmem-check-enabled=false  --files $HIVE_CONF_DIR/hive-site.xml CDT-1.0-SNAPSHOT.jar $lastdt $task_num $i $output_table $code_table &
   pid[i]=$!
done

for p in ${pid[@]};
  do
    wait ${p}
  done

# 所有品类跑完，写入dt标记
sleep 10
time=`date +'%Y-%m-%d %H:%M:%S'`
echo "time:""$time"
hive -e "insert into table app.app_vdp_sink_finish_dt values ('cdt','$lastdt','$time');"
pid=()

# 删掉to run临时表
hive -e "drop table if exists $code_table;"
pid=()


