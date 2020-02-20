##############################################################################################################################################
### Command: nohup spark-submit --executor-memory 12G --driver-cores 1 --num-executors 30 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.maxExecutors=30 --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --executor-cores 4 full_data_merge.py <<table_name>> <<freq>> <<table_location>> <<Factor>> > w_activity_f.log 2>&1 &
### Command sample: spark-submit --executor-memory 12G --driver-cores 1 --num-executors 30 --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.maxExecutors=30 --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --executor-cores 4 full_data_merge.py test_tbl daily s3://test_bucket/test_tbl 65000
###Author: Abhishek1994chaurasiya
##############################################################################################################################################

# Importing required packages
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast
import sys
import time
import json
import os
from multiprocessing import Pool
from common import Common
obj_common = Common()

def freq_check(freq):
    if freq not in ('daily','weekly'):
        print("Incorrect Parametre: "+sys.argv[1]+", Please pass from ('daily' or 'weekly')")
        sys.exit(1)

def hadoop_remove(list_of_rm):
    '''
    This method to remove partition directory from s3 or hdfs 
    (mainly for s3 because s3 delete directory is too slow, so you can use multi threading to get the high performance)
    '''
    str_of_rm=' '.join(list_of_rm)
    try:
        os.system('hadoop fs -rm -r -skipTrash '+str_of_rm)
    except:
        print('few new partition will be created')


def full_data_merge_load(table_name,freq,table_location,main_db,delta_db,backup_db):
    spark = SparkSession.builder.appName("MERGE_"+table_name).config("fs.s3a.multiobjectdelete.enable","true").config("fs.s3a.fast.upload","true").config("spark.sql.parquet.filterPushdown", "true").config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2").config("spark.speculation","false").config("fs.s3.maxRetries","20").config("fs.s3.consistent.retryPolicyType","exponential").config("spark.sql.parquet.fs.optimized.committer.optimization-enabled","true").getOrCreate()
    
    if(len(sys.argv) !=4):
        FACT=1
        Factor=sys.argv[4]
        spark.conf.set("spark.sql.hive.convertMetastoreParquet","false")
    else:
        FACT=0

    print("table name:"+table_name)
    #reading the current metadata from MYSQL server
    mysql_query=""" select staging_backup,join_condition from full_gdm_tasks where table_name = \""""+table_name+"""\" and DAG_ID IN ('DAG_DEV','DAG_DEV_PHASE2_MERGE') """
    common_flg=obj_common.mysql_connector(mysql_query,"SELECT")
    staging_backup=common_flg[0][0]
    join_column=common_flg[0][1]
    flag=0
    print(main_db)
    target_tbl=main_db+"."+table_name
    BKP=0
    WKL=0
    """if want to add for 3 years data load logic create backup table with extension '_STAGING_BACKUP' to load delta data before 3 years into it. 
        finally once in week here we can run the _STAGING_BACKUP to load the data into main table. 
        Hence it would help in daily optimisation and once in a week (or any freq) it will sync full data. 
        If data does not matter with before 3 year of data that much """
    if (staging_backup.lower() =='yes' and table_name[-15:]== '_STAGING_BACKUP'):
        stg_tbl=delta_db+"."+table_name.split('_STAGING_BACKUP')[0]
        stg_query="select * from "+stg_tbl + " where created_on_dt <= date_sub(current_date,3*365)"
        df_stg=spark.sql(stg_query)
    elif (freq=='weekly'):
        stg_tbl=main_db+"."+table_name+"_STAGING_BACKUP"
        stg_query="select *,floor(cast(ROW_WID as bigint)/"+str(Factor)+") ROW_PARTITION_ID from "+stg_tbl
        df_stg=spark.sql(stg_query)
        dis_rowid=df_stg.select("ROW_PARTITION_ID").distinct().collect()
        list_rowid=[str(row_id["ROW_PARTITION_ID"]) for row_id in dis_rowid]
        filter_val=",".join(list_rowid)
    elif (staging_backup.lower() == 'yes' ):
        stg_tbl=delta_db+"."+table_name
        stg_query="select *,floor(cast(ROW_WID as bigint)/"+str(Factor)+") ROW_PARTITION_ID from "+stg_tbl+" where created_on_dt > date_sub(current_date,3*365)"
        df_stg=spark.sql(stg_query)
        dis_rowid=df_stg.select("ROW_PARTITION_ID").distinct().collect()
        list_rowid=[str(row_id["ROW_PARTITION_ID"]) for row_id in dis_rowid]
        filter_val=",".join(list_rowid)
    elif(not FACT):
        stg_tbl=delta_db+"."+table_name
        stg_query="select * from "+stg_tbl
        df_stg=spark.sql(stg_query)
    else:
        stg_tbl=delta_db+"."+table_name
        stg_query="select *,floor(cast(ROW_WID as bigint)/"+str(Factor)+") ROW_PARTITION_ID from "+stg_tbl
        df_stg=spark.sql(stg_query)
        dis_rowid=df_stg.select("ROW_PARTITION_ID").distinct().collect()
        list_rowid=[str(row_id["ROW_PARTITION_ID"]) for row_id in dis_rowid]
        filter_val=",".join(list_rowid)
        
    #Read the data from backup table considering DB: backup
    if (not FACT):
        main_tbl=backup_db+"."+table_name
        main_query="select * from "+main_tbl
    else:
        main_query="select * from "+backup_db+"."+table_name+" where ROW_PARTITION_ID in ("+filter_val+")"
    df_main=spark.sql(main_query)

    #if column mismatch then adding column to main table
    if len(df_main.columns) != len(df_stg.columns):
        print("cols:",df_main.columns)
        print("cols2:",df_stg.columns)
        list_main=map(str.lower,df_main.columns)
        list_stg=map(str.lower,df_stg.columns)
        new_column = list(set(list_stg) - set(list_main))
        if len(new_column) == 0 :
            print("Main Table have extra column:" + str(list(set(list_stg) - set(list_main))))
            sys.exit(1)
        df_new_column = df_stg.selectExpr(new_column)
        new_column_dtype= []
        new_column_to_add=[]
        for i in range(0,len(new_column)):
            new_column_dtype.append(df_new_column.dtypes[i][1])
        for i in range(len(new_column)):
            new_column_to_add.append("`"+ str(new_column[i]) + "` " + str(new_column_dtype[i]))
        bkp_cmd_present="alter table "+backup_db+"."+table_name+ " add columns (" + ",".join(new_column_to_add) +")"
        alter_cmd_present="alter table "+ target_tbl + " add columns (" + ",".join(new_column_to_add) +")"
        spark.sql(bkp_cmd_present)
        spark.sql(alter_cmd_present)
        df_main=spark.sql(main_query)
        spark.conf.set("spark.sql.hive.convertMetastoreParquet","false") #failure due to column added in table but not in parquet file hence added property 


    #finding dupicate column on primary key if found then remove.
    #This is not possible, but may be duplicate come any how.
    if (df_main.select(join_column).distinct().count() != df_main.select(join_column).count()):
        df_main=df_main.drop_duplicates(subset=[join_column])
    if (df_stg.select(join_column).distinct().count() != df_stg.select(join_column).count()):
        df_stg=df_stg.drop_duplicates(subset=[join_column])
    df_stg_alias=df_stg.alias("df_stg_alias")
    df_main_alias=df_main.alias("df_main_alias")
    df_rest=df_main_alias.join(broadcast(df_stg_alias),df_main_alias[join_column]==df_stg_alias[join_column],"leftanti")
    df_final=df_stg_alias.unionByName(df_rest)

    if (not FACT):
        df_final.write.parquet(table_location,'overwrite')
    else:
        list_of_rm=[table_location+'/row_partition_id='+val for val in list_rowid]
        parallel_list_of_rm=[list_of_rm[i:i+20] for i in range(0,len(list_of_rm),20)]
        pool=Pool()
        pool.map(hadoop_remove, parallel_list_of_rm)
        df_final.repartition("row_partition_id").write.partitionBy('row_partition_id').parquet(table_location,'append')
        spark.sql('msck repair table '+target_tbl)

    if (freq == 'weekly'):
        spark.sql('truncate table '+stg_tbl)

def __name__== '__main__':
    start_time=time.time()
    table_name=sys.argv[1]
    freq=sys.argv[2]
    table_location=sys.argv[3]    
    ###### Below Databases can be changed as per your need. below is just example
    main_db="warehouse"
    delta_db="staging"
    backup_db="backup"
    freq_check(freq)
    try:
        full_data_merge_load(table_name,freq,table_location,main_db,delta_db,backup_db)
    except:
        print("FAILED: Script failed")
        sys.exit(1)
    end_time=time.time()
    diff_time=end_time-start_time
    print("time taken:"+str(diff_time))
