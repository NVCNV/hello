（一）高铁系统容量版本：
1、本版本实现功能
实现了《高铁系统容量模块需求》文档第4节的后台统计功能：
	4      后台统计表
	4.1       VOLTE用户表
	4.2       高铁用户识别表
	4.3       小区统计表（分钟级）
	4.4       子脉冲统计表（分钟级）
	4.5       脉冲基础信息统计表
	4.6       脉冲用户明细表
	4.7       脉冲负载均衡表
	4.8       高铁用户占用频段表
	4.9       高铁冲击力小区大天级表
	4.10     平时普通用户多小区天级表
	4.11     高铁用户脉冲时长异常小区天级表
	4.12     负载均衡效果差小区天级表
2、发布文件说明：
   2.1 高铁原有功能说明
      bin 目录为脚本目录，放在/dt目录下
      2.1.1 bin 目录下repeat_analy.sh为小时级分析调度脚本（每小时调度一次），需传入两个参数  日期 小时
      2.1.2 bin 目录下repeat_analy_day.sh为天级分析调度脚本（每天调度一次），需传入一个参数 日期 
      2.1.3 bin 目录下hdfs2db.sh为sqoop脚本（不需要单独调用，已集成到其他脚本）
   2.2 jar包说明：
    2.2.2 dt_mobile.jar 为高铁用户识别、上下车、车次识别、同车任务jar包，放入/dt/lib下面；
    2.2.2 DT_Shanxi-1.0-SNAPSHOT.jar 为容量任务jar包，放入 /dt/lib目录下；
    2.2.3 DT_Core-1.0-SNAPSHOT.jar、DT_Spark-1.0-SNAPSHOT.jar 这两个jar包为辅助jar包，放入spark安装目录（/opt/app/spark/jars)下；
  
   2.3 脚本说明
   2.3.1 VolumeAnalyseInitTable.sh 为hive建表语句，执行时须传入四个参数，第一个参数为目标数据库名称（如果不存在，可自动创建），
       第二个 参数为hive外部表要使用的路径，即山西结果数据所在路径。第三个为原始数据数据库，第四个为原始数据路径  
       例如： sh VolumeAnalyseInitTable.sh shanxi /datang2/output/xdrnew  ShanxiBase   hdfs://dtcluster/datang2
   2.3.2 VolumeAnalyseHDFS2db.sh 为sqoop脚本，用于hdfs导入Oracle数据库，此脚本主要用来被其他脚本调用。
   2.3.3 ScheduleHDFS2DB.sh 为调用sqoop脚本，每小时调用一次，用于将容量的小时级分析数据导入Oracle，
       须传入三个参数，第一个为日期，
       第二个小时，第三个数据库名称
   2.3.4 ScheduleHDFS2DB_day.sh     为调用sqoop脚本，每天调用一次，用于将容量的小时级分析数据导入Oracle，须传入三个参数，第一个为日期，
     第二个小时，第三个数据库名称

3、操作步骤：
   3.1、首先建hive表，hive建表脚本需要指定数据库，例如：sh VolumeAnalyseInitTable.sh shanxi
   3.2、然后将DT_Shanxi-1.0-SNAPSHOT.jar包放入/dt/lib目录下面，将DT_Core-1.0-SNAPSHOT.jar、DT_Spark-1.0-SNAPSHOT.jar 放入spark安装目录（/opt/app/spark/jars)下
   3.3任务调度
   3.3.1 执行repeat_analy.sh为小时级分析调度脚本（每小时调度一次），完成高铁用户识别、上下车、车次识别、同车任务
   3.3.2执行VolumeRun.sh调度脚本，完成高铁系统容量功能
    日期 小时  源数据库  目标数据库  master地址 数据库地址   数据存放路径
     示例：sh VolumeRun.sh   20170227 09 shanxi shanxi spark://172.30.4.189:7077 172.30.4.159:1521/umv602 datang
   3.4、每小时分析任务完成后，要执行ScheduleHDFS2DB.sh 将小时分析数据导入Oracle数据库

   3.5、执行repeat_analy_day.sh完成天级车次、上下车、u4任务（sqoop脚本已集成，自动导数据）
   3.6、执行ScheduleHDFS2DB_day.sh 将容量天级分析数据导入Oracle数据库

 4、用到Oracle中的工参表包括：

   4.1、小区表 ltecell
   4.2、视图：gt_publicandprofess_new_cell（山西环境已经有了）
   4.3、门限配置表 gt_capacity_config （山西环境还没有）
   4.4、专网小区表 t_profess_net_cell 

5、结果输出表
	gt_balence_baseday
	gt_pulse_detail
	gt_commusermore_baseday
	gt_freq_baseday
	gt_highattach_baseday
	gt_overtimelen_baseday
	gt_pulse_cell_base60
	gt_pulse_load_balence60
	gt_pulse_detail_base60
	gt_shorttimelen_baseday
	gt_pulse_cell_min
	volte_user_data

6、注意事项
    6.1、由于容量的业务是基于基于高铁用户识别的，因此必须保证高铁用户识别有数据。
    6.2、容量后面的业务是基于高铁用户识别表的，所以其他业务要延迟一个小时执行，故第一次运行，只有volte_user_data有数据，其他输出表要在第二次跑完才会有数据生成。
    6.3、高铁用户识别、上下车、车次识别、同车任务调度脚本和sqoop调度脚本写在一起，因此不需要单独调用。


（二）业务面统计功能：

1、本版本实现功能
1.1、辽宁、山西业务面功能两个版本合并为一个版本
1.2、新增业务面KPI统计指标实现
1.3、原有KPI需要变更实现
1.4、业务类型占比分析实现

2、发布文件说明：
    2.1 把shell放在/dt目录下
   2.2 DT_shanxiUserKpi-1.0-SNAPSHOT.jar 为任务jar包，放入 /dt/lib目录下；
   2.3 先创建database(create databases xxxx),VolumeAnalyseInitTable.sh 为hive建表语句，执行时须传入两个参数，第一个参数为数据库名称（如果不存在，可自动创建），
       第二个 参数为hive外部表要使用的路径，即山西原始数据所在路径。  
       例如： sh VolumeAnalyseInitTable.sh shanxi hdfs://dtcluster/datang2
   2.4 VolumeAnalyseHDFS2db.sh 为sqoop脚本，用于hdfs导入Oracle数据库，此脚本主要用来被其他脚本调用。
   2.5 KPIhdfs2db.sh为调用sqoop脚本，每小时调用一次，用于业务面小时级分析数据导入Oracle，
       须传入三个参数，第一个为日期，
       第二个小时，第三个数据库名称
   2.6 KPIhdfs2db_day.sh为调用sqoop脚本，每天调用一次，用于将业务面小时级分析数据导入Oracle，须传入三个参数，第一个为日期，
      第三个数据库名称
注：所有文件需加入hadoop组，并赋可执行权限

3、操作步骤：
3.1、首先建hive表，hive建表脚本需要指定数据库，例如：sh VolumeAnalyseInitTable.sh shanxi(与容量建表语句相同，可不用重复执行)
3.2、然后将DT_shanxiUserKpi-1.0-SNAPSHOT.jar包放入/dt/lib目录下面
3.3、运行BusKpi.sh，脚本参数顺序为：
日期 小时  master地址  源hive数据库  目标hive数据库    Oracle数据库地址   原始数据存放路径 版本类型
示例：./BusKpi.sh 20170421 09 172.30.4.189 liaoning morpho3 172.30.4.159:1521/umv602 datang 1
版本类型说明：0---辽宁版本
               1---山西版本
3.4、执行KpiException2db.sh、KpiException2dbday.sh，将分析结果导入数据库。脚本参数顺序为：
KpiException2db.sh 天 小时
KpiException2dbday.sh 天

4、用到的表
工参表包括：
         Ltecell  小区工参表
         Warnningtable 小区告警表

原始数据表：
lte_mro_source
tb_xdr_ifc_http

结果表有：
cell_day_http
cell_hour_http
imsi_cell_day_http
imsi_cell_hour_http
sgw_day_http
sgw_hour_http
sp_day_http
sp_hour_http
tac_day_http
tac_hour_http
ue_day_http
ue_hour_http
t_xdr_event_msg
business_type_detail

注意事项：
该版本信令面KPI统计功能暂时不提交！！