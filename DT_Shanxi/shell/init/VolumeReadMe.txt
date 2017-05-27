一、本版本实现功能
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
二、发布文件说明：
   1、DT_Shanxi-1.0-SNAPSHOT.jar 为任务jar包，放入 /dt/bin目录下；
   2、DT_Core-1.0-SNAPSHOT.jar、DT_Spark-1.0-SNAPSHOT.jar 这两个jar包为辅助jar包，放入spark安装目录下的jars目录下；
   3、VolumeAnalyseInitTable.sh 为hive建表语句，执行时须传入两个参数，第一个参数为数据库名称（如果不存在，可自动创建），
      第二个 参数为hive外部表要使用的路径，即山西原始数据所在路径。  
      例如： sh VolumeAnalyseInitTable.sh shanxi hdfs://dtcluster/datang2
   4、VolumeAnalyseHDFS2db.sh 为sqoop脚本，用于hdfs导入Oracle数据库，此脚本主要用来被其他脚本调用。
   5、ScheduleHDFS2DB.sh 为调用sqoop脚本，每小时调用一次，用于将容量的小时级分析数据导入Oracle，
     须传入三个参数，第一个为日期，
     第二个小时，第三个数据库名称
   6、ScheduleHDFS2DB_day.sh     为调用sqoop脚本，每天调用一次，用于将容量的小时级分析数据导入Oracle，须传入三个参数，第一个为日期，
     第二个小时，第三个数据库名称
三、操作步骤：
1、首先建hive表，hive建表脚本需要指定数据库，例如：sh VolumeAnalyseInitTable.sh shanxi
2、然后将DT_Shanxi-1.0-SNAPSHOT.jar包放入/dt/bin目录下面，将DT_Core-1.0-SNAPSHOT.jar、DT_Spark-1.0-SNAPSHOT.jar 放入spark安装目录下的jars目录下
3、执行VolumeRun.sh脚本参数顺序为：
    日期 小时  源数据库  目标数据库  master地址 数据库地址   数据存放路径
 示例：sh VolumeRun.sh   20170227 09 shanxi shanxi spark://172.30.4.189:7077 172.30.4.159:1521/umv602 datang
 4、每小时分析任务完成后，要执行ScheduleHDFS2DB.sh 将小时分析数据导入Oracle数据库
 5、每天的分析任务完成后，要执行ScheduleHDFS2DB_day.sh 将天级分析数据导入Oracle数据库

 四、用到Oracle中的工参表包括：

   1、小区表 ltecell
   2、视图：gt_publicandprofess_new_cell（山西环境已经有了）
   3、门限配置表 gt_capacity_config （山西环境还没有）
   4、专网小区表 t_profess_net_cell （山西环境已经有了）
五、结果输出表

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

