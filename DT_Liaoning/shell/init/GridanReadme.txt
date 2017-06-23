一、这个版本实现的功能：

	lte_mr_weakcover_ana(); 弱覆盖分析，关联用户分析

	lte_mr_overcover_ana(begintime_,i_enodebid);过覆盖,NewMR分析

	lte_mr_weakgridcover_ana();弱覆盖栅格，重叠覆盖栅格，MR栅格，MR分析，重叠覆盖

	disturbAnalysis(begintime_,endTime_,1,anahour); 干扰分析

	disturbMixAna(i_enodebid,begintime_,endTime_,1,anahour);干扰分析

	disturbSecAna(begintime_,endTime_,1,anahour); 干扰分析

	LTE_MRO_ADJ_COVER_ANA(begintime_,endTime_,anahour); 干扰分析

	LTE_MRO_DISTURB_PRETREATE(begintime_,i_enodebid); pci优化分析




二、发布文件说明：
（1）DT_Liaoning-1.0-SNAPSHOT.jar 为任务jar包
（2）GridanInitTable.sh 为hive建表语句
（3）GridanRun.sh 为任务调度脚本。
（4）DT_Core-1.0-SNAPSHOT.jar、DT_Spark-1.0-SNAPSHOT.jar 为辅助jar包，放到spark的安装目录下的jars目录
 (5) 为sqoop导数据脚本 被其他脚本调用
 (6) GridScheduleHDFS2DB.sh 为sqoop调度脚本，每个小时调度一次

三、操作步骤：
1、首先建hive表，hive建表脚本需要传入参数(第一个为结果表数据库，第二个为原始表数据库，第三个为外部表路径)，例如：sh GridanInitTable.sh  morpho liaoning datang
2、然后将DT_Liaoning-1.0-SNAPSHOT.jar包放入/dt/lib目录下面
3、在Oracle创建grid_view视图（创建语句在下面）
3、执行GridanRun.sh脚本参数顺序为：
    日期 小时  源数据库  目标数据库  master地址 数据库地址   数据存放路径
 示例：20170512 11 liaoning morpho2 spark://172.30.4.189:7077 172.30.4.187:1521/morpho0307  datang
4、将公参数据grid放入到hive表grid_view表路径下。
5、 执行GridScheduleHDFS2DB.sh 参数顺序为：日期 小时 hive结果数据库 Oracle地址 脚本路径
四、用到Oracle中的工参表包括：
	grid_view
	ltecell
	lte2lteadj
	ltecover_degree_condition
	ltedisturb_degree_condition
	ltemrsegment_config
	ltemrsegment_type
	lteadjcell_degree_condition

五、结果输出表

    lte_mro_disturb_ana
    lte_mro_disturb_mix
    lte_mro_disturb_sec
    lte_mrs_dlbestrow_grid_ana60
    lte_mro_overlap_grid_ana60
    grid_ltemrkpi60
    cell_ltemrkpitemp
    lte_mro_overlap_b_ana60
    lte_mro_adjcover_ana60
    lte_mrs_overcover_ana60
    LTE_MRO_DISTURB_PRETREATE60
    lte_mrs_dlbestrow_ana60
    LTE_MRO_JOINUSER_ANA60
    cell_ltenewmrkpi60



附录：

创建 grid_view 视图：

CREATE VIEW grid_view
AS SELECT OBJECTID,m.shape.entity as shapeentity,m.shape.numpts as shapenumpts,m.shape.minx as shapeminx,m.shape.miny as shapeminy,m.shape.maxx as shapemaxx,
m.shape.maxy as shapemaxy,m.shape.minz as shapeminz,m.shape.maxz as shapemaxz,m.shape.minm as shapeminm,m.shape.maxm as shapemaxm,m.shape.area as shapearea,m.shape.len as shapelen,m.shape.srid as shapesrid,m.shape.points as shapepoints,x,y,x1,y1
FROM grid m;



 



