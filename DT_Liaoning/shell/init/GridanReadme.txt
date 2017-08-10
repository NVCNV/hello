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
（2）createtable.sh 为hive建表语句
（3）run.sh 为任务调度脚本。

三、操作步骤：
1、首先建hive表，hive建表脚本需要指定数据库，例如：sh createtable.sh morpho
2、然后将jar包放入/dt/bin目录下面
3、在Oracle创建grid_view视图（创建语句在下面）
3、执行run.sh脚本参数顺序为：
    日期 小时  源数据库  目标数据库  master地址 数据库地址   数据存放路径
 示例：20170512 11 liaoning morpho2 spark://172.30.4.189:7077 172.30.4.187:1521/morpho0307  datang

四、用到Oracle中的工参表包括：
	grid_view
	ltecell
	lte2lteadj
	ltecover_degree_condition
	ltedisturb_degree_condition
	ltemrsegment_config
	ltemrsegment_type
	lteadjcell_degree_condition


附录：

创建 grid_view 视图：

CREATE VIEW grid_view
AS SELECT OBJECTID_1,OBJECTID,m.shape.entity as shapeentity,m.shape.numpts as shapenumpts,m.shape.minx as shapeminx,m.shape.miny as shapeminy,m.shape.maxx as shapemaxx,
m.shape.maxy as shapemaxy,m.shape.minz as shapeminz,m.shape.maxz as shapemaxz,m.shape.minm as shapeminm,m.shape.maxm as shapemaxm,m.shape.area as shapearea,m.shape.len as shapelen,m.shape.srid as shapesrid,m.shape.points as shapepoints,x,y,x1,y1
FROM grid m;



