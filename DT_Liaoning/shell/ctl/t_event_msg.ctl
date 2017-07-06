OPTIONS(SKIP=1)
LOAD DATA
characterset UTF8
APPEND INTO TABLE t_event_msg
FIELDS TERMINATED BY ','
trailing nullcols
(
event_name,
procedurestarttime,
imsi,
proceduretype,
etype,
cellid,
targetcellid,
falurecause,
celltype,
cellregion,
cellkey,
interface,
prointerface,
rangetime date "yyyy-mm-dd hh24:mi:ss",
ELONG,
ELAT,
EUPORDOWN
)