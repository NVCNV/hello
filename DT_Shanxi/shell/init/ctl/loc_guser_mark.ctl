LOAD DATA
characterset UTF8
APPEND INTO TABLE loc_guser_mark
FIELDS TERMINATED BY ','
trailing nullcols
(
id  "decode(:id,'\n',null, :id)",
imsi,
slong,
slat,
dlong,
dlat,
avgspeed,
speedv0,
speedv1,
speedv2,
timet,
timet0,
timet1,
timet2,
timetend,
rangetime date "yyyy-mm-dd hh24:mi:ss",
speedok,
flag,
chkflag,
eupordown,
swp,
remark2,
remark3,
remark4,
remark5,
switchfpdistance
)