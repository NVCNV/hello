LOAD DATA
characterset UTF8
APPEND INTO TABLE u2
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
xdrid,
cellid,
targetcellid,
upordown,
procedurestarttime,
rangetime date "yyyy-mm-dd hh24:mi:ss",
seqnum,
groupname,
hour
)