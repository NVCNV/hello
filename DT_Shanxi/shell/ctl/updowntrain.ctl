LOAD DATA
characterset UTF8
APPEND INTO TABLE upordown
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
xdrid,
upordown,
groupmap,
intime,
in_time date "yyyy-mm-dd hh24:mi:ss",
instation,
outtime,
out_time date "yyyy-mm-dd hh24:mi:ss",
outstation
)