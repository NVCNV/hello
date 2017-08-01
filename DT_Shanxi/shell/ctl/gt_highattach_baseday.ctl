LOAD DATA
characterset UTF8
APPEND INTO TABLE gt_highattach_baseday
FIELDS TERMINATED BY ','
trailing nullcols
(
line_name,
city,
ttime date "yyyy-mm-dd hh24:mi:ss",
cellid,
cellname,
maxusers
)