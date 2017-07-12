LOAD DATA
characterset UTF8
APPEND INTO TABLE gt_pulse_detail
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
hours,
minutes,
cellid,
imsi,
imei,
gtuser_flag,
volteuser_flag,
sub_pulse_mark
)