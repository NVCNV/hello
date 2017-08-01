LOAD DATA
characterset UTF8
APPEND INTO TABLE gt_pulse_detail_base60
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
hours,
cellid,
imsi,
pulse_mark,
pulse_type,
pulse_timelen,
first_pulse_mark,
gtuser_flag,
volteuser_flag
)