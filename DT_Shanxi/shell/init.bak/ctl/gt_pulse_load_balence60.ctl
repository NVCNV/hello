LOAD DATA
characterset UTF8
APPEND INTO TABLE gt_pulse_load_balence60
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
hours,
line,
city,
pulse_mark,
cellprop,
cellname,
cellid,
first_pulse_mark,
pulse_timelen,
pairname,
f1users,
f2users,
balusers,
balratio
)