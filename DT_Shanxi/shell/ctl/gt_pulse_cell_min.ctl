LOAD DATA
characterset UTF8
APPEND INTO TABLE gt_pulse_cell_min
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
hours,
minutes,
cellid,
sub_pulse_mark,
sub_pulse_type,
users,
gt_users,
comm_users,
volte_users
)