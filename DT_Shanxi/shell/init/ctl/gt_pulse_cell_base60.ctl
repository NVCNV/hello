LOAD DATA
characterset UTF8
APPEND INTO TABLE gt_pulse_cell_base60
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
hours,
cellid,
pulse_mark,
pulse_type,
pulse_timelen,
first_pulse_mark,
sub_users_peak,
sub_gtusers_peak,
sub_volteusers_peak,
sub_commusers_peak,
users,
gt_users,
volte_users
)