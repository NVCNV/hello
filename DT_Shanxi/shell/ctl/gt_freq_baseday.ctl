LOAD DATA
characterset UTF8
APPEND INTO TABLE gt_freq_baseday
FIELDS TERMINATED BY ','
trailing nullcols
(
line_name,
city,
ttime date "yyyy-mm-dd hh24:mi:ss",
cell_feq,
cell_num,
gtusers,
commusers,
cellavgusers
)