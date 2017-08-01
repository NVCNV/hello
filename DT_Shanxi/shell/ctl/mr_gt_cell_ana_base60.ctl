LOAD DATA
characterset UTF8
APPEND INTO TABLE mr_gt_cell_ana_base60
FIELDS TERMINATED BY ','
trailing nullcols
(
cellid,
ttime date "yyyy-mm-dd hh24:mi:ss",
dir_state,
avgrsrpx,
commy,
avgrsrqx,
ltecoverratex,
weakcoverratex,
overlapcoverratex,
overlapcoverratey,
upsigrateavgx,
upsigrateavgy,
updiststrox,
updiststroy,
model3diststrox,
model3diststroy,
uebootx,
uebooty
)