LOAD DATA
characterset UTF8
APPEND INTO TABLE mr_gt_grid_ana_baseday
FIELDS TERMINATED BY ','
trailing nullcols
(
gridid,
ttime date "yyyy-mm-dd hh24:mi:ss",
dir_state,
cellid,
rruid,
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