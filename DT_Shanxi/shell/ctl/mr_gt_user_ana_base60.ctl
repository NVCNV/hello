LOAD DATA
characterset UTF8
APPEND INTO TABLE mr_gt_user_ana_base60
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
imei,
msisdn,
cellid,
rruid,
gridid,
ttime date "yyyy-mm-dd hh24:mi:ss",
dir_state,
elong,
elat,
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
uebooty,
upsinrHighRatex
)