LOAD DATA
characterset UTF8
APPEND INTO TABLE volte_gt_free_user_data
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
cellid,
proceduretype,
procedurestatus,
rangetime date "yyyy-mm-dd hh24:mi:ss",
imei,
msisdn,
procedurestarttime,
procedureendtime,
dir_state,
seqnum,
ispub,
linename
)