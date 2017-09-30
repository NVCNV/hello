LOAD DATA
characterset UTF8
APPEND INTO TABLE volte_gt_busi_user_data
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
cellid,
targetcellid,
proceduretype,
procedurestatus,
rangetime date "yyyy-mm-dd hh24:mi:ss",
imei,
msisdn,
procedurestarttime,
procedureendtime,
enbid,
targetenbid,
dir_state,
seqnum,
ispub
)