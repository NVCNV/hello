OPTIONS(SKIP=1)
LOAD DATA
characterset UTF8
APPEND INTO TABLE t_xdr_event_msg
FIELDS TERMINATED BY ','
trailing nullcols
(
city,
xdrid,
PROCEDURESTARTTIME,
TTIME date "yyyy-mm-dd hh24:mi:ss",
procedureendtime,
IMSI,
IMEI,
TEtac,
msisdn,
CELLID,
sgw,
sp,
apptype,
appsubtype,
appstatus,
etype,
failcause,
CELLREGION
)