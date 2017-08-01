LOAD DATA
characterset UTF8
APPEND INTO TABLE t_xdr_event_msg
FIELDS TERMINATED BY ','
trailing nullcols
(
City,
XDRID,
ProcedureStartTime,
ttime date "yyyy-mm-dd hh24:mi:ss",
ProcedureEndTime,
IMSI,
IMEI,
TEtac,
msisdn,
Cellid,
SGW,
SP,
AppType,
AppSubtype,
AppStatus,
etype,
failcause,
CELLREGION
)