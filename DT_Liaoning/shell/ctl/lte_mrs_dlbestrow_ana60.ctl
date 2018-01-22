LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mrs_dlbestrow_ana60
FIELDS TERMINATED BY ','
trailing nullcols
(
STARTTIME date "yyyy-mm-dd hh24:mi:ss",
ENDTIME date "yyyy-mm-dd hh24:mi:ss",
TIMESEQ,
ENODEBID,
CELLID,
USERCOUNT,
idrUserCount,
RSRPSUM,
idrRsrpsum,
RSRPCOUNT,
idrRsrpcount,
WEAKBESTROWMRCOUNT,
idrWEAKBESTROWMRCOUNT,
GOODBESTROWMRCOUNT,
POWERHEADROOMLOWMRCOUNT,
POWERHEADROOMTOTALCOUNT,
POWERHEADROOMSUM,
RXTXTIMEDIFFBIGMRCOUNT,
RXTXTIMEDIFFTOTALCOUNT,
RXTXTIMEDIFFSUM,
AOAcount,
AOAsum,
AOAdeviates
)