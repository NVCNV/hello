LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mrs_dlbestrow_grid_ana60
FIELDS TERMINATED BY ','
trailing nullcols
(
oid,
starttime date "yyyy-mm-dd hh24:mi:ss",
endtime date "yyyy-mm-dd hh24:mi:ss",
timeseq,
enodebid,
cellid,
gridcenterlongitude,
gridcenterlatitude,
usercount,
idrUserCount,
rsrpsum,
idrRsrpsum,
rsrpcount,
idrRsrpcount,
weakbestrowmrcount,
idrWEAKBESTROWMRCOUNT,
powerheadroomtotalcount,
powerheadroomlowmrcount
)