LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mro_overlap_grid_ana60
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
gridcenterLatitude,
usercount,
overlapbestrowcellcount,
adjacentareainterferenceintens,
rsrqcount,
rsrqsum,
celloverlapbestrowmrcount,
rsrpcount,
rsrpsum
)