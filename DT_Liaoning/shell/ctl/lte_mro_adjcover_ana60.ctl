LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mro_adjcover_ana60
FIELDS TERMINATED BY ','
trailing nullcols
(
starttime date "yyyy-mm-dd hh24:mi:ss",
endtime date "yyyy-mm-dd hh24:mi:ss",
timeseq,
mmegroupid,
mmeid,
enodebid,
cellid,
nclackpoorcovercount,
poorcoversumval,
ncovercovercount,
overcoversumval
)