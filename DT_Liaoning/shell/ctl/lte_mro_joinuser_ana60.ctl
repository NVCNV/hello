LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mro_joinuser_ana60
FIELDS TERMINATED BY ','
trailing nullcols
(
STARTTIME date "yyyy-mm-dd hh24:mi:ss",
ENDTIME date "yyyy-mm-dd hh24:mi:ss",
TIMESEQ,
MMEGROUPID,
MMEID,
ENODEBID,
CELLID,
MMES1APUEID,
CELLMRCOUNT,
USERRSRPSUM,
USERRSRPCOUNT,
WEAKBESTROWMRCOUNT,
GOODBESTROWMRCOUNT,
LASTRSRPCOUNT
)