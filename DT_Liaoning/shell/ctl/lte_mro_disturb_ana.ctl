LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mro_disturb_ana
FIELDS TERMINATED BY ','
trailing nullcols
(
starttime date "yyyy-mm-dd hh24:mi:ss",
endtime date "yyyy-mm-dd hh24:mi:ss",
period,
timeseq,
mmegroupid,
mmeid,
enodebid,
cellid,
cellname,
pci,
sfn,
adjdisturbtotalnum,
adjavailablenum,
celldisturbrate,
isstrongdisturbcell,
asadjcellrsrptotalvalue,
asadjcellrsrptotalnum,
asadjcellavgrsrp,
srvcellrsrptotalvalue,
srvcellrsrptotalnum,
srvcellavgrsrp
)