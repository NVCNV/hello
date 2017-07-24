LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mro_disturb_mix
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
disturbmmegroupid,
disturbmmeid,
disturbenodebid,
disturbcellid,
disturbcellname,
disturbpci,
disturbsfn,
disturbnum,
adjdisturbtotalnum,
disturbrate,
asadjcellrsrptotalvalue,
asadjcellrsrptotalnum,
asadjcellavgrsrp
)