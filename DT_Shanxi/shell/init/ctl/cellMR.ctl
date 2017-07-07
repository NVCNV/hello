LOAD DATA
characterset UTF8
APPEND INTO TABLE VOLTE_CELLMR_DATA
FIELDS TERMINATED BY ','
trailing nullcols
(
cellid,
meatime date "yyyy-mm-dd hh24:mi:ss",
xdrid,
rip
)