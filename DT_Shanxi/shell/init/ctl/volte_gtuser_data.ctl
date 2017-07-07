LOAD DATA
characterset UTF8
APPEND INTO TABLE volte_gtuser_data
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
cellid,
ttime date "yyyy-mm-dd hh24:mi:ss"
)