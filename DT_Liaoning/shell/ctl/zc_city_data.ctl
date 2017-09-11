LOAD DATA
characterset UTF8
APPEND INTO TABLE zc_city_data
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
city,
cellid,
businessdelay,
pageDownKps,
etype
)