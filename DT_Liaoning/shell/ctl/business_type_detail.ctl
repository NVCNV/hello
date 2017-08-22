LOAD DATA
characterset UTF8
APPEND INTO TABLE business_type_detail
FIELDS TERMINATED BY ','
trailing nullcols
(
TTIME date "yyyy-mm-dd hh24:mi:ss",
city,
region,
CELLID,
app_type,
app_sub_type,
uldata,
dldata
)