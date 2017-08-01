LOAD DATA
characterset UTF8
APPEND INTO TABLE volte_user_data
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
hours,
imsi,
volte_start,
volte_end
)