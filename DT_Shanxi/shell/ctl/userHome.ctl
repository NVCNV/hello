LOAD DATA
characterset UTF8
APPEND INTO TABLE user_home
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
pnumber,
home
)