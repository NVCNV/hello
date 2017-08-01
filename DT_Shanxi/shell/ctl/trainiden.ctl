LOAD DATA
characterset UTF8
APPEND INTO TABLE numberiden
FIELDS TERMINATED BY ','
trailing nullcols
(
trainnumber,
upordown,
groupname
)