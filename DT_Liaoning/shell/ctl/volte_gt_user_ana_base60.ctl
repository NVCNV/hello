OPTIONS(SKIP=1)
LOAD DATA
characterset UTF8
APPEND INTO TABLE volte_gt_user_ana_base60
FIELDS TERMINATED BY ','
trailing nullcols
(
imsi,
imei,
msisdn,
cellid,
ttime date "yyyy-mm-dd hh24:mi:ss",
voltemcsucc,
voltemcatt,
voltevdsucc,
voltevdatt,
voltetime,
voltemctime,
voltemctimey,
voltevdtime,
voltevdtimey,
voltemchandover,
volteanswer,
voltevdhandover,
voltevdanswer,
srvccsucc,
srvccatt,
srvcctime,
lteswsucc,
lteswatt,
srqatt,
srqsucc,
tauatt,
tausucc,
rrcrebuild,
rrcsucc,
rrcreq,
imsiregatt,
imsiregsucc,
wirelessdrop,
wireless,
eabdrop,
eab,
eabs1swx,
eabs1swy,
s1tox2swx,
s1tox2swy,
enbx2swx,
enbx2swy,
uuenbswx,
uuenbswy,
uuenbinx,
uuenbiny,
swx,
swy,
attachx,
attachy,
voltesucc,
srvccsuccS1
)