LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mro_source_new
FIELDS TERMINATED BY ','
trailing nullcols
(
objectid,
vid,
fileformatversion,
starttime,
endtime,
period,
enbid,
userlabel,
mrname,
cellid,
earfcn,
subframenbr,
prbnbr,
mmeues1apid,
mmegroupid,
mmecode,
meatime date "yyyy-mm-dd hh24:mi:ss",
eventtype,
gridcenterlongitude,
gridcenterlatitude,
kpi1 "decode(:kpi1,'\n',null, :kpi1)",
kpi2 "decode(:kpi2,'\n',null, :kpi2)",
kpi3 "decode(:kpi3,'\n',null, :kpi3)",
kpi4 "decode(:kpi4,'\n',null, :kpi4)",
kpi5 "decode(:kpi5,'\n',null, :kpi5)",
kpi6 "decode(:kpi6,'\n',null, :kpi6)",
kpi7 "decode(:kpi7,'\n',null, :kpi7)",
kpi8 "decode(:kpi8,'\n',null, :kpi8)",
kpi9 "decode(:kpi9,'\n',null, :kpi9)",
kpi10 "decode(:kpi10,'\n',null, :kpi10)",
kpi11 "decode(:kpi11,'\n',null, :kpi11)",
kpi12 "decode(:kpi12,'\n',null, :kpi12)",
kpi13 "decode(:kpi13,'\n',null, :kpi13)",
kpi14 "decode(:kpi14,'\n',null, :kpi14)",
kpi15 "decode(:kpi15,'\n',null, :kpi15)",
kpi16 "decode(:kpi16,'\n',null, :kpi16)",
kpi17 "decode(:kpi17,'\n',null, :kpi17)",
kpi18 "decode(:kpi18,'\n',null, :kpi18)",
kpi19 "decode(:kpi19,'\n',null, :kpi19)",
kpi20 "decode(:kpi20,'\n',null, :kpi20)",
kpi21 "decode(:kpi21,'\n',null, :kpi21)",
kpi22 "decode(:kpi22,'\n',null, :kpi22)",
kpi23 "decode(:kpi23,'\n',null, :kpi23)",
kpi24 "decode(:kpi24,'\n',null, :kpi24)",
kpi25 "decode(:kpi25,'\n',null, :kpi25)",
kpi26 "decode(:kpi26,'\n',null, :kpi26)",
kpi27 "decode(:kpi27,'\n',null, :kpi27)",
kpi28 "decode(:kpi28,'\n',null, :kpi28)",
kpi29 "decode(:kpi29,'\n',null, :kpi29)",
kpi30 "decode(:kpi30,'\n',null, :kpi30)",
kpi31 "decode(:kpi31,'\n',null, :kpi31)",
kpi32 "decode(:kpi32,'\n',null, :kpi32)",
kpi33 "decode(:kpi33,'\n',null, :kpi33)",
kpi34 "decode(:kpi34,'\n',null, :kpi34)",
kpi35 "decode(:kpi35,'\n',null, :kpi35)",
kpi36 "decode(:kpi36,'\n',null, :kpi36)",
kpi37 "decode(:kpi37,'\n',null, :kpi37)",
kpi38 "decode(:kpi38,'\n',null, :kpi38)",
kpi39 "decode(:kpi39,'\n',null, :kpi39)",
kpi40 "decode(:kpi40,'\n',null, :kpi40)",
kpi41 "decode(:kpi41,'\n',null, :kpi41)",
kpi42 "decode(:kpi42,'\n',null, :kpi42)",
kpi43 "decode(:kpi43,'\n',null, :kpi43)",
kpi44 "decode(:kpi44,'\n',null, :kpi44)",
kpi45 "decode(:kpi45,'\n',null, :kpi45)",
kpi46 "decode(:kpi46,'\n',null, :kpi46)",
kpi47 "decode(:kpi47,'\n',null, :kpi47)",
kpi48 "decode(:kpi48,'\n',null, :kpi48)",
kpi49 "decode(:kpi49,'\n',null, :kpi49)",
kpi50 "decode(:kpi50,'\n',null, :kpi50)",
kpi51 "decode(:kpi51,'\n',null, :kpi51)",
kpi52 "decode(:kpi52,'\n',null, :kpi52)",
kpi53 "decode(:kpi53,'\n',null, :kpi53)",
kpi54 "decode(:kpi54,'\n',null, :kpi54)",
kpi55 "decode(:kpi55,'\n',null, :kpi55)",
kpi56 "decode(:kpi56,'\n',null, :kpi56)",
kpi57 "decode(:kpi57,'\n',null, :kpi57)",
kpi58 "decode(:kpi58,'\n',null, :kpi58)",
kpi59 "decode(:kpi59,'\n',null, :kpi59)",
kpi60 "decode(:kpi60,'\n',null, :kpi60)",
kpi61 "decode(:kpi61,'\n',null, :kpi61)",
kpi62 "decode(:kpi62,'\n',null, :kpi62)",
kpi63 "decode(:kpi63,'\n',null, :kpi63)",
kpi64 "decode(:kpi64,'\n',null, :kpi64)",
kpi65 "decode(:kpi65,'\n',null, :kpi65)",
kpi66 "decode(:kpi66,'\n',null, :kpi66)",
kpi67 "decode(:kpi67,'\n',null, :kpi67)",
kpi68 "decode(:kpi68,'\n',null, :kpi68)",
kpi69 "decode(:kpi69,'\n',null, :kpi69)",
kpi70 "decode(:kpi70,'\n',null, :kpi70)",
kpi71 "decode(:kpi71,'\n',null, :kpi71)",
length,
city,
xdrtype,
interface,
xdrid,
rat,
imsi,
imei,
msisdn,
mrtype,
neighborcellnumber,
gsmneighborcellnumber,
tdsneighborcellnumber,
v_enb,
mrtime,
gridid,
slong,
slat,
dlong,
dlat,
distance,
espeed,
elong,
elat,
falurecause,
flag,
beforeflag,
eupordown,
railline,
etype
)