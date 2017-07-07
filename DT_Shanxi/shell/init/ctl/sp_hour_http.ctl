LOAD DATA
characterset UTF8
APPEND INTO TABLE sp_hour_http
FIELDS TERMINATED BY ','
trailing nullcols
(
ttime date "yyyy-mm-dd hh24:mi:ss",
sp,
browsedownloadvisits,
videoservicevisits,
instantmessagevisits,
appvisits,
browsedownloadbusiness,
videoservicebusiness,
instantmessagebusiness,
appbusiness,
dnsQuerySucc,
dnsQueryAtt,
tcpSetupSucc,
tcpSetupReq,
BearerULTCPRetransmit,
BearerULTCPTransmit,
BearerDLTCPRetransmit,
BearerDLTCPTransmit,
BearerULTCPMissequence,
BearerDLTCPMissequence,
pageresp,
pagereq,
pageresptimeall,
pageshowsucc,
pageshowtimeall,
httpdownflow,
httpdowntime,
mediasucc,
mediareq,
mediadownflow,
mediadowntime,
ServiceIMSucc,
ServiceIMReq,
readvisits,
wbvisits,
navigationvisits,
musicvisits,
gamevisits,
payvisits,
Animevisits,
mailvisits,
p2pvisits,
voipvisits,
MultimediaMsgvisits,
financialvisits,
securityvisits,
shoppingvisits,
travelvisits,
cloudstoragevisits,
othervisits,
readbusiness,
wbbusiness,
navigationbusiness,
musicbusiness,
gamebusiness,
paybusiness,
Animebusiness,
mailbusiness,
p2pbusiness,
voipbusiness,
MultimediaMsgbusiness,
financialbusiness,
securitybusiness,
shoppingbusiness,
travelbusiness,
cloudstoragebusiness,
otherbusiness,
mediaRespTimeall,
mediaResp,
ServiceIMTrans,
ServiceIMdownflow,
ServiceIMdownTime,
readsucc,
readreq,
readresptimeall,
readresp,
readdownflow,
readdowntime,
wbsucc,
wbreq,
wbresptimeall,
wbresp,
wbdownflow,
wbdowntime,
navigationsucc,
navigationreq,
navigationresptimeall,
navigationresp,
navigationdownflow,
navigationdowntime,
musicsucc,
musicreq,
musicresptimeall,
musicresp,
musicdownflow,
musicdowntime,
appsucc,
appreq,
appresptimeall,
appresp,
appdownflow,
appdowntime,
gamesucc,
gamereq,
gameresptimeall,
gameresp,
gamedownflow,
gamedowntime,
paysucc,
payreq,
payresptimeall,
payresp,
paydownflow,
paydowntime,
Animesucc,
Animereq,
Animeresptimeall,
Animeresp,
Animedownflow,
Animedowntime,
mailsucc,
mailreq,
mailresptimeall,
mailresp,
maildownflow,
maildowntime,
P2Psucc,
P2Preq,
P2Presptimeall,
P2Presp,
P2Pdownflow,
P2Pdowntime,
VoIPsucc,
VoIPreq,
VoIPresptimeall,
VoIPresp,
VoIPdownflow,
VoIPdowntime,
MultimediaMsgsucc,
MultimediaMsgreq,
MultimediaMsgresptimeall,
MultimediaMsgresp,
MultimediaMsgdownflow,
MultimediaMsgdowntime,
financialsucc,
financialreq,
financialresptimeall,
financialresp,
financialdownflow,
financialdowntime,
securitysucc,
securityreq,
securityresptimeall,
securityresp,
securitydownflow,
securitydowntime,
shoppingsucc,
shoppingreq,
shoppingresptimeall,
shoppingresp,
shoppingdownflow,
shoppingdowntime,
travelsucc,
travelreq,
travelresptimeall,
travelresp,
traveldownflow,
traveldowntime,
cloudstoragesucc,
cloudstoragereq,
cloudstorageresptimeall,
cloudstorageresp,
cloudstoragedownflow,
cloudstoragedowntime,
internetsucc,
internetreq,
internetresptimeall,
internetresp,
internetdownflow,
internetdowntime,
migusucc,
migureq,
miguresptimeall,
miguresp,
migudownflow,
migudowntime,
othersucc,
otherreq,
otherresptimeall,
otherresp,
otherdownflow,
otherdowntime,
instantmessageTime,
readTime,
wbTime,
navigationTime,
videoserviceTime,
musicTime,
appTime,
gameTime,
payTime,
AnimeTime,
mailTime,
p2pTime,
voipTime,
MultimediaMsgTime,
browsedownloadTime,
financialTime,
securityTime,
shoppingTime,
travelTime,
cloudstorageTime,
internetTime,
miguTime,
otherTime,
browsesurfresp,
browsesurfreq,
browsesurfresptimeall,
browsesurfshowsucc,
browsesurfshowtimeall,
browsesurfdownflow,
browsesurfdowntime,
browsesurfFlow,
browsesurfTime,
browsetencentresp,
browsetencentreq,
browsetencentresptimeall,
browsetencentshowsucc,
browsetencentshowtimeall,
browsetencentdownflow,
browsetencentdowntime,
browsetencentFlow,
browsetencentTime,
browseqqNewsresp,
browseqqNewsreq,
browseqqNewsresptimeall,
browseqqNewsshowsucc,
browseqqNewsshowtimeall,
browseqqNewsdownflow,
browseqqNewsdowntime,
browseqqNewsFlow,
browseqqNewsTime,
browsetoutiaoresp,
browsetoutiaoreq,
browsetoutiaoresptimeall,
browsetoutiaoshowsucc,
browsetoutiaoshowtimeall,
browsetoutiaodownflow,
browsetoutiaodowntime,
browsetoutiaoFlow,
browsetoutiaoTime,
browseChromeresp,
browseChromereq,
browseChromeresptimeall,
browseChromeshowsucc,
browseChromeshowtimeall,
browseChromedownflow,
browseChromedowntime,
browseChromeFlow,
browseChromeTime,
otherhttpresp,
otherhttpreq,
otherhttpresptimeall,
otherhttpshowsucc,
otherhttpshowtimeall,
otherhttpdownflow,
otherhttpdowntime,
otherhttpFlow,
otherhttpTime,
videomobileTVsucc,
videomobileTVreq,
videomobileTVresptimeall,
videomobileTVresp,
videomobileTVdownflow,
videomobileTVdowntime,
videomobileTVFlow,
videomobileTVTime,
videomiguvideosucc,
videomiguvideoreq,
videomiguvideoresptimeall,
videomiguvideoresp,
videomiguvideodownflow,
videomiguvideodowntime,
videomiguvideoFlow,
videomiguvideoTime,
videoiqiyisucc,
videoiqiyireq,
videoiqiyiresptimeall,
videoiqiyiresp,
videoiqiyidownflow,
videoiqiyidowntime,
videoiqiyiFlow,
videoiqiyiTime,
imFechatsucc,
imFechatreq,
imFechatresptimeall,
imFechatresp,
imFechatdownflow,
imFechatdowntime,
imFechatFlow,
imFechatTime,
imFetionsucc,
imFetionreq,
imFetionresptimeall,
imFetionresp,
imFetiondownflow,
imFetiondowntime,
imFetionFlow,
imFetionTime,
imQQsucc,
imQQreq,
imQQresptimeall,
imQQresp,
imQQdownflow,
imQQdowntime,
imQQFlow,
imQQTime,
imwechatsucc,
imwechatreq,
imwechatresptimeall,
imwechatresp,
imwechatdownflow,
imwechatdowntime,
imwechatFlow,
imwechatTime,
readcmreadsucc,
readcmreadreq,
readcmreadresptimeall,
readcmreadresp,
readcmreaddownflow,
readcmreaddowntime,
readcmreadFlow,
readcmreadTime,
readsurfsucc,
readsurfreq,
readsurfresptimeall,
readsurfresp,
readsurfdownflow,
readsurfdowntime,
readsurtFlow,
readsurfTime,
readQQReadsucc,
readQQReadreq,
readQQReadresptimeall,
readQQReadresp,
readQQReaddownflow,
readQQReaddowntime,
readQQReadFlow,
readQQReadTime,
weibo139wbsucc,
weibo139wbreq,
weibo139wbresptimeall,
weibo139wbresp,
weibo139wbdownflow,
weibo139wbdowntime,
weibo139wbFlow,
weibo139wbTime,
weibosinaWeibosucc,
weibosinaWeiboreq,
weibosinaWeiboresptimeall,
weibosinaWeiboresp,
weibosinaWeibodownflow,
weibosinaWeibodowntime,
weibosinaWeiboFlow,
weibosinaWeiboTime,
navigationheMapsucc,
navigationheMapreq,
navigationheMapresptimeall,
navigationheMapresp,
navigationheMapdownflow,
navigationheMapdowntime,
navigationheMapFlow,
navigationheMapTime,
navigationaMapsucc,
navigationaMapreq,
navigationaMapresptimeall,
navigationaMapresp,
navigationaMapdownflow,
navigationaMapdowntime,
navigationaMapFlow,
navigationaMapTime,
musicmigumusicsucc,
musicmigumusicreq,
musicmigumusicresptimeall,
musicmigumusicresp,
musicmigumusicdownflow,
musicmigumusicdowntime,
musicmigumusicFlow,
musicmigumusicTime,
musicqqmusicsucc,
musicqqmusicreq,
musicqqmusicresptimeall,
musicqqmusicresp,
musicqqmusicdownflow,
musicqqmusicdowntime,
musicqqmusicFlow,
musicqqmusicTime,
AppStoremmsucc,
AppStoremmreq,
AppStoremmresptimeall,
AppStoremmresp,
AppStoremmdownflow,
AppStoremmdowntime,
AppStoremmFlow,
AppStoremmTime,
AppStoresucc,
AppStorereq,
AppStoreresptimeall,
AppStoreresp,
AppStoredownflow,
AppStoredowntime,
AppStoreFlow,
AppStoreTime,
GamemiguGamesucc,
GamemiguGamereq,
GamemiguGameresptimeall,
GamemiguGameresp,
GamemiguGamedownflow,
GamemiguGamedowntime,
GamemiguGameFlow,
GamemiguGameTime,
Game4399Gamesucc,
Game4399Gamereq,
Game4399Gameresptimeall,
Game4399Gameresp,
Game4399Gamedownflow,
Game4399Gamedowntime,
Game4399GameFlow,
Game4399GameTime,
PayNFCSIMsucc,
PayNFCSIMreq,
PayNFCSIMresptimeall,
PayNFCSIMresp,
PayNFCSIMdownflow,
PayNFCSIMdowntime,
PayNFCSIMFlow,
PayNFCSIMTime,
PaymobilePaysucc,
PaymobilePayreq,
PaymobilePayresptimeall,
PaymobilePayresp,
PaymobilePaydownflow,
PaymobilePaydowntime,
PaymobilePayFlow,
PaymobilePayTime,
PayAliPaysucc,
PayAliPayreq,
PayAliPayresptimeall,
PayAliPayresp,
PayAliPaydownflow,
PayAliPaydowntime,
PayAliPayFlow,
PayAliPayTime,
Animemigudmsucc,
Animemigudmreq,
Animemigudmresptimeall,
Animemigudmresp,
Animemigudmdownflow,
Animemigudmdowntime,
AnimemigudmFlow,
AnimemigudmTime,
AnimeqqComicsucc,
AnimeqqComicreq,
AnimeqqComicresptimeall,
AnimeqqComicresp,
AnimeqqComicdownflow,
AnimeqqComicdowntime,
AnimeqqComicFlow,
AnimeqqComicTime,
mail139mailsucc,
mail139mailreq,
mail139mailresptimeall,
mail139mailresp,
mail139maildownflow,
mail139maildowntime,
mail139mailFlow,
mail139mailTime,
MailneteaseMailsucc,
MailneteaseMailreq,
MailneteaseMailresptimeall,
MailneteaseMailresp,
MailneteaseMaildownflow,
MailneteaseMaildowntime,
MailneteaseMailFlow,
MailneteaseMailTime,
financialsjsjsucc,
financialsjsjreq,
financialsjsjresptimeall,
financialsjsjresp,
financialsjsjdownflow,
financialsjsjdowntime,
financialsjsjFlow,
financialsjsjTime,
financialsjzqsucc,
financialsjzqreq,
financialsjzqresptimeall,
financialsjzqresp,
financialsjzqdownflow,
financialsjzqdowntime,
financialsjzqFlow,
financialsjzqTime,
financialzxgsucc,
financialzxgreq,
financialzxgresptimeall,
financialzxgresp,
financialzxgdownflow,
financialzxgdowntime,
financialzxgFlow,
financialzxgTime,
Othergdqqtsucc,
Othergdqqtreq,
Othergdqqtresptimeall,
Othergdqqtresp,
Othergdqqtdownflow,
Othergdqqtdowntime,
OthergdqqtFlow,
OthergdqqtTime,
Otherydzssucc,
Otherydzsreq,
Otherydzsresptimeall,
Otherydzsresp,
Otherydzsdownflow,
Otherydzsdowntime,
OtherydzsFlow,
OtherydzsTime,
Othernxbstsucc,
Othernxbstreq,
Othernxbstresptimeall,
Othernxbstresp,
Othernxbstdownflow,
Othernxbstdowntime,
OthernxbstFlow,
OthernxbstTime,
Othernxtsucc,
Othernxtreq,
Othernxtresptimeall,
Othernxtresp,
Othernxtdownflow,
Othernxtdowntime,
OthernxtFlow,
OthernxtTime,
Otheryjtsucc,
Otheryjtreq,
Otheryjtresptimeall,
Otheryjtresp,
Otheryjtdownflow,
Otheryjtdowntime,
OtheryjtFlow,
OtheryjtTime,
internetvisits,
miguvisits,
internetbusiness,
migubusiness,
ServiceIMresptimeall
)