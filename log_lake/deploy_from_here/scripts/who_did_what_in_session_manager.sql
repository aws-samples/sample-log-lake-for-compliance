SELECT t.eventsource 
,t.eventname 
,t.eventtime 
,w.logaccountid 
,w.loggroup 
,w.subscriptionfilters 
,w.eventtime as eventtime_from_cloudwatch
,w."session" as session_from_cloudwatch
 FROM loglakeblog.from_cloudwatch_readready w 
 inner join loglakeblog.from_cloudtrail_readready t
on  w.f_sessionid=t.f_sessionid 
