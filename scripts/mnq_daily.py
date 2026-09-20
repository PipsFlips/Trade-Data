#!/usr/bin/env python3
"""
MNQ Premarket Data Engine v2.1
TopstepX / ProjectX REST snapshot + derived analytics.

Read-only:
- Auth
- Contract search/list
- Historical bars

No account, order, position, or execution endpoints.
"""

import json, os, statistics, sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone, time
from pathlib import Path
from urllib import request, error
from zoneinfo import ZoneInfo

API_BASE = os.getenv("TOPSTEP_API_BASE", "https://api.topstepx.com").rstrip("/")
USERNAME = os.getenv("TOPSTEP_USERNAME")
API_KEY = os.getenv("TOPSTEP_API_KEY")
LIVE = os.getenv("TOPSTEP_LIVE_DATA", "false").lower() in {"1","true","yes","y"}

LA = ZoneInfo("America/Los_Angeles")
UTC = timezone.utc
OUT = Path("public/mnq_latest.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

TICK_FALLBACK = 0.25
GLOBEX_OPEN_PT = time(15,0)
RTH_OPEN_PT = time(6,30)
RTH_CLOSE_PT = time(13,0)
CME_BREAK_PT = time(14,0)
ASIA_START_PT = time(17,0)
ASIA_END_PT = time(0,0)
LONDON_START_PT = time(0,0)
LONDON_END_PT = time(5,20)

def now_utc(): return datetime.now(UTC)
def iso_z(dt): return dt.astimezone(UTC).isoformat().replace("+00:00","Z")

def parse_dt(raw):
    s = str(raw)
    if s.endswith("Z"): s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)

def post_json(path, payload, token=None):
    headers={"Accept":"text/plain","Content-Type":"application/json","User-Agent":"MNQ-Premarket-v2.1"}
    if token: headers["Authorization"]=f"Bearer {token}"
    req=request.Request(API_BASE+path,data=json.dumps(payload).encode(),headers=headers,method="POST")
    try:
        with request.urlopen(req,timeout=45) as r:
            return json.loads(r.read().decode())
    except error.HTTPError as e:
        body=e.read().decode(errors="replace")
        raise RuntimeError(f"{path}: HTTP {e.code}: {body}") from e

def authenticate():
    if not USERNAME or not API_KEY: raise RuntimeError("Missing GitHub secrets.")
    r=post_json("/api/Auth/loginKey",{"userName":USERNAME,"apiKey":API_KEY})
    if not r.get("success") or not r.get("token"):
        raise RuntimeError(f"Authentication failed: {r.get('errorCode')} {r.get('errorMessage')}")
    return r["token"]

def is_mnq(c):
    s=" ".join(str(c.get(k,"")) for k in ("id","name","description","symbolId")).upper()
    return "MNQ" in s or "MICRO E-MINI NASDAQ" in s

def active_mnq(token):
    r=post_json("/api/Contract/search",{"searchText":"MNQ","live":LIVE},token)
    cs=[c for c in (r.get("contracts") or []) if is_mnq(c)]
    active=[c for c in cs if c.get("activeContract") is True]
    if active: return active[0]
    if cs: return cs[0]
    r=post_json("/api/Contract/available",{"live":LIVE},token)
    cs=[c for c in (r.get("contracts") or []) if is_mnq(c)]
    active=[c for c in cs if c.get("activeContract") is True]
    if active: return active[0]
    if cs: return cs[0]
    raise RuntimeError("No MNQ contract found.")

def get_bars(token,cid,unit,n,days,limit=20000):
    end=now_utc(); start=end-timedelta(days=days)
    r=post_json("/api/History/retrieveBars",{
        "contractId":cid,"live":LIVE,"startTime":iso_z(start),"endTime":iso_z(end),
        "unit":unit,"unitNumber":n,"limit":limit,"includePartialBar":True
    },token)
    out=[]
    for b in (r.get("bars") or []):
        out.append({
            "t":b.get("t") or b.get("time") or b.get("timestamp"),
            "o":b.get("o",b.get("open")),"h":b.get("h",b.get("high")),
            "l":b.get("l",b.get("low")),"c":b.get("c",b.get("close")),
            "v":b.get("v",b.get("volume")),
        })
    out.sort(key=lambda x: parse_dt(x["t"]))
    return out

def ldt(b): return parse_dt(b["t"]).astimezone(LA)

def between(rows,s,e):
    return [b for b in rows if s <= ldt(b) < e]

def summary(rows):
    if not rows: return None
    return {
        "open":float(rows[0]["o"]),
        "high":max(float(x["h"]) for x in rows),
        "low":min(float(x["l"]) for x in rows),
        "close":float(rows[-1]["c"]),
        "volume":sum(float(x.get("v") or 0) for x in rows),
        "start":rows[0]["t"],"end":rows[-1]["t"],"bars":len(rows),
    }

def previous_rth(rows, ref):
    d=ref-timedelta(days=1)
    for _ in range(8):
        s=datetime.combine(d,RTH_OPEN_PT,tzinfo=LA); e=datetime.combine(d,RTH_CLOSE_PT,tzinfo=LA)
        x=between(rows,s,e)
        if x:
            z=summary(x); z["date"]=d.isoformat(); return z
        d-=timedelta(days=1)

def current_overnight(rows, ref):
    s=datetime.combine(ref-timedelta(days=1),GLOBEX_OPEN_PT,tzinfo=LA)
    x=between(rows,s,datetime.now(LA))
    return summary(x)

def named(rows, ref, st, et, prior=False):
    d=ref-timedelta(days=1) if prior else ref
    s=datetime.combine(d,st,tzinfo=LA)
    ed=d if et>st else d+timedelta(days=1)
    e=datetime.combine(ed,et,tzinfo=LA)
    return summary(between(rows,s,e))

def prior_cme_week(rows, ref):
    # Previous CME equity-index trading week:
    # Sunday 15:00 PT -> Friday 14:00 PT.
    this_monday = ref - timedelta(days=ref.weekday())
    prev_sunday = this_monday - timedelta(days=1)
    start = datetime.combine(prev_sunday, GLOBEX_OPEN_PT, tzinfo=LA)
    end = datetime.combine(this_monday + timedelta(days=4), CME_BREAK_PT, tzinfo=LA)
    x=between(rows,start,end)
    z=summary(x)
    if z:
        z.update({"sessionStartPacific":start.isoformat(),"sessionEndPacific":end.isoformat()})
    return z

def previous_full_globex(rows,ref):
    d=ref-timedelta(days=1)
    for _ in range(8):
        s=datetime.combine(d-timedelta(days=1),GLOBEX_OPEN_PT,tzinfo=LA)
        e=datetime.combine(d,CME_BREAK_PT,tzinfo=LA)
        x=between(rows,s,e)
        if x:
            z=summary(x); z["sessionDate"]=d.isoformat(); return z
        d-=timedelta(days=1)

def vwap(rows):
    num=den=0.0
    for b in rows:
        vol=float(b.get("v") or 0)
        if vol<=0: continue
        typ=(float(b["h"])+float(b["l"])+float(b["c"]))/3
        num+=typ*vol; den+=vol
    return num/den if den else None

def round_tick(x,tick): return round(round(x/tick)*tick,10)

def est_profile(rows,tick):
    buckets=defaultdict(float)
    for b in rows:
        vol=float(b.get("v") or 0)
        if vol<=0: continue
        lo=round_tick(float(b["l"]),tick); hi=round_tick(float(b["h"]),tick)
        n=max(1,int(round((hi-lo)/tick))+1); pv=vol/n
        for i in range(n):
            buckets[round_tick(lo+i*tick,tick)] += pv
    if not buckets: return None
    prices=sorted(buckets); poc=max(prices,key=lambda p:buckets[p])
    total=sum(buckets.values()); target=total*.70
    idx=prices.index(poc); selected={poc}; cum=buckets[poc]; li=idx-1; hi=idx+1
    while cum<target and (li>=0 or hi<len(prices)):
        lv=buckets[prices[li]] if li>=0 else -1
        hv=buckets[prices[hi]] if hi<len(prices) else -1
        if hv>=lv and hi<len(prices):
            p=prices[hi]; hi+=1
        else:
            p=prices[li]; li-=1
        if p not in selected: selected.add(p); cum+=buckets[p]
    extrema_hi=[]; extrema_lo=[]
    for i in range(1,len(prices)-1):
        a,b,c=buckets[prices[i-1]],buckets[prices[i]],buckets[prices[i+1]]
        if b>a and b>=c: extrema_hi.append((prices[i],b))
        if b<a and b<=c: extrema_lo.append((prices[i],b))
    return {
        "method":"estimated from 1m OHLCV; uniform volume across each bar range",
        "isTrueTradeByTradeProfile":False,
        "valueAreaPercent":0.70,
        "poc":poc,"vah":max(selected),"val":min(selected),
        "hvnCandidates":[{"price":p,"estimatedVolume":round(v,2)} for p,v in sorted(extrema_hi,key=lambda x:x[1],reverse=True)[:5]],
        "lvnCandidates":[{"price":p,"estimatedVolume":round(v,2)} for p,v in sorted(extrema_lo,key=lambda x:x[1])[:5]],
    }

def profile_prev_rth(one,ref,tick):
    r=previous_rth(one,ref)
    if not r: return None
    d=datetime.fromisoformat(r["date"]).date()
    s=datetime.combine(d,RTH_OPEN_PT,tzinfo=LA); e=datetime.combine(d,RTH_CLOSE_PT,tzinfo=LA)
    return est_profile(between(one,s,e),tick)

def profile_overnight(one,ref,tick):
    s=datetime.combine(ref-timedelta(days=1),GLOBEX_OPEN_PT,tzinfo=LA)
    return est_profile(between(one,s,datetime.now(LA)),tick)

def atr(daily,n=14):
    if len(daily)<n+1: return None
    tr=[]
    for i in range(1,len(daily)):
        p=float(daily[i-1]["c"]); b=daily[i]
        tr.append(max(float(b["h"])-float(b["l"]),abs(float(b["h"])-p),abs(float(b["l"])-p)))
    a=tr[-n:]; return sum(a)/len(a) if a else None

def ranges(daily,n=20):
    a=[float(b["h"])-float(b["l"]) for b in daily[-n:]]
    if not a:return None
    return {"sampleDays":len(a),"averageRange":sum(a)/len(a),"medianRange":statistics.median(a),"maxRange":max(a),"minRange":min(a)}

def pivots_scored(hour_rows,current_price,atr14,left=2,right=2):
    raw=[]
    if len(hour_rows)<left+right+1:return raw
    atr_ref=atr14 or 300
    for i in range(left,len(hour_rows)-right):
        b=hour_rows[i]; h=float(b["h"]); l=float(b["l"])
        hi=all(h>float(hour_rows[j]["h"]) for j in range(i-left,i)) and all(h>=float(hour_rows[j]["h"]) for j in range(i+1,i+right+1))
        lo=all(l<float(hour_rows[j]["l"]) for j in range(i-left,i)) and all(l<=float(hour_rows[j]["l"]) for j in range(i+1,i+right+1))
        if not (hi or lo):continue
        typ="high" if hi else "low"; price=h if hi else l
        later=hour_rows[i+right+1:]
        swept=False; close_through=None
        max_excursion=0
        for x in later:
            if typ=="high":
                if float(x["h"])>price:swept=True
                if float(x["c"])>price and close_through is None:close_through=x["t"]
                max_excursion=max(max_excursion,price-float(x["l"]))
            else:
                if float(x["l"])<price:swept=True
                if float(x["c"])<price and close_through is None:close_through=x["t"]
                max_excursion=max(max_excursion,float(x["h"])-price)
        age_hours=max(0,(datetime.now(UTC)-parse_dt(b["t"])).total_seconds()/3600)
        distance=abs(current_price-price)
        score=0.0
        if not swept: score+=45
        score += min(25,(max_excursion/atr_ref)*20)
        score += max(0,15-(distance/atr_ref)*5)
        score += max(0,15-age_hours/48)
        raw.append({
            "type":typ,"price":price,"time":b["t"],
            "sweptLater":swept,"liquidityStatus":"untouched" if not swept else "swept",
            "closeThroughTime":close_through,
            "maxSubsequentExcursionPoints":round(max_excursion,2),
            "distanceFromCurrentPoints":round(distance,2),
            "significanceScore":round(score,1),
        })
    raw.sort(key=lambda x:x["significanceScore"],reverse=True)
    return {"topSignificant":raw[:6],"allRecent":sorted(raw[-20:],key=lambda x:x["time"])}

def monthly_weekly_open(five,ref):
    month=[b for b in five if ldt(b).year==ref.year and ldt(b).month==ref.month]
    monday=ref-timedelta(days=ref.weekday())
    week=[b for b in five if ldt(b)>=datetime.combine(monday,time(0,0),tzinfo=LA)]
    return {
        "monthOpen":float(month[0]["o"]) if month else None,
        "calendarWeekOpen":float(week[0]["o"]) if week else None,
    }

def main():
    token=authenticate(); c=active_mnq(token); cid=c["id"]; tick=float(c.get("tickSize") or TICK_FALLBACK)
    one=get_bars(token,cid,2,1,10,15000)
    five=get_bars(token,cid,2,5,45,15000)
    hour=get_bars(token,cid,3,1,120,5000)
    day=get_bars(token,cid,4,1,500,1200)
    week=get_bars(token,cid,5,1,1800,600)
    if not five: raise RuntimeError("No 5m bars returned.")
    ref=datetime.now(LA).date(); prev=previous_rth(five,ref); ov=current_overnight(five,ref)
    asia=named(five,ref,ASIA_START_PT,ASIA_END_PT,True)
    london=named(five,ref,LONDON_START_PT,LONDON_END_PT,False)
    gstart=datetime.combine(ref-timedelta(days=1),GLOBEX_OPEN_PT,tzinfo=LA)
    atr14=atr(day,14); current=float(five[-1]["c"])
    payload={
        "schemaVersion":2.1,
        "generatedUtc":iso_z(now_utc()),"generatedPacific":datetime.now(LA).isoformat(),
        "source":"TopstepX / ProjectX CME market data","liveRequested":LIVE,
        "contract":{"id":c.get("id"),"name":c.get("name"),"description":c.get("description"),"symbolId":c.get("symbolId"),
                    "tickSize":c.get("tickSize"),"tickValue":c.get("tickValue"),"activeContract":c.get("activeContract")},
        "latest":{"bar1m":one[-1] if one else None,"bar5m":five[-1],"bar1h":hour[-1] if hour else None,"bar1d":day[-1] if day else None},
        "analytics":{
            "previousRTH":prev,
            "previousFullGlobexSession":previous_full_globex(five,ref),
            "previousCMETradingWeek":prior_cme_week(five,ref),
            "asia":asia,"london":london,"currentOvernight":ov,
            "vwap":{"globex":vwap(between(one,gstart,datetime.now(LA))) if one else None},
            "profiles":{
                "previousRTH_estimated":profile_prev_rth(one,ref,tick),
                "currentOvernight_estimated":profile_overnight(one,ref,tick),
                "warning":"Estimated profile. Exact VAP comes from realtime GatewayTrade collector."
            },
            "oneHourPivots":pivots_scored(hour,current,atr14),
            "volatility":{"ATR14Daily":atr14,"dailyRange20":ranges(day,20)},
            "opens":monthly_weekly_open(five,ref),
            "previousSettlementProxy": prev["close"] if prev else None,
        },
        "barCounts":{"1m":len(one),"5m":len(five),"1h":len(hour),"1d":len(day),"1w":len(week)},
        "bars":{"1mRecent":one[-3000:],"5mRecent":five[-3000:],"1hRecent":hour[-1000:],"1dRecent":day[-400:],"1wRecent":week[-200:]},
    }
    OUT.write_text(json.dumps(payload,indent=2),encoding="utf-8")
    print(f"Wrote {OUT}")
    print(f"Contract {c.get('name')} | latest 5m {five[-1]}")
    print(f"Top pivots: {payload['analytics']['oneHourPivots']['topSignificant'][:3]}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print(f"ERROR: {e}",file=sys.stderr); sys.exit(1)
    r=post('/api/Auth/loginKey',{'userName':USER,'apiKey':KEY})
    if not r.get('success') or not r.get('token'): raise RuntimeError(f'Auth failed: {r.get("errorMessage")}')
    return r['token']

def contract(token):
    r=post('/api/Contract/search',{'searchText':'MNQ','live':LIVE},token)
    cs=[c for c in (r.get('contracts') or []) if 'MNQ' in json.dumps(c).upper()]
    cs=[c for c in cs if c.get('activeContract')] or cs
    if not cs: raise RuntimeError('No MNQ contract returned')
    return cs[0]

def bars(token,cid,unit,num,days,limit):
    r=post('/api/History/retrieveBars',{'contractId':cid,'live':LIVE,'startTime':iso(now()-timedelta(days=days)),'endTime':iso(now()),'unit':unit,'unitNumber':num,'limit':limit,'includePartialBar':True},token)
    a=[{'t':b.get('t'),'o':b.get('o'),'h':b.get('h'),'l':b.get('l'),'c':b.get('c'),'v':b.get('v')} for b in (r.get('bars') or [])]
    a.sort(key=lambda b:pdt(b['t']))
    return a

def between(a,s,e): return [b for b in a if s<=ldt(b)<e]
def summary(a):
    if not a:return None
    return {'open':float(a[0]['o']),'high':max(float(x['h']) for x in a),'low':min(float(x['l']) for x in a),'close':float(a[-1]['c']),'volume':sum(float(x.get('v') or 0) for x in a),'start':a[0]['t'],'end':a[-1]['t'],'bars':len(a)}
def previous_rth(a,ref):
    d=ref-timedelta(days=1)
    for _ in range(7):
        x=between(a,datetime.combine(d,RTH_OPEN,tzinfo=LA),datetime.combine(d,RTH_CLOSE,tzinfo=LA))
        if x:
            z=summary(x);z['date']=d.isoformat();return z
        d-=timedelta(days=1)
def previous_globex(a,ref):
    d=ref-timedelta(days=1)
    for _ in range(7):
        x=between(a,datetime.combine(d-timedelta(days=1),GLOBEX,tzinfo=LA),datetime.combine(d,BREAK,tzinfo=LA))
        if x:
            z=summary(x);z['sessionDate']=d.isoformat();return z
        d-=timedelta(days=1)
def session(a,s,e): return summary(between(a,s,e))
def prior_week(a,ref):
    mon=ref-timedelta(days=ref.weekday()); a0=mon-timedelta(days=7); b0=mon-timedelta(days=1)
    z=summary([x for x in a if a0<=ldt(x).date()<=b0])
    if z:z.update({'startDate':a0.isoformat(),'endDate':b0.isoformat()})
    return z

def vwap(a):
    den=0;num=0
    for b in a:
        v=float(b.get('v') or 0)
        if v<=0: continue
        tp=(float(b['h'])+float(b['l'])+float(b['c']))/3;num+=tp*v;den+=v
    return num/den if den else None

def atr(daily,n=14):
    if len(daily)<n+1:return None
    tr=[]
    for i in range(1,len(daily)):
        p=float(daily[i-1]['c']);b=daily[i]
        tr.append(max(float(b['h'])-float(b['l']),abs(float(b['h'])-p),abs(float(b['l'])-p)))
    return sum(tr[-n:])/n

def profile(a,tick=.25,pct=.70):
    if not a:return None
    d=defaultdict(float)
    for b in a:
        lo=round(float(b['l'])/tick)*tick; hi=round(float(b['h'])/tick)*tick; vol=float(b.get('v') or 0)
        n=max(1,int(round((hi-lo)/tick))+1); share=vol/n
        for i in range(n): d[round(lo+i*tick,10)]+=share
    if not d:return None
    prices=sorted(d);poc=max(prices,key=lambda p:d[p]);target=sum(d.values())*pct
    chosen={poc};cum=d[poc];i=prices.index(poc)-1;j=i+2
    while cum<target and (i>=0 or j<len(prices)):
        lv=d[prices[i]] if i>=0 else -1; rv=d[prices[j]] if j<len(prices) else -1
        if rv>=lv and j<len(prices):p=prices[j];j+=1
        else:p=prices[i];i-=1
        if p not in chosen:chosen.add(p);cum+=d[p]
    hv=[];lv=[]
    for k in range(1,len(prices)-1):
        p=prices[k];x=d[p]
        if x>d[prices[k-1]] and x>=d[prices[k+1]]:hv.append((p,x))
        if x<d[prices[k-1]] and x<=d[prices[k+1]]:lv.append((p,x))
    return {'method':'estimated from 1m OHLCV; uniform volume across each bar range','isTrueTradeByTradeProfile':False,'valueAreaPercent':pct,'poc':poc,'vah':max(chosen),'val':min(chosen),'hvnCandidates':[{'price':p,'estimatedVolume':round(v,2)} for p,v in sorted(hv,key=lambda x:x[1],reverse=True)[:5]],'lvnCandidates':[{'price':p,'estimatedVolume':round(v,2)} for p,v in sorted(lv,key=lambda x:x[1])[:5]]}

def pivots(a,left=2,right=2):
    out=[]
    for i in range(left,len(a)-right):
        h=float(a[i]['h']);l=float(a[i]['l'])
        hi=all(h>float(a[j]['h']) for j in range(i-left,i)) and all(h>=float(a[j]['h']) for j in range(i+1,i+right+1))
        lo=all(l<float(a[j]['l']) for j in range(i-left,i)) and all(l<=float(a[j]['l']) for j in range(i+1,i+right+1))
        if not (hi or lo):continue
        typ='high' if hi else 'low';price=h if hi else l;swept=False;closeThrough=None
        for b in a[i+right+1:]:
            if typ=='high': swept=swept or float(b['h'])>price; closeThrough=closeThrough or (b['t'] if float(b['c'])>price else None)
            else: swept=swept or float(b['l'])<price; closeThrough=closeThrough or (b['t'] if float(b['c'])<price else None)
        out.append({'type':typ,'price':price,'time':a[i]['t'],'sweptLater':swept,'liquidityStatus':'swept' if swept else 'untouched','closeThroughTime':closeThrough})
    return out[-16:]

def main():
    if not should_run(): print('Skipping duplicate DST cron'); return
    t=auth();c=contract(t);cid=c['id'];ref=datetime.now(LA).date();tick=float(c.get('tickSize') or .25)
    m1=bars(t,cid,2,1,8,12000);m5=bars(t,cid,2,5,30,10000);h1=bars(t,cid,3,1,120,5000);d1=bars(t,cid,4,1,450,1000);w1=bars(t,cid,5,1,1500,500)
    if not m5:raise RuntimeError('No 5m bars')
    prev=previous_rth(m5,ref);pg=previous_globex(m5,ref)
    asia_s=datetime.combine(ref-timedelta(days=1),ASIA_START,tzinfo=LA);asia_e=datetime.combine(ref,ASIA_END,tzinfo=LA)
    lon_s=datetime.combine(ref,LONDON_START,tzinfo=LA);lon_e=datetime.combine(ref,LONDON_END,tzinfo=LA)
    glob_s=datetime.combine(ref-timedelta(days=1),GLOBEX,tzinfo=LA)
    on=summary(between(m5,glob_s,datetime.now(LA)))
    prev_prof=None
    if prev:
        pd=datetime.fromisoformat(prev['date']).date();prev_prof=profile(between(m1,datetime.combine(pd,RTH_OPEN,tzinfo=LA),datetime.combine(pd,RTH_CLOSE,tzinfo=LA)),tick)
    out={'schemaVersion':2,'generatedUtc':iso(now()),'generatedPacific':datetime.now(LA).isoformat(),'source':'TopstepX / ProjectX CME market data','liveRequested':LIVE,'contract':{'id':c.get('id'),'name':c.get('name'),'description':c.get('description'),'symbolId':c.get('symbolId'),'tickSize':c.get('tickSize'),'tickValue':c.get('tickValue'),'activeContract':c.get('activeContract')},'latest':{'bar1m':m1[-1] if m1 else None,'bar5m':m5[-1],'bar1h':h1[-1] if h1 else None},'analytics':{'previousRTH':prev,'previousFullGlobexSession':pg,'previousWeek':prior_week(m5,ref),'asia':session(m5,asia_s,asia_e),'london':session(m5,lon_s,lon_e),'currentOvernight':on,'vwap':{'globexSession':vwap(between(m1,glob_s,datetime.now(LA)))},'profiles':{'previousRTH_estimated':prev_prof,'currentOvernight_estimated':profile(between(m1,glob_s,datetime.now(LA)),tick),'warning':'Estimated from 1m OHLCV. Exact VAP requires realtime GatewayTrade collection.'},'oneHourPivots':pivots(h1),'volatility':{'ATR14Daily':atr(d1),'dailyRange20':{'average':statistics.mean([float(x['h'])-float(x['l']) for x in d1[-20:]]) if len(d1)>=1 else None}}},'barCounts':{'1m':len(m1),'5m':len(m5),'1h':len(h1),'1d':len(d1),'1w':len(w1)},'bars':{'1mRecent':m1[-2500:],'5mRecent':m5[-2500:],'1hRecent':h1[-1000:],'1dRecent':d1[-400:],'1wRecent':w1[-200:]},'security':{'containsCredentials':False,'containsAccountData':False,'containsOrdersOrPositions':False},'capabilities':{'exactWithRealtimeCollector':['trade-by-trade VAP','POC/VAH/VAL','5m delta','CVD','aggressive buy/sell imbalance','absorption/exhaustion candidates','DOM stacking/pulling when entitlement supports depth']}}
    OUT.write_text(json.dumps(out,indent=2));print('Wrote',OUT);print('Contract',c.get('name'));print('Latest 5m',m5[-1]);print('Counts',out['barCounts'])
if __name__=='__main__':
    try:main()
    except Exception as e:print('ERROR:',e,file=sys.stderr);sys.exit(1)
