#!/usr/bin/env python3
import json, os, statistics, sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone, time
from pathlib import Path
from urllib import request, error
from zoneinfo import ZoneInfo

API=os.getenv('TOPSTEP_API_BASE','https://api.topstepx.com').rstrip('/')
USER=os.getenv('TOPSTEP_USERNAME'); KEY=os.getenv('TOPSTEP_API_KEY')
LIVE=os.getenv('TOPSTEP_LIVE_DATA','false').lower() in {'1','true','yes'}
LA=ZoneInfo('America/Los_Angeles'); UTC=timezone.utc
OUT=Path('public/mnq_latest.json'); OUT.parent.mkdir(parents=True,exist_ok=True)
GLOBEX=time(15,0); RTH_OPEN=time(6,30); RTH_CLOSE=time(13,0); BREAK=time(14,0)
ASIA_START=time(17,0); ASIA_END=time(0,0); LONDON_START=time(0,0); LONDON_END=time(5,20)

def now(): return datetime.now(UTC)
def iso(dt): return dt.astimezone(UTC).isoformat().replace('+00:00','Z')
def pdt(s):
    s=str(s); s=s[:-1]+'+00:00' if s.endswith('Z') else s
    d=datetime.fromisoformat(s); return (d if d.tzinfo else d.replace(tzinfo=UTC)).astimezone(UTC)
def ldt(b): return pdt(b['t']).astimezone(LA)
def should_run():
    if os.getenv('GITHUB_EVENT_NAME')=='workflow_dispatch': return True
    x=datetime.now(LA); return x.weekday()<5 and x.hour==5

def post(path,payload,token=None):
    h={'Accept':'text/plain','Content-Type':'application/json','User-Agent':'MNQ-Premarket/2'}
    if token: h['Authorization']='Bearer '+token
    q=request.Request(API+path,data=json.dumps(payload).encode(),headers=h,method='POST')
    try:
        with request.urlopen(q,timeout=45) as r: return json.loads(r.read().decode())
    except error.HTTPError as e: raise RuntimeError(f'{path} HTTP {e.code}: {e.read().decode(errors="replace")}')

def auth():
    if not USER or not KEY: raise RuntimeError('Missing GitHub secrets')
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
