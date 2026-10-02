const express = require("express");
const { buildIBStrategy } = require("./ib-strategy");
const { buildORBStrategy } = require("./orb-strategy");
const { buildMESFailureStrategy } = require("./mes-failure-strategy");
const { readReport: readHistoryCoverage, runAudit: runHistoryAudit } = require("./history-coverage");
const { read: readBackfill, runBackfill, exportData: exportBackfill } = require("./backtest-history");
const signalR = require("@microsoft/signalr");
const { DateTime } = require("luxon");

const API_BASE = process.env.TOPSTEP_API_BASE || "https://api.topstepx.com";
const HUB = process.env.TOPSTEP_MARKET_HUB || "https://rtc.topstepx.com/hubs/market";
const USERNAME = process.env.TOPSTEP_USERNAME;
const API_KEY = process.env.TOPSTEP_API_KEY;
const LIVE = String(process.env.TOPSTEP_LIVE_DATA || "false").toLowerCase() === "true";
const PORT = Number(process.env.PORT || 8080);
const DATA_DIR = process.env.DATA_DIR || "/data";
const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS || 300000);
const STATE_SAVE_MS = Number(process.env.STATE_SAVE_MS || 30000);
const STATE_FILE = `${DATA_DIR}/trade-profile-state.json`;
const ZONE = "America/Los_Angeles";
const ALERT_SCORE_THRESHOLD = Number(process.env.ALERT_SCORE_THRESHOLD || 55);
const REALTIME_RELAY_TOKEN = process.env.REALTIME_RELAY_TOKEN || "";
const DIRECT_TOPSTEP_REALTIME = String(process.env.DIRECT_TOPSTEP_REALTIME || "false").toLowerCase()==="true";
const MARKET_SYMBOL = String(process.env.MARKET_SYMBOL || "MNQ").toUpperCase();
const MORNING_CUTOFF_FILE = `${DATA_DIR}/morning-cutoff-${MARKET_SYMBOL}.json`;
const MARKET_DESC = MARKET_SYMBOL==="MES" ? "MICRO E-MINI S&P" : "MICRO E-MINI NASDAQ";
const MES_SERVICE_URL = process.env.MES_SERVICE_URL || "";
const MARKET_PARAMS = MARKET_SYMBOL==="MES"
  ? {obMin:2,obCap:5,orbMin:2.5,orbCap:6,majorMin:3,majorCap:8,gapMin:3.5,clusterMin:.75}
  : {obMin:8,obCap:20,orbMin:10,orbCap:25,majorMin:12,majorCap:30,gapMin:12,clusterMin:2};
const ALERT_SMS_TO = process.env.ALERT_SMS_TO || "";
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";
const TWILIO_FROM_NUMBER = process.env.TWILIO_FROM_NUMBER || "";

if (!USERNAME || !API_KEY) throw new Error("Missing TOPSTEP_USERNAME / TOPSTEP_API_KEY");

const fs = require("fs");
fs.mkdirSync(DATA_DIR, { recursive: true });

let token = null;
let tokenIssuedAt = 0;
let contract = null;
let latestQuote = null;
let connected = false;
let lastTradeAt = null;
let lastSnapshotAt = null;
let latestSnapshot = null;
let profiles = {};
let flowBars = { oneMin:{}, fiveMin:{} };
let stateDirty = false;
let tradeEventsReceived = 0;
let tradeEventsMatched = 0;
let quoteEventsReceived = 0;
let depthEventsReceived = 0;
let lastRawTradeEvent = null;
let lastRawQuoteEvent = null;
let lastRawDepthEvent = null;
let reconnectCount = 0;
let subscriptionResults = {quotes:null,trades:null,depth:null};
let lastSignalAlert = {key:null,at:0};
let relayState = null;
let absorptionDepthHistory=[];
let lastAbsorptionDepthSnapshotAt=0;
let morningCutoff = null;
let morningCutoffCaptureBusy = false;
let signalStability = {setupKey:null,cvdAlignedSince:null,targetRoomAward:null};
let actionableSignalState={active:null,pending:null,pendingSince:0,emptySince:0};
let analysisDisplayState={active:null,pending:null,pendingSince:0};
const activeViewers=new Map();
const VIEWER_TTL_MS=45000;
function activeViewerCount(){
  const now=Date.now();
  for(const [id,at] of activeViewers) if(now-at>VIEWER_TTL_MS) activeViewers.delete(id);
  return activeViewers.size;
}

async function authenticate() {
  const r = await fetch(API_BASE + "/api/Auth/loginKey", {
    method: "POST",
    headers: {"Content-Type":"application/json","Accept":"text/plain"},
    body: JSON.stringify({userName: USERNAME, apiKey: API_KEY})
  });
  if (!r.ok) throw new Error(`Auth HTTP ${r.status}: ${await r.text()}`);
  const j = await r.json();
  if (!j.success || !j.token) throw new Error(`Auth failed: ${j.errorCode} ${j.errorMessage || ""}`);
  token = j.token;
  tokenIssuedAt = Date.now();
}
async function getToken() {
  if (!token || Date.now() - tokenIssuedAt > 18 * 3600000) await authenticate();
  return token;
}
async function post(apiPath, payload, retry=true, timeoutMs=null) {
  const t = await getToken();
  const r = await fetch(API_BASE + apiPath, {
    method: "POST",
    ...(timeoutMs ? {signal:AbortSignal.timeout(timeoutMs)} : {}),
    headers: {"Content-Type":"application/json","Accept":"text/plain","Authorization":`Bearer ${t}`},
    body: JSON.stringify(payload)
  });
  if (r.status === 401 && retry) {
    await authenticate();
    return post(apiPath, payload, false, timeoutMs);
  }
  if (!r.ok) throw new Error(`${apiPath} HTTP ${r.status}: ${await r.text()}`);
  return r.json();
}
async function findContract() {
  const r = await post("/api/Contract/search", {searchText:MARKET_SYMBOL, live:LIVE});
  const cs = (r.contracts || []).filter(c =>
    JSON.stringify(c).toUpperCase().includes(MARKET_SYMBOL) ||
    String(c.description || "").toUpperCase().includes(MARKET_DESC)
  );
  contract = cs.find(c => c.activeContract) || cs[0];
  if (!contract) throw new Error(MARKET_SYMBOL+" contract not found");
}
async function bars(unit, unitNumber, days, limit=20000) {
  const end = DateTime.utc();
  const start = end.minus({days});
  const r = await post("/api/History/retrieveBars", {
    contractId: contract.id,
    live: LIVE,
    startTime: start.toISO(),
    endTime: end.toISO(),
    unit,
    unitNumber,
    limit,
    includePartialBar: true
  });
  const out = (r.bars || []).map(b => ({
    t:b.t ?? b.time ?? b.timestamp,
    o:b.o ?? b.open, h:b.h ?? b.high, l:b.l ?? b.low,
    c:b.c ?? b.close, v:b.v ?? b.volume
  }));
  out.sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
  return out;
}

function dt(b){ return DateTime.fromISO(b.t, {setZone:true}).toUTC(); }
function between(rows, start, end) {
  const s = start.toUTC().toMillis(), e = end.toUTC().toMillis();
  return rows.filter(b => {
    const t = dt(b).toMillis();
    return t >= s && t < e;
  });
}
function summary(rows) {
  if (!rows.length) return null;
  return {
    open:+rows[0].o,
    high:Math.max(...rows.map(x=>+x.h)),
    low:Math.min(...rows.map(x=>+x.l)),
    close:+rows.at(-1).c,
    volume:rows.reduce((s,x)=>s+(+x.v||0),0),
    start:rows[0].t,
    end:rows.at(-1).t,
    bars:rows.length
  };
}
function vwap(rows) {
  let n=0,d=0;
  for(const b of rows){
    const vol=+b.v||0;
    if(vol<=0) continue;
    const tp=(+b.h + +b.l + +b.c)/3;
    n += tp*vol;
    d += vol;
  }
  return d ? n/d : null;
}
function roundTick(x,t){ return Math.round(x/t)*t; }

function estimatedProfile(rows,tick) {
  const buckets = new Map();
  for(const b of rows){
    const vol=+b.v||0;
    if(vol<=0) continue;
    const lo=roundTick(+b.l,tick), hi=roundTick(+b.h,tick);
    const count=Math.max(1,Math.round((hi-lo)/tick)+1), per=vol/count;
    for(let i=0;i<count;i++){
      const p=+(lo+i*tick).toFixed(10);
      buckets.set(p,(buckets.get(p)||0)+per);
    }
  }
  if(!buckets.size) return null;
  const arr=[...buckets.entries()].sort((a,b)=>a[0]-b[0]).map(([price,volume])=>({price,volume}));
  const poc=arr.reduce((a,b)=>b.volume>a.volume?b:a);
  const total=arr.reduce((s,x)=>s+x.volume,0), target=total*.70;
  let idx=arr.indexOf(poc),lo=idx-1,hi=idx+1,cum=poc.volume;
  const selected=new Set([idx]);
  while(cum<target&&(lo>=0||hi<arr.length)){
    const lv=lo>=0?arr[lo].volume:-1, hv=hi<arr.length?arr[hi].volume:-1;
    let i;
    if(hv>=lv&&hi<arr.length) i=hi++;
    else i=lo--;
    if(i>=0&&i<arr.length&&!selected.has(i)){
      selected.add(i);
      cum+=arr[i].volume;
    }
  }
  const ps=[...selected].map(i=>arr[i].price);
  return {
    method:"estimated from 1m OHLCV",
    isTrueTradeByTradeProfile:false,
    valueAreaPercent:.70,
    poc:poc.price,
    vah:Math.max(...ps),
    val:Math.min(...ps)
  };
}

function atr(day,n=14) {
  if(day.length<n+1) return null;
  const tr=[];
  for(let i=1;i<day.length;i++){
    const p=+day[i-1].c,b=day[i];
    tr.push(Math.max(+b.h-+b.l,Math.abs(+b.h-p),Math.abs(+b.l-p)));
  }
  const a=tr.slice(-n);
  return a.reduce((s,x)=>s+x,0)/a.length;
}
function rangeStats(day,n=20) {
  const a=day.slice(-n).map(b=>+b.h-+b.l).sort((a,b)=>a-b);
  if(!a.length) return null;
  return {
    sampleDays:a.length,
    averageRange:a.reduce((s,x)=>s+x,0)/a.length,
    medianRange:a[Math.floor(a.length/2)],
    minRange:a[0],
    maxRange:a.at(-1)
  };
}

function nowPT(){ return DateTime.now().setZone(ZONE); }

function previousRTH(rows) {
  let d = nowPT().minus({days:1}).startOf("day");
  for(let i=0;i<8;i++){
    const start=d.set({hour:6,minute:30,second:0,millisecond:0});
    const end=d.set({hour:13,minute:0,second:0,millisecond:0});
    const x=between(rows,start,end);
    if(x.length){
      const z=summary(x);
      z.date=d.toISODate();
      z.sessionStartPacific=start.toISO();
      z.sessionEndPacific=end.toISO();
      return z;
    }
    d=d.minus({days:1});
  }
  return null;
}

function previousFullGlobex(rows) {
  let sessionDate = nowPT().minus({days:1}).startOf("day");
  for(let i=0;i<8;i++){
    const start=sessionDate.minus({days:1}).set({hour:15,minute:0,second:0,millisecond:0});
    const end=sessionDate.set({hour:14,minute:0,second:0,millisecond:0});
    const x=between(rows,start,end);
    if(x.length){
      const z=summary(x);
      z.sessionDate=sessionDate.toISODate();
      z.sessionStartPacific=start.toISO();
      z.sessionEndPacific=end.toISO();
      return z;
    }
    sessionDate=sessionDate.minus({days:1});
  }
  return null;
}

function currentSessions(rows) {
  // Canonical wall-clock windows in New York; calendar arithmetic preserves DST.
  const now=nowPT().setZone("America/New_York");
  const today=now.startOf("day");
  const sessionStart=(now.hour>=18?today:today.minus({days:1})).set({hour:18,minute:0,second:0,millisecond:0});
  const nextDay=sessionStart.plus({days:1}).startOf("day");
  const overnightEnd=nextDay.set({hour:9,minute:30,second:0,millisecond:0});
  const asiaStart=sessionStart.set({hour:20,minute:0,second:0,millisecond:0});
  const asiaEnd=nextDay.set({hour:3,minute:0,second:0,millisecond:0});
  const londonStart=asiaEnd;
  const londonEnd=nextDay.set({hour:8,minute:30,second:0,millisecond:0});

  // Overnight levels/profile are defined only through the 06:30 PT RTH open.
  // Before 06:30 they build live; after 06:30 they are frozen for the day.
  const onStop=now<overnightEnd?now:overnightEnd;
  const asiaStop=now<asiaEnd?now:asiaEnd;
  const londonStop=now<londonEnd?now:londonEnd;
  const overnightRows=between(rows,sessionStart,onStop);

  return {
    overnight:summary(overnightRows),
    asia:now>=asiaStart?summary(between(rows,asiaStart,asiaStop)):null,
    london:now>=londonStart?summary(between(rows,londonStart,londonStop)):null,
    overnightRows,
    bounds:{
      timeZone:"America/New_York",
      asiaStartEastern:asiaStart.toISO(),
      asiaEndEastern:asiaEnd.toISO(),
      londonStartEastern:londonStart.toISO(),
      londonEndEastern:londonEnd.toISO(),
      // Preserve the existing Pacific fields for payload consumers.
      globexStartPacific:sessionStart.setZone(ZONE).toISO(),
      overnightEndPacific:overnightEnd.setZone(ZONE).toISO(),
      asiaStartPacific:asiaStart.setZone(ZONE).toISO(),
      asiaEndPacific:asiaEnd.setZone(ZONE).toISO(),
      londonStartPacific:londonStart.setZone(ZONE).toISO(),
      londonEndPacific:londonEnd.setZone(ZONE).toISO()
    }
  };
}

function previousCMEWeek(rows) {
  const now=nowPT();
  const thisMonday=now.startOf("week"); // Luxon: Monday
  const priorSunday=thisMonday.minus({days:8}).startOf("day");
  const priorFriday=thisMonday.minus({days:3}).startOf("day");

  const start=priorSunday.set({hour:15,minute:0,second:0,millisecond:0});
  const end=priorFriday.set({hour:14,minute:0,second:0,millisecond:0});

  const z=summary(between(rows,start,end));
  if(z){
    z.sessionStartPacific=start.toISO();
    z.sessionEndPacific=end.toISO();
  }
  return z;
}

function aggregateFourHourBars(hour){
  const groups=new Map();
  for(const b of hour||[]){
    const t=DateTime.fromISO(b.t,{setZone:true}).setZone("America/New_York");
    if(!t.isValid) continue;
    const anchor=(t.hour>=18?t:t.minus({days:1})).startOf("day").set({hour:18,minute:0,second:0,millisecond:0});
    const elapsed=t.toMillis()-anchor.toMillis();
    if(elapsed<0) continue;
    const slot=Math.floor(elapsed/(4*3600000));
    if(slot<0||slot>5) continue;
    const start=anchor.plus({hours:slot*4});
    const key=start.toUTC().toISO();
    let x=groups.get(key);
    if(!x){
      x={t:key,o:+b.o,h:+b.h,l:+b.l,c:+b.c,v:+b.v||0,_times:new Set()};
      groups.set(key,x);
    }else{
      x.h=Math.max(x.h,+b.h); x.l=Math.min(x.l,+b.l); x.c=+b.c; x.v+=(+b.v||0);
    }
    x._times.add(t.toUTC().toMillis());
  }
  const now=DateTime.now().setZone("America/New_York");
  return [...groups.values()].filter(x=>{
    const start=DateTime.fromISO(x.t,{setZone:true}).setZone("America/New_York");
    const shortened=start.hour===14;
    const expected=shortened?3:4;
    const end=start.plus({hours:shortened?3:4});
    return x._times.size>=expected&&now>=end;
  }).map(x=>({t:x.t,o:x.o,h:x.h,l:x.l,c:x.c,v:x.v}))
    .sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
}

function scoredPivots(hour,current,a14) {
  const raw=[], ref=a14||300;
  for(let i=2;i<hour.length-2;i++){
    const b=hour[i],h=+b.h,l=+b.l;
    const hi=hour.slice(i-2,i).every(x=>h>+x.h)&&hour.slice(i+1,i+3).every(x=>h>=+x.h);
    const lo=hour.slice(i-2,i).every(x=>l<+x.l)&&hour.slice(i+1,i+3).every(x=>l<=+x.l);
    if(!hi&&!lo) continue;

    const type=hi?"high":"low", price=hi?h:l, later=hour.slice(i+3);
    let swept=false,exc=0;

    for(const x of later){
      if(type==="high"){
        if(+x.h>price) swept=true;
        exc=Math.max(exc,price-+x.l);
      } else {
        if(+x.l<price) swept=true;
        exc=Math.max(exc,+x.h-price);
      }
    }

    const dist=Math.abs(current-price);
    const age=Math.max(0,(Date.now()-Date.parse(b.t))/3600000);
    const score=(!swept?45:0)+Math.min(25,(exc/ref)*20)+Math.max(0,15-(dist/ref)*5)+Math.max(0,15-age/48);

    raw.push({
      type,price,time:b.t,
      sweptLater:swept,
      liquidityStatus:swept?"swept":"untouched",
      distanceFromCurrentPoints:+dist.toFixed(2),
      significanceScore:+score.toFixed(1)
    });
  }
  return raw.sort((a,b)=>b.significanceScore-a.significanceScore).slice(0,8);
}


function median(nums){
  const a=nums.filter(Number.isFinite).sort((x,y)=>x-y);
  if(!a.length) return null;
  const m=Math.floor(a.length/2);
  return a.length%2?a[m]:(a[m-1]+a[m])/2;
}
function trueRangeSeries(rows){
  const out=[];
  for(let i=1;i<rows.length;i++){
    const b=rows[i],pc=+rows[i-1].c;
    out.push(Math.max(+b.h-+b.l,Math.abs(+b.h-pc),Math.abs(+b.l-pc)));
  }
  return out;
}
function current5mAtr(rows,n=20){
  const a=trueRangeSeries(rows.slice(-(n+1)));
  return a.length? a.reduce((x,y)=>x+y,0)/a.length : null;
}
function lastClosedBar(rows,minutes){
  const now=Date.now();
  for(let i=rows.length-1;i>=0;i--){
    const t=Date.parse(rows[i].t);
    if(Number.isFinite(t) && t + minutes*60000 <= now) return rows[i];
  }
  return null;
}
function nearestLevelDistance(levels,price,side){
  const vals=levels.map(l=>+l.price).filter(Number.isFinite);
  const xs=side==="BUY"?vals.filter(x=>x>price):vals.filter(x=>x<price);
  if(!xs.length) return null;
  return side==="BUY"?Math.min(...xs)-price:price-Math.max(...xs);
}
function mergeLiveFiveMinuteBars(historical,relayBars){
  const map=new Map();
  for(const b of historical||[]){
    const t=Date.parse(b.t); if(!Number.isFinite(t)) continue;
    const k=Math.floor(t/300000)*300000;
    map.set(k,{...b,t:new Date(k).toISOString(),v:+b.v||0});
  }
  for(const r of relayBars||[]){
    const t=Date.parse(r.t); if(!Number.isFinite(t)) continue;
    const k=Math.floor(t/300000)*300000;
    const base=map.get(k);
    if(base){
      map.set(k,{
        ...base,
        t:new Date(k).toISOString(),
        o:Number.isFinite(+base.o)?+base.o:+r.o,
        h:Math.max(Number.isFinite(+base.h)?+base.h:-Infinity,+r.h),
        l:Math.min(Number.isFinite(+base.l)?+base.l:Infinity,+r.l),
        c:+r.c,
        // REST may already contain part of this bar, so do not add volumes and double-count.
        v:Math.max(+base.v||0,+r.volume||0),
        live:true
      });
    }else{
      map.set(k,{
        t:new Date(k).toISOString(),o:+r.o,h:+r.h,l:+r.l,c:+r.c,
        v:+r.volume||0,live:true
      });
    }
  }
  return [...map.values()].sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
}

function aggregateBars(rows,minutes){
  const out=[], by=new Map(), ms=minutes*60000;
  for(const b of rows||[]){
    const t=Date.parse(b.t); if(!Number.isFinite(t)) continue;
    const k=Math.floor(t/ms)*ms;
    let x=by.get(k);
    if(!x){
      x={t:new Date(k).toISOString(),o:+b.o,h:+b.h,l:+b.l,c:+b.c,v:+b.v||0,volume:+b.volume||0,buyVolume:+b.buyVolume||0,sellVolume:+b.sellVolume||0,delta:+b.delta||0,trades:+b.trades||0};
      by.set(k,x); out.push(x);
    }else{
      x.h=Math.max(x.h,+b.h); x.l=Math.min(x.l,+b.l); x.c=+b.c;
      x.v+=(+b.v||0); x.volume+=(+b.volume||0); x.buyVolume+=(+b.buyVolume||0);
      x.sellVolume+=(+b.sellVolume||0); x.delta+=(+b.delta||0); x.trades+=(+b.trades||0);
    }
  }
  return out.sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
}
function deltaTrend15m(f5){
  const bars=aggregateBars(f5||[],15).filter(b=>Date.parse(b.t)+15*60000<=Date.now()).slice(-4);
  if(!bars.length) return {direction:"NEUTRAL",sum:0,bars:0};
  const sum=bars.reduce((s,b)=>s+(+b.delta||0),0);
  const pos=bars.filter(b=>+b.delta>0).length,neg=bars.filter(b=>+b.delta<0).length;
  const direction=(sum>0&&pos>=2)?"BULLISH":(sum<0&&neg>=2)?"BEARISH":"NEUTRAL";
  return {direction,sum,bars:bars.length,last:+bars.at(-1).delta||0};
}

function currentAtr(rows,n=20){
  const a=trueRangeSeries((rows||[]).slice(-(n+1)));
  return a.length?a.reduce((x,y)=>x+y,0)/a.length:null;
}
function detectOrderBlocks(rows,levels,flowRows,currentPrice,rthVwap,globexVwap,timeframeMinutes=5){
  rows=(rows||[]).slice(-240);
  if(rows.length<12) return [];
  const atr=currentAtr(rows,20)||20;
  const medBody=median(rows.slice(-40).map(b=>Math.abs(+b.c-+b.o)))||4;
  const flowMap=new Map((flowRows||[]).map(x=>[Date.parse(x.t),x]));
  const blocks=[];
  for(let i=4;i<rows.length-3;i++){
    const origin=rows[i];
    const next=rows.slice(i+1,Math.min(rows.length,i+4));
    if(!next.length) continue;
    const prior=rows.slice(Math.max(0,i-6),i);
    const swingHigh=Math.max(...prior.map(b=>+b.h));
    const swingLow=Math.min(...prior.map(b=>+b.l));
    const impulseHigh=Math.max(...next.map(b=>+b.h));
    const impulseLow=Math.min(...next.map(b=>+b.l));
    const impulseClose=+next.at(-1).c;
    const moveUp=impulseHigh-(+origin.h);
    const moveDn=(+origin.l)-impulseLow;
    const bosPad=Math.max(.5,(+contract?.tickSize||.25)*2);
    const bullish=(+origin.c<+origin.o) && impulseClose>swingHigh+bosPad && moveUp>=1.25*atr;
    const bearish=(+origin.c>+origin.o) && impulseClose<swingLow-bosPad && moveDn>=1.25*atr;
    if(!bullish&&!bearish) continue;

    const side=bullish?"BUY":"SELL";
    const low=bullish?Math.min(+origin.o,+origin.l):Math.min(+origin.o,+origin.h);
    const high=bullish?Math.max(+origin.o,+origin.l):Math.max(+origin.o,+origin.h);
    const bodyQuality=Math.abs(+next[0].c-+next[0].o)>=1.25*medBody;
    const post=rows.slice(i+1);
    const invalid=bullish?post.some(b=>+b.c<low):post.some(b=>+b.c>high);
    if(invalid) continue;
    const touches=post.filter(b=>+b.h>=low && +b.l<=high).length;
    const fresh=touches<=1;
    const center=(low+high)/2;
    const confl=levels.some(l=>Math.abs(+l.price-center)<=Math.max(5,atr*.15));
    const vwap=Number.isFinite(+rthVwap)?+rthVwap:+globexVwap;
    const vwapAligned=Number.isFinite(vwap)?(bullish?center<=vwap:center>=vwap):false;
    const f=flowMap.get(Date.parse(next[0].t));
    const deltaAligned=f?(bullish?+f.delta>0:+f.delta<0):false;
    const risk=Math.max(8,atr*.25);
    const targetDist=nearestLevelDistance(levels,center,side);
    const room=Number.isFinite(targetDist)&&targetDist>=risk*1.5;
    let score=25;
    if(moveUp>=1.25*atr||moveDn>=1.25*atr) score+=15;
    if(bodyQuality) score+=10;
    if(fresh) score+=10;
    if(confl) score+=10;
    if(vwapAligned) score+=10;
    if(deltaAligned) score+=10;
    if(room) score+=10;
    blocks.push({
      side,time:origin.t,low:+low.toFixed(2),high:+high.toFixed(2),
      score:Math.min(100,score),fresh,touches,confluence:confl,
      deltaConfirmed:deltaAligned,vwapAligned,targetRoom:room,
      timeframeMinutes,invalidated:false
    });
  }
  const unique=[];
  for(const b of blocks.sort((a,b)=>b.score-a.score||Date.parse(b.time)-Date.parse(a.time))){
    if(unique.some(x=>x.side===b.side && Math.abs(((x.low+x.high)/2)-((b.low+b.high)/2))<2)) continue;
    unique.push(b);
    if(unique.filter(x=>x.side==="BUY").length>=2 && unique.filter(x=>x.side==="SELL").length>=2) break;
  }
  return unique.slice(0,4);
}
function detectOpeningGaps(five,currentPrice,atr5){
  const rows=(five||[]).slice().sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
  if(!rows.length) return [];
  const byDate=new Map();
  for(const b of rows){
    const p=DateTime.fromISO(b.t,{setZone:true}).setZone(ZONE);
    const d=p.toISODate();
    if(!byDate.has(d)) byDate.set(d,[]);
    byDate.get(d).push({bar:b,pt:p,mins:p.hour*60+p.minute});
  }
  const dates=[...byDate.keys()].sort();
  const gaps=[];
  const addGap=(kind,label,openRow,priorCloseRow)=>{
    if(!openRow||!priorCloseRow) return;
    const open=+openRow.bar.o, prevClose=+priorCloseRow.bar.c;
    if(!Number.isFinite(open)||!Number.isFinite(prevClose)||open===prevClose) return;
    const low=Math.min(open,prevClose),high=Math.max(open,prevClose);
    const dir=open>prevClose?"UP":"DOWN";
    const after=rows.filter(b=>Date.parse(b.t)>=Date.parse(openRow.bar.t));
    let filledAt=null;
    for(const b of after){
      if(dir==="UP" && +b.l<=prevClose){filledAt=b.t;break;}
      if(dir==="DOWN" && +b.h>=prevClose){filledAt=b.t;break;}
    }
    const distance=currentPrice<low?low-currentPrice:currentPrice>high?currentPrice-high:0;
    const proximity=Math.max(MARKET_PARAMS.gapMin,(Number.isFinite(+atr5)?+atr5:20)*.65);
    gaps.push({
      id:kind+"-"+openRow.pt.toISODate(),
      kind,label,date:openRow.pt.toISODate(),direction:dir,
      priorClose:+prevClose.toFixed(2),open:+open.toFixed(2),
      low:+low.toFixed(2),high:+high.toFixed(2),
      size:+Math.abs(open-prevClose).toFixed(2),
      filled:Boolean(filledAt),filledAt,
      distance:+distance.toFixed(2),
      near:!filledAt && distance<=proximity,
      fillDirection:dir==="UP"?"SELL":"BUY",
      fillTarget:+prevClose.toFixed(2),
      triggerEdge:+open.toFixed(2)
    });
  };

  for(let di=1;di<dates.length;di++){
    const d=dates[di],prevDate=dates[di-1];
    const day=byDate.get(d)||[],prev=byDate.get(prevDate)||[];

    // CME session gap: prior session's last trade before 14:00 PT to 15:00 PT reopen.
    const sessOpen=day.find(x=>x.mins===900);
    const prevClose=[...prev].reverse().find(x=>x.mins<840);
    if(sessOpen&&prevClose) addGap("SESSION","Session",sessOpen,prevClose);

    // RTH gap: previous RTH close (last 5m bar before 13:00 PT) to 06:30 PT open.
    const rthOpen=day.find(x=>x.mins===390);
    let prevRth=null;
    for(let j=di-1;j>=0&&!prevRth;j--){
      const pr=byDate.get(dates[j])||[];
      prevRth=[...pr].reverse().find(x=>x.mins>=390&&x.mins<780);
    }
    if(rthOpen&&prevRth) addGap("RTH","RTH",rthOpen,prevRth);
  }

  return gaps
    .filter(g=>!g.filled)
    .sort((a,b)=>(a.distance-b.distance)||Date.parse(b.date)-Date.parse(a.date))
    .slice(0,8);
}

function currentOrb(five,f5){
  const now=nowPT();
  const day=now.startOf("day");
  const start=day.set({hour:6,minute:30,second:0,millisecond:0});
  const end=day.set({hour:6,minute:45,second:0,millisecond:0});
  if(now<end) return {formed:false,start:start.toISO(),end:end.toISO(),high:null,low:null,volume:null,median5mVolume:null,volumeRatio:null};
  const rows=between(five||[],start,end);
  if(!rows.length) return {formed:false,start:start.toISO(),end:end.toISO(),high:null,low:null,volume:null,median5mVolume:null,volumeRatio:null};
  const z=summary(rows);
  const prior=(five||[]).filter(b=>Date.parse(b.t)<start.toUTC().toMillis()).slice(-20);
  const med=median(prior.map(b=>+b.v||0));
  const orbVol=z.volume||0;
  return {
    formed:true,start:start.toISO(),end:end.toISO(),
    high:z.high,low:z.low,open:z.open,close:z.close,volume:orbVol,
    median5mVolume:med,
    volumeRatio:med?+((orbVol/3)/med).toFixed(2):null
  };
}

const EDGEFUL_12M=require("./edgeful-baselines.json");

// Historical context for the chart and analysis. It never substitutes collector prices or live order flow.
function buildEdgefulChartContext(bars5m){
  const now=nowPT().setZone("America/New_York");
  const day=(now.hour>=18?now.plus({days:1}):now).startOf("day");
  const at=(hour,minute=0)=>day.set({hour,minute,second:0,millisecond:0});
  const open=at(9,30),close=at(16),stop=now<close?now:close;
  const root=MARKET_SYMBOL==="MES"?"ES":"NQ",e=EDGEFUL_12M[root];
  if(!e) return {items:[],sessionDate:day.toISODate(),note:"Historical baseline unavailable"};
  const rows=(bars5m||[]).filter(b=>Date.parse(b.t)+300000<=now.toMillis()&&[b.o,b.h,b.l,b.c].every(v=>v!==null&&Number.isFinite(+v))).sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
  const window=(s,t)=>between(rows,s,t);
  const complete=(s,t)=>{
    const times=new Set(window(s,t).map(b=>Date.parse(b.t)));
    for(let ms=s.toMillis();ms<t.toMillis();ms+=300000) if(!times.has(ms)) return false;
    return now>=t;
  };
  const range=bs=>{
    if(!bs.length)return null;
    const z=summary(bs);
    return {...z,highTime:bs.find(b=>+b.h===z.high).t,lowTime:bs.find(b=>+b.l===z.low).t};
  };
  const rth=window(open,stop),r=range(rth),items=[];
  const pct=v=>Number.isFinite(+v)?(+v).toFixed(1)+"%":"unavailable";
  const signedPct=v=>Number.isFinite(+v)?((+v>=0?"+":"")+(+v).toFixed(2)+"%"):"unavailable";
  const continuationSummary=c=>{
    const p=c?.performance||{},r=c?.retracement||{};
    const up=p.avgUpExtensionPct??p.avgBreakoutExtensionPct;
    const down=p.avgDownExtensionPct??p.avgBreakdownExtensionPct;
    const upHalf=r.breakoutHalf,downHalf=r.breakdownHalf;
    const out=[];
    if(Number.isFinite(+up)||Number.isFinite(+down)) out.push("avg extension up "+signedPct(up)+" / down "+signedPct(down));
    if(Number.isFinite(+upHalf)||Number.isFinite(+downHalf)) out.push("0.5x retrace up "+pct(upHalf)+" / down "+pct(downHalf));
    return out.join(" · ");
  };
  const continuationDetail=c=>{
    const p=c?.performance||{},r=c?.retracement||{};
    const out=[];
    const up=p.avgUpExtensionPct??p.avgBreakoutExtensionPct;
    const down=p.avgDownExtensionPct??p.avgBreakdownExtensionPct;
    const maxUp=p.maxUpExtensionPct??p.maxBreakoutExtensionPct;
    const maxDown=p.maxDownExtensionPct??p.minBreakdownExtensionPct;
    if(Number.isFinite(+up)||Number.isFinite(+down)) out.push("average first-break extension up "+signedPct(up)+" / down "+signedPct(down));
    if(Number.isFinite(+maxUp)||Number.isFinite(+maxDown)) out.push("maximum observed extension up "+signedPct(maxUp)+" / down "+signedPct(maxDown));
    if(Number.isFinite(+r.breakoutHalf)||Number.isFinite(+r.breakdownHalf)) out.push("0.5x retracement up "+pct(r.breakoutHalf)+" / down "+pct(r.breakdownHalf));
    if(Number.isFinite(+r.breakoutOne)||Number.isFinite(+r.breakdownOne)) out.push("1x retracement up "+pct(r.breakoutOne)+" / down "+pct(r.breakdownOne));
    const samples=[];
    const upN=p.breakoutSample??r.breakoutSample;
    const downN=p.breakdownSample??r.breakdownSample;
    if(Number.isFinite(+upN)) samples.push("up n="+upN);
    if(Number.isFinite(+downN)) samples.push("down n="+downN);
    if(samples.length) out.push("samples "+samples.join(" / "));
    return out.join("; ");
  };
  const add=(key,label,state,stat,geometry=null,priority=0,extra={})=>items.push({key,label,state,stat,geometry,priority,...extra});
  const geometry=(z,s,t)=>z?{high:z.high,low:z.low,highTime:z.highTime,lowTime:z.lowTime,start:s.toISO(),end:t.toISO(),monitorEnd:close.toISO()}:null;
  const breakState=(bs,z,method="close")=>{
    const up=bs.some(b=>+(method==="close"?b.c:b.h)>z.high),down=bs.some(b=>+(method==="close"?b.c:b.l)<z.low);
    return up&&down?"Both sides broken":up?"High broken":down?"Low broken":"No break yet";
  };
  const globex=day.minus({days:1}).set({hour:18}),on=range(window(globex,now<open?now:open));
  const onComplete=complete(globex,open);
  const overnightContinuation=e.overnight.continuation||{};
  const overnightStat="Any-side break "+pct(e.overnight.anyBreak)+" · one side "+pct(e.overnight.oneSide)+" · both "+pct(e.overnight.double)+" · neither "+pct(e.overnight.noBreak)+
    (Number.isFinite(+overnightContinuation.greenToGreen)||Number.isFinite(+overnightContinuation.redToRed)?" · continuation green→green "+pct(overnightContinuation.greenToGreen)+" · red→red "+pct(overnightContinuation.redToRed):"");
  add("overnight","Overnight",now<open?"Building · completed bars":onComplete?breakState(rth,on,"wick"):"Collector coverage incomplete",
    overnightStat,
    now<open?geometry(on,globex,now):onComplete?geometry(on,globex,open):null,10,
    {detail:"Full-session historical outcomes; continuation is the next RTH close color after the overnight direction (samples: "+(overnightContinuation.sample||"unavailable")+"). Building overnight levels can change."});
  const rangeCondition=(key,label,s,t,hist,priority,detail)=>{
    if(now<t){add(key,label,"Forms by "+t.toFormat("HH:mm")+" ET","Waiting for completed collector bars");return null;}
    if(!complete(s,t)){add(key,label,"Collector coverage incomplete","Historical condition unavailable");return null;}
    const z=range(window(s,t)),after=window(t,stop);
    add(key,label,breakState(after,z),hist,geometry(z,s,t),priority,{detail:detail||"Break state uses completed 5-minute closes. Historical frequencies describe the full observation window."});
    return z;
  };
  const orb15Continuation=e.orb15.continuation||{};
  const orb15ContinuationSummary=continuationSummary(orb15Continuation);
  const z15=rangeCondition("orb15","15m ORB",open,at(9,45),"5m-close: one side "+pct(e.orb15.oneSide)+" · both "+pct(e.orb15.double)+" · neither "+pct(e.orb15.noBreak)+(orb15ContinuationSummary?" · "+orb15ContinuationSummary:""),30,
    "Break state uses completed 5-minute closes. Edgeful continuation: "+(continuationDetail(orb15Continuation)||"unavailable")+". Historical frequencies describe the full observation window.");
  const ib60Continuation=e.ib60.continuation||{};
  const ib60ContinuationSummary=continuationSummary(ib60Continuation);
  rangeCondition("ib60","60m IB",open,at(10,30),"Single break "+pct(e.ib60.single)+" · both "+pct(e.ib60.double)+" · neither "+pct(e.ib60.neither)+(ib60ContinuationSummary?" · "+ib60ContinuationSummary:""),40,
    "Break state uses completed 5-minute closes. Edgeful continuation: "+(continuationDetail(ib60Continuation)||"unavailable")+". Historical frequencies describe the full observation window.");
  if(z15){
    const green=z15.close>z15.open,doji=z15.close===z15.open;
    const openSample=green?e.opening15.greenOpenSample:e.opening15.redOpenSample;
    add("opening15","Opening candle",doji?"Doji · no directional condition":green?"Green opening 15m":"Red opening 15m",doji?"No doji baseline provided":"Same-color RTH close "+pct(green?e.opening15.greenToGreen:e.opening15.redToRed)+(Number.isFinite(+openSample)?" · n="+openSample:""),geometry(z15,open,at(9,45)),20,{detail:"RTH candle color continuation through the RTH close; this is not the return from an entry at the opening-range break. Conditional sample count: "+(openSample||"unavailable")+"."});
  }else add("opening15","Opening candle","Waiting for complete 09:30–09:45 ET range","Historical condition unavailable");
  // Find a complete preceding RTH session, including Friday before Monday.
  let prior=null,priorDay=null;
  for(let n=1;n<=7&&!prior;n++){
    const d=day.minus({days:n}),s=d.set({hour:9,minute:30}),t=d.set({hour:16});
    if(complete(s,t)){prior=range(window(s,t));priorDay=d;}
    else if(window(s,t).length) break; // Never silently skip a partial prior session.
  }
  for(const [key,label,side,follow] of [["pdh","Prior high","high",e.prevDay.highFollowGreen],["pdl","Prior low","low",e.prevDay.lowFollowRed]]){
    const broken=prior&&r&&(side==="high"?r.high>prior.high:r.low<prior.low);
    const price=prior?.[side],anchor=prior?.[side+"Time"];
    add(key,label,!prior?"Prior RTH coverage incomplete":broken?"Broken during RTH":now<open?"Waiting for RTH":"Not broken during RTH",
      broken?"Of "+(side==="high"?"PDH":"PDL")+"-break sessions, RTH closes "+(side==="high"?"green ":"red ")+pct(follow):"Historical follow-through activates after the level breaks",
      prior?{high:price,low:price,highTime:anchor,lowTime:anchor,start:anchor,end:open.toISO(),monitorEnd:close.toISO()}:null,broken?60:5,
      {detail:"Prior RTH: "+(priorDay?.toISODate()||"unavailable")+". Conditional sample count unavailable; candle color is not a trade win rate."});
  }
  const first=rth.find(b=>Date.parse(b.t)===open.toMillis());
  if(prior&&first){
    const up=+first.o>prior.close,equal=+first.o===prior.close;
    const filled=up?r.low<=prior.close:r.high>=prior.close;
    add("gap","RTH gap",equal?"No opening gap":filled?"Filled during RTH":up?"Gap up · unfilled":"Gap down · unfilled",
      equal?"No gap condition":"Full-fill frequency "+pct(up?e.gapFill.up:e.gapFill.down),
      equal?null:{high:Math.max(+first.o,prior.close),low:Math.min(+first.o,prior.close),highTime:open.toISO(),lowTime:open.toISO(),start:open.toISO(),end:close.toISO(),monitorEnd:close.toISO()},filled?15:50,
      {detail:"Today's RTH gap only. Prior close "+prior.close.toFixed(2)+"; conditional sample count unavailable."});
  }else add("gap","RTH gap","Waiting for RTH open / prior-close coverage","Historical condition unavailable");
  if(r){
    const points=r.high-r.low,adr=e.adr14.value;
    add("range","Range / ADR",points.toFixed(2)+" pts · "+(adr>0?(100*points/adr).toFixed(0)+"% of saved ADR":"ADR unavailable"),
      "ADR respected "+pct(e.adr14.respected)+" · ATR respected "+pct(e.atr14.respected),geometry(r,open,stop),25,
      {detail:"Saved 14-day ADR reference: "+adr+" points, as of "+e.asOf+". ATR history is separate; no current ATR14 projection or price target is inferred."});
  }else add("range","Range / ADR","Waiting for RTH","No current RTH range");
  const mid=e.middayOrb?.close||{},middayContinuation=e.middayOrb?.continuation||{};
  const middayContinuationSummary=continuationSummary(middayContinuation);
  rangeCondition("midday","14–15 ET ORB",at(14),at(15),"5m-close: one side "+pct(mid.oneSidePct)+" · both "+pct(mid.doublePct)+" · neither "+pct(mid.noBreakPct)+(middayContinuationSummary?" · "+middayContinuationSummary:""),70,
    "Break state uses completed 5-minute closes. Edgeful continuation: "+(continuationDetail(middayContinuation)||"unavailable")+". Historical frequencies describe the 11:00–12:00 PT range and the final monitored hour.");
  const midday=items.find(x=>x.key==="midday");
  if(midday) midday.detail="Range 14:00–15:00 ET (11 AM–noon PT); observe 15:00–16:00 ET. Break sample: "+(e.middayOrb?.sample||"unavailable")+" sessions. "+(continuationDetail(middayContinuation)||"Continuation sample unavailable")+".";
  const lastCompleted=rows.at(-1)?.t||null;
  const collectorStale=!lastCompleted||now.toMillis()-Date.parse(lastCompleted)>600000;
  const expectedStop=stop.set({minute:Math.floor(stop.minute/5)*5,second:0,millisecond:0});
  const rthCoverage=now<open?null:complete(open,expectedStop);
  const stale=now.diff(DateTime.fromISO(e.asOf,{zone:"America/New_York"}),"days").days>10;
  return {version:1,lastCompleted,collectorStale,rthCoverage,sessionDate:day.toISODate(),phase:now<open?"PRE-RTH":now>=close?"RTH COMPLETE":"RTH",items,
    source:"Edgeful",ticker:root,asOf:e.asOf,lookback:EDGEFUL_12M.lookback,baselineSessions:e.sample,stale,
    note:"Historical frequencies and continuation outcomes, not trade win rates. Collector prices; completed 5m bars. Analysis may use these as a low-weight qualifier, never as standalone live evidence."};
}

function buildEdgefulContext({bars5m}){
  const chart=buildEdgefulChartContext(bars5m);
  const e=EDGEFUL_12M[chart.ticker]||{};
  return {
    chart,source:chart.source||"Edgeful",ticker:chart.ticker,lookback:chart.lookback,
    asOf:chart.asOf,refreshedOn:EDGEFUL_12M.refreshedOn||null,
    items:chart.items.map(x=>({key:x.key,label:x.label,text:x.state+" · "+x.stat,applicable:Boolean(x.geometry)})),
    excluded:[{key:"londonNy",reason:e.londonNy?.note||"Excluded"}],
    note:"Historical conditional frequencies and continuation outcomes only; not current-trade probabilities. Analysis uses them as a low-weight qualifier while live collector evidence remains primary."
  };
}

function currentMiddayOrb(five){
  const now=nowPT();
  const day=now.startOf("day");
  const start=day.set({hour:11,minute:0,second:0,millisecond:0});
  const end=day.set({hour:12,minute:0,second:0,millisecond:0});
  const close=day.set({hour:13,minute:0,second:0,millisecond:0});
  const edgeRoot=MARKET_SYMBOL==="MES"?"ES":"NQ";
  const edgeBase=EDGEFUL_12M[edgeRoot]||{};
  const mid=edgeBase.middayOrb||{};
  const stats={
    source:EDGEFUL_12M.source||"Edgeful",
    lookback:EDGEFUL_12M.lookback||"12mo",
    asOf:edgeBase.asOf||null,
    refreshedOn:EDGEFUL_12M.refreshedOn||null,
    sampleSize:mid.sample||null,
    wick:mid.wick||{},
    close:mid.close||{},
    continuation:mid.continuation||{}
  };
  if(now<end) return {formed:false,start:start.toISO(),end:end.toISO(),monitorEnd:close.toISO(),high:null,low:null,stats};
  const rows=between(five||[],start,end);
  if(!rows.length) return {formed:false,start:start.toISO(),end:end.toISO(),monitorEnd:close.toISO(),high:null,low:null,stats};
  const z=summary(rows);
  const afterEnd=now<close?now:close;
  const after=between(five||[],end,afterEnd);
  let wickUp=false,wickDown=false,closeUp=false,closeDown=false,firstWickBreak=null,firstCloseBreak=null;
  for(const b of after){
    if(!firstWickBreak){
      if(+b.h>z.high) firstWickBreak={side:"UP",time:b.t,price:+b.h};
      else if(+b.l<z.low) firstWickBreak={side:"DOWN",time:b.t,price:+b.l};
    }
    if(+b.h>z.high) wickUp=true;
    if(+b.l<z.low) wickDown=true;
    if(!firstCloseBreak){
      if(+b.c>z.high) firstCloseBreak={side:"UP",time:b.t,price:+b.c};
      else if(+b.c<z.low) firstCloseBreak={side:"DOWN",time:b.t,price:+b.c};
    }
    if(+b.c>z.high) closeUp=true;
    if(+b.c<z.low) closeDown=true;
  }
  const state=(up,down)=>up&&down?"DOUBLE BREAK":up?"UP BREAK":down?"DOWN BREAK":"NO BREAK";
  return {
    formed:true,start:start.toISO(),end:end.toISO(),monitorEnd:close.toISO(),
    high:z.high,low:z.low,open:z.open,close:z.close,
    wickState:state(wickUp,wickDown),closeState:state(closeUp,closeDown),
    wickUp,wickDown,closeUp,closeDown,firstWickBreak,firstCloseBreak,
    active:now>=end&&now<close,
    stats
  };
}

function stabilizeAnalysis(raw){
  const key=(raw?.bias||"NEUTRAL")+"|"+((raw?.setups||[])[0]?.title||"none");
  const now=Date.now();
  if(!analysisDisplayState.active){
    analysisDisplayState.active=raw;
    analysisDisplayState.pending=null;
    return raw;
  }
  const activeKey=(analysisDisplayState.active?.bias||"NEUTRAL")+"|"+((analysisDisplayState.active?.setups||[])[0]?.title||"none");
  if(key===activeKey){
    analysisDisplayState.active=raw;
    analysisDisplayState.pending=null;
    return raw;
  }
  if(analysisDisplayState.pending?.key!==key){
    analysisDisplayState.pending={key,raw};
    analysisDisplayState.pendingSince=now;
    return analysisDisplayState.active;
  }
  analysisDisplayState.pending.raw=raw;
  if(now-analysisDisplayState.pendingSince>=15000){
    analysisDisplayState.active=raw;
    analysisDisplayState.pending=null;
  }
  return analysisDisplayState.active;
}

function stabilizeActionableSignal(candidate){
  const now=Date.now();
  if(!candidate || candidate.score<50 || candidate.side==="NEUTRAL"){
    if(actionableSignalState.active){
      if(!actionableSignalState.emptySince) actionableSignalState.emptySince=now;
      if(now-actionableSignalState.emptySince<15000) return actionableSignalState.active;
    }
    actionableSignalState={active:null,pending:null,pendingSince:0,emptySince:0};
    return {side:"NEUTRAL",score:0,reasons:[],components:{}};
  }
  actionableSignalState.emptySince=0;
  const key=candidate.side+"|"+candidate.key;
  if(actionableSignalState.active?.key===candidate.key && actionableSignalState.active?.side===candidate.side){
    actionableSignalState.active=candidate;
    actionableSignalState.pending=null;
    return candidate;
  }
  if(!actionableSignalState.active){
    if(actionableSignalState.pending?.fullKey!==key){
      actionableSignalState.pending={...candidate,fullKey:key};
      actionableSignalState.pendingSince=now;
      return {side:"NEUTRAL",score:0,reasons:[],components:{}};
    }
    actionableSignalState.pending={...candidate,fullKey:key};
    if(now-actionableSignalState.pendingSince>=12000){
      actionableSignalState.active=candidate;
      actionableSignalState.pending=null;
      return candidate;
    }
    return {side:"NEUTRAL",score:0,reasons:[],components:{}};
  }
  if(actionableSignalState.pending?.fullKey!==key){
    actionableSignalState.pending={...candidate,fullKey:key};
    actionableSignalState.pendingSince=now;
    return actionableSignalState.active;
  }
  actionableSignalState.pending={...candidate,fullKey:key};
  if(now-actionableSignalState.pendingSince>=12000){
    actionableSignalState.active=candidate;
    actionableSignalState.pending=null;
  }
  return actionableSignalState.active;
}

function buildMarketAnalysis({currentPrice,levels,f1,f5,signal,traps,orderBlocks,globexVwap,rthVwap,sessionCvd,atr5,orb,middayOrb,bars5m,delta15,profiles,icebergs,gaps}){
  const closed1=lastClosedBar(f1||[],1);
  const closed5=lastClosedBar(f5||[],5);
  const closed5s=(f5||[]).filter(b=>Date.parse(b.t)+300000<=Date.now());
  const prior5=closed5s.length>1?closed5s.at(-2):null;
  const activeVwap=Number.isFinite(+rthVwap)?+rthVwap:+globexVwap;
  let biasPoints=0;
  const reasons=[];
  const profile=profiles?.rth||profiles?.session||null;
  const edgeRoot=MARKET_SYMBOL==="MES"?"ES":"NQ";
  const edgeHistory=EDGEFUL_12M[edgeRoot]||{};
  const selectedEdge=edgeHistory.selectedQualifiers||{};
  const edgefulChecks=[];
  let edgefulPoints=0;
  const edgeClamp=v=>Math.max(-1,Math.min(1,+v||0));
  const edgePct=v=>Number.isFinite(+v)?(+v).toFixed(1)+"%":"unavailable";
  const edgeSignedPct=v=>Number.isFinite(+v)?((+v>=0?"+":"")+(+v).toFixed(2)+"%"):"unavailable";
  const edgeRateCheck=(rate,text)=>({score:edgeClamp((+rate-50)/10),text:text||("historical continuation "+edgePct(rate))});
  const edgeSideCheck=(entry,side)=>{
    const c=entry?.continuation||{},p=c.performance||{},r=c.retracement||{};
    const long=side==="BUY";
    const avg=long?(p.avgUpExtensionPct??p.avgBreakoutExtensionPct):(p.avgDownExtensionPct??p.avgBreakdownExtensionPct);
    const half=long?r.breakoutHalf:r.breakdownHalf;
    const sample=long?(p.breakoutSample??r.breakoutSample):(p.breakdownSample??r.breakdownSample);
    const parts=[];let score=0;
    const breakRate=entry?.oneSide??entry?.single??entry?.close?.oneSidePct;
    if(Number.isFinite(+breakRate)) parts.push("break profile "+edgePct(breakRate));
    if(Number.isFinite(+avg)){
      score+=(long?(+avg>0):(+avg<0)) ? .5 : -.5;
      parts.push("avg extension "+edgeSignedPct(avg));
    }
    if(Number.isFinite(+half)){
      score+=(+half<=50) ? .5 : -.5;
      parts.push("0.5x retrace "+edgePct(half));
    }
    if(Number.isFinite(+sample)) parts.push("n="+sample);
    return {score:edgeClamp(score),text:parts.join(" · ")||"continuation sample unavailable"};
  };
  const addEdgefulCheck=(label,side,check)=>{
    if(!side||!check||!Number.isFinite(+check.score)) return;
    const support=edgeClamp(check.score);
    edgefulPoints+=side==="BUY"?support:-support;
    edgefulChecks.push({label,side,support:+support.toFixed(2),text:check.text});
    if(Math.abs(support)>=.25) reasons.push("Edgeful "+label+" "+(support>0?"supports ":"conflicts with ")+side+" · "+check.text);
  };

  if(Number.isFinite(activeVwap)){
    if(currentPrice>activeVwap){biasPoints+=2;reasons.push("price above active VWAP");}
    else if(currentPrice<activeVwap){biasPoints-=2;reasons.push("price below active VWAP");}
  }
  if(profile){
    if(Number.isFinite(+profile.vah)&&currentPrice>+profile.vah){biasPoints+=1;reasons.push("price above profile value");}
    else if(Number.isFinite(+profile.val)&&currentPrice<+profile.val){biasPoints-=1;reasons.push("price below profile value");}
    else if(Number.isFinite(+profile.poc)){reasons.push("profile POC "+(+profile.poc).toFixed(2));}
  }
  if(closed5&&prior5){
    if(+closed5.c>+prior5.c){biasPoints+=1;reasons.push("5m structure pushing higher");}
    else if(+closed5.c<+prior5.c){biasPoints-=1;reasons.push("5m structure pushing lower");}
  }
  if(closed5&&Number.isFinite(+closed5.delta)){
    if(+closed5.delta>0){biasPoints+=1;reasons.push("closed 5m delta positive");}
    else if(+closed5.delta<0){biasPoints-=1;reasons.push("closed 5m delta negative");}
  }
  if(Number.isFinite(+sessionCvd)){
    if(+sessionCvd>0){biasPoints+=1;reasons.push("session CVD positive");}
    else if(+sessionCvd<0){biasPoints-=1;reasons.push("session CVD negative");}
  }
  if(delta15?.direction==="BULLISH"){biasPoints+=2;reasons.push("15m delta trend positive");}
  else if(delta15?.direction==="BEARISH"){biasPoints-=2;reasons.push("15m delta trend negative");}
  const newestTrap=(traps||[])[0];
  if(newestTrap && Date.now()-Date.parse(newestTrap.time)<=20*60000){
    if(newestTrap.side==="BUY"){biasPoints+=2;reasons.push("confirmed seller trap");}
    if(newestTrap.side==="SELL"){biasPoints-=2;reasons.push("confirmed buyer trap");}
  }
  const iceberg=(icebergs||[]).filter(x=>(+x.ageSec||999)<=20 && (+x.score||0)>=60)[0]||null;
  if(iceberg){
    if(iceberg.side==="BUY"){biasPoints+=2;reasons.push("probable buy iceberg "+iceberg.price.toFixed(2));}
    else if(iceberg.side==="SELL"){biasPoints-=2;reasons.push("probable sell iceberg "+iceberg.price.toFixed(2));}
  }
  if(signal?.side==="BUY" && signal.score>=65){biasPoints+=2;reasons.push("BUY setup score "+signal.score);}
  if(signal?.side==="SELL" && signal.score>=65){biasPoints-=2;reasons.push("SELL setup score "+signal.score);}

  // Edgeful is a low-weight historical qualifier. It can strengthen or weaken
  // a live direction, but it never creates a setup without collector evidence.
  if(orb?.formed && Number.isFinite(+orb.open)&&Number.isFinite(+orb.close) && +orb.close!==+orb.open){
    const side=+orb.close>+orb.open?"BUY":"SELL";
    const rate=side==="BUY"?edgeHistory.opening15?.greenToGreen:edgeHistory.opening15?.redToRed;
    addEdgefulCheck("opening-candle continuation",side,edgeRateCheck(rate,"same-color RTH close "+edgePct(rate)));
  }
  const closedBars=(bars5m||[]).filter(b=>Date.parse(b.t)+300000<=Date.now());
  const ptNow=nowPT();
  const rthDay=ptNow.startOf("day");
  const rthStart=rthDay.set({hour:6,minute:30,second:0,millisecond:0});
  const rthEnd=rthDay.set({hour:13,minute:0,second:0,millisecond:0});
  const rthStop=ptNow<rthEnd?ptNow:rthEnd;
  const currentRthBars=between(closedBars,rthStart,rthStop);
  let priorRth=null,priorRthDate=null;
  for(let n=1;n<=7&&!priorRth;n++){
    const d=rthDay.minus({days:n}),s=d.set({hour:6,minute:30,second:0,millisecond:0}),t=d.set({hour:13,minute:0,second:0,millisecond:0});
    const xs=between(closedBars,s,t);
    if(xs.length){
      // Do not skip a partial/early-close prior session and accidentally substitute an older day.
      if(xs.length>=70){priorRth=summary(xs);priorRthDate=d.toISODate();}
      break;
    }
  }
  const pdh=(levels||[]).find(x=>x.id==="pdh"),pdl=(levels||[]).find(x=>x.id==="pdl");
  const rangeBreak=(end,high,low,stop=rthEnd)=>{
    if(!end||!Number.isFinite(+high)||!Number.isFinite(+low)) return {up:false,down:false};
    const endMs=end?.toMillis?.()??Date.parse(end),stopMs=stop?.toMillis?.()??Date.parse(stop);
    const after=closedBars.filter(b=>Date.parse(b.t)>=endMs&&(!Number.isFinite(stopMs)||Date.parse(b.t)<stopMs));
    return {up:after.some(b=>+b.c>+high),down:after.some(b=>+b.c<+low)};
  };
  const currentPriorBreak={
    up:Boolean(pdh&&currentRthBars.some(b=>+b.c>+pdh.price)),
    down:Boolean(pdl&&currentRthBars.some(b=>+b.c<+pdl.price))
  };

  if(orb?.formed){
    const br=rangeBreak(orb.end,orb.high,orb.low);
    const side=br.up&&!br.down?"BUY":br.down&&!br.up?"SELL":null;
    if(side) addEdgefulCheck("15m ORB continuation",side,edgeSideCheck(edgeHistory.orb15,side));
  }

  const ibStart=rthStart;
  const ibEnd=rthDay.set({hour:7,minute:30,second:0,millisecond:0});
  if(ptNow>=ibEnd){
    const ibRows=between(closedBars,ibStart,ibEnd);
    if(ibRows.length>=12){
      const ib=summary(ibRows),br=rangeBreak(ibEnd,ib.high,ib.low);
      const side=br.up&&!br.down?"BUY":br.down&&!br.up?"SELL":null;
      if(side){
        const q=selectedEdge.ib60Size;
        const ibPct=Number.isFinite(+ib.open)&&+ib.open!==0?100*(+ib.high-+ib.low)/Math.abs(+ib.open):null;
        const sizeMatch=edgeRoot==="NQ"&&q&&Number.isFinite(ibPct)&&ibPct>=+q.bucketMinPct&&ibPct<=+q.bucketMaxPct;
        if(sizeMatch){
          addEdgefulCheck("60m IB size bucket",side,edgeRateCheck(q.singleBreak,
            q.bucketMinPct.toFixed(1)+"–"+q.bucketMaxPct.toFixed(2)+"% IB → one-side 5m-close break "+edgePct(q.singleBreak)+" · n="+q.sample+" · current IB "+ibPct.toFixed(2)+"%"));
        }else{
          addEdgefulCheck("60m IB continuation",side,edgeSideCheck(edgeHistory.ib60,side));
        }
      }
    }
  }

  // Inside-day breakout frequency is an expansion qualifier only. Direction is supplied
  // by the first live, close-confirmed break of the prior RTH range.
  const insideQ=selectedEdge.insideDayBreakout;
  if(priorRth&&currentRthBars.length&&insideQ){
    const currentOpen=+currentRthBars[0].o;
    const openedInside=currentOpen>+priorRth.low&&currentOpen<+priorRth.high;
    const priorRangePct=+priorRth.close!==0?100*(+priorRth.high-+priorRth.low)/Math.abs(+priorRth.close):null;
    const bucketMatch=Number.isFinite(priorRangePct)&&priorRangePct>=+insideQ.bucketMinPct&&priorRangePct<=+insideQ.bucketMaxPct;
    const side=currentPriorBreak.up&&!currentPriorBreak.down?"BUY":currentPriorBreak.down&&!currentPriorBreak.up?"SELL":null;
    if(openedInside&&bucketMatch&&side){
      addEdgefulCheck("inside-day breakout bucket",side,edgeRateCheck(insideQ.rate,
        "inside RTH open · prior range "+priorRangePct.toFixed(2)+"% · breakout frequency "+edgePct(insideQ.rate)+" · n="+insideQ.sample+" · direction from live 5m close"));
    }
  }

  if(middayOrb?.formed && middayOrb.active && (middayOrb.closeState==="UP BREAK"||middayOrb.closeState==="DOWN BREAK")){
    const side=middayOrb.closeState==="UP BREAK"?"BUY":"SELL";
    addEdgefulCheck("11-12 ORB continuation",side,edgeSideCheck(edgeHistory.middayOrb,side));
  }
  let priorColorLevelApplied=false;
  if(edgeRoot==="ES"&&priorRth&&currentRthBars.length){
    const q=selectedEdge.priorDayColorLevel||{};
    const previousRed=+priorRth.close<+priorRth.open,previousGreen=+priorRth.close>+priorRth.open;
    if(previousRed&&currentPriorBreak.up&&pdh&&currentPrice>+pdh.price&&q.previousRedHighBreakGreenClose){
      const s=q.previousRedHighBreakGreenClose;
      addEdgefulCheck("prior-red + PDH break","BUY",edgeRateCheck(s.rate,
        "previous red day → PDH broken / green RTH close "+edgePct(s.rate)+" · n="+s.sample+" · prior RTH "+priorRthDate));
      priorColorLevelApplied=true;
    }else if(previousGreen&&currentPriorBreak.down&&pdl&&currentPrice<+pdl.price&&q.previousGreenLowBreakRedClose){
      const s=q.previousGreenLowBreakRedClose;
      addEdgefulCheck("prior-green + PDL break","SELL",edgeRateCheck(s.rate,
        "previous green day → PDL broken / red RTH close "+edgePct(s.rate)+" · n="+s.sample+" · prior RTH "+priorRthDate));
      priorColorLevelApplied=true;
    }
  }
  if(orb?.formed&&!priorColorLevelApplied){
    if(pdh&&Number.isFinite(+pdh.price)&&+currentPrice>+pdh.price){
      addEdgefulCheck("prior-high follow-through","BUY",edgeRateCheck(edgeHistory.prevDay?.highFollowGreen,"RTH close green after PDH break "+edgePct(edgeHistory.prevDay?.highFollowGreen)));
    }else if(pdl&&Number.isFinite(+pdl.price)&&+currentPrice<+pdl.price){
      addEdgefulCheck("prior-low follow-through","SELL",edgeRateCheck(edgeHistory.prevDay?.lowFollowRed,"RTH close red after PDL break "+edgePct(edgeHistory.prevDay?.lowFollowRed)));
    }
  }
  const activeGap=(gaps||[]).find(g=>g.kind==="RTH"&&g.near&&!g.filled);
  if(activeGap){
    const side=activeGap.fillDirection;
    const q=selectedEdge.smallGapFill;
    const gapPct=Number.isFinite(+activeGap.priorClose)&&+activeGap.priorClose!==0?100*(+activeGap.size)/Math.abs(+activeGap.priorClose):null;
    const bucketMatch=q&&Number.isFinite(gapPct)&&gapPct>0&&gapPct<=+q.bucketMaxPct;
    const sized=bucketMatch?(activeGap.direction==="UP"?q.up:q.down):null;
    if(sized&&Number.isFinite(+sized.rate)){
      addEdgefulCheck("size-conditioned gap fill",side,edgeRateCheck(sized.rate,
        (activeGap.direction==="UP"?"up":"down")+" RTH gap "+gapPct.toFixed(2)+"% · full fill "+edgePct(sized.rate)+" · n="+sized.sample));
    }else{
      const rate=activeGap.direction==="UP"?edgeHistory.gapFill?.up:edgeHistory.gapFill?.down;
      addEdgefulCheck("gap-fill tendency",side,edgeRateCheck(rate,(activeGap.direction==="UP"?"up":"down")+" gap full-fill "+edgePct(rate)));
    }
  }
  const overnightStart=(ptNow.hour>=15?ptNow:ptNow.minus({days:1})).startOf("day").set({hour:15,minute:0,second:0,millisecond:0});
  const overnightEnd=overnightStart.plus({days:1}).set({hour:6,minute:30,second:0,millisecond:0});
  if(ptNow>=overnightEnd){
    const overnightRows=between(closedBars,overnightStart,overnightEnd);
    if(overnightRows.length){
      const overnight=summary(overnightRows),side=+overnight.close>+overnight.open?"BUY":+overnight.close<+overnight.open?"SELL":null;
      if(side){
        const rate=side==="BUY"?edgeHistory.overnight?.continuation?.greenToGreen:edgeHistory.overnight?.continuation?.redToRed;
        addEdgefulCheck("overnight continuation",side,edgeRateCheck(rate,(side==="BUY"?"green":"red")+" overnight to same-color RTH close "+edgePct(rate)));
      }
    }
  }
  biasPoints+=edgefulPoints;

  const bias=biasPoints>=3?"BULLISH":biasPoints<=-3?"BEARISH":Math.abs(biasPoints)<=1?"NEUTRAL":"MIXED";
  const strength=Math.abs(biasPoints)>=6?"strong":Math.abs(biasPoints)>=3?"moderate":"light";
  const major=(levels||[]).filter(l=>Number.isFinite(+l.price)&&l.priority>=80);
  const above=major.filter(l=>+l.price>currentPrice).sort((a,b)=>+a.price-+b.price);
  const below=major.filter(l=>+l.price<currentPrice).sort((a,b)=>+b.price-+a.price);
  const setups=[];
  const edgefulBias=edgefulPoints>=.75?"BUY":edgefulPoints<=-.75?"SELL":"NEUTRAL";
  const edgefulSetupAdjustment=side=>edgefulBias==="NEUTRAL"?0:edgefulBias===side?5:-5;
  const edgefulSetupContext=side=>{
    const checks=edgefulChecks.filter(x=>x.side===side&&Math.abs(x.support)>=.25).slice(0,2);
    if(!checks.length) return edgefulBias!=="NEUTRAL"&&edgefulBias!==side?"Edgeful qualifier conflicts; require stronger live confirmation":"";
    return "Edgeful qualifier "+(edgefulBias===side?"supports ":edgefulBias!=="NEUTRAL"?"conflicts with ":"")+side+" · "+checks.map(x=>x.label+" "+x.text).join("; ");
  };
  const edgeContinuationContext=(entry,side)=>{
    const c=entry?.continuation||{},p=c.performance||{},r=c.retracement||{};
    const long=side==="BUY";
    const avg=long?(p.avgUpExtensionPct??p.avgBreakoutExtensionPct):(p.avgDownExtensionPct??p.avgBreakdownExtensionPct);
    const half=long?r.breakoutHalf:r.breakdownHalf;
    const sample=long?(p.breakoutSample??r.breakoutSample):(p.breakdownSample??r.breakdownSample);
    const out=[];
    if(Number.isFinite(+avg)) out.push("avg first-break extension "+edgeSignedPct(avg));
    if(Number.isFinite(+half)) out.push("0.5x retrace "+(+half).toFixed(1)+"%");
    if(Number.isFinite(+sample)) out.push("n="+sample);
    return out.join(" · ");
  };

  const targetFor=(side,from)=>{
    const xs=major.filter(l=>side==="BUY"?+l.price>from:+l.price<from)
      .sort((a,b)=>side==="BUY"?+a.price-+b.price:+b.price-+a.price);
    return xs[0]||null;
  };

  if(iceberg){
    const side=iceberg.side,tgt=targetFor(side,currentPrice);
    const long=side==="BUY";
    setups.push({
      side,
      title:(long?"Buy":"Sell")+" iceberg absorption "+iceberg.score,
      trigger:(long
        ?"Aggressive selling fails to break "+iceberg.price.toFixed(2)+"; enter only after price reclaims/holds above the level with positive 1m delta."
        :"Aggressive buying fails to lift "+iceberg.price.toFixed(2)+"; enter only after price rejects/holds below the level with negative 1m delta."),
      invalidation:(long
        ?"Acceptance below the iceberg price after confirmation."
        :"Acceptance above the iceberg price after confirmation."),
      target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next major liquidity level",
      quality:iceberg.score,
      context:"Probable hidden "+(long?"buyer":"seller")+" · "+Math.round(iceberg.aggressorVolume)+" aggressive contracts absorbed · "+iceberg.refreshes+" depth replenishments"
    });
  }

  if(newestTrap && Date.now()-Date.parse(newestTrap.time)<=20*60000){
    const side=newestTrap.side;
    const tgt=targetFor(side,currentPrice);
    setups.push({
      side,
      title:(newestTrap.type==="seller-trap"?"Seller-trap reversal":"Buyer-trap reversal")+" at "+newestTrap.label,
      trigger:side==="BUY"
        ?"Hold/reclaim "+newestTrap.label+" with positive closed 1m delta."
        :"Reject/hold below "+newestTrap.label+" with negative closed 1m delta.",
      invalidation:side==="BUY"
        ?"Closed 1m acceptance back below "+newestTrap.label+"."
        :"Closed 1m acceptance back above "+newestTrap.label+".",
      target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next major liquidity level",
      quality:signal?.side===side?signal.score:null
    });
  }

  const atr=Number.isFinite(+atr5)?+atr5:20;
  const nearbyOb=(orderBlocks||[])
    .filter(o=>o.fresh && o.score>=65)
    .sort((a,b)=>{
      const da=currentPrice<+a.low?+a.low-currentPrice:currentPrice>+a.high?currentPrice-+a.high:0;
      const db=currentPrice<+b.low?+b.low-currentPrice:currentPrice>+b.high?currentPrice-+b.high:0;
      return da-db || b.score-a.score;
    })[0];
  if(nearbyOb){
    const dist=currentPrice<+nearbyOb.low?+nearbyOb.low-currentPrice:currentPrice>+nearbyOb.high?currentPrice-+nearbyOb.high:0;
    const obProximity=Math.min(MARKET_PARAMS.obCap,Math.max(MARKET_PARAMS.obMin,atr*.9));
    if(dist<=obProximity){
      const tgt=targetFor(nearbyOb.side,(nearbyOb.low+nearbyOb.high)/2);
      setups.push({
        side:nearbyOb.side,
        title:(nearbyOb.side==="BUY"?"Bullish":"Bearish")+" 15m OB retest "+nearbyOb.score,
        trigger:"Price trades into "+(+nearbyOb.low).toFixed(2)+"–"+(+nearbyOb.high).toFixed(2)+" and closed 1m/5m order flow confirms "+(nearbyOb.side==="BUY"?"buying":"selling")+" away from the zone.",
        invalidation:"5m close through the "+(nearbyOb.side==="BUY"?"low ":"high ")+(nearbyOb.side==="BUY"?+nearbyOb.low:+nearbyOb.high).toFixed(2)+".",
        target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next major liquidity level",
        quality:nearbyOb.score
      });
    }
  }

  // Opening Range Breakout: first 15 minutes of NY RTH (06:30-06:45 PT).
  if(orb?.formed && closed5){
    const vols=(bars5m||[]).slice(-40,-1).map(b=>+b.v||0).filter(x=>x>0);
    const medVol=median(vols)||orb.median5mVolume||0;
    const liveVol=+((bars5m||[]).at(-1)?.v||closed5.volume||0);
    const volRatio=medVol?liveVol/medVol:null;
    const d=+closed5.delta||0;
    const distHi=Math.abs(currentPrice-+orb.high),distLo=Math.abs(currentPrice-+orb.low);
    const orbProximity=Math.min(MARKET_PARAMS.orbCap,Math.max(MARKET_PARAMS.orbMin,atr*.75));
    const nearHi=distHi<=orbProximity;
    const nearLo=distLo<=orbProximity;
    const longBias=bias==="BULLISH"||bias==="MIXED";
    const shortBias=bias==="BEARISH"||bias==="MIXED";

    const orbSetup=(side)=>{
      const long=side==="BUY", level=long?+orb.high:+orb.low;
      const tgt=targetFor(side,level+(long?.01:-.01));
      const biasAligned=long?longBias:shortBias;
      const deltaAligned=long?d>0:d<0;
      const vwapAligned=Number.isFinite(activeVwap)?(long?currentPrice>activeVwap:currentPrice<activeVwap):false;
      const volumeStrong=Number.isFinite(volRatio)&&volRatio>=1.2;
      let quality=25+(biasAligned?20:0)+(volumeStrong?15:0)+(deltaAligned?15:0)+(vwapAligned?10:0);
      if(tgt) quality+=10;
      if(signal?.side===side&&signal.score>=65) quality+=5;
      quality=Math.min(100,quality);
      const edgeContinuation=edgeContinuationContext(edgeHistory.orb15,side);
      setups.push({
        side,
        title:"ORB "+(long?"high":"low")+" breakout",
        trigger:"5m close "+(long?"above ":"below ")+(long?"ORB High ":"ORB Low ")+level.toFixed(2)+
          " with "+(long?"positive":"negative")+" delta and volume expansion"+
          (medVol?" (prefer ≥1.2× recent 5m median).":"."),
        invalidation:"5m close back "+(long?"below ORB High.":"above ORB Low."),
        target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next major liquidity level",
        quality,
        context:"Bias "+bias+" · volume "+(Number.isFinite(volRatio)?volRatio.toFixed(2)+"×":"n/a")+" · delta "+(d>0?"+":"")+Math.round(d)+(edgeContinuation?" · Edgeful 12mo "+edgeContinuation:"")
      });
    };
    if(nearHi && (longBias || currentPrice>=+orb.high)) orbSetup("BUY");
    if(nearLo && (shortBias || currentPrice<=+orb.low)) orbSetup("SELL");
  }

  // Midday ORB: 11:00-12:00 PT range, monitored from 12:00-13:00 PT.
  // Edgeful 12-month context is shown separately as historical frequency, not a trade probability.
  if(middayOrb?.formed && middayOrb.active && closed5 && (middayOrb.closeState==="UP BREAK"||middayOrb.closeState==="DOWN BREAK")){
    const side=middayOrb.closeState==="UP BREAK"?"BUY":"SELL";
    const long=side==="BUY",level=long?+middayOrb.high:+middayOrb.low;
    const d=+closed5.delta||0;
    const deltaAligned=long?d>0:d<0;
    const vwapAligned=Number.isFinite(activeVwap)?(long?currentPrice>activeVwap:currentPrice<activeVwap):false;
    const biasAligned=long?(bias==="BULLISH"||bias==="MIXED"):(bias==="BEARISH"||bias==="MIXED");
    const tgt=targetFor(side,level+(long?.01:-.01));
    let quality=40+(deltaAligned?15:0)+(vwapAligned?10:0)+(biasAligned?10:0)+(tgt?10:0);
    if(signal?.side===side&&signal.score>=65) quality+=5;
    quality=Math.min(100,quality);
    const hist=middayOrb.stats?.close||{};
    const edgeContinuation=edgeContinuationContext(edgeHistory.middayOrb,side);
    setups.push({
      side,
      title:"11-12 PT ORB "+(long?"high":"low")+" confirmed",
      trigger:"5m close "+(long?"above ":"below ")+(long?"11-12 ORB High ":"11-12 ORB Low ")+level.toFixed(2)+" with aligned delta; prefer acceptance/retest rather than first touch.",
      invalidation:"5m close back inside the 11-12 PT range.",
      target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next major liquidity level",
      quality,
      context:"Edgeful 12mo close-confirmed context: one-side break "+Number(hist.oneSidePct||0).toFixed(1)+"%, double break "+Number(hist.doublePct||0).toFixed(1)+"% · "+(edgeContinuation||"continuation unavailable")+" · n="+(middayOrb.stats?.sampleSize||"—")+" · as of "+(middayOrb.stats?.asOf||"—")
    });
  }

  const nearGap=(gaps||[]).filter(g=>g.near&&!g.filled)[0]||null;
  if(nearGap){
    const side=nearGap.fillDirection;
    const long=side==="BUY";
    const d1=+closed1?.delta||0,d5=+closed5?.delta||0;
    const biasAligned=long?(bias==="BULLISH"||bias==="MIXED"):(bias==="BEARISH"||bias==="MIXED");
    const flowAligned=long?(d1>0&&d5>0):(d1<0&&d5<0);
    const vwapAligned=Number.isFinite(activeVwap)?(long?currentPrice>activeVwap:currentPrice<activeVwap):false;
    const vols=(bars5m||[]).slice(-21,-1).map(b=>+b.v||0).filter(x=>x>0);
    const medVol=median(vols)||0;
    const liveVol=+((bars5m||[]).at(-1)?.v||0);
    const volRatio=medVol?liveVol/medVol:null;
    let quality=30+(biasAligned?20:0)+(flowAligned?20:0)+(vwapAligned?10:0)+(Number.isFinite(volRatio)&&volRatio>=1.15?10:0);
    if(signal?.side===side&&signal.score>=65) quality+=10;
    quality=Math.min(100,quality);
    setups.push({
      side,
      title:nearGap.label+" gap fill "+nearGap.size.toFixed(2)+" pts",
      trigger:(long
        ?"Acceptance above the lower gap edge "+nearGap.open.toFixed(2)+" with positive 1m/5m delta."
        :"Acceptance below the upper gap edge "+nearGap.open.toFixed(2)+" with negative 1m/5m delta."),
      invalidation:(long
        ?"5m rejection back below the gap-open edge."
        :"5m rejection back above the gap-open edge."),
      target:"Gap fill "+nearGap.fillTarget.toFixed(2),
      quality,
      context:"Unfilled "+nearGap.label+" "+nearGap.direction.toLowerCase()+" gap · "+nearGap.distance.toFixed(2)+" pts away · volume "+(Number.isFinite(volRatio)?volRatio.toFixed(2)+"×":"n/a")
    });
  }

  const majorProximity=Math.min(MARKET_PARAMS.majorCap,Math.max(MARKET_PARAMS.majorMin,atr));
  if((bias==="BULLISH"||bias==="MIXED") && above[0] && Math.abs(+above[0].price-currentPrice)<=majorProximity){
    const lvl=above[0],tgt=targetFor("BUY",+lvl.price+0.01);
    setups.push({
      side:"BUY",title:"Breakout acceptance above "+lvl.label,
      trigger:"Closed 5m acceptance above "+lvl.label+" "+(+lvl.price).toFixed(2)+" with positive 1m/5m delta.",
      invalidation:"5m close back below "+lvl.label+".",
      target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next higher major level",
      quality:Math.min(100,45+(closed5&&+closed5.delta>0?15:0)+(Number.isFinite(activeVwap)&&currentPrice>activeVwap?10:0)+(delta15?.direction==="BULLISH"?15:0))
    });
  } else if((bias==="BEARISH"||bias==="MIXED") && below[0] && Math.abs(currentPrice-+below[0].price)<=majorProximity){
    const lvl=below[0],tgt=targetFor("SELL",+lvl.price-0.01);
    setups.push({
      side:"SELL",title:"Breakdown acceptance below "+lvl.label,
      trigger:"Closed 5m acceptance below "+lvl.label+" "+(+lvl.price).toFixed(2)+" with negative 1m/5m delta.",
      invalidation:"5m close back above "+lvl.label+".",
      target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next lower major level",
      quality:Math.min(100,45+(closed5&&+closed5.delta<0?15:0)+(Number.isFinite(activeVwap)&&currentPrice<activeVwap?10:0)+(delta15?.direction==="BEARISH"?15:0))
    });
  }

  // Apply the net Edgeful qualifier to every candidate suggestion. This is
  // deliberately small: live collector flow, structure, VWAP, and location
  // remain the primary evidence and can override a historical lean.
  for(const setup of setups){
    if(Number.isFinite(+setup.quality)) setup.quality=Math.max(0,Math.min(100,+setup.quality+edgefulSetupAdjustment(setup.side)));
    const edgeContext=edgefulSetupContext(setup.side);
    if(edgeContext) setup.context=(setup.context?setup.context+" · ":"")+edgeContext;
  }
  const dedup=[];
  for(const x of setups.sort((a,b)=>(+b.quality||0)-(+a.quality||0))){
    if(dedup.some(y=>y.title===x.title)) continue;
    dedup.push(x);
    if(dedup.length>=3) break;
  }
  return {
    bias,strength,points:+biasPoints.toFixed(2),reasons:reasons.slice(0,5),setups:dedup,
    edgeful:{bias:edgefulBias,points:+edgefulPoints.toFixed(2),checks:edgefulChecks},
    asOf:new Date().toISOString(),
    note:"Conditional setups only; live collector evidence remains primary. Edgeful 12-month condition and continuation statistics are low-weight qualifiers for bias and buy/sell suggestions, not standalone trade probabilities."
  };
}

async function sendSmsAlert(message){
  if(!ALERT_SMS_TO||!TWILIO_ACCOUNT_SID||!TWILIO_AUTH_TOKEN||!TWILIO_FROM_NUMBER) return false;
  const auth=Buffer.from(TWILIO_ACCOUNT_SID+":"+TWILIO_AUTH_TOKEN).toString("base64");
  const body=new URLSearchParams({To:ALERT_SMS_TO,From:TWILIO_FROM_NUMBER,Body:message});
  const r=await fetch("https://api.twilio.com/2010-04-01/Accounts/"+TWILIO_ACCOUNT_SID+"/Messages.json",{
    method:"POST",headers:{"Authorization":"Basic "+auth,"Content-Type":"application/x-www-form-urlencoded"},body
  });
  if(!r.ok){console.error("sms alert",r.status,await r.text());return false;}
  return true;
}
function maybeSendSignalAlert(signal,currentPrice){
  if(!signal||signal.score<ALERT_SCORE_THRESHOLD||signal.side==="NEUTRAL") return;
  const key=signal.side+":"+Math.floor(signal.score/5);
  if(lastSignalAlert.key===key && Date.now()-lastSignalAlert.at<30*60000) return;
  lastSignalAlert={key,at:Date.now()};
  const msg=`${MARKET_SYMBOL} ${signal.side} score ${signal.score}/100 at ${Number(currentPrice).toFixed(2)}. ${(signal.reasons||[]).slice(0,3).join(" | ")}`;
  sendSmsAlert(msg).catch(e=>console.error("sms alert",e.message));
}

/* ---------- realtime exact profiles ---------- */

function emptyProfileState() {
  return {byPrice:{},buyVolume:0,sellVolume:0,tradeCount:0,firstTradeAt:null,lastTradeAt:null};
}
function profileKey(kind,dateStr){ return `${kind}:${dateStr}`; }

function tradeSessionKeys(ts) {
  const t=DateTime.fromISO(ts,{setZone:true}).setZone(ZONE);
  const mins=t.hour*60+t.minute;
  const globexSessionDate=(t.hour>=15 ? t.plus({days:1}) : t).toISODate();
  const keys=[profileKey("globex",globexSessionDate)];

  if(mins>=390 && mins<780){
    keys.push(profileKey("rth",t.toISODate()));
  }
  return keys;
}

function applyTradeToState(state,d) {
  const price=+d.price,vol=+d.volume||0,type=+d.type;
  if(!Number.isFinite(price)||!Number.isFinite(vol)||vol<=0) return;

  const k=price.toFixed(2);
  const x=state.byPrice[k]||{price,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};

  x.volume+=vol;
  x.trades++;

  if(type===0){
    x.buyVolume+=vol;
    x.delta+=vol;
    state.buyVolume+=vol;
  } else if(type===1){
    x.sellVolume+=vol;
    x.delta-=vol;
    state.sellVolume+=vol;
  }

  state.byPrice[k]=x;
  state.tradeCount++;

  const ts=d.timestamp||new Date().toISOString();
  if(!state.firstTradeAt) state.firstTradeAt=ts;
  state.lastTradeAt=ts;
}

function bucketStartIso(ts,minutes) {
  const ms=Date.parse(ts);
  const bucketMs=minutes*60000;
  return new Date(Math.floor(ms/bucketMs)*bucketMs).toISOString();
}
function updateFlowBucket(store,ts,d,minutes) {
  const key=bucketStartIso(ts,minutes);
  const price=+d.price, vol=+d.volume||0, type=+d.type;
  if(!Number.isFinite(price)||!Number.isFinite(vol)||vol<=0) return;
  let b=store[key];
  if(!b) b=store[key]={t:key,o:price,h:price,l:price,c:price,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};
  b.h=Math.max(b.h,price); b.l=Math.min(b.l,price); b.c=price;
  b.volume+=vol; b.trades++;
  if(type===0){ b.buyVolume+=vol; b.delta+=vol; }
  else if(type===1){ b.sellVolume+=vol; b.delta-=vol; }
  store[key]=b;
}
function updateFlowBars(ts,d) {
  updateFlowBucket(flowBars.oneMin,ts,d,1);
  updateFlowBucket(flowBars.fiveMin,ts,d,5);
}
function addTrade(d) {
  const ts=d.timestamp||new Date().toISOString();
  for(const key of tradeSessionKeys(ts)){
    if(!profiles[key]) profiles[key]=emptyProfileState();
    applyTradeToState(profiles[key],d);
  }
  updateFlowBars(ts,d);
  lastTradeAt=ts;
  stateDirty=true;
}

function finalizeProfile(state) {
  if(!state) return null;

  const arr=Object.values(state.byPrice||{}).sort((a,b)=>a.price-b.price);
  if(!arr.length) return null;

  const total=arr.reduce((s,x)=>s+x.volume,0);
  const poc=arr.reduce((a,b)=>b.volume>a.volume?b:a);
  const idx=arr.indexOf(poc);

  let lo=idx-1,hi=idx+1,cum=poc.volume;
  const selected=new Set([idx]);

  while(cum<total*.70&&(lo>=0||hi<arr.length)){
    const lv=lo>=0?arr[lo].volume:-1;
    const hv=hi<arr.length?arr[hi].volume:-1;
    let i;
    if(hv>=lv&&hi<arr.length) i=hi++;
    else i=lo--;

    if(i>=0&&i<arr.length&&!selected.has(i)){
      selected.add(i);
      cum+=arr[i].volume;
    }
  }

  const ps=[...selected].map(i=>arr[i].price);

  return {
    method:"exact GatewayTrade volume-at-price",
    isTrueTradeByTradeProfile:true,
    valueAreaPercent:.70,
    totalVolume:total,
    buyVolume:state.buyVolume,
    sellVolume:state.sellVolume,
    delta:state.buyVolume-state.sellVolume,
    cvd:state.buyVolume-state.sellVolume,
    tradeCount:state.tradeCount,
    firstTradeAt:state.firstTradeAt,
    lastTradeAt:state.lastTradeAt,
    poc:poc.price,
    vah:Math.max(...ps),
    val:Math.min(...ps),
    strongestVolumeNodes:[...arr].sort((a,b)=>b.volume-a.volume).slice(0,10),
    strongestDeltaPrices:[...arr].sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta)).slice(0,15)
  };
}

function currentTradingSessionDate() {
  const t=nowPT();
  return (t.hour>=15 ? t.plus({days:1}) : t).toISODate();
}

function priorBusinessDateStr() {
  let t=nowPT().minus({days:1}).startOf("day");
  for(let i=0;i<7;i++){
    if(t.weekday<=5) return t.toISODate();
    t=t.minus({days:1});
  }
  return null;
}

function pruneProfiles() {
  const cutoff=Date.now()-14*86400000;
  for(const key of Object.keys(profiles)){
    const state=profiles[key];
    const t=state?.lastTradeAt ? Date.parse(state.lastTradeAt) : 0;
    if(t && t<cutoff) delete profiles[key];
  }
  const flowCutoff=Date.now()-4*86400000;
  for(const store of [flowBars.oneMin,flowBars.fiveMin]){
    for(const key of Object.keys(store)){
      if(Date.parse(key)<flowCutoff) delete store[key];
    }
  }
}
function saveState(force=false) {
  if(!force&&!stateDirty) return;
  pruneProfiles();
  const tmp=STATE_FILE+".tmp";
  fs.writeFileSync(tmp,JSON.stringify({schemaVersion:2,savedAt:new Date().toISOString(),profiles,flowBars},null,2));
  fs.renameSync(tmp,STATE_FILE);
  stateDirty=false;
}
function loadState() {
  if(!fs.existsSync(STATE_FILE)) return;
  try{
    const j=JSON.parse(fs.readFileSync(STATE_FILE,"utf8"));
    if(j?.profiles) profiles=j.profiles;
    if(j?.flowBars) flowBars={oneMin:j.flowBars.oneMin||{},fiveMin:j.flowBars.fiveMin||{}};
  }catch(e){
    console.warn("Could not load profile state:",e.message);
  }
}


function loadMorningCutoff() {
  if(!fs.existsSync(MORNING_CUTOFF_FILE)) return;
  try{
    const j=JSON.parse(fs.readFileSync(MORNING_CUTOFF_FILE,"utf8"));
    if(j?.cutoffPacificDate) morningCutoff=j;
  }catch(e){
    console.warn("Could not load morning cutoff:",e.message);
  }
}

function saveMorningCutoff(payload) {
  const tmp=MORNING_CUTOFF_FILE+".tmp";
  fs.writeFileSync(tmp,JSON.stringify(payload,null,2));
  fs.renameSync(tmp,MORNING_CUTOFF_FILE);
  morningCutoff=payload;
}

async function captureMorningCutoffIfDue() {
  if(morningCutoffCaptureBusy) return;
  const t=nowPT();
  if(t.weekday>5 || t.hour!==5 || t.minute!==30) return;
  const date=t.toISODate();
  if(morningCutoff?.cutoffPacificDate===date) return;
  morningCutoffCaptureBusy=true;
  try{
    // Refresh the slower historical/structure layer at the cutoff before freezing it.
    await buildSnapshot();
    const relayFresh=relayState && Date.now()-Date.parse(relayState.receivedAt)<10000;
    const payload={
      schemaVersion:"1.0",
      marketSymbol:MARKET_SYMBOL,
      cutoffPacificDate:date,
      cutoffTimePacific:"05:30:00",
      capturedUtc:DateTime.utc().toISO(),
      capturedPacific:nowPT().toISO(),
      contract:contract?{id:contract.id,name:contract.name,symbolId:contract.symbolId,tickSize:contract.tickSize,tickValue:contract.tickValue}:null,
      collectorFreshAtCapture:Boolean(relayFresh),
      snapshot:latestSnapshot,
      indicator:buildIndicatorPayload(),
      realtime:relayFresh?relayState:null
    };
    saveMorningCutoff(payload);
    console.log("MORNING_CUTOFF_CAPTURED",MARKET_SYMBOL,payload.capturedPacific);
  }catch(e){
    console.error("MORNING_CUTOFF_CAPTURE_ERR",MARKET_SYMBOL,e.message);
  }finally{
    morningCutoffCaptureBusy=false;
  }
}

/* ---------- live indicator engine ---------- */

function exactVwapFromState(state) {
  if(!state) return null;
  let num=0,den=0;
  for(const x of Object.values(state.byPrice||{})){
    const v=+x.volume||0,p=+x.price;
    if(v>0&&Number.isFinite(p)){ num+=p*v; den+=v; }
  }
  return den?num/den:null;
}
function flowArray(store,minutesBack=720) {
  const cutoff=Date.now()-minutesBack*60000;
  return Object.values(store||{}).filter(b=>Date.parse(b.t)>=cutoff).sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
}
function withCvd(rows) {
  let cvd=0;
  return rows.map(b=>({...b,cvd:(cvd+=+b.delta||0)}));
}
function uniqueLevels(levels,tick=0.25) {
  const seen=new Set(),out=[];
  for(const l of levels){
    if(!Number.isFinite(+l.price)) continue;
    const k=Math.round(+l.price/tick);
    if(seen.has(k)) continue;
    seen.add(k); out.push({...l,price:+l.price});
  }
  return out.sort((a,b)=>b.priority-a.priority);
}

function calculateMarketStructure(bars5m,bars15m,sessionCvd,asOf=Date.now()){
  const closed=(bars5m||[]).filter(b=>Date.parse(b.t)+300000<=asOf).slice(-36);
  if(closed.length<12) return {state:"TRANSITION",score:0,direction:"NEUTRAL",reasons:["Waiting for enough completed 5m structure"],range:null,trend:null,metrics:{}};
  const atr=current5mAtr(closed,20)||Math.max(1,median(closed.slice(-12).map(b=>+b.h-+b.l))||1);
  const recent=closed.slice(-18);
  const closes=recent.map(b=>+b.c);
  const path=closes.slice(1).reduce((s,v,i)=>s+Math.abs(v-closes[i]),0);
  const net=closes.at(-1)-closes[0];
  const efficiency=path>0?Math.min(1,Math.abs(net)/path):0;

  const n=closes.length, xm=(n-1)/2, ym=closes.reduce((s,x)=>s+x,0)/n;
  let num=0,den=0;
  for(let i=0;i<n;i++){num+=(i-xm)*(closes[i]-ym);den+=(i-xm)*(i-xm);}
  const slope=den?num/den:0;
  const normSlope=Math.min(1,Math.abs(slope)/(atr*.12||1));
  const direction=slope>0?"UP":slope<0?"DOWN":"NEUTRAL";

  const pivH=[],pivL=[];
  for(let i=2;i<recent.length-2;i++){
    const b=recent[i];
    if(+b.h>+recent[i-1].h&&+b.h>=+recent[i+1].h&&+b.h>+recent[i-2].h&&+b.h>=+recent[i+2].h) pivH.push({i,price:+b.h,t:b.t});
    if(+b.l<+recent[i-1].l&&+b.l<=+recent[i+1].l&&+b.l<+recent[i-2].l&&+b.l<=+recent[i+2].l) pivL.push({i,price:+b.l,t:b.t});
  }
  let structureDir="NEUTRAL";
  if(pivH.length>=2&&pivL.length>=2){
    const hh=pivH.at(-1).price>pivH.at(-2).price, hl=pivL.at(-1).price>pivL.at(-2).price;
    const lh=pivH.at(-1).price<pivH.at(-2).price, ll=pivL.at(-1).price<pivL.at(-2).price;
    if(hh&&hl) structureDir="UP"; else if(lh&&ll) structureDir="DOWN";
  }

  let overlap=0;
  for(let i=1;i<recent.length;i++){
    const a=recent[i-1],b=recent[i], inter=Math.max(0,Math.min(+a.h,+b.h)-Math.max(+a.l,+b.l));
    const span=Math.max(+a.h,+b.h)-Math.min(+a.l,+b.l);
    overlap+=span>0?inter/span:0;
  }
  overlap/=(recent.length-1);

  const rvwap=(rows)=>{
    let pv=0,v=0;
    for(const b of rows){const vol=+b.v||0,tp=(+b.h+ +b.l+ +b.c)/3;pv+=tp*vol;v+=vol;}
    return v?pv/v:null;
  };
  const vwNow=rvwap(recent.slice(-12)),vwPrev=rvwap(recent.slice(-18,-6));
  const vwapSlope=Number.isFinite(vwNow)&&Number.isFinite(vwPrev)?vwNow-vwPrev:0;
  const vwapAligned=Number.isFinite(vwNow)&&((direction==="UP"&&closes.at(-1)>vwNow&&vwapSlope>0)||(direction==="DOWN"&&closes.at(-1)<vwNow&&vwapSlope<0));

  let crosses=0;
  if(Number.isFinite(vwNow)) for(let i=1;i<recent.length;i++) if((+recent[i-1].c-vwNow)*(+recent[i].c-vwNow)<0) crosses++;
  const crossScore=Math.min(1,crosses/5);

  const r15=(bars15m||[]).filter(b=>Date.parse(b.t)+900000<=asOf).slice(-5);
  let dir15="NEUTRAL";
  if(r15.length>=4){const d=+r15.at(-1).c-+r15[0].c;if(Math.abs(d)>=atr*.5)dir15=d>0?"UP":"DOWN";}
  const aligned15=direction!=="NEUTRAL"&&dir15===direction;

  const rangeHigh=Math.max(...recent.map(b=>+b.h)),rangeLow=Math.min(...recent.map(b=>+b.l));
  const rangeWidth=rangeHigh-rangeLow, widthAtr=rangeWidth/atr;
  const compression=Math.max(0,Math.min(1,(5-widthAtr)/3));

  let trend100=efficiency*35+normSlope*25+(structureDir===direction?20:structureDir==="NEUTRAL"?6:0)+(vwapAligned?10:0)+(aligned15?10:0);
  if(direction==="NEUTRAL") trend100*=.5;
  let range100=(1-efficiency)*30+overlap*25+crossScore*20+compression*15+(structureDir==="NEUTRAL"?10:2);
  trend100=Math.max(0,Math.min(100,trend100));
  range100=Math.max(0,Math.min(100,range100));

  let state="TRANSITION",score=Math.max(trend100,range100),reasons=[];
  if(trend100>=65&&trend100>=range100+8){
    state=direction==="UP"?"TREND UP":"TREND DOWN";score=trend100;
    reasons.push("directional efficiency "+Math.round(efficiency*100)+"%");
    if(structureDir===direction) reasons.push(direction==="UP"?"5m HH/HL":"5m LH/LL");
    if(vwapAligned) reasons.push("price and rolling VWAP aligned");
    if(aligned15) reasons.push("15m structure aligned");
  }else if(range100>=65&&range100>=trend100){
    state="RANGE";score=range100;
    reasons.push("high candle overlap");
    if(crosses>=2) reasons.push(crosses+" VWAP crossings");
    reasons.push("low directional efficiency");
  }else{
    reasons.push(trend100>range100?"trend evidence building":"range evidence building");
    if(structureDir!=="NEUTRAL") reasons.push("5m structure "+structureDir.toLowerCase());
  }

  const trendStart=recent[0],trendEnd=recent.at(-1);
  const startFit=ym+slope*(0-xm),endFit=ym+slope*((n-1)-xm);

  // Adaptive regression channel. Residual dispersion sets the natural width;
  // ATR provides a floor so quiet periods do not collapse the channel.
  const residuals=closes.map((v,i)=>v-(ym+slope*(i-xm)));
  const residualMean=residuals.reduce((s,x)=>s+x,0)/residuals.length;
  const residualSigma=Math.sqrt(residuals.reduce((s,x)=>s+Math.pow(x-residualMean,2),0)/residuals.length);
  const sigmaMult=1.75;
  const minHalfWidth=atr*.35;
  const channelHalfWidth=Math.max(minHalfWidth,residualSigma*sigmaMult);
  const upperStart=startFit+channelHalfWidth,upperEnd=endFit+channelHalfWidth;
  const lowerStart=startFit-channelHalfWidth,lowerEnd=endFit-channelHalfWidth;

  return {
    state,score:+(score/10).toFixed(1),direction,
    trendScore:+(trend100/10).toFixed(1),rangeScore:+(range100/10).toFixed(1),
    reasons,
    range:{low:+rangeLow.toFixed(2),high:+rangeHigh.toFixed(2),mid:+((rangeLow+rangeHigh)/2).toFixed(2),width:+rangeWidth.toFixed(2),widthAtr:+widthAtr.toFixed(2),startTime:recent[0].t,endTime:recent.at(-1).t},
    trend:{
      startTime:trendStart.t,endTime:trendEnd.t,
      startPrice:+startFit.toFixed(2),endPrice:+endFit.toFixed(2),
      upperStart:+upperStart.toFixed(2),upperEnd:+upperEnd.toFixed(2),
      lowerStart:+lowerStart.toFixed(2),lowerEnd:+lowerEnd.toFixed(2),
      halfWidth:+channelHalfWidth.toFixed(2),residualSigma:+residualSigma.toFixed(2),
      sigmaMultiplier:sigmaMult,structureDirection:structureDir
    },
    metrics:{efficiency:+efficiency.toFixed(3),overlap:+overlap.toFixed(3),vwapCrosses:crosses,normalizedSlope:+normSlope.toFixed(3),direction15m:dir15,sessionCvd:Number.isFinite(+sessionCvd)?+sessionCvd:null}
  };
}


function recordAbsorptionDepthSnapshot(relay){
  const now=Date.now();
  if(now-lastAbsorptionDepthSnapshotAt<1500) return;
  lastAbsorptionDepthSnapshotAt=now;
  const levels=(relay?.restingLiquidity?.levels||[]).slice(0,8).map(x=>({
    side:x.side,price:+x.price,size:+x.size||0,status:String(x.status||""),
    refreshes:+x.refreshes||0
  })).filter(x=>Number.isFinite(x.price));
  const icebergs=(relay?.icebergs||[]).slice(0,4).map(x=>({
    side:x.side,price:+x.price,score:+x.score||0,refreshes:+x.refreshes||0,
    aggressorVolume:+x.aggressorVolume||0
  })).filter(x=>Number.isFinite(x.price));
  absorptionDepthHistory.push({at:now,price:+relay?.currentPrice,levels,icebergs});
  const cut=now-5*60000;
  absorptionDepthHistory=absorptionDepthHistory.filter(x=>x.at>=cut);
}

function calculateAbsorptionZones({f1,bars5m,currentPrice,levels}){
  const now=Date.now(),cut=now-5*60000;
  const rows=(f1||[]).filter(b=>{
    const t=Date.parse(b.t);
    return Number.isFinite(t)&&t>=cut;
  }).sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
  if(rows.length<2||!Number.isFinite(+currentPrice)) return [];

  const sum=(xs,k1,k2)=>xs.reduce((s,x)=>s+(Number.isFinite(+x[k1])?+x[k1]:(k2&&Number.isFinite(+x[k2])?+x[k2]:0)),0);
  const volume=Math.max(1,sum(rows,"volume","v"));
  const delta=sum(rows,"delta");
  const deltaRatio=Math.max(-1,Math.min(1,delta/volume));
  if(Math.abs(deltaRatio)<.055) return [];

  const highs=rows.map(x=>+x.h).filter(Number.isFinite),lows=rows.map(x=>+x.l).filter(Number.isFinite);
  if(!highs.length||!lows.length) return [];
  const high=Math.max(...highs),low=Math.min(...lows),range=Math.max(+contract?.tickSize||.25,high-low);
  const firstOpen=Number.isFinite(+rows[0].o)?+rows[0].o:+rows[0].c;
  const lastPrice=+currentPrice;
  const atr5=current5mAtr(bars5m||[],20)||range;
  const tick=+contract?.tickSize||.25;
  const minZone=MARKET_SYMBOL==="MES"?.75:2.0;
  const maxZone=MARKET_SYMBOL==="MES"?3.0:10.0;
  const zoneWidth=Math.max(minZone,Math.min(maxZone,Math.max(tick*4,atr5*.16)));
  const side=deltaRatio<0?"BUY":"SELL";
  const zoneLow=side==="BUY"?low:Math.max(low,high-zoneWidth);
  const zoneHigh=side==="BUY"?Math.min(high,low+zoneWidth):high;
  const zoneMid=(zoneLow+zoneHigh)/2;

  const pressureScore=Math.min(32,Math.abs(deltaRatio)*150);
  const signedProgress=side==="BUY"?(firstOpen-lastPrice)/range:(lastPrice-firstOpen)/range;
  const limitedProgress=Math.max(0,1-Math.max(0,Math.min(1,signedProgress)));
  const stallScore=limitedProgress*18;
  const closeLocation=(lastPrice-low)/range;
  const rejection=side==="BUY"?Math.max(0,Math.min(1,closeLocation)):Math.max(0,Math.min(1,1-closeLocation));
  const rejectionScore=rejection*10;

  const prior=(f1||[]).filter(b=>{
    const t=Date.parse(b.t);
    return Number.isFinite(t)&&t<cut&&t>=cut-30*60000;
  });
  const priorVols=prior.map(x=>+(x.volume??x.v)||0).filter(x=>x>0).sort((a,b)=>a-b);
  const med1=priorVols.length?priorVols[Math.floor(priorVols.length/2)]:0;
  const avg1=volume/Math.max(1,rows.length);
  const volumeMultiple=med1>0?avg1/med1:1;
  const volumeScore=Math.max(0,Math.min(12,(volumeMultiple-.8)*10));

  const hist=absorptionDepthHistory.filter(x=>x.at>=cut);
  const wanted=side==="BUY"?"BID":"ASK";
  let maxRefresh=0,replenishingSnapshots=0,maxDisplayedSize=0,matchingSnapshots=0;
  const seenPrices=new Set();
  for(const snap of hist){
    for(const z of (snap.levels||[])){
      if(z.side!==wanted||z.price<zoneLow-zoneWidth*.45||z.price>zoneHigh+zoneWidth*.45) continue;
      matchingSnapshots++;
      seenPrices.add(z.price.toFixed(2));
      maxRefresh=Math.max(maxRefresh,+z.refreshes||0);
      maxDisplayedSize=Math.max(maxDisplayedSize,+z.size||0);
      if(z.status==="REPLENISHING"||z.status==="BEING HIT") replenishingSnapshots++;
    }
    for(const ice of (snap.icebergs||[])){
      if(ice.side!==side||ice.price<zoneLow-zoneWidth*.45||ice.price>zoneHigh+zoneWidth*.45) continue;
      maxRefresh=Math.max(maxRefresh,+ice.refreshes||0);
      replenishingSnapshots+=2;
    }
  }
  const depthScore=Math.min(22,maxRefresh*4+Math.min(10,replenishingSnapshots*.6)+Math.min(4,seenPrices.size));

  const confluenceDistance=Math.max(MARKET_PARAMS.clusterMin,atr5*.12);
  const nearby=(levels||[]).filter(l=>Number.isFinite(+l.price)&&Math.abs(+l.price-zoneMid)<=confluenceDistance)
    .sort((a,b)=>(+b.priority||0)-(+a.priority||0));
  const bestLevel=nearby[0]||null;
  const confluenceScore=bestLevel?Math.min(10,4+(+bestLevel.priority||50)/20):0;

  let score=Math.min(100,pressureScore+stallScore+rejectionScore+volumeScore+depthScore+confluenceScore);
  const reasons=[];
  reasons.push((deltaRatio<0?"negative":"positive")+" 5m delta "+Math.round(deltaRatio*100)+"%");
  if(stallScore>=7) reasons.push(side==="BUY"?"selling not producing proportional downside":"buying not producing proportional upside");
  if(rejectionScore>=4) reasons.push(side==="BUY"?"price rejecting/recovering from 5m low":"price rejecting/fading from 5m high");
  if(maxRefresh>=1||replenishingSnapshots>=2) reasons.push((side==="BUY"?"bid":"ask")+" replenishment across zone");
  if(seenPrices.size>=2) reasons.push("absorption spread across "+seenPrices.size+" nearby prices");
  if(volumeMultiple>=1.2) reasons.push(volumeMultiple.toFixed(1)+"x recent 1m volume intensity");
  if(bestLevel) reasons.push(bestLevel.label+" confluence");

  // Strong aggressive pressure is required; without either stalled price or depth evidence,
  // keep it below the display threshold to avoid labeling every directional impulse absorption.
  if(depthScore<4&&stallScore<7&&rejectionScore<5) score=Math.min(score,49);
  if(score<55) return [];

  return [{
    side,type:side==="BUY"?"buy-absorption":"sell-absorption",
    low:+zoneLow.toFixed(2),high:+zoneHigh.toFixed(2),mid:+zoneMid.toFixed(2),
    score:+(score/10).toFixed(1),delta5m:Math.round(delta),deltaRatio:+deltaRatio.toFixed(3),
    volume:Math.round(volume),volumeMultiple:+volumeMultiple.toFixed(2),
    refreshes:maxRefresh,replenishingSnapshots,priceLevelsHit:seenPrices.size,maxDisplayedSize,
    confluence:bestLevel?{id:bestLevel.id,label:bestLevel.label,price:+bestLevel.price}:null,
    startTime:rows[0].t,endTime:rows.at(-1).t,
    reasons:reasons.slice(0,5),
    note:"3-5 minute absorption candidate; not a standalone reversal signal."
  }];
}

function calculateClosePressure({f1,bars5m,currentPrice,rthVwap,restingLiquidity,icebergs}){
  const pt=nowPT(),mins=pt.hour*60+pt.minute;
  const active=pt.weekday<=5 && mins>=750 && mins<780; // 12:30-13:00 PT only
  if(!active) return {active:false,startPacific:"12:30",endPacific:"13:00",label:"CLOSE PRESSURE"};

  const rows=(f1||[]).filter(b=>Date.parse(b.t)+60000<=Date.now()).slice(-30);
  if(rows.length<5) return {active:true,state:"BALANCED",score:0,reasons:["Waiting for enough closing-window order flow"],startPacific:"12:30",endPacific:"13:00",label:"CLOSE PRESSURE"};

  const sum=(xs,k)=>xs.reduce((s,x)=>s+(+x[k]||0),0);
  const delta30=sum(rows,"delta"),vol30=Math.max(1,sum(rows,"volume")||sum(rows,"v"));
  const ratio=Math.max(-1,Math.min(1,delta30/vol30));
  let signed=ratio*175; // ~20% delta/volume ratio contributes about 35 points.
  const reasons=[];

  const recent=rows.slice(-10),prior=rows.slice(0,Math.max(1,rows.length-10));
  const recentRate=sum(recent,"delta")/Math.max(1,recent.length);
  const priorRate=sum(prior,"delta")/Math.max(1,prior.length);
  const accel=(recentRate-priorRate)/Math.max(1,vol30/rows.length);
  signed+=Math.max(-15,Math.min(15,accel*40));

  const first=rows[0],atr5=current5mAtr(bars5m||[],20)||1;
  const startPrice=Number.isFinite(+first.c)?+first.c:null;
  if(Number.isFinite(startPrice)&&Number.isFinite(+currentPrice)){
    const move=(+currentPrice-startPrice)/atr5;
    signed+=Math.max(-20,Math.min(20,move*20));
  }

  if(Number.isFinite(+rthVwap)&&Number.isFinite(+currentPrice)){
    signed+=(+currentPrice>+rthVwap?10:+currentPrice<+rthVwap?-10:0);
  }

  const liq=(restingLiquidity?.levels||[]).slice(0,8);
  const bidSize=liq.filter(x=>x.side==="BID").reduce((s,x)=>s+(+x.size||0),0);
  const askSize=liq.filter(x=>x.side==="ASK").reduce((s,x)=>s+(+x.size||0),0);
  if(bidSize+askSize>0) signed+=10*((bidSize-askSize)/(bidSize+askSize));

  const ice=(icebergs||[]).slice(0,4);
  const iceNet=ice.reduce((s,x)=>s+(x.side==="BUY"?1:x.side==="SELL"?-1:0)*Math.min(100,+x.score||0),0);
  signed+=Math.max(-10,Math.min(10,iceNet/10));

  signed=Math.max(-100,Math.min(100,signed));
  const abs=Math.abs(signed),state=abs<25?"BALANCED":signed>0?"BUY":"SELL";
  const score=+(abs/10).toFixed(1);

  if(Math.abs(ratio)>=.03) reasons.push((ratio>0?"positive":"negative")+" 30m delta");
  if(Math.abs(accel)>=.08) reasons.push((accel>0?"buy":"sell")+" pressure accelerating");
  if(Number.isFinite(startPrice)&&Number.isFinite(+currentPrice)&&Math.abs((+currentPrice-startPrice)/atr5)>=.25) reasons.push((+currentPrice>startPrice?"price pushing higher":"price pushing lower"));
  if(Number.isFinite(+rthVwap)&&Number.isFinite(+currentPrice)) reasons.push(+currentPrice>+rthVwap?"above NY VWAP":"below NY VWAP");
  if(bidSize+askSize>0 && Math.abs(bidSize-askSize)/(bidSize+askSize)>.15) reasons.push(bidSize>askSize?"bid liquidity heavier":"ask liquidity heavier");
  if(iceNet!==0) reasons.push(iceNet>0?"buy absorption present":"sell absorption present");

  return {
    active:true,label:"CLOSE PRESSURE",state,score,
    startPacific:"12:30",endPacific:"13:00",
    delta30:Math.round(delta30),deltaRatio:+ratio.toFixed(3),
    reasons:reasons.slice(0,4),
    note:"Collector-derived closing pressure proxy; not exchange MOC imbalance data."
  };
}

function buildIndicatorPayload() {
  if(!latestSnapshot||!contract) return {};
  const a=latestSnapshot.analytics||{}, tick=+contract.tickSize||0.25;
  const currentSession=currentTradingSessionDate();
  const today=nowPT().toISODate();
  const currentGlobexState=profiles[profileKey("globex",currentSession)];
  const currentRthState=profiles[profileKey("rth",today)];
  const relayFreshNow=relayState && Date.now()-Date.parse(relayState.receivedAt)<10000;
  const nowForRth=nowPT(),rthMins=nowForRth.hour*60+nowForRth.minute;
  const rthActive=rthMins>=390&&rthMins<780;
  const relayGlobexProfile=relayFreshNow?relayState.profiles?.session:null;
  const relayRthProfile=relayFreshNow&&rthActive?relayState.profiles?.rth:null;
  const globexExact=relayGlobexProfile||finalizeProfile(currentGlobexState);
  const rthExact=rthActive?(relayRthProfile||finalizeProfile(currentRthState)):null;
  const currentPrice=+(relayFreshNow ? relayState.currentPrice : (latestQuote?.lastPrice ?? latestQuote?.price ?? latestSnapshot.latest?.bar1m?.c ?? latestSnapshot.latest?.bar5m?.c));
  const p=a.profiles||{};
  const levels=uniqueLevels([
    {id:"pdh",label:"PDH",price:a.previousRTH?.high,kind:"resistance",priority:100},
    {id:"pdl",label:"PDL",price:a.previousRTH?.low,kind:"support",priority:100},
    {id:"pdc",label:"PDC",price:a.previousRTH?.close,kind:"reference",priority:65},
    {id:"pwh",label:"PWH",price:a.previousCMETradingWeek?.high,kind:"weekly",priority:78},
    {id:"pwl",label:"PWL",price:a.previousCMETradingWeek?.low,kind:"weekly",priority:78},
    {id:"onh",label:"ON High",price:a.currentOvernight?.high,kind:"resistance",priority:98},
    {id:"onl",label:"ON Low",price:a.currentOvernight?.low,kind:"support",priority:98},
    {id:"asiaH",label:"Asia H",price:a.asia?.high,kind:"resistance",priority:80},
    {id:"asiaL",label:"Asia L",price:a.asia?.low,kind:"support",priority:80},
    {id:"londonH",label:"London H",price:a.london?.high,kind:"resistance",priority:82},
    {id:"londonL",label:"London L",price:a.london?.low,kind:"support",priority:82},
    {id:"prevVah",label:"Prev VAH",price:p.previousRTH_exact?.vah ?? p.previousRTH_estimated?.vah,kind:"profile",priority:88},
    {id:"prevPoc",label:"Prev POC",price:p.previousRTH_exact?.poc ?? p.previousRTH_estimated?.poc,kind:"profile",priority:90},
    {id:"prevVal",label:"Prev VAL",price:p.previousRTH_exact?.val ?? p.previousRTH_estimated?.val,kind:"profile",priority:88},
    {id:"onVah",label:"ON VAH",price:p.currentOvernight_estimated?.vah,kind:"profile",priority:84},
    {id:"onPoc",label:"ON POC",price:p.currentOvernight_estimated?.poc,kind:"profile",priority:86},
    {id:"onVal",label:"ON VAL",price:p.currentOvernight_estimated?.val,kind:"profile",priority:84},
    ...((a.fourHourPivots||[]).filter(x=>!x.sweptLater).slice(0,6).map((x,i)=>({
      id:"pivot"+i,label:`4H ${x.type==="high"?"H":"L"} ${x.significanceScore}`,price:x.price,
      kind:x.type==="high"?"resistance":"support",priority:70-i
    })))
  ],tick);

  const relayFresh=relayState && Date.now()-Date.parse(relayState.receivedAt)<10000;
  const f1=relayFresh?(relayState.oneMin||[]):withCvd(flowArray(flowBars.oneMin,360));
  const f5=relayFresh?(relayState.fiveMin||[]):withCvd(flowArray(flowBars.fiveMin,720));
  const liveBars5m=mergeLiveFiveMinuteBars(latestSnapshot.bars?.fiveMinRecent||[],f5);
  const last5=f5.at(-1)||null;
  const globexVwap=relayFresh && Number.isFinite(+relayState.sessionVwap)?+relayState.sessionVwap:(exactVwapFromState(currentGlobexState) ?? a.vwap?.globex ?? null);
  const rthVwap=rthActive?(relayFresh && Number.isFinite(+relayState.rthVwap)?+relayState.rthVwap:exactVwapFromState(currentRthState)):null;

  const candidateHighs=levels.filter(l=>["resistance","profile"].includes(l.kind)&&l.price>=currentPrice-tick*8).slice(0,5);
  const candidateLows=levels.filter(l=>["support","profile"].includes(l.kind)&&l.price<=currentPrice+tick*8).slice(0,5);

  // Trap detection: completed 1m sweep + reclaim + next-bar follow-through.
  // Old/invalidated traps are removed and nearby duplicate levels are clustered.
  const closed1=f1.filter(b=>Date.parse(b.t)+60000<=Date.now()).slice(-30);
  const trapCandidatesRaw=[];
  const maxTrapAgeMs=20*60000;
  const atr5ForTraps=current5mAtr(latestSnapshot.bars?.fiveMinRecent||[],20)||20;
  const clusterDistance=Math.max(MARKET_PARAMS.clusterMin,atr5ForTraps*.08);

  for(const l of levels.filter(x=>x.priority>=80)){
    for(let i=Math.max(0,closed1.length-22);i<closed1.length-1;i++){
      const b=closed1[i],n=closed1[i+1];
      const age=Date.now()-Date.parse(b.t);
      if(age<0||age>maxTrapAgeMs) continue;

      // Buyers trapped above resistance/liquidity: positive delta into sweep,
      // close back below, then next closed minute fails to reclaim.
      const buyerSweep=+b.h>=+l.price+tick*2 && +b.c<+l.price && +b.delta>0;
      const buyerFollow=buyerSweep && +n.c<+l.price && +n.h<+b.h;
      if(buyerFollow){
        const invalidated=closed1.slice(i+2).some(x=>+x.c>=+l.price+tick*2);
        if(!invalidated){
          trapCandidatesRaw.push({
            side:"SELL",type:"buyer-trap",level:+l.price,label:l.label,time:b.t,
            delta:+b.delta,priority:l.priority,followTime:n.t,
            strength:(l.priority||0)+Math.min(20,Math.abs(+b.delta)/100)
          });
        }
      }

      // Sellers trapped below support/liquidity: negative delta into sweep,
      // close back above, then next closed minute fails to break back down.
      const sellerSweep=+b.l<=+l.price-tick*2 && +b.c>+l.price && +b.delta<0;
      const sellerFollow=sellerSweep && +n.c>+l.price && +n.l>+b.l;
      if(sellerFollow){
        const invalidated=closed1.slice(i+2).some(x=>+x.c<=+l.price-tick*2);
        if(!invalidated){
          trapCandidatesRaw.push({
            side:"BUY",type:"seller-trap",level:+l.price,label:l.label,time:b.t,
            delta:+b.delta,priority:l.priority,followTime:n.t,
            strength:(l.priority||0)+Math.min(20,Math.abs(+b.delta)/100)
          });
        }
      }
    }
  }

  // Keep the strongest recent trap in each nearby price cluster, then at most one per side.
  const clustered=[];
  for(const t of trapCandidatesRaw.sort((a,b)=>
    (b.strength-a.strength) || (Date.parse(b.time)-Date.parse(a.time)) ||
    (Math.abs(a.level-currentPrice)-Math.abs(b.level-currentPrice))
  )){
    if(clustered.some(x=>x.side===t.side && Math.abs(x.level-t.level)<=clusterDistance)) continue;
    clustered.push(t);
  }
  const traps=[];
  for(const side of ["BUY","SELL"]){
    const best=clustered
      .filter(x=>x.side===side)
      .sort((a,b)=>
        (Date.parse(b.time)-Date.parse(a.time)) ||
        (b.strength-a.strength) ||
        (Math.abs(a.level-currentPrice)-Math.abs(b.level-currentPrice))
      )[0];
    if(best) traps.push(best);
  }
  traps.sort((x,y)=>Date.parse(y.time)-Date.parse(x.time));

  let trapSignal={side:"NEUTRAL",score:0,reasons:[],components:{}};
  const latestTrap=traps[0];
  if(latestTrap && Date.now()-Date.parse(latestTrap.time)<=20*60000){
    trapSignal.side=latestTrap.side;
    const setupKey=[latestTrap.side,latestTrap.type,latestTrap.label,latestTrap.time].join("|");
    if(signalStability.setupKey!==setupKey){
      signalStability={setupKey,cvdAlignedSince:null,targetRoomAward:null};
    }

    trapSignal.score+=25; trapSignal.components.trap=25;
    trapSignal.reasons.push(latestTrap.type+" at "+latestTrap.label);

    const last1=lastClosedBar(f1,1);
    const last5=lastClosedBar(f5,5);
    const closed5=lastClosedBar(latestSnapshot.bars?.fiveMinRecent||[],5);
    const structureOk=closed5 ? (trapSignal.side==="BUY"?+closed5.c>+latestTrap.level:+closed5.c<+latestTrap.level) : false;
    if(structureOk){
      trapSignal.score+=20;trapSignal.components.structure=20;trapSignal.reasons.push("closed 5m structure confirmed");
    }

    if(last1 && (trapSignal.side==="BUY"?+last1.delta>0:+last1.delta<0)){
      trapSignal.score+=10;trapSignal.components.delta1m=10;trapSignal.reasons.push("closed 1m delta aligned");
    }
    if(last5 && (trapSignal.side==="BUY"?+last5.delta>0:+last5.delta<0)){
      trapSignal.score+=10;trapSignal.components.delta5m=10;trapSignal.reasons.push("closed 5m delta aligned");
    }

    const sessionCvd=relayFresh?relayState.currentGlobexCvd:(globexExact?.cvd);
    const cvdAligned=Number.isFinite(+sessionCvd) && (trapSignal.side==="BUY"?+sessionCvd>0:+sessionCvd<0);
    if(cvdAligned){
      if(!signalStability.cvdAlignedSince) signalStability.cvdAlignedSince=Date.now();
      if(Date.now()-signalStability.cvdAlignedSince>=10000){
        trapSignal.score+=10;trapSignal.components.cvd=10;trapSignal.reasons.push("session CVD aligned 10s");
      }
    }else{
      signalStability.cvdAlignedSince=null;
    }

    const activeVwap=Number.isFinite(+rthVwap)?+rthVwap:+globexVwap;
    if(Number.isFinite(activeVwap) && (trapSignal.side==="BUY"?currentPrice>activeVwap:currentPrice<activeVwap)){
      trapSignal.score+=10;trapSignal.components.vwap=10;trapSignal.reasons.push("VWAP aligned");
    }

    const trapLevel=levels.find(l=>l.label===latestTrap.label);
    if(trapLevel && trapLevel.priority>=80){
      trapSignal.score+=10;trapSignal.components.level=10;trapSignal.reasons.push("major level confluence");
    }

    if(signalStability.targetRoomAward===null){
      const atr5=current5mAtr(latestSnapshot.bars?.fiveMinRecent||[],20)||20;
      const estRisk=Math.max(8,atr5*.25);
      const targetDist=nearestLevelDistance(levels,currentPrice,trapSignal.side);
      signalStability.targetRoomAward=Boolean(Number.isFinite(targetDist)&&targetDist>=estRisk*1.5);
    }
    if(signalStability.targetRoomAward){
      trapSignal.score+=5;trapSignal.components.targetRoom=5;trapSignal.reasons.push(">=1.5R target room");
    }
  }else{
    signalStability={setupKey:null,cvdAlignedSince:null,targetRoomAward:null};
  }
  trapSignal.score=Math.min(100,trapSignal.score);
  if(trapSignal.score<50) trapSignal.side="NEUTRAL";

  const bars5m=liveBars5m;
  const bars15m=aggregateBars(bars5m,15);
  const flow15m=aggregateBars(f5,15);
  const marketStructure=calculateMarketStructure(
    bars5m,bars15m,relayFresh?relayState.currentGlobexCvd:(globexExact?.cvd??null)
  );
  const ibStrategy=buildIBStrategy({
    bars:bars5m.slice(-500),market:MARKET_SYMBOL,fresh:Boolean(relayFresh),currentPrice,
    structureAt:(closed,asOf)=>calculateMarketStructure(closed,aggregateBars(closed,15),null,asOf).state
  });
  const orbStrategy=buildORBStrategy({
    bars:bars5m.slice(-500),market:MARKET_SYMBOL,fresh:Boolean(relayFresh),currentPrice,
    structureAt:(closed,asOf)=>calculateMarketStructure(closed,aggregateBars(closed,15),null,asOf).state
  });
  const failureStrategy=buildMESFailureStrategy({bars:bars5m.slice(-500),market:MARKET_SYMBOL,fresh:Boolean(relayFresh),currentPrice});
  const absorptionZones=relayFresh?calculateAbsorptionZones({f1,bars5m,currentPrice,levels}):[];
  const closePressure=calculateClosePressure({
    f1,bars5m,currentPrice,rthVwap,
    restingLiquidity:relayFresh?(relayState.restingLiquidity||{}):{},
    icebergs:relayFresh?(relayState.icebergs||[]):[]
  });
  const orderBlocks=detectOrderBlocks(
    bars15m,levels,flow15m,currentPrice,rthVwap,globexVwap,15
  );
  const orb=currentOrb(bars5m,f5);
  const middayOrb=currentMiddayOrb(bars5m);
  const gaps=detectOpeningGaps(bars5m,currentPrice,current5mAtr(bars5m,20));
  const edgefulContext=buildEdgefulContext({bars5m,orb,middayOrb,gaps,levels});
  const rawAnalysis=buildMarketAnalysis({
    currentPrice,levels,f1,f5,signal:trapSignal,traps,orderBlocks,globexVwap,rthVwap,
    sessionCvd:relayFresh?relayState.currentGlobexCvd:(globexExact?.cvd??null),
    atr5:current5mAtr(bars5m,20),orb,middayOrb,bars5m,
    delta15:deltaTrend15m(f5),
    profiles:{session:globexExact,rth:rthExact},
    icebergs:relayFresh?(relayState.icebergs||[]):[],
    gaps
  });
  const analysis=stabilizeAnalysis(rawAnalysis);
  const bestSetup=(analysis.setups||[])
    .filter(x=>Number.isFinite(+x.quality))
    .sort((a,b)=>+b.quality-+a.quality)[0]||null;
  const candidateSignal=bestSetup?{
    side:bestSetup.side,
    score:Math.round(+bestSetup.quality),
    key:bestSetup.title,
    reasons:[bestSetup.title,bestSetup.context||bestSetup.trigger].filter(Boolean),
    components:{setupQuality:Math.round(+bestSetup.quality)}
  }:(trapSignal.side!=="NEUTRAL"?{...trapSignal,key:"trap|"+(trapSignal.reasons?.[0]||"signal")}:null);
  const signal=stabilizeActionableSignal(candidateSignal);
  maybeSendSignalAlert(signal,currentPrice);

  return {
    schemaVersion:"1.9",
    marketSymbol:MARKET_SYMBOL,
    cutoff0530Pacific:(morningCutoff?.cutoffPacificDate===nowPT().toISODate())?{
      cutoffPacificDate:morningCutoff.cutoffPacificDate,
      cutoffTimePacific:morningCutoff.cutoffTimePacific,
      capturedUtc:morningCutoff.capturedUtc,
      capturedPacific:morningCutoff.capturedPacific,
      collectorFreshAtCapture:morningCutoff.collectorFreshAtCapture,
      indicator:morningCutoff.indicator
    }:null,
    generatedUtc:DateTime.utc().toISO(),
    generatedPacific:nowPT().toISO(),
    connected,
    contract:{id:contract.id,name:contract.name,tickSize:contract.tickSize,tickValue:contract.tickValue},
    currentPrice:Number.isFinite(currentPrice)?currentPrice:null,
    vwap:{globex:globexVwap,rth:rthVwap},
    flow:{
      oneMin:f1.slice(-240),
      fiveMin:f5.slice(-144),
      currentGlobexDelta:relayFresh?relayState.currentGlobexDelta:(globexExact?.delta??null),
      currentRthDelta:rthActive?(relayFresh?relayState.currentRthDelta:(rthExact?.delta??null)):null,
      currentGlobexCvd:relayFresh?relayState.currentGlobexCvd:(globexExact?.cvd??null),
      currentRthCvd:rthActive?(relayFresh?relayState.currentRthCvd:(rthExact?.cvd??null)):null,
      rthActive,
      lastTradeAt:relayFresh?(relayState.lastTradeAt||null):(lastTradeAt||null),
      lastTradeReceivedAt:relayFresh?(relayState.lastTradeReceivedAt||null):null,
      lastQuoteReceivedAt:relayFresh?(relayState.lastQuoteReceivedAt||null):null,
      lastDepthReceivedAt:relayFresh?(relayState.lastDepthReceivedAt||null):null
    },
    levels:levels.slice(0,18),
    trapCandidates:{buyer:candidateHighs,seller:candidateLows},
    confirmedTraps:traps.slice(0,8),
    orderBlocks,
    icebergs:relayFresh?(relayState.icebergs||[]):[],
    absorptionZones,
    restingLiquidity:relayFresh?(relayState.restingLiquidity||{levels:[],events:[],threshold:null}):{levels:[],events:[],threshold:null},
    gaps,
    orb,
    middayOrb,
    edgefulContext,
    signal,
    analysis,
    marketStructure,
    ibStrategy,
    orbStrategy,
    failureStrategy,
    closePressure,
    volatility:{atr5m20:current5mAtr(liveBars5m,20),atr14Daily:latestSnapshot.analytics?.volatility?.ATR14Daily??null},
    diagnostics:{tradeEventsReceived,tradeEventsMatched,quoteEventsReceived,depthEventsReceived,lastRawTradeEvent,lastRawQuoteEvent,lastRawDepthEvent,reconnectCount,subscriptionResults,relayFresh:Boolean(relayFresh),relayReceivedAt:relayState?.receivedAt||null,lastTradeAt:relayState?.lastTradeAt||lastTradeAt||null,lastTradeReceivedAt:relayState?.lastTradeReceivedAt||null,lastQuoteReceivedAt:relayState?.lastQuoteReceivedAt||null,lastDepthReceivedAt:relayState?.lastDepthReceivedAt||null,alertScoreThreshold:ALERT_SCORE_THRESHOLD,smsConfigured:Boolean(ALERT_SMS_TO&&TWILIO_ACCOUNT_SID&&TWILIO_AUTH_TOKEN&&TWILIO_FROM_NUMBER)},
    profiles:{currentGlobex:globexExact,currentRTH:rthExact},
    background:{
      delta15:deltaTrend15m(f5),
      volume:{
        current5m:+((bars5m||[]).at(-1)?.v||0),
        median5m:median((bars5m||[]).slice(-21,-1).map(b=>+b.v||0).filter(x=>x>0))
      },
      volumeProfile:{
        currentGlobex:globexExact,
        currentRTH:rthExact
      }
    },
    bars5m:liveBars5m.slice(-400),
    bars1h:(latestSnapshot.bars?.oneHourRecent||[]).slice(-240),
    bars4h:(latestSnapshot.bars?.fourHourRecent||[]).slice(-120),
    bars1d:(latestSnapshot.bars?.dailyRecent||[]).slice(-60)
  };
}

/* ---------- snapshot ---------- */

async function buildSnapshot() {
  const [one,five,hour,day,week]=await Promise.all([
    bars(2,1,10,15000),
    bars(2,5,45,15000),
    bars(3,1,120,5000),
    bars(4,1,500,1200),
    bars(5,1,1800,600)
  ]);

  const four=aggregateFourHourBars(hour);
  const tick=+contract.tickSize||0.25;
  const current=+five.at(-1).c;
  const a14=atr(day,14);
  const sessions=currentSessions(five);
  const prevR=previousRTH(five);

  let prevProfileEstimate=null;
  const prev1=previousRTH(one);

  if(prev1){
    const d=DateTime.fromISO(prev1.date,{zone:ZONE});
    prevProfileEstimate=estimatedProfile(
      between(
        one,
        d.set({hour:6,minute:30}),
        d.set({hour:13,minute:0})
      ),
      tick
    );
  }

  const currentSession=currentTradingSessionDate();
  const priorRthDate=priorBusinessDateStr();

  latestSnapshot={
    schemaVersion:3.4,
    generatedUtc:DateTime.utc().toISO(),
    generatedPacific:nowPT().toISO(),
    source:"TopstepX / ProjectX CME market data",

    contract:{
      id:contract.id,
      name:contract.name,
      description:contract.description,
      symbolId:contract.symbolId,
      tickSize:contract.tickSize,
      tickValue:contract.tickValue,
      activeContract:contract.activeContract
    },

    latest:{
      bar1m:one.at(-1)||null,
      bar5m:five.at(-1)||null,
      bar1h:hour.at(-1)||null,
      bar4h:four.at(-1)||null,
      bar1d:day.at(-1)||null,
      quote:latestQuote
    },

    analytics:{
      previousRTH:prevR,
      previousFullGlobexSession:previousFullGlobex(five),
      previousCMETradingWeek:previousCMEWeek(five),

      asia:sessions.asia,
      london:sessions.london,
      currentOvernight:sessions.overnight,
      sessionBounds:sessions.bounds,

      vwap:{
        globex:vwap(sessions.overnightRows)
      },

      profiles:{
        previousRTH_estimated:prevProfileEstimate,
        currentOvernight_estimated:estimatedProfile(sessions.overnightRows,tick),
        previousRTH_exact:priorRthDate ? finalizeProfile(profiles[profileKey("rth",priorRthDate)]) : null,
        currentGlobex_exact:finalizeProfile(profiles[profileKey("globex",currentSession)])
      },

      fourHourPivots:scoredPivots(four,current,a14),
      oneHourPivots:scoredPivots(hour,current,a14),

      volatility:{
        ATR14Daily:a14,
        dailyRange20:rangeStats(day,20)
      },

      previousSettlementProxy:prevR?.close??null
    },

    barCounts:{
      oneMin:one.length,
      fiveMin:five.length,
      oneHour:hour.length,
      fourHour:four.length,
      daily:day.length,
      weekly:week.length
    },

    bars:{
      oneMinRecent:one.slice(-3000),
      fiveMinRecent:five.slice(-3000),
      oneHourRecent:hour.slice(-1000),
      fourHourRecent:four.slice(-400),
      dailyRecent:day.slice(-400),
      weeklyRecent:week.slice(-200)
    }
  };

  lastSnapshotAt=latestSnapshot.generatedUtc;
}

async function connectStream() {
  const conn=new signalR.HubConnectionBuilder()
    .withUrl(HUB,{
      skipNegotiation:true,
      transport:signalR.HttpTransportType.WebSockets,
      accessTokenFactory:async()=>await getToken(),
      timeout:10000
    })
    .withAutomaticReconnect()
    .build();

  conn.on("GatewayQuote",(id,d)=>{
    const rows=Array.isArray(d)?d:[d];
    quoteEventsReceived+=rows.length;
    for(const row of rows){
      lastRawQuoteEvent={id,receivedAt:new Date().toISOString(),lastPrice:row?.lastPrice??row?.price??null,bid:row?.bestBid??row?.bid??null,ask:row?.bestAsk??row?.ask??null};
      if(id===contract.id || id===contract.symbolId || row?.symbolId===contract.symbolId) latestQuote=row;
    }
  });
  conn.on("GatewayDepth",(id,d)=>{
    const rows=Array.isArray(d)?d:[d];
    depthEventsReceived+=rows.length;
    const row=rows.at(-1)||{};
    lastRawDepthEvent={id,receivedAt:new Date().toISOString(),symbolId:row?.symbolId||null,type:row?.type??null,price:row?.price??null,volume:row?.volume??null};
  });
  conn.on("GatewayTrade",(id,d)=>{
    const rows=Array.isArray(d)?d:[d];
    tradeEventsReceived+=rows.length;
    for(const row of rows){
      lastRawTradeEvent={id,receivedAt:new Date().toISOString(),symbolId:row?.symbolId||null,price:row?.price??null,volume:row?.volume??null,type:row?.type??null,timestamp:row?.timestamp??null};
      const match=id===contract.id || id===contract.symbolId || row?.symbolId===contract.symbolId;
      if(match){ tradeEventsMatched++; addTrade(row); }
    }
  });

  const subscribe=async()=>{
    try{
      subscriptionResults.quotes=await conn.invoke("SubscribeContractQuotes",contract.id);
      console.log("Subscribed contract quotes",contract.id,subscriptionResults.quotes);
    }catch(e){subscriptionResults.quotes={error:e.message};console.error("quote subscribe",e.message);}
    try{
      subscriptionResults.trades=await conn.invoke("SubscribeContractTrades",contract.id);
      console.log("Subscribed contract trades",contract.id,contract.symbolId,subscriptionResults.trades);
    }catch(e){subscriptionResults.trades={error:e.message};console.error("trade subscribe",e.message);}
    try{
      subscriptionResults.depth=await conn.invoke("SubscribeContractMarketDepth",contract.id);
      console.log("Subscribed contract depth",contract.id,subscriptionResults.depth);
    }catch(e){subscriptionResults.depth={error:e.message};console.error("depth subscribe",e.message);}
  };

  conn.onreconnecting(()=>connected=false);
  conn.onreconnected(async()=>{ reconnectCount++; connected=true; await subscribe(); });
  conn.onclose(()=>connected=false);

  await conn.start();
  connected=true;
  await subscribe();
}

async function init() {
  loadState();
  loadMorningCutoff();
  await authenticate();
  await findContract();
  await buildSnapshot();

  if(DIRECT_TOPSTEP_REALTIME){
    await connectStream();
    console.log("Direct Topstep realtime enabled");
  } else {
    connected=false;
    console.log("Direct Topstep realtime disabled; using Mac relay for realtime");
  }

  setInterval(()=>buildSnapshot().catch(e=>console.error("snapshot",e)),SNAPSHOT_INTERVAL_MS);
  setInterval(()=>saveState(false),STATE_SAVE_MS);
  setInterval(()=>getToken().catch(e=>console.error("token refresh",e)),3600000);
  setInterval(()=>captureMorningCutoffIfDue().catch(e=>console.error("morning cutoff",e)),5000);
  // One bounded audit per revision, reusing our existing authenticated session.
  setTimeout(()=>runHistoryAudit({market:MARKET_SYMBOL,live:LIVE,
    post:(path,payload)=>post(path,payload,true,15000),
    file:`${DATA_DIR}/history-coverage-${MARKET_SYMBOL}.json`
  }).catch(()=>console.warn("Historical coverage audit could not complete")),15000);
  setTimeout(()=>runBackfill({market:MARKET_SYMBOL,live:LIVE,
    post:(path,payload)=>post(path,payload,true,20000),dir:`${DATA_DIR}/backtest-archive-${MARKET_SYMBOL}`
  }).catch(()=>console.warn("Historical backfill could not complete")),30000);
}

process.on("SIGTERM",()=>{ try{saveState(true);}finally{process.exit(0);} });
process.on("SIGINT",()=>{ try{saveState(true);}finally{process.exit(0);} });

const app=express();
app.use(express.json({limit:"256kb"}));

app.post("/realtime-relay",(req,res)=>{
  if(!REALTIME_RELAY_TOKEN) return res.status(503).json({ok:false,error:"relay token not configured"});
  const auth=req.headers.authorization||"";
  if(auth!==("Bearer "+REALTIME_RELAY_TOKEN)) return res.status(401).json({ok:false,error:"unauthorized"});
  const b=req.body||{};
  if(!b.receivedAt || !Array.isArray(b.oneMin) || !Array.isArray(b.fiveMin)) return res.status(400).json({ok:false,error:"invalid payload"});
  relayState=b;
  recordAbsorptionDepthSnapshot(b);
  res.json({ok:true,receivedAt:b.receivedAt});
});

app.get("/morning-cutoff.json",(req,res)=>{
  const today=nowPT().toISODate();
  if(!morningCutoff || morningCutoff.cutoffPacificDate!==today){
    return res.status(404).json({ok:false,error:"No 05:30 Pacific cutoff snapshot for today",marketSymbol:MARKET_SYMBOL,todayPacific:today});
  }
  res.json(morningCutoff);
});

app.get("/health",(req,res)=>res.json({
  ok:true,
  connected,
  contract:contract?.name||null,
  version:"3.4",
  lastTradeAt,
  lastSnapshotAt,
  persistedProfileKeys:Object.keys(profiles).sort(),
  directTopstepRealtime:DIRECT_TOPSTEP_REALTIME,
  tradeEventsReceived,tradeEventsMatched,quoteEventsReceived,depthEventsReceived,
  lastRawTradeEvent,lastRawQuoteEvent,lastRawDepthEvent,reconnectCount,subscriptionResults
}));

app.get("/indicator.json",(req,res)=>res.json(buildIndicatorPayload()));
app.get("/mnq-indicator.json",(req,res)=>res.json(buildIndicatorPayload()));
app.get("/mes-indicator.json",async(req,res)=>{
  if(MARKET_SYMBOL==="MES") return res.json(buildIndicatorPayload());
  if(!MES_SERVICE_URL) return res.status(503).json({error:"MES service not configured"});
  try{
    const r=await fetch(MES_SERVICE_URL.replace(/\/$/,"")+"/indicator.json",{headers:{"Accept":"application/json"}});
    if(!r.ok) return res.status(502).json({error:"MES upstream "+r.status});
    res.json(await r.json());
  }catch(e){res.status(502).json({error:"MES upstream unavailable"});}
});

app.post("/viewer-heartbeat",(req,res)=>{
  const id=String(req.body?.id||"").trim();
  if(!id || id.length>128) return res.status(400).json({ok:false,error:"invalid viewer id"});
  activeViewers.set(id,Date.now());
  res.json({ok:true,count:activeViewerCount()});
});

app.get("/viewer-count",(req,res)=>res.json({count:activeViewerCount()}));

app.get("/indicator",(req,res)=>{
  try{ res.type("html").send(fs.readFileSync(__dirname+"/indicator.html","utf8")); }
  catch(e){ res.status(500).type("text").send("Indicator UI unavailable"); }
});

app.get("/snapshot.json",(req,res)=>res.json(latestSnapshot||{}));
app.get("/history-coverage.json",(req,res)=>{
  res.set("Cache-Control","no-store");
  res.json(readHistoryCoverage(`${DATA_DIR}/history-coverage-${MARKET_SYMBOL}.json`)||{market:MARKET_SYMBOL,status:"pending"});
});
app.get("/backtest-history.json",(req,res)=>{
  res.set("Cache-Control","no-store");
  const dir=`${DATA_DIR}/backtest-archive-${MARKET_SYMBOL}`;
  res.json(req.query.export==='bars'?exportBackfill(dir):(readBackfill(`${dir}/manifest.json`)||{market:MARKET_SYMBOL,status:"pending"}));
});
app.get("/mnq-snapshot.json",(req,res)=>res.json(latestSnapshot||{}));

app.get("/profile.json",(req,res)=>{
  const currentSession=currentTradingSessionDate();
  const priorRthDate=priorBusinessDateStr();

  res.json({
    generatedUtc:DateTime.utc().toISO(),
    generatedPacific:nowPT().toISO(),
    contract:contract?.name||null,
    connected,
    currentTradingSessionDate:currentSession,
    previousRTHDate:priorRthDate,
    currentGlobex:finalizeProfile(profiles[profileKey("globex",currentSession)]),
    previousRTH:priorRthDate ? finalizeProfile(profiles[profileKey("rth",priorRthDate)]) : null
  });
});
app.get("/mnq-profile.json",(req,res)=>res.redirect(307,"/profile.json"));

app.get("/mnq-morning.json",(req,res)=>{
  const currentSession=currentTradingSessionDate();
  const priorRthDate=priorBusinessDateStr();
  const today=nowPT().toISODate();
  const cutoff=(morningCutoff?.cutoffPacificDate===today)?morningCutoff:null;

  res.json({
    generatedUtc:DateTime.utc().toISO(),
    generatedPacific:nowPT().toISO(),
    cutoff0530Pacific:cutoff,
    snapshot:cutoff?.snapshot||latestSnapshot,
    realtime:cutoff?.realtime||{
      connected,
      lastTradeAt,
      currentTradingSessionDate:currentSession,
      currentGlobex:finalizeProfile(profiles[profileKey("globex",currentSession)]),
      previousRTHDate:priorRthDate,
      previousRTH:priorRthDate ? finalizeProfile(profiles[profileKey("rth",priorRthDate)]) : null
    }
  });
});

app.get("/",(req,res)=>res.type("text").send(MARKET_SYMBOL+" unified market-data service v3.5\n"));

app.listen(PORT,()=>console.log(`HTTP on :${PORT}`));

init().catch(e=>{
  console.error(e);
  process.exit(1);
});
