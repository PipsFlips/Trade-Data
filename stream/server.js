const express = require("express");
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
let signalStability = {setupKey:null,cvdAlignedSince:null,targetRoomAward:null};

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
async function post(apiPath, payload, retry=true) {
  const t = await getToken();
  const r = await fetch(API_BASE + apiPath, {
    method: "POST",
    headers: {"Content-Type":"application/json","Accept":"text/plain","Authorization":`Bearer ${t}`},
    body: JSON.stringify(payload)
  });
  if (r.status === 401 && retry) {
    await authenticate();
    return post(apiPath, payload, false);
  }
  if (!r.ok) throw new Error(`${apiPath} HTTP ${r.status}: ${await r.text()}`);
  return r.json();
}
async function findContract() {
  const r = await post("/api/Contract/search", {searchText:"MNQ", live:LIVE});
  const cs = (r.contracts || []).filter(c =>
    JSON.stringify(c).toUpperCase().includes("MNQ") ||
    String(c.description || "").toUpperCase().includes("MICRO E-MINI NASDAQ")
  );
  contract = cs.find(c => c.activeContract) || cs[0];
  if (!contract) throw new Error("MNQ contract not found");
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
  const now=nowPT();
  const today=now.startOf("day");
  const prev=today.minus({days:1});

  const globexStart=prev.set({hour:15,minute:0,second:0,millisecond:0});
  const asiaStart=prev.set({hour:17,minute:0,second:0,millisecond:0});
  const asiaEnd=today.set({hour:0,minute:0,second:0,millisecond:0});
  const londonStart=today.set({hour:0,minute:0,second:0,millisecond:0});
  const londonEnd=today.set({hour:5,minute:20,second:0,millisecond:0});

  const overnightRows=between(rows,globexStart,now);
  return {
    overnight:summary(overnightRows),
    asia:summary(between(rows,asiaStart,asiaEnd)),
    london:summary(between(rows,londonStart,londonEnd)),
    overnightRows,
    bounds:{
      globexStartPacific:globexStart.toISO(),
      asiaStartPacific:asiaStart.toISO(),
      asiaEndPacific:asiaEnd.toISO(),
      londonStartPacific:londonStart.toISO(),
      londonEndPacific:londonEnd.toISO()
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

function buildMarketAnalysis({currentPrice,levels,f1,f5,signal,traps,orderBlocks,globexVwap,rthVwap,sessionCvd,atr5,orb,bars5m,delta15,profiles}){
  const closed1=lastClosedBar(f1||[],1);
  const closed5=lastClosedBar(f5||[],5);
  const closed5s=(f5||[]).filter(b=>Date.parse(b.t)+300000<=Date.now());
  const prior5=closed5s.length>1?closed5s.at(-2):null;
  const activeVwap=Number.isFinite(+rthVwap)?+rthVwap:+globexVwap;
  let biasPoints=0;
  const reasons=[];
  const profile=profiles?.rth||profiles?.session||null;

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
  if(signal?.side==="BUY" && signal.score>=65){biasPoints+=2;reasons.push("BUY setup score "+signal.score);}
  if(signal?.side==="SELL" && signal.score>=65){biasPoints-=2;reasons.push("SELL setup score "+signal.score);}

  const bias=biasPoints>=3?"BULLISH":biasPoints<=-3?"BEARISH":Math.abs(biasPoints)<=1?"NEUTRAL":"MIXED";
  const strength=Math.abs(biasPoints)>=6?"strong":Math.abs(biasPoints)>=3?"moderate":"light";
  const major=(levels||[]).filter(l=>Number.isFinite(+l.price)&&l.priority>=80);
  const above=major.filter(l=>+l.price>currentPrice).sort((a,b)=>+a.price-+b.price);
  const below=major.filter(l=>+l.price<currentPrice).sort((a,b)=>+b.price-+a.price);
  const setups=[];

  const targetFor=(side,from)=>{
    const xs=major.filter(l=>side==="BUY"?+l.price>from:+l.price<from)
      .sort((a,b)=>side==="BUY"?+a.price-+b.price:+b.price-+a.price);
    return xs[0]||null;
  };

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
    if(dist<=atr*.9){
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
    const nearHi=distHi<=atr*.75 || currentPrice>=+orb.high;
    const nearLo=distLo<=atr*.75 || currentPrice<=+orb.low;
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
      setups.push({
        side,
        title:"ORB "+(long?"high":"low")+" breakout",
        trigger:"5m close "+(long?"above ":"below ")+(long?"ORB High ":"ORB Low ")+level.toFixed(2)+
          " with "+(long?"positive":"negative")+" delta and volume expansion"+
          (medVol?" (prefer ≥1.2× recent 5m median).":"."),
        invalidation:"5m close back "+(long?"below ORB High.":"above ORB Low."),
        target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next major liquidity level",
        quality,
        context:"Bias "+bias+" · volume "+(Number.isFinite(volRatio)?volRatio.toFixed(2)+"×":"n/a")+" · delta "+(d>0?"+":"")+Math.round(d)
      });
    };
    if(nearHi && (longBias || currentPrice>=+orb.high)) orbSetup("BUY");
    if(nearLo && (shortBias || currentPrice<=+orb.low)) orbSetup("SELL");
  }

  if((bias==="BULLISH"||bias==="MIXED") && above[0]){
    const lvl=above[0],tgt=targetFor("BUY",+lvl.price+0.01);
    setups.push({
      side:"BUY",title:"Breakout acceptance above "+lvl.label,
      trigger:"Closed 5m acceptance above "+lvl.label+" "+(+lvl.price).toFixed(2)+" with positive 1m/5m delta.",
      invalidation:"5m close back below "+lvl.label+".",
      target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next higher major level",quality:null
    });
  } else if((bias==="BEARISH"||bias==="MIXED") && below[0]){
    const lvl=below[0],tgt=targetFor("SELL",+lvl.price-0.01);
    setups.push({
      side:"SELL",title:"Breakdown acceptance below "+lvl.label,
      trigger:"Closed 5m acceptance below "+lvl.label+" "+(+lvl.price).toFixed(2)+" with negative 1m/5m delta.",
      invalidation:"5m close back above "+lvl.label+".",
      target:tgt?(tgt.label+" "+(+tgt.price).toFixed(2)):"next lower major level",quality:null
    });
  }

  const dedup=[];
  for(const x of setups.sort((a,b)=>(+b.quality||0)-(+a.quality||0))){
    if(dedup.some(y=>y.title===x.title)) continue;
    dedup.push(x);
    if(dedup.length>=3) break;
  }
  return {
    bias,strength,points:biasPoints,reasons:reasons.slice(0,5),setups:dedup,
    asOf:new Date().toISOString(),
    note:"Conditional setups only; bias, volume, delta, VWAP, traps, 15m OBs and ORB confluence are evaluated."
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
  const msg=`MNQ ${signal.side} score ${signal.score}/100 at ${Number(currentPrice).toFixed(2)}. ${(signal.reasons||[]).slice(0,3).join(" | ")}`;
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
function buildIndicatorPayload() {
  if(!latestSnapshot||!contract) return {};
  const a=latestSnapshot.analytics||{}, tick=+contract.tickSize||0.25;
  const currentSession=currentTradingSessionDate();
  const today=nowPT().toISODate();
  const currentGlobexState=profiles[profileKey("globex",currentSession)];
  const currentRthState=profiles[profileKey("rth",today)];
  const relayFreshNow=relayState && Date.now()-Date.parse(relayState.receivedAt)<10000;
  const relayGlobexProfile=relayFreshNow?relayState.profiles?.session:null;
  const relayRthProfile=relayFreshNow?relayState.profiles?.rth:null;
  const globexExact=relayGlobexProfile||finalizeProfile(currentGlobexState);
  const rthExact=relayRthProfile||finalizeProfile(currentRthState);
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
    {id:"onVah",label:"ON VAH",price:globexExact?.vah ?? p.currentOvernight_estimated?.vah,kind:"profile",priority:84},
    {id:"onPoc",label:"ON POC",price:globexExact?.poc ?? p.currentOvernight_estimated?.poc,kind:"profile",priority:86},
    {id:"onVal",label:"ON VAL",price:globexExact?.val ?? p.currentOvernight_estimated?.val,kind:"profile",priority:84},
    ...((a.oneHourPivots||[]).filter(x=>!x.sweptLater).slice(0,6).map((x,i)=>({
      id:"pivot"+i,label:`1H ${x.type==="high"?"H":"L"} ${x.significanceScore}`,price:x.price,
      kind:x.type==="high"?"resistance":"support",priority:70-i
    })))
  ],tick);

  const relayFresh=relayState && Date.now()-Date.parse(relayState.receivedAt)<10000;
  const f1=relayFresh?(relayState.oneMin||[]):withCvd(flowArray(flowBars.oneMin,360));
  const f5=relayFresh?(relayState.fiveMin||[]):withCvd(flowArray(flowBars.fiveMin,720));
  const last5=f5.at(-1)||null;
  const globexVwap=relayFresh && Number.isFinite(+relayState.sessionVwap)?+relayState.sessionVwap:(exactVwapFromState(currentGlobexState) ?? a.vwap?.globex ?? null);
  const rthVwap=relayFresh && Number.isFinite(+relayState.rthVwap)?+relayState.rthVwap:exactVwapFromState(currentRthState);

  const candidateHighs=levels.filter(l=>["resistance","profile"].includes(l.kind)&&l.price>=currentPrice-tick*8).slice(0,5);
  const candidateLows=levels.filter(l=>["support","profile"].includes(l.kind)&&l.price<=currentPrice+tick*8).slice(0,5);

  // Trap detection: completed 1m sweep + reclaim + next-bar follow-through.
  // Old/invalidated traps are removed and nearby duplicate levels are clustered.
  const closed1=f1.filter(b=>Date.parse(b.t)+60000<=Date.now()).slice(-30);
  const trapCandidatesRaw=[];
  const maxTrapAgeMs=20*60000;
  const atr5ForTraps=current5mAtr(latestSnapshot.bars?.fiveMinRecent||[],20)||20;
  const clusterDistance=Math.max(2.0,atr5ForTraps*.08);

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

  let signal={side:"NEUTRAL",score:0,reasons:[],components:{}};
  const latestTrap=traps[0];
  if(latestTrap && Date.now()-Date.parse(latestTrap.time)<=20*60000){
    signal.side=latestTrap.side;
    const setupKey=[latestTrap.side,latestTrap.type,latestTrap.label,latestTrap.time].join("|");
    if(signalStability.setupKey!==setupKey){
      signalStability={setupKey,cvdAlignedSince:null,targetRoomAward:null};
    }

    signal.score+=25; signal.components.trap=25;
    signal.reasons.push(latestTrap.type+" at "+latestTrap.label);

    const last1=lastClosedBar(f1,1);
    const last5=lastClosedBar(f5,5);
    const closed5=lastClosedBar(latestSnapshot.bars?.fiveMinRecent||[],5);
    const structureOk=closed5 ? (signal.side==="BUY"?+closed5.c>+latestTrap.level:+closed5.c<+latestTrap.level) : false;
    if(structureOk){
      signal.score+=20;signal.components.structure=20;signal.reasons.push("closed 5m structure confirmed");
    }

    if(last1 && (signal.side==="BUY"?+last1.delta>0:+last1.delta<0)){
      signal.score+=10;signal.components.delta1m=10;signal.reasons.push("closed 1m delta aligned");
    }
    if(last5 && (signal.side==="BUY"?+last5.delta>0:+last5.delta<0)){
      signal.score+=10;signal.components.delta5m=10;signal.reasons.push("closed 5m delta aligned");
    }

    const sessionCvd=relayFresh?relayState.currentGlobexCvd:(globexExact?.cvd);
    const cvdAligned=Number.isFinite(+sessionCvd) && (signal.side==="BUY"?+sessionCvd>0:+sessionCvd<0);
    if(cvdAligned){
      if(!signalStability.cvdAlignedSince) signalStability.cvdAlignedSince=Date.now();
      if(Date.now()-signalStability.cvdAlignedSince>=10000){
        signal.score+=10;signal.components.cvd=10;signal.reasons.push("session CVD aligned 10s");
      }
    }else{
      signalStability.cvdAlignedSince=null;
    }

    const activeVwap=Number.isFinite(+rthVwap)?+rthVwap:+globexVwap;
    if(Number.isFinite(activeVwap) && (signal.side==="BUY"?currentPrice>activeVwap:currentPrice<activeVwap)){
      signal.score+=10;signal.components.vwap=10;signal.reasons.push("VWAP aligned");
    }

    const trapLevel=levels.find(l=>l.label===latestTrap.label);
    if(trapLevel && trapLevel.priority>=80){
      signal.score+=10;signal.components.level=10;signal.reasons.push("major level confluence");
    }

    if(signalStability.targetRoomAward===null){
      const atr5=current5mAtr(latestSnapshot.bars?.fiveMinRecent||[],20)||20;
      const estRisk=Math.max(8,atr5*.25);
      const targetDist=nearestLevelDistance(levels,currentPrice,signal.side);
      signalStability.targetRoomAward=Boolean(Number.isFinite(targetDist)&&targetDist>=estRisk*1.5);
    }
    if(signalStability.targetRoomAward){
      signal.score+=5;signal.components.targetRoom=5;signal.reasons.push(">=1.5R target room");
    }
  }else{
    signalStability={setupKey:null,cvdAlignedSince:null,targetRoomAward:null};
  }
  signal.score=Math.min(100,signal.score);
  if(signal.score<50) signal.side="NEUTRAL";

  const bars5m=latestSnapshot.bars?.fiveMinRecent||[];
  const bars15m=aggregateBars(bars5m,15);
  const flow15m=aggregateBars(f5,15);
  const orderBlocks=detectOrderBlocks(
    bars15m,levels,flow15m,currentPrice,rthVwap,globexVwap,15
  );
  const orb=currentOrb(bars5m,f5);
  const analysis=buildMarketAnalysis({
    currentPrice,levels,f1,f5,signal,traps,orderBlocks,globexVwap,rthVwap,
    sessionCvd:relayFresh?relayState.currentGlobexCvd:(globexExact?.cvd??null),
    atr5:current5mAtr(bars5m,20),orb,bars5m,
    delta15:deltaTrend15m(f5),
    profiles:{session:globexExact,rth:rthExact}
  });
  maybeSendSignalAlert(signal,currentPrice);

  return {
    schemaVersion:"1.3",
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
      currentRthDelta:relayFresh?relayState.currentRthDelta:(rthExact?.delta??null),
      currentGlobexCvd:relayFresh?relayState.currentGlobexCvd:(globexExact?.cvd??null),
      currentRthCvd:relayFresh?relayState.currentRthCvd:(rthExact?.cvd??null),
      lastTradeAt:relayFresh?(relayState.lastTradeAt||null):(lastTradeAt||null),
      lastTradeReceivedAt:relayFresh?(relayState.lastTradeReceivedAt||null):null,
      lastQuoteReceivedAt:relayFresh?(relayState.lastQuoteReceivedAt||null):null,
      lastDepthReceivedAt:relayFresh?(relayState.lastDepthReceivedAt||null):null
    },
    levels:levels.slice(0,18),
    trapCandidates:{buyer:candidateHighs,seller:candidateLows},
    confirmedTraps:traps.slice(0,8),
    orderBlocks,
    orb,
    signal,
    analysis,
    volatility:{atr5m20:current5mAtr(latestSnapshot.bars?.fiveMinRecent||[],20),atr14Daily:latestSnapshot.analytics?.volatility?.ATR14Daily??null},
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
    bars5m:(latestSnapshot.bars?.fiveMinRecent||[]).slice(-400)
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
      daily:day.length,
      weekly:week.length
    },

    bars:{
      oneMinRecent:one.slice(-3000),
      fiveMinRecent:five.slice(-3000),
      oneHourRecent:hour.slice(-1000),
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
  res.json({ok:true,receivedAt:b.receivedAt});
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

app.get("/mnq-indicator.json",(req,res)=>res.json(buildIndicatorPayload()));

app.get("/indicator",(req,res)=>{
  try{ res.type("html").send(fs.readFileSync(__dirname+"/indicator.html","utf8")); }
  catch(e){ res.status(500).type("text").send("Indicator UI unavailable"); }
});

app.get("/mnq-snapshot.json",(req,res)=>res.json(latestSnapshot||{}));

app.get("/mnq-profile.json",(req,res)=>{
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

app.get("/mnq-morning.json",(req,res)=>{
  const currentSession=currentTradingSessionDate();
  const priorRthDate=priorBusinessDateStr();

  res.json({
    generatedUtc:DateTime.utc().toISO(),
    generatedPacific:nowPT().toISO(),
    snapshot:latestSnapshot,
    realtime:{
      connected,
      lastTradeAt,
      currentTradingSessionDate:currentSession,
      currentGlobex:finalizeProfile(profiles[profileKey("globex",currentSession)]),
      previousRTHDate:priorRthDate,
      previousRTH:priorRthDate ? finalizeProfile(profiles[profileKey("rth",priorRthDate)]) : null
    }
  });
});

app.get("/",(req,res)=>res.type("text").send("MNQ unified market-data service v3.4\n"));

app.listen(PORT,()=>console.log(`HTTP on :${PORT}`));

init().catch(e=>{
  console.error(e);
  process.exit(1);
});
