const fs = require("fs");
const path = require("path");
const express = require("express");
const signalR = require("@microsoft/signalr");

const API_BASE = process.env.TOPSTEP_API_BASE || "https://api.topstepx.com";
const HUB = process.env.TOPSTEP_MARKET_HUB || "https://rtc.topstepx.com/hubs/market";
const USERNAME = process.env.TOPSTEP_USERNAME;
const API_KEY = process.env.TOPSTEP_API_KEY;
const LIVE = String(process.env.TOPSTEP_LIVE_DATA || "false").toLowerCase() === "true";
const PORT = Number(process.env.PORT || 8080);
const DATA_DIR = process.env.DATA_DIR || "/data";
const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS || 300000);
const STATE_SAVE_MS = Number(process.env.STATE_SAVE_MS || 30000);
const STATE_FILE = path.join(DATA_DIR, "trade-profile-state.json");

if (!USERNAME || !API_KEY) throw new Error("Missing TOPSTEP_USERNAME / TOPSTEP_API_KEY");
fs.mkdirSync(DATA_DIR, {recursive:true});

let token = null;
let tokenIssuedAt = 0;
let contract = null;
let latestQuote = null;
let connected = false;
let lastTradeAt = null;
let lastSnapshotAt = null;
let latestSnapshot = null;
let stateDirty = false;

/*
profiles:
{
  "globex:2026-09-21": { byPrice: {...}, buyVolume, sellVolume, tradeCount, firstTradeAt, lastTradeAt },
  "rth:2026-09-18":    { ... }
}
*/
let profiles = {};

async function authenticate() {
  const r = await fetch(API_BASE + "/api/Auth/loginKey", {
    method:"POST",
    headers:{"Content-Type":"application/json","Accept":"text/plain"},
    body:JSON.stringify({userName:USERNAME, apiKey:API_KEY})
  });
  if (!r.ok) throw new Error(`Auth HTTP ${r.status}: ${await r.text()}`);
  const j = await r.json();
  if (!j.success || !j.token) throw new Error(`Auth failed: ${j.errorCode} ${j.errorMessage || ""}`);
  token = j.token;
  tokenIssuedAt = Date.now();
  return token;
}

async function getToken() {
  // Refresh well before the documented ~24h token lifetime.
  if (!token || Date.now() - tokenIssuedAt > 18 * 3600000) await authenticate();
  return token;
}

async function post(apiPath, payload, retry=true) {
  const t = await getToken();
  const r = await fetch(API_BASE + apiPath, {
    method:"POST",
    headers:{"Content-Type":"application/json","Accept":"text/plain","Authorization":`Bearer ${t}`},
    body:JSON.stringify(payload)
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
  const end = new Date(), start = new Date(end.getTime() - days * 86400000);
  const r = await post("/api/History/retrieveBars", {
    contractId:contract.id, live:LIVE,
    startTime:start.toISOString(), endTime:end.toISOString(),
    unit, unitNumber, limit, includePartialBar:true
  });
  const out = (r.bars || []).map(b => ({
    t:b.t ?? b.time ?? b.timestamp,
    o:b.o ?? b.open, h:b.h ?? b.high, l:b.l ?? b.low,
    c:b.c ?? b.close, v:b.v ?? b.volume
  }));
  out.sort((a,b)=>new Date(a.t)-new Date(b.t));
  return out;
}

function ptParts(d=new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone:"America/Los_Angeles", hour12:false,
    year:"numeric",month:"2-digit",day:"2-digit",
    hour:"2-digit",minute:"2-digit",second:"2-digit",weekday:"short"
  }).formatToParts(d);
  return Object.fromEntries(parts.filter(x=>x.type!=="literal").map(x=>[x.type,x.value]));
}

function zonedDate(y,m,d,h,min=0) {
  let guess = new Date(Date.UTC(y,m-1,d,h+8,min));
  for (let i=0;i<3;i++) {
    const p = ptParts(guess);
    const got = Date.UTC(+p.year,+p.month-1,+p.day,+p.hour,+p.minute);
    const want = Date.UTC(y,m-1,d,h,min);
    guess = new Date(guess.getTime() + (want-got));
  }
  return guess;
}
function addDays(y,m,d,n) {
  const x = new Date(Date.UTC(y,m-1,d) + n*86400000);
  return {y:x.getUTCFullYear(),m:x.getUTCMonth()+1,d:x.getUTCDate()};
}
function ymd(x) {
  return `${x.y}-${String(x.m).padStart(2,"0")}-${String(x.d).padStart(2,"0")}`;
}
function dowNum(shortName) {
  return ({Sun:0,Mon:1,Tue:2,Wed:3,Thu:4,Fri:5,Sat:6})[shortName];
}

function between(rows,s,e) {
  return rows.filter(b => { const t=new Date(b.t); return t>=s && t<e; });
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
    const vol=+b.v||0; if(vol<=0) continue;
    const tp=(+b.h + +b.l + +b.c)/3;
    n += tp*vol; d += vol;
  }
  return d ? n/d : null;
}
function roundTick(x,t){ return Math.round(x/t)*t; }

function estimatedProfile(rows,tick) {
  const buckets = new Map();
  for(const b of rows){
    const vol=+b.v||0; if(vol<=0) continue;
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
    if(i>=0&&i<arr.length&&!selected.has(i)){ selected.add(i); cum+=arr[i].volume; }
  }
  const ps=[...selected].map(i=>arr[i].price);
  return {
    method:"estimated from 1m OHLCV",
    isTrueTradeByTradeProfile:false,
    valueAreaPercent:.70,
    poc:poc.price, vah:Math.max(...ps), val:Math.min(...ps)
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
    minRange:a[0], maxRange:a.at(-1)
  };
}

function previousRTH(rows) {
  const p=ptParts(); let cur=addDays(+p.year,+p.month,+p.day,-1);
  for(let i=0;i<8;i++){
    const s=zonedDate(cur.y,cur.m,cur.d,6,30), e=zonedDate(cur.y,cur.m,cur.d,13,0);
    const x=between(rows,s,e);
    if(x.length){
      const z=summary(x); z.date=ymd(cur); return z;
    }
    cur=addDays(cur.y,cur.m,cur.d,-1);
  }
  return null;
}

function previousFullGlobex(rows) {
  const p=ptParts(); let cur=addDays(+p.year,+p.month,+p.day,-1);
  for(let i=0;i<8;i++){
    const prev=addDays(cur.y,cur.m,cur.d,-1);
    const s=zonedDate(prev.y,prev.m,prev.d,15,0), e=zonedDate(cur.y,cur.m,cur.d,14,0);
    const x=between(rows,s,e);
    if(x.length){
      const z=summary(x); z.sessionDate=ymd(cur); return z;
    }
    cur=addDays(cur.y,cur.m,cur.d,-1);
  }
  return null;
}

function currentSessions(rows) {
  const p=ptParts(), today={y:+p.year,m:+p.month,d:+p.day}, prev=addDays(today.y,today.m,today.d,-1);
  const overnightRows=between(rows,zonedDate(prev.y,prev.m,prev.d,15,0),new Date());
  const asiaRows=between(rows,zonedDate(prev.y,prev.m,prev.d,17,0),zonedDate(today.y,today.m,today.d,0,0));
  const londonRows=between(rows,zonedDate(today.y,today.m,today.d,0,0),zonedDate(today.y,today.m,today.d,5,20));
  return {
    overnight:summary(overnightRows),
    asia:summary(asiaRows),
    london:summary(londonRows),
    overnightRows
  };
}

function previousCMEWeek(rows) {
  /*
    Always return the COMPLETED week before the current CME week:
    prior Sunday 15:00 PT -> prior Friday 14:00 PT.
    Example Monday 9/21 -> 9/13 15:00 through 9/18 14:00.
    Example Friday 9/25 -> still 9/13 15:00 through 9/18 14:00.
  */
  const p=ptParts();
  const dow=dowNum(p.weekday);
  const thisMonday=addDays(+p.year,+p.month,+p.day,-((dow+6)%7));
  const priorSunday=addDays(thisMonday.y,thisMonday.m,thisMonday.d,-8);
  const priorFriday=addDays(thisMonday.y,thisMonday.m,thisMonday.d,-3);

  const start=zonedDate(priorSunday.y,priorSunday.m,priorSunday.d,15,0);
  const end=zonedDate(priorFriday.y,priorFriday.m,priorFriday.d,14,0);
  const z=summary(between(rows,start,end));
  if(z){
    z.sessionStartPacific=`${ymd(priorSunday)} 15:00`;
    z.sessionEndPacific=`${ymd(priorFriday)} 14:00`;
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
      if(type==="high"){ if(+x.h>price)swept=true; exc=Math.max(exc,price-+x.l); }
      else { if(+x.l<price)swept=true; exc=Math.max(exc,+x.h-price); }
    }
    const dist=Math.abs(current-price), age=Math.max(0,(Date.now()-new Date(b.t))/3600000);
    const score=(!swept?45:0)+Math.min(25,(exc/ref)*20)+Math.max(0,15-(dist/ref)*5)+Math.max(0,15-age/48);
    raw.push({
      type,price,time:b.t,sweptLater:swept,
      liquidityStatus:swept?"swept":"untouched",
      distanceFromCurrentPoints:+dist.toFixed(2),
      significanceScore:+score.toFixed(1)
    });
  }
  return raw.sort((a,b)=>b.significanceScore-a.significanceScore).slice(0,8);
}

/* ---------- exact realtime profile persistence ---------- */

function emptyProfileState() {
  return {byPrice:{},buyVolume:0,sellVolume:0,tradeCount:0,firstTradeAt:null,lastTradeAt:null};
}
function profileKey(kind,dateStr){ return `${kind}:${dateStr}`; }

function tradeSessionKeys(ts) {
  const d=new Date(ts), p=ptParts(d);
  const y=+p.year,m=+p.month,day=+p.day,h=+p.hour,min=+p.minute;
  let sessionDate;
  if(h>=15){
    const n=addDays(y,m,day,1);
    sessionDate=ymd(n);
  } else {
    sessionDate=ymd({y,m,d:day});
  }

  const mins=h*60+min;
  const keys=[profileKey("globex",sessionDate)];

  if(mins>=390 && mins<780){ // 06:30-13:00
    keys.push(profileKey("rth",ymd({y,m,d:day})));
  }
  if(mins>=1020 || mins<0){ /* placeholder */ }

  return keys;
}

function applyTradeToState(state,d) {
  const price=+d.price,vol=+d.volume||0,type=+d.type;
  if(!Number.isFinite(price)||!Number.isFinite(vol)||vol<=0) return;

  const k=price.toFixed(2);
  const x=state.byPrice[k]||{price,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};
  x.volume+=vol; x.trades++;
  if(type===0){ x.buyVolume+=vol; x.delta+=vol; state.buyVolume+=vol; }
  else if(type===1){ x.sellVolume+=vol; x.delta-=vol; state.sellVolume+=vol; }
  state.byPrice[k]=x;
  state.tradeCount++;
  const ts=d.timestamp||new Date().toISOString();
  if(!state.firstTradeAt) state.firstTradeAt=ts;
  state.lastTradeAt=ts;
}

function addTrade(d) {
  const ts=d.timestamp||new Date().toISOString();
  for(const key of tradeSessionKeys(ts)){
    if(!profiles[key]) profiles[key]=emptyProfileState();
    applyTradeToState(profiles[key],d);
  }
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
    const lv=lo>=0?arr[lo].volume:-1, hv=hi<arr.length?arr[hi].volume:-1;
    let i;
    if(hv>=lv&&hi<arr.length) i=hi++;
    else i=lo--;
    if(i>=0&&i<arr.length&&!selected.has(i)){
      selected.add(i); cum+=arr[i].volume;
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
  const p=ptParts();
  const y=+p.year,m=+p.month,d=+p.day,h=+p.hour;
  if(h>=15) return ymd(addDays(y,m,d,1));
  return ymd({y,m,d});
}

function priorBusinessDateStr() {
  const p=ptParts();
  let cur=addDays(+p.year,+p.month,+p.day,-1);
  for(let i=0;i<7;i++){
    const wd=new Date(Date.UTC(cur.y,cur.m-1,cur.d)).getUTCDay();
    if(wd!==0&&wd!==6) return ymd(cur);
    cur=addDays(cur.y,cur.m,cur.d,-1);
  }
  return null;
}

function pruneProfiles() {
  const keys=Object.keys(profiles);
  const cutoff=Date.now()-14*86400000;
  for(const key of keys){
    const state=profiles[key];
    const t=state?.lastTradeAt ? new Date(state.lastTradeAt).getTime() : 0;
    if(t && t<cutoff) delete profiles[key];
  }
}

function saveState(force=false) {
  if(!force && !stateDirty) return;
  pruneProfiles();
  const tmp=STATE_FILE+".tmp";
  fs.writeFileSync(tmp,JSON.stringify({schemaVersion:1,savedAt:new Date().toISOString(),profiles},null,2));
  fs.renameSync(tmp,STATE_FILE);
  stateDirty=false;
}

function loadState() {
  if(!fs.existsSync(STATE_FILE)) return;
  try{
    const j=JSON.parse(fs.readFileSync(STATE_FILE,"utf8"));
    if(j && j.profiles) profiles=j.profiles;
  }catch(e){
    console.warn("Could not load prior profile state:",e.message);
  }
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
    const [Y,M,D]=prev1.date.split("-").map(Number);
    prevProfileEstimate=estimatedProfile(
      between(one,zonedDate(Y,M,D,6,30),zonedDate(Y,M,D,13,0)),
      tick
    );
  }

  const currentSession=currentTradingSessionDate();
  const priorRthDate=priorBusinessDateStr();

  latestSnapshot={
    schemaVersion:3.2,
    generatedUtc:new Date().toISOString(),
    source:"TopstepX / ProjectX CME market data",
    contract:{
      id:contract.id,name:contract.name,description:contract.description,
      symbolId:contract.symbolId,tickSize:contract.tickSize,tickValue:contract.tickValue,
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
      vwap:{globex:vwap(sessions.overnightRows)},
      profiles:{
        previousRTH_estimated:prevProfileEstimate,
        currentOvernight_estimated:estimatedProfile(sessions.overnightRows,tick),
        previousRTH_exact:priorRthDate ? finalizeProfile(profiles[profileKey("rth",priorRthDate)]) : null,
        currentGlobex_exact:finalizeProfile(profiles[profileKey("globex",currentSession)])
      },
      oneHourPivots:scoredPivots(hour,current,a14),
      volatility:{ATR14Daily:a14,dailyRange20:rangeStats(day,20)},
      previousSettlementProxy:prevR?.close??null
    },
    barCounts:{
      oneMin:one.length,fiveMin:five.length,oneHour:hour.length,
      daily:day.length,weekly:week.length
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

  conn.on("GatewayQuote",(id,d)=>{ if(id===contract.id) latestQuote=d; });
  conn.on("GatewayTrade",(id,d)=>{ if(id===contract.id) addTrade(d); });

  const subscribe=async()=>{
    await conn.invoke("SubscribeContractQuotes",contract.id);
    await conn.invoke("SubscribeContractTrades",contract.id);
    try{ await conn.invoke("SubscribeContractMarketDepth",contract.id); }catch(e){}
  };

  conn.onreconnecting(()=>connected=false);
  conn.onreconnected(async()=>{ connected=true; await subscribe(); });
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
  await connectStream();

  setInterval(()=>buildSnapshot().catch(e=>console.error("snapshot",e)),SNAPSHOT_INTERVAL_MS);
  setInterval(()=>saveState(false),STATE_SAVE_MS);
  setInterval(()=>getToken().catch(e=>console.error("token refresh",e)),3600000);
}

process.on("SIGTERM",()=>{ try{saveState(true);}finally{process.exit(0);} });
process.on("SIGINT",()=>{ try{saveState(true);}finally{process.exit(0);} });

const app=express();

app.get("/health",(req,res)=>res.json({
  ok:true,connected,contract:contract?.name||null,
  lastTradeAt,lastSnapshotAt,
  persistedProfileKeys:Object.keys(profiles).sort()
}));

app.get("/mnq-snapshot.json",(req,res)=>res.json(latestSnapshot||{}));

app.get("/mnq-profile.json",(req,res)=>{
  const currentSession=currentTradingSessionDate();
  const priorRthDate=priorBusinessDateStr();
  res.json({
    generatedUtc:new Date().toISOString(),
    contract:contract?.name||null,
    connected,
    currentTradingSessionDate:currentSession,
    previousRTHDate:priorRthDate,
    currentGlobex:finalizeProfile(profiles[profileKey("globex",currentSession)]),
    previousRTH:priorRthDate ? finalizeProfile(profiles[profileKey("rth",priorRthDate)]) : null
  });
});

app.get("/mnq-morning.json",(req,res)=>res.json({
  generatedUtc:new Date().toISOString(),
  snapshot:latestSnapshot,
  realtime:{
    connected,
    lastTradeAt,
    currentTradingSessionDate:currentTradingSessionDate(),
    currentGlobex:finalizeProfile(profiles[profileKey("globex",currentTradingSessionDate())]),
    previousRTHDate:priorBusinessDateStr(),
    previousRTH:priorBusinessDateStr() ? finalizeProfile(profiles[profileKey("rth",priorBusinessDateStr())]) : null
  }
}));

app.get("/",(req,res)=>res.type("text").send("MNQ unified market-data service v3.2\n"));
app.listen(PORT,()=>console.log(`HTTP on :${PORT}`));

init().catch(e=>{ console.error(e); process.exit(1); });
