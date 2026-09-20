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
let stateDirty = false;

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
}
function saveState(force=false) {
  if(!force&&!stateDirty) return;
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
    if(j?.profiles) profiles=j.profiles;
  }catch(e){
    console.warn("Could not load profile state:",e.message);
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
    schemaVersion:3.3,
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
  ok:true,
  connected,
  contract:contract?.name||null,
  version:"3.3",
  lastTradeAt,
  lastSnapshotAt,
  persistedProfileKeys:Object.keys(profiles).sort()
}));

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

app.get("/",(req,res)=>res.type("text").send("MNQ unified market-data service v3.3\n"));

app.listen(PORT,()=>console.log(`HTTP on :${PORT}`));

init().catch(e=>{
  console.error(e);
  process.exit(1);
});
