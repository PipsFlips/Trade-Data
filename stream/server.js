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
const SNAPSHOT_INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS || 300000); // 5 min

if (!USERNAME || !API_KEY) throw new Error("Missing TOPSTEP_USERNAME / TOPSTEP_API_KEY");

fs.mkdirSync(DATA_DIR, { recursive: true });

let token = null;
let contract = null;
let latestQuote = null;
let connected = false;
let lastTradeAt = null;
let lastSnapshotAt = null;
let latestSnapshot = null;

let byPrice = new Map();
let buyVolume = 0;
let sellVolume = 0;
let tradeCount = 0;

async function post(pathname, payload, authToken = token) {
  const headers = { "Content-Type": "application/json", "Accept": "text/plain" };
  if (authToken) headers.Authorization = `Bearer ${authToken}`;
  const r = await fetch(API_BASE + pathname, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(`${pathname} HTTP ${r.status}: ${await r.text()}`);
  return await r.json();
}

async function authenticate() {
  const r = await post("/api/Auth/loginKey", { userName: USERNAME, apiKey: API_KEY }, null);
  if (!r.success || !r.token) throw new Error(`Auth failed: ${r.errorCode} ${r.errorMessage || ""}`);
  token = r.token;
  return token;
}

async function findContract() {
  const r = await post("/api/Contract/search", { searchText: "MNQ", live: LIVE });
  const cs = (r.contracts || []).filter(c =>
    JSON.stringify(c).toUpperCase().includes("MNQ") ||
    String(c.description || "").toUpperCase().includes("MICRO E-MINI NASDAQ")
  );
  contract = cs.find(c => c.activeContract) || cs[0];
  if (!contract) throw new Error("MNQ contract not found");
  return contract;
}

function parseTs(x) {
  return new Date(x);
}

async function bars(unit, unitNumber, days, limit = 20000) {
  const end = new Date();
  const start = new Date(end.getTime() - days * 86400000);
  const r = await post("/api/History/retrieveBars", {
    contractId: contract.id,
    live: LIVE,
    startTime: start.toISOString(),
    endTime: end.toISOString(),
    unit,
    unitNumber,
    limit,
    includePartialBar: true,
  });
  const out = (r.bars || []).map(b => ({
    t: b.t ?? b.time ?? b.timestamp,
    o: b.o ?? b.open,
    h: b.h ?? b.high,
    l: b.l ?? b.low,
    c: b.c ?? b.close,
    v: b.v ?? b.volume,
  }));
  out.sort((a,b)=>parseTs(a.t)-parseTs(b.t));
  return out;
}

function summary(rows) {
  if (!rows.length) return null;
  return {
    open: Number(rows[0].o),
    high: Math.max(...rows.map(x => Number(x.h))),
    low: Math.min(...rows.map(x => Number(x.l))),
    close: Number(rows[rows.length - 1].c),
    volume: rows.reduce((s,x)=>s+Number(x.v||0),0),
    start: rows[0].t,
    end: rows[rows.length - 1].t,
    bars: rows.length,
  };
}

function roundTick(x, tick=0.25) {
  return Math.round(x / tick) * tick;
}

function estimatedProfile(rows, tick=0.25) {
  const buckets = new Map();
  for (const b of rows) {
    const vol = Number(b.v || 0);
    if (vol <= 0) continue;
    const lo = roundTick(Number(b.l), tick);
    const hi = roundTick(Number(b.h), tick);
    const count = Math.max(1, Math.round((hi - lo) / tick) + 1);
    const per = vol / count;
    for (let i=0;i<count;i++) {
      const p = Number((lo + i*tick).toFixed(10));
      buckets.set(p, (buckets.get(p)||0)+per);
    }
  }
  if (!buckets.size) return null;
  const arr = [...buckets.entries()].sort((a,b)=>a[0]-b[0]).map(([price,volume])=>({price,volume}));
  const poc = arr.reduce((a,b)=>b.volume>a.volume?b:a);
  const total = arr.reduce((s,x)=>s+x.volume,0);
  const target = total*0.70;
  const idx = arr.indexOf(poc);
  let lo = idx-1, hi = idx+1, cum=poc.volume;
  const selected = new Set([idx]);
  while (cum<target && (lo>=0 || hi<arr.length)) {
    const lv = lo>=0 ? arr[lo].volume : -1;
    const hv = hi<arr.length ? arr[hi].volume : -1;
    let i;
    if (hv>=lv && hi<arr.length) i=hi++;
    else i=lo--;
    if (i>=0 && i<arr.length && !selected.has(i)) {
      selected.add(i); cum += arr[i].volume;
    }
  }
  const sel=[...selected].map(i=>arr[i].price);
  return {
    method:"estimated from 1m OHLCV",
    isTrueTradeByTradeProfile:false,
    poc:poc.price,
    vah:Math.max(...sel),
    val:Math.min(...sel),
  };
}

function exactProfile() {
  const arr=[...byPrice.values()].sort((a,b)=>a.price-b.price);
  if(!arr.length) return null;
  const total=arr.reduce((s,x)=>s+x.volume,0);
  const poc=arr.reduce((a,b)=>b.volume>a.volume?b:a);
  const idx=arr.indexOf(poc);
  const selected=new Set([idx]);
  let cum=poc.volume,lo=idx-1,hi=idx+1;
  while(cum<total*0.70 && (lo>=0 || hi<arr.length)){
    const lv=lo>=0?arr[lo].volume:-1;
    const hv=hi<arr.length?arr[hi].volume:-1;
    let i;
    if(hv>=lv && hi<arr.length)i=hi++;
    else i=lo--;
    if(i>=0 && i<arr.length && !selected.has(i)){selected.add(i);cum+=arr[i].volume;}
  }
  const sel=[...selected].map(i=>arr[i].price);
  return {
    method:"exact GatewayTrade volume-at-price",
    isTrueTradeByTradeProfile:true,
    totalVolume:total,
    buyVolume,
    sellVolume,
    delta:buyVolume-sellVolume,
    cvd:buyVolume-sellVolume,
    tradeCount,
    poc:poc.price,
    vah:Math.max(...sel),
    val:Math.min(...sel),
    strongestVolumeNodes:[...arr].sort((a,b)=>b.volume-a.volume).slice(0,10),
    strongestDeltaPrices:[...arr].sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta)).slice(0,15),
    priceLevels:arr
  };
}

function addTrade(d) {
  const price=Number(d.price);
  const vol=Number(d.volume||0);
  const type=Number(d.type);
  if(!Number.isFinite(price)||!Number.isFinite(vol)||vol<=0) return;
  const key=price.toFixed(2);
  const x=byPrice.get(key)||{price,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};
  x.volume += vol; x.trades += 1;
  if(type===0){x.buyVolume+=vol;x.delta+=vol;buyVolume+=vol;}
  else if(type===1){x.sellVolume+=vol;x.delta-=vol;sellVolume+=vol;}
  byPrice.set(key,x);
  tradeCount++;
  lastTradeAt=d.timestamp||new Date().toISOString();
}

async function buildSnapshot() {
  const [one,five,hour,day,week] = await Promise.all([
    bars(2,1,10,15000),
    bars(2,5,45,15000),
    bars(3,1,120,5000),
    bars(4,1,500,1200),
    bars(5,1,1800,600),
  ]);

  const tick = Number(contract.tickSize || 0.25);
  latestSnapshot = {
    schemaVersion:3,
    generatedUtc:new Date().toISOString(),
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
      recent1mProfileEstimate:estimatedProfile(one.slice(-500),tick),
      realtimeTradeProfile:exactProfile()
    },
    barCounts:{oneMin:one.length,fiveMin:five.length,oneHour:hour.length,daily:day.length,weekly:week.length},
    bars:{
      oneMinRecent:one.slice(-3000),
      fiveMinRecent:five.slice(-3000),
      oneHourRecent:hour.slice(-1000),
      dailyRecent:day.slice(-400),
      weeklyRecent:week.slice(-200),
    }
  };
  lastSnapshotAt=latestSnapshot.generatedUtc;
  fs.writeFileSync(path.join(DATA_DIR,"mnq-snapshot.json"),JSON.stringify(latestSnapshot,null,2));
  return latestSnapshot;
}

async function connectStream() {
  const conn = new signalR.HubConnectionBuilder()
    .withUrl(HUB,{
      skipNegotiation:true,
      transport:signalR.HttpTransportType.WebSockets,
      accessTokenFactory:()=>token,
      timeout:10000
    })
    .withAutomaticReconnect()
    .build();

  conn.on("GatewayQuote",(contractId,d)=>{
    if(contractId!==contract.id)return;
    latestQuote=d;
  });

  conn.on("GatewayTrade",(contractId,d)=>{
    if(contractId!==contract.id)return;
    addTrade(d);
  });

  conn.on("GatewayDepth",(contractId,d)=>{
    if(contractId!==contract.id)return;
  });

  async function subscribe(){
    await conn.invoke("SubscribeContractQuotes",contract.id);
    await conn.invoke("SubscribeContractTrades",contract.id);
    try{await conn.invoke("SubscribeContractMarketDepth",contract.id);}catch(e){}
  }

  conn.onreconnecting(()=>connected=false);
  conn.onreconnected(async()=>{connected=true;await subscribe();});
  conn.onclose(()=>connected=false);

  await conn.start();
  connected=true;
  await subscribe();
}

async function init() {
  await authenticate();
  await findContract();
  await buildSnapshot();
  await connectStream();
  setInterval(()=>buildSnapshot().catch(e=>console.error("snapshot",e)),SNAPSHOT_INTERVAL_MS);
}

const app=express();
app.get("/health",(req,res)=>res.json({
  ok:true,
  connected,
  contract:contract?.name||null,
  lastTradeAt,
  lastSnapshotAt
}));
app.get("/mnq-snapshot.json",(req,res)=>res.json(latestSnapshot||{}));
app.get("/mnq-profile.json",(req,res)=>res.json({
  generatedUtc:new Date().toISOString(),
  contract:contract?.name||null,
  connected,
  latestQuote,
  profile:exactProfile()
}));
app.get("/mnq-morning.json",(req,res)=>res.json({
  generatedUtc:new Date().toISOString(),
  snapshot:latestSnapshot,
  realtime:{
    connected,
    lastTradeAt,
    profile:exactProfile()
  }
}));
app.get("/",(req,res)=>res.type("text").send("MNQ unified market-data service\n"));

app.listen(PORT,()=>console.log(`HTTP server on :${PORT}`));

init().catch(e=>{
  console.error(e);
  process.exit(1);
});
