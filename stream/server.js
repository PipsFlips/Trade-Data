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
const DATA_DIR = process.env.DATA_DIR || "./data";
const VALUE_AREA = 0.70;

if (!USERNAME || !API_KEY) throw new Error("Missing TOPSTEP_USERNAME / TOPSTEP_API_KEY");
fs.mkdirSync(DATA_DIR, {recursive:true});

let contract = null;
let latestQuote = null;
let connected = false;
let lastTradeAt = null;
let byPrice = new Map();
let buyVolume = 0;
let sellVolume = 0;
let tradeCount = 0;

async function post(url, payload, token) {
  const h={"Content-Type":"application/json","Accept":"text/plain"};
  if (token) h.Authorization=`Bearer ${token}`;
  const r=await fetch(API_BASE+url,{method:"POST",headers:h,body:JSON.stringify(payload)});
  if (!r.ok) throw new Error(`${url} ${r.status}: ${await r.text()}`);
  return await r.json();
}
async function auth(){
  const r=await post("/api/Auth/loginKey",{userName:USERNAME,apiKey:API_KEY});
  if(!r.success||!r.token) throw new Error(`Auth failed: ${r.errorCode} ${r.errorMessage||""}`);
  return r.token;
}
async function getContract(token){
  const r=await post("/api/Contract/search",{searchText:"MNQ",live:LIVE},token);
  const cs=(r.contracts||[]).filter(c=>JSON.stringify(c).toUpperCase().includes("MNQ"));
  return cs.find(c=>c.activeContract)||cs[0];
}
function sessionKey(ts){
  // Use UTC date in raw storage. Analytics payload includes timestamps and can be session-mapped downstream.
  return new Date(ts).toISOString().slice(0,10);
}
function append(file,obj){ fs.appendFileSync(path.join(DATA_DIR,file),JSON.stringify(obj)+"\n"); }
function addTrade(d, persist=true){
  const price=Number(d.price), vol=Number(d.volume||0), type=Number(d.type);
  if(!Number.isFinite(price)||!Number.isFinite(vol)||vol<=0)return;
  const k=price.toFixed(2);
  const x=byPrice.get(k)||{price,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};
  x.volume+=vol; x.trades+=1;
  if(type===0){x.buyVolume+=vol; x.delta+=vol; buyVolume+=vol;}
  else if(type===1){x.sellVolume+=vol; x.delta-=vol; sellVolume+=vol;}
  byPrice.set(k,x); tradeCount+=1; lastTradeAt=d.timestamp||new Date().toISOString();
  if(persist) append(`trades-${sessionKey(lastTradeAt)}.jsonl`,d);
}
function rebuildToday(){
  const key=new Date().toISOString().slice(0,10);
  const file=path.join(DATA_DIR,`trades-${key}.jsonl`);
  if(!fs.existsSync(file))return;
  for(const line of fs.readFileSync(file,"utf8").split("\n")){
    if(!line.trim())continue;
    try{addTrade(JSON.parse(line),false)}catch{}
  }
}
function profile(){
  const arr=[...byPrice.values()].sort((a,b)=>a.price-b.price);
  if(!arr.length)return null;
  const total=arr.reduce((s,x)=>s+x.volume,0);
  let poc=arr.reduce((a,b)=>b.volume>a.volume?b:a);
  const idx=arr.indexOf(poc), selected=new Set([idx]);
  let cum=poc.volume, lo=idx-1, hi=idx+1;
  while(cum<total*VALUE_AREA && (lo>=0||hi<arr.length)){
    const lv=lo>=0?arr[lo].volume:-1, hv=hi<arr.length?arr[hi].volume:-1;
    let i;
    if(hv>=lv && hi<arr.length)i=hi++;
    else i=lo--;
    if(i>=0 && i<arr.length && !selected.has(i)){selected.add(i);cum+=arr[i].volume;}
  }
  const sel=[...selected].map(i=>arr[i].price);
  const hvn=[...arr].sort((a,b)=>b.volume-a.volume).slice(0,8);
  const deltaByPrice=[...arr].sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta)).slice(0,12);
  return {
    method:"exact GatewayTrade volume-at-price",
    isTrueTradeByTradeProfile:true,
    valueAreaPercent:VALUE_AREA,
    totalVolume:total,
    poc:poc.price,
    vah:Math.max(...sel),
    val:Math.min(...sel),
    buyVolume,sellVolume,delta:buyVolume-sellVolume,
    cvdProxy:buyVolume-sellVolume,
    tradeCount,
    hvn:hvn,
    strongestDeltaPrices:deltaByPrice,
    priceLevels:arr
  };
}
function snapshot(){
  return {
    generatedUtc:new Date().toISOString(),
    connected,
    contract,
    latestQuote,
    lastTradeAt,
    profile:profile(),
    caveats:[
      "TradeLogType 0=Buy, 1=Sell per ProjectX documentation.",
      "CVD here is cumulative signed GatewayTrade volume since the collector's current data window/rebuild.",
      "DOM analytics require GatewayDepth entitlement and are collected separately."
    ]
  };
}
async function run(){
  const token=await auth();
  contract=await getContract(token);
  if(!contract)throw new Error("MNQ contract not found");
  rebuildToday();

  const conn=new signalR.HubConnectionBuilder()
    .withUrl(HUB,{
      skipNegotiation:true,
      transport:signalR.HttpTransportType.WebSockets,
      accessTokenFactory:()=>token,
      timeout:10000
    }).withAutomaticReconnect().build();

  conn.on("GatewayQuote",(contractId,d)=>{
    if(contractId!==contract.id)return;
    latestQuote=d;
    append(`quotes-${sessionKey(d.timestamp||Date.now())}.jsonl`,d);
  });
  conn.on("GatewayTrade",(contractId,d)=>{
    if(contractId!==contract.id)return;
    addTrade(d,true);
  });
  conn.on("GatewayDepth",(contractId,d)=>{
    if(contractId!==contract.id)return;
    append(`depth-${sessionKey(d.timestamp||Date.now())}.jsonl`,d);
  });
  const subscribe=async()=>{
    await conn.invoke("SubscribeContractQuotes",contract.id);
    await conn.invoke("SubscribeContractTrades",contract.id);
    try{ await conn.invoke("SubscribeContractMarketDepth",contract.id); }catch(e){ console.warn("Depth subscription unavailable:",e.message); }
  };
  conn.onreconnecting(()=>{connected=false});
  conn.onreconnected(async()=>{connected=true;await subscribe()});
  conn.onclose(()=>{connected=false});
  await conn.start(); connected=true; await subscribe();
  console.log(`Streaming ${contract.name} ${contract.id}`);
}
const app=express();
app.get("/health",(req,res)=>res.json({ok:true,connected,contract:contract?.name,lastTradeAt}));
app.get("/mnq-profile.json",(req,res)=>res.json(snapshot()));
app.get("/",(req,res)=>res.type("text").send("MNQ market-data collector\n"));
app.listen(PORT,()=>console.log(`HTTP on :${PORT}`));
run().catch(e=>{console.error(e);process.exit(1)});
