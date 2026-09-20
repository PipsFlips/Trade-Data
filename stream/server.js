const express = require("express");
const signalR = require("@microsoft/signalr");

const API_BASE = process.env.TOPSTEP_API_BASE || "https://api.topstepx.com";
const HUB = process.env.TOPSTEP_MARKET_HUB || "https://rtc.topstepx.com/hubs/market";
const USERNAME = process.env.TOPSTEP_USERNAME;
const API_KEY = process.env.TOPSTEP_API_KEY;
const LIVE = String(process.env.TOPSTEP_LIVE_DATA || "false").toLowerCase() === "true";
const PORT = Number(process.env.PORT || 8080);
const SNAPSHOT_INTERVAL_MS = 300000;

if (!USERNAME || !API_KEY) throw new Error("Missing TOPSTEP_USERNAME / TOPSTEP_API_KEY");

let token, contract, latestQuote = null, connected = false, lastTradeAt = null, lastSnapshotAt = null, latestSnapshot = null;
let byPrice = new Map(), buyVolume = 0, sellVolume = 0, tradeCount = 0;

async function post(path, payload, auth = token) {
  const headers = {"Content-Type":"application/json","Accept":"text/plain"};
  if (auth) headers.Authorization = `Bearer ${auth}`;
  const r = await fetch(API_BASE + path, {method:"POST", headers, body:JSON.stringify(payload)});
  if (!r.ok) throw new Error(`${path} HTTP ${r.status}: ${await r.text()}`);
  return r.json();
}
async function authenticate() {
  const r = await post("/api/Auth/loginKey", {userName:USERNAME, apiKey:API_KEY}, null);
  if (!r.success || !r.token) throw new Error(`Auth failed: ${r.errorCode} ${r.errorMessage || ""}`);
  token = r.token;
}
async function findContract() {
  const r = await post("/api/Contract/search", {searchText:"MNQ", live:LIVE});
  const cs = (r.contracts || []).filter(c => JSON.stringify(c).toUpperCase().includes("MNQ"));
  contract = cs.find(c => c.activeContract) || cs[0];
  if (!contract) throw new Error("MNQ contract not found");
}
async function bars(unit, unitNumber, days, limit=20000) {
  const end = new Date(), start = new Date(end.getTime() - days*86400000);
  const r = await post("/api/History/retrieveBars", {
    contractId:contract.id, live:LIVE, startTime:start.toISOString(), endTime:end.toISOString(),
    unit, unitNumber, limit, includePartialBar:true
  });
  const out = (r.bars || []).map(b => ({
    t:b.t ?? b.time ?? b.timestamp, o:b.o ?? b.open, h:b.h ?? b.high,
    l:b.l ?? b.low, c:b.c ?? b.close, v:b.v ?? b.volume
  }));
  out.sort((a,b)=>new Date(a.t)-new Date(b.t));
  return out;
}
function ptParts(d=new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone:"America/Los_Angeles", hour12:false,
    year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",weekday:"short"
  }).formatToParts(d);
  return Object.fromEntries(parts.filter(x=>x.type!=="literal").map(x=>[x.type,x.value]));
}
function zonedDate(y,m,d,h,min) {
  let guess = new Date(Date.UTC(y,m-1,d,h+8,min));
  for (let i=0;i<3;i++) {
    const p=ptParts(guess);
    const got=Date.UTC(+p.year,+p.month-1,+p.day,+p.hour,+p.minute);
    const want=Date.UTC(y,m-1,d,h,min);
    guess=new Date(guess.getTime()+(want-got));
  }
  return guess;
}
function addDays(y,m,d,n){
  const x=new Date(Date.UTC(y,m-1,d)+n*86400000);
  return {y:x.getUTCFullYear(),m:x.getUTCMonth()+1,d:x.getUTCDate()};
}
function summary(rows){
  if(!rows.length)return null;
  return {
    open:+rows[0].o, high:Math.max(...rows.map(x=>+x.h)), low:Math.min(...rows.map(x=>+x.l)),
    close:+rows.at(-1).c, volume:rows.reduce((s,x)=>s+(+x.v||0),0),
    start:rows[0].t, end:rows.at(-1).t, bars:rows.length
  };
}
function between(rows,s,e){ return rows.filter(b=>{const t=new Date(b.t);return t>=s&&t<e;}); }
function vwap(rows){
  let n=0,d=0;
  for(const b of rows){const vol=+b.v||0;if(vol<=0)continue;const tp=(+b.h + +b.l + +b.c)/3;n+=tp*vol;d+=vol;}
  return d?n/d:null;
}
function roundTick(x,t){return Math.round(x/t)*t;}
function estProfile(rows,tick){
  const buckets=new Map();
  for(const b of rows){
    const vol=+b.v||0;if(vol<=0)continue;
    const lo=roundTick(+b.l,tick), hi=roundTick(+b.h,tick);
    const cnt=Math.max(1,Math.round((hi-lo)/tick)+1), per=vol/cnt;
    for(let i=0;i<cnt;i++){const p=+(lo+i*tick).toFixed(10);buckets.set(p,(buckets.get(p)||0)+per);}
  }
  if(!buckets.size)return null;
  const arr=[...buckets.entries()].sort((a,b)=>a[0]-b[0]).map(([price,volume])=>({price,volume}));
  const poc=arr.reduce((a,b)=>b.volume>a.volume?b:a), total=arr.reduce((s,x)=>s+x.volume,0), target=total*.7;
  let idx=arr.indexOf(poc),lo=idx-1,hi=idx+1,cum=poc.volume;const sel=new Set([idx]);
  while(cum<target&&(lo>=0||hi<arr.length)){const lv=lo>=0?arr[lo].volume:-1,hv=hi<arr.length?arr[hi].volume:-1;let i;if(hv>=lv&&hi<arr.length)i=hi++;else i=lo--;if(i>=0&&i<arr.length&&!sel.has(i)){sel.add(i);cum+=arr[i].volume;}}
  const ps=[...sel].map(i=>arr[i].price);
  return {method:"estimated from 1m OHLCV",isTrueTradeByTradeProfile:false,poc:poc.price,vah:Math.max(...ps),val:Math.min(...ps)};
}
function atr(day,n=14){
  if(day.length<n+1)return null;
  const tr=[]; for(let i=1;i<day.length;i++){const p=+day[i-1].c,b=day[i];tr.push(Math.max(+b.h-+b.l,Math.abs(+b.h-p),Math.abs(+b.l-p)));}
  const a=tr.slice(-n); return a.reduce((s,x)=>s+x,0)/a.length;
}
function rangeStats(day,n=20){
  const a=day.slice(-n).map(b=>+b.h-+b.l).sort((a,b)=>a-b); if(!a.length)return null;
  return {sampleDays:a.length,averageRange:a.reduce((s,x)=>s+x,0)/a.length,medianRange:a[Math.floor(a.length/2)],minRange:a[0],maxRange:a.at(-1)};
}
function previousRTH(rows){
  const p=ptParts(); let cur=addDays(+p.year,+p.month,+p.day,-1);
  for(let i=0;i<8;i++){const s=zonedDate(cur.y,cur.m,cur.d,6,30),e=zonedDate(cur.y,cur.m,cur.d,13,0),x=between(rows,s,e);if(x.length){const z=summary(x);z.date=`${cur.y}-${String(cur.m).padStart(2,"0")}-${String(cur.d).padStart(2,"0")}`;return z;}cur=addDays(cur.y,cur.m,cur.d,-1);}
  return null;
}
function currentSessions(rows){
  const p=ptParts(), y=+p.year,m=+p.month,d=+p.day, prev=addDays(y,m,d,-1);
  const overnight=between(rows,zonedDate(prev.y,prev.m,prev.d,15,0),new Date());
  const asia=between(rows,zonedDate(prev.y,prev.m,prev.d,17,0),zonedDate(y,m,d,0,0));
  const london=between(rows,zonedDate(y,m,d,0,0),zonedDate(y,m,d,5,20));
  return {overnight:summary(overnight),asia:summary(asia),london:summary(london),overnightRows:overnight};
}
function previousGlobex(rows){
  const p=ptParts(); let cur=addDays(+p.year,+p.month,+p.day,-1);
  for(let i=0;i<8;i++){const prev=addDays(cur.y,cur.m,cur.d,-1),x=between(rows,zonedDate(prev.y,prev.m,prev.d,15,0),zonedDate(cur.y,cur.m,cur.d,14,0));if(x.length){const z=summary(x);z.sessionDate=`${cur.y}-${String(cur.m).padStart(2,"0")}-${String(cur.d).padStart(2,"0")}`;return z;}cur=addDays(cur.y,cur.m,cur.d,-1);}
  return null;
}
function previousCMEWeek(rows){
  const p=ptParts(); const map={Sun:0,Mon:1,Tue:2,Wed:3,Thu:4,Fri:5,Sat:6}; const dow=map[p.weekday];
  const monday=addDays(+p.year,+p.month,+p.day,-((dow+6)%7));
  const sun=addDays(monday.y,monday.m,monday.d,-1), fri=addDays(monday.y,monday.m,monday.d,4);
  const x=between(rows,zonedDate(sun.y,sun.m,sun.d,15,0),zonedDate(fri.y,fri.m,fri.d,14,0)); return summary(x);
}
function pivots(hour,current,atr14){
  const raw=[], ref=atr14||300;
  for(let i=2;i<hour.length-2;i++){
    const b=hour[i],h=+b.h,l=+b.l,hi=hour.slice(i-2,i).every(x=>h>+x.h)&&hour.slice(i+1,i+3).every(x=>h>=+x.h),lo=hour.slice(i-2,i).every(x=>l<+x.l)&&hour.slice(i+1,i+3).every(x=>l<=+x.l);
    if(!hi&&!lo)continue;
    const type=hi?"high":"low",price=hi?h:l,later=hour.slice(i+3);let swept=false,exc=0;
    for(const x of later){if(type==="high"){if(+x.h>price)swept=true;exc=Math.max(exc,price-+x.l);}else{if(+x.l<price)swept=true;exc=Math.max(exc,+x.h-price);}}
    const dist=Math.abs(current-price),age=Math.max(0,(Date.now()-new Date(b.t))/3600000);
    const score=(!swept?45:0)+Math.min(25,(exc/ref)*20)+Math.max(0,15-(dist/ref)*5)+Math.max(0,15-age/48);
    raw.push({type,price,time:b.t,sweptLater:swept,liquidityStatus:swept?"swept":"untouched",distanceFromCurrentPoints:+dist.toFixed(2),significanceScore:+score.toFixed(1)});
  }
  return raw.sort((a,b)=>b.significanceScore-a.significanceScore).slice(0,8);
}
function exactProfile(){
  const arr=[...byPrice.values()].sort((a,b)=>a.price-b.price);if(!arr.length)return null;
  const total=arr.reduce((s,x)=>s+x.volume,0),poc=arr.reduce((a,b)=>b.volume>a.volume?b:a);let idx=arr.indexOf(poc),lo=idx-1,hi=idx+1,cum=poc.volume;const sel=new Set([idx]);
  while(cum<total*.7&&(lo>=0||hi<arr.length)){const lv=lo>=0?arr[lo].volume:-1,hv=hi<arr.length?arr[hi].volume:-1;let i;if(hv>=lv&&hi<arr.length)i=hi++;else i=lo--;if(i>=0&&i<arr.length&&!sel.has(i)){sel.add(i);cum+=arr[i].volume;}}
  const ps=[...sel].map(i=>arr[i].price);
  return {method:"exact GatewayTrade volume-at-price",isTrueTradeByTradeProfile:true,totalVolume:total,buyVolume,sellVolume,delta:buyVolume-sellVolume,cvd:buyVolume-sellVolume,tradeCount,poc:poc.price,vah:Math.max(...ps),val:Math.min(...ps),strongestVolumeNodes:[...arr].sort((a,b)=>b.volume-a.volume).slice(0,10),strongestDeltaPrices:[...arr].sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta)).slice(0,15)};
}
function addTrade(d){const price=+d.price,vol=+d.volume||0,type=+d.type;if(!Number.isFinite(price)||!Number.isFinite(vol)||vol<=0)return;const k=price.toFixed(2),x=byPrice.get(k)||{price,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};x.volume+=vol;x.trades++;if(type===0){x.buyVolume+=vol;x.delta+=vol;buyVolume+=vol;}else if(type===1){x.sellVolume+=vol;x.delta-=vol;sellVolume+=vol;}byPrice.set(k,x);tradeCount++;lastTradeAt=d.timestamp||new Date().toISOString();}

async function buildSnapshot(){
  const [one,five,hour,day,week]=await Promise.all([bars(2,1,10,15000),bars(2,5,45,15000),bars(3,1,120,5000),bars(4,1,500,1200),bars(5,1,1800,600)]);
  const tick=+contract.tickSize||0.25, current=+five.at(-1).c, a14=atr(day,14), sessions=currentSessions(five), prevR=previousRTH(five);
  let prevProfile=null, prev1=previousRTH(one);
  if(prev1){const [Y,M,D]=prev1.date.split("-").map(Number);prevProfile=estProfile(between(one,zonedDate(Y,M,D,6,30),zonedDate(Y,M,D,13,0)),tick);}
  latestSnapshot={
    schemaVersion:3.1,generatedUtc:new Date().toISOString(),source:"TopstepX / ProjectX CME market data",
    contract:{id:contract.id,name:contract.name,description:contract.description,symbolId:contract.symbolId,tickSize:contract.tickSize,tickValue:contract.tickValue,activeContract:contract.activeContract},
    latest:{bar1m:one.at(-1)||null,bar5m:five.at(-1)||null,bar1h:hour.at(-1)||null,bar1d:day.at(-1)||null,quote:latestQuote},
    analytics:{
      previousRTH:prevR,previousFullGlobexSession:previousGlobex(five),previousCMETradingWeek:previousCMEWeek(five),
      asia:sessions.asia,london:sessions.london,currentOvernight:sessions.overnight,
      vwap:{globex:vwap(sessions.overnightRows)},profiles:{previousRTH_estimated:prevProfile,currentOvernight_estimated:estProfile(sessions.overnightRows,tick),realtime_exact:exactProfile()},
      oneHourPivots:pivots(hour,current,a14),volatility:{ATR14Daily:a14,dailyRange20:rangeStats(day,20)},previousSettlementProxy:prevR?.close??null
    },
    barCounts:{oneMin:one.length,fiveMin:five.length,oneHour:hour.length,daily:day.length,weekly:week.length},
    bars:{oneMinRecent:one.slice(-3000),fiveMinRecent:five.slice(-3000),oneHourRecent:hour.slice(-1000),dailyRecent:day.slice(-400),weeklyRecent:week.slice(-200)}
  };
  lastSnapshotAt=latestSnapshot.generatedUtc;
}
async function connectStream(){
  const conn=new signalR.HubConnectionBuilder().withUrl(HUB,{skipNegotiation:true,transport:signalR.HttpTransportType.WebSockets,accessTokenFactory:()=>token,timeout:10000}).withAutomaticReconnect().build();
  conn.on("GatewayQuote",(id,d)=>{if(id===contract.id)latestQuote=d;});
  conn.on("GatewayTrade",(id,d)=>{if(id===contract.id)addTrade(d);});
  const sub=async()=>{await conn.invoke("SubscribeContractQuotes",contract.id);await conn.invoke("SubscribeContractTrades",contract.id);try{await conn.invoke("SubscribeContractMarketDepth",contract.id);}catch{}};
  conn.onreconnecting(()=>connected=false);conn.onreconnected(async()=>{connected=true;await sub();});conn.onclose(()=>connected=false);
  await conn.start();connected=true;await sub();
}
async function init(){await authenticate();await findContract();await buildSnapshot();await connectStream();setInterval(()=>buildSnapshot().catch(console.error),SNAPSHOT_INTERVAL_MS);}
const app=express();
app.get("/health",(req,res)=>res.json({ok:true,connected,contract:contract?.name||null,lastTradeAt,lastSnapshotAt}));
app.get("/mnq-snapshot.json",(req,res)=>res.json(latestSnapshot||{}));
app.get("/mnq-profile.json",(req,res)=>res.json({generatedUtc:new Date().toISOString(),contract:contract?.name||null,connected,latestQuote,profile:exactProfile()}));
app.get("/mnq-morning.json",(req,res)=>res.json({generatedUtc:new Date().toISOString(),snapshot:latestSnapshot,realtime:{connected,lastTradeAt,profile:exactProfile()}}));
app.get("/",(req,res)=>res.type("text").send("MNQ unified market-data service v3.1\n"));
app.listen(PORT,()=>console.log(`HTTP on :${PORT}`));
init().catch(e=>{console.error(e);process.exit(1);});
