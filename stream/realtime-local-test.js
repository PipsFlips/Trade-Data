const signalR = require("@microsoft/signalr");
const { DateTime } = require("luxon");
const fs = require("fs");
const path = require("path");
const os = require("os");

const API_BASE = process.env.TOPSTEP_API_BASE || "https://api.topstepx.com";
const HUB = process.env.TOPSTEP_MARKET_HUB || "https://rtc.topstepx.com/hubs/market";
const USERNAME = process.env.TOPSTEP_USERNAME;
const API_KEY = process.env.TOPSTEP_API_KEY;
const LIVE = String(process.env.TOPSTEP_LIVE_DATA || "false").toLowerCase()==="true";
const RELAY_URL = process.env.RELAY_URL || "https://trade-data-production.up.railway.app/realtime-relay";
const RELAY_TOKEN = process.env.REALTIME_RELAY_TOKEN;
const ZONE="America/Los_Angeles";
const LEGACY_STATE_FILE=path.join(__dirname,"collector-state.json");
const STATE_FILE=process.env.COLLECTOR_STATE_FILE||path.join(os.homedir(),".mnq-collector-state.json");
const STATE_SAVE_MS=30000;
const TOKEN_REFRESH_MS=18*60*60*1000;

if(!USERNAME||!API_KEY||!RELAY_TOKEN){console.error("Set TOPSTEP_USERNAME, TOPSTEP_API_KEY, REALTIME_RELAY_TOKEN");process.exit(1);}

let authToken=null,authIssuedAt=0;
async function authenticate(){
  const auth=await post("/api/Auth/loginKey",{userName:USERNAME,apiKey:API_KEY});
  if(!auth.success||!auth.token) throw new Error("Auth failed");
  authToken=auth.token;
  authIssuedAt=Date.now();
  return authToken;
}
async function getToken(){
  if(!authToken||Date.now()-authIssuedAt>TOKEN_REFRESH_MS) await authenticate();
  return authToken;
}
async function post(path,payload,token){
  const r=await fetch(API_BASE+path,{method:"POST",headers:{"Content-Type":"application/json","Accept":"text/plain",...(token?{"Authorization":"Bearer "+token}:{})},body:JSON.stringify(payload)});
  if(!r.ok) throw new Error(path+" "+r.status+" "+await r.text());
  return r.json();
}
function bucketStartIso(ts,minutes){
  const ms=Date.parse(ts),bm=minutes*60000;
  return new Date(Math.floor(ms/bm)*bm).toISOString();
}
const oneMin={},fiveMin={};
let sessionProfile={},rthProfile={};
let latestPrice=null,latestQuote=null;
let sessionBuy=0,sessionSell=0,rthBuy=0,rthSell=0;
let sessionPV=0,sessionVol=0,rthPV=0,rthVol=0;
let sessionKey=null,rthDate=null,lastTradeTs=null;
let lastTradeReceivedAt=null,lastQuoteReceivedAt=null,lastDepthReceivedAt=null;
let lastRecoveryAt=0;
let collectorStartedAt=new Date().toISOString();
let recoveryCount=0;
const icebergTape=new Map();
const depthBook={ask:new Map(),bid:new Map()};
function pxKey(p){return (Math.round((+p)*4)/4).toFixed(2);}
function pruneIcebergTape(){
  const cutoff=Date.now()-30000;
  for(const [k,x] of icebergTape) if((x.lastAt||0)<cutoff) icebergTape.delete(k);
}
function recordIcebergTrade(d){
  const p=+d.price,v=+d.volume||0,t=+d.type;
  if(!Number.isFinite(p)||v<=0||(t!==0&&t!==1)) return;
  const k=pxKey(p);
  const now=Date.now();
  let x=icebergTape.get(k)||{price:+k,buyVol:0,sellVol:0,buyTrades:0,sellTrades:0,askRefresh:0,bidRefresh:0,firstAt:now,lastAt:now};
  if(now-x.firstAt>30000){x={price:+k,buyVol:0,sellVol:0,buyTrades:0,sellTrades:0,askRefresh:0,bidRefresh:0,firstAt:now,lastAt:now};}
  if(t===0){x.buyVol+=v;x.buyTrades++;}else{x.sellVol+=v;x.sellTrades++;}
  x.lastAt=now;icebergTape.set(k,x);pruneIcebergTape();
}
function recordDepth(row){
  const p=+row?.price;
  if(!Number.isFinite(p)) return;
  const type=+row.type;
  const side=(type===1||type===3||type===10)?"ask":(type===2||type===4||type===9)?"bid":null;
  if(!side) return;
  const cur=Number.isFinite(+row.currentVolume)?+row.currentVolume:(Number.isFinite(+row.volume)?+row.volume:null);
  if(!Number.isFinite(cur)) return;
  const k=pxKey(p),book=depthBook[side],prev=book.get(k);
  book.set(k,{currentVolume:cur,at:Date.now()});
  if(prev && cur>prev.currentVolume){
    const x=icebergTape.get(k);
    if(x && Date.now()-x.lastAt<=15000){
      if(side==="ask") x.askRefresh++;
      else x.bidRefresh++;
      icebergTape.set(k,x);
    }
  }
}
function detectedIcebergs(){
  pruneIcebergTape();
  const now=Date.now(),out=[];
  for(const x of icebergTape.values()){
    const age=(now-x.lastAt)/1000;
    // Passive seller absorbing aggressive buys = bearish candidate.
    if(x.buyVol>=12 && x.buyTrades>=4 && x.askRefresh>=2){
      const score=Math.min(100,35+Math.min(30,x.buyVol)+Math.min(20,x.askRefresh*5)+Math.min(15,x.buyTrades*2));
      out.push({side:"SELL",type:"sell-iceberg",price:x.price,score,aggressorVolume:x.buyVol,refreshes:x.askRefresh,trades:x.buyTrades,ageSec:+age.toFixed(1)});
    }
    // Passive buyer absorbing aggressive sells = bullish candidate.
    if(x.sellVol>=12 && x.sellTrades>=4 && x.bidRefresh>=2){
      const score=Math.min(100,35+Math.min(30,x.sellVol)+Math.min(20,x.bidRefresh*5)+Math.min(15,x.sellTrades*2));
      out.push({side:"BUY",type:"buy-iceberg",price:x.price,score,aggressorVolume:x.sellVol,refreshes:x.bidRefresh,trades:x.sellTrades,ageSec:+age.toFixed(1)});
    }
  }
  return out.sort((a,b)=>b.score-a.score||a.ageSec-b.ageSec).slice(0,4);
}
function pruneFlowStores(){
  const now=Date.now();
  const cut1=now-8*60*60*1000;
  const cut5=now-18*60*60*1000;
  for(const k of Object.keys(oneMin)) if(Date.parse(oneMin[k]?.t||k)<cut1) delete oneMin[k];
  for(const k of Object.keys(fiveMin)) if(Date.parse(fiveMin[k]?.t||k)<cut5) delete fiveMin[k];
}
function pruneDepthBook(){
  const cut=Date.now()-60000;
  for(const book of [depthBook.ask,depthBook.bid]){
    for(const [k,x] of book) if((x?.at||0)<cut) book.delete(k);
  }
}
function saveState(){
  try{
    pruneFlowStores();
    pruneDepthBook();
    const tmp=STATE_FILE+".tmp";
    fs.writeFileSync(tmp,JSON.stringify({
      savedAt:new Date().toISOString(),sessionKey,rthDate,
      sessionBuy,sessionSell,rthBuy,rthSell,sessionPV,sessionVol,rthPV,rthVol,
      sessionProfile,rthProfile,oneMin,fiveMin,lastTradeTs,latestPrice
    }));
    fs.renameSync(tmp,STATE_FILE);
  }catch(e){console.error("STATE_SAVE_ERR",e.message);}
}
function loadState(){
  try{
    let source=STATE_FILE;
    if(!fs.existsSync(source) && fs.existsSync(LEGACY_STATE_FILE)) source=LEGACY_STATE_FILE;
    if(!fs.existsSync(source)) return;
    const x=JSON.parse(fs.readFileSync(source,"utf8"));
    const now=DateTime.now().setZone(ZONE);
    const expectedSession=(now.hour>=15?now.plus({days:1}):now).toISODate();
    const expectedRth=now.toISODate();

    if(x.sessionKey===expectedSession){
      sessionKey=x.sessionKey;
      sessionBuy=+x.sessionBuy||0;sessionSell=+x.sessionSell||0;
      sessionPV=+x.sessionPV||0;sessionVol=+x.sessionVol||0;
      sessionProfile=x.sessionProfile||{};
      Object.assign(oneMin,x.oneMin||{});
      Object.assign(fiveMin,x.fiveMin||{});
      lastTradeTs=x.lastTradeTs||null;
      latestPrice=Number.isFinite(+x.latestPrice)?+x.latestPrice:null;
    }
    if(x.rthDate===expectedRth){
      rthDate=x.rthDate;
      rthBuy=+x.rthBuy||0;rthSell=+x.rthSell||0;
      rthPV=+x.rthPV||0;rthVol=+x.rthVol||0;
      rthProfile=x.rthProfile||{};
    }
    console.log("STATE_LOADED",{source,session:sessionKey,rth:rthDate,lastTrade:lastTradeTs});
    if(source===LEGACY_STATE_FILE && STATE_FILE!==LEGACY_STATE_FILE){
      try{saveState();console.log("STATE_MIGRATED",STATE_FILE);}catch{}
    }
  }catch(e){console.error("STATE_LOAD_ERR",e.message);}
}
function pt(ts){return DateTime.fromISO(ts,{setZone:true}).setZone(ZONE);}
function currentSessionKey(d){return (d.hour>=15?d.plus({days:1}):d).toISODate();}
function ensureSessions(ts){
  const d=pt(ts),sk=currentSessionKey(d),rd=d.toISODate(),m=d.hour*60+d.minute;
  if(sk!==sessionKey){sessionKey=sk;sessionBuy=sessionSell=sessionPV=sessionVol=0;sessionProfile={};}
  // NY/RTH state begins at 06:30 PT. Do not roll it at midnight.
  // The first trade in the new RTH window performs the reset.
  if(m>=390&&m<780&&rd!==rthDate){
    rthDate=rd;rthBuy=rthSell=rthPV=rthVol=0;rthProfile={};
  }
}
function updateBucket(store,ts,d,mins){
  const k=bucketStartIso(ts,mins),p=+d.price,v=+d.volume||0,t=+d.type;
  if(!Number.isFinite(p)||v<=0)return;
  let b=store[k]||{t:k,o:p,h:p,l:p,c:p,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};
  b.h=Math.max(b.h,p);b.l=Math.min(b.l,p);b.c=p;b.volume+=v;b.trades++;
  if(t===0){b.buyVolume+=v;b.delta+=v;} else if(t===1){b.sellVolume+=v;b.delta-=v;}
  store[k]=b;
}
function updateProfile(store,p,v,t){
  const k=(Math.round(p*4)/4).toFixed(2);
  const x=store[k]||{price:+k,volume:0,buyVolume:0,sellVolume:0,delta:0};
  x.volume+=v;
  if(t===0){x.buyVolume+=v;x.delta+=v;}
  else if(t===1){x.sellVolume+=v;x.delta-=v;}
  store[k]=x;
}
function finalizeProfile(store){
  const arr=Object.values(store||{}).sort((a,b)=>a.price-b.price);
  if(!arr.length) return null;
  const total=arr.reduce((s,x)=>s+x.volume,0);
  const poc=arr.reduce((a,b)=>b.volume>a.volume?b:a);
  const idx=arr.indexOf(poc),selected=new Set([idx]);
  let lo=idx-1,hi=idx+1,cum=poc.volume;
  while(cum<total*.70&&(lo>=0||hi<arr.length)){
    const lv=lo>=0?arr[lo].volume:-1,hv=hi<arr.length?arr[hi].volume:-1;
    const i=(hv>=lv&&hi<arr.length)?hi++:lo--;
    if(i>=0&&i<arr.length&&!selected.has(i)){selected.add(i);cum+=arr[i].volume;}
  }
  const ps=[...selected].map(i=>arr[i].price);
  return {
    method:"exact local GatewayTrade volume-at-price",
    totalVolume:total,poc:poc.price,vah:Math.max(...ps),val:Math.min(...ps),
    strongestVolumeNodes:[...arr].sort((a,b)=>b.volume-a.volume).slice(0,12),
    strongestDeltaPrices:[...arr].sort((a,b)=>Math.abs(b.delta)-Math.abs(a.delta)).slice(0,12)
  };
}

function addTrade(d){
  const ts=d.timestamp||new Date().toISOString(),p=+d.price,v=+d.volume||0,t=+d.type;
  if(!Number.isFinite(p)||v<=0)return;
  ensureSessions(ts);
  latestPrice=p;lastTradeTs=ts;recordIcebergTrade(d);
  updateBucket(oneMin,ts,d,1);updateBucket(fiveMin,ts,d,5);
  if(t===0)sessionBuy+=v; else if(t===1)sessionSell+=v;
  sessionPV+=p*v;sessionVol+=v;updateProfile(sessionProfile,p,v,t);
  const x=pt(ts),m=x.hour*60+x.minute;
  if(m>=390&&m<780){if(t===0)rthBuy+=v;else if(t===1)rthSell+=v;rthPV+=p*v;rthVol+=v;updateProfile(rthProfile,p,v,t);}
}
function arr(store,minutes){
  const cut=Date.now()-minutes*60000;
  return Object.values(store).filter(x=>Date.parse(x.t)>=cut).sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
}
async function sendRelay(){
  const nowPt=DateTime.now().setZone(ZONE),nowMins=nowPt.hour*60+nowPt.minute;
  const rthActive=nowMins>=390&&nowMins<780&&rthDate===nowPt.toISODate();
  const body={
    receivedAt:new Date().toISOString(),
    lastTradeAt:lastTradeTs,
    lastTradeReceivedAt,
    lastQuoteReceivedAt,
    lastDepthReceivedAt,
    currentPrice:latestPrice,
    quote:latestQuote,
    oneMin:arr(oneMin,360),
    fiveMin:arr(fiveMin,720),
    currentGlobexDelta:sessionBuy-sessionSell,
    currentRthDelta:rthActive?(rthBuy-rthSell):null,
    currentGlobexCvd:sessionBuy-sessionSell,
    currentRthCvd:rthActive?(rthBuy-rthSell):null,
    sessionVwap:sessionVol?sessionPV/sessionVol:null,
    rthVwap:rthActive&&rthVol?rthPV/rthVol:null,
    rthActive,
    profiles:{
      session:finalizeProfile(sessionProfile),
      rth:rthActive?finalizeProfile(rthProfile):null
    },
    collector:{
      startedAt:collectorStartedAt,
      recoveryCount
    },
    icebergs:detectedIcebergs()
  };
  const r=await fetch(RELAY_URL,{method:"POST",headers:{"Content-Type":"application/json","Authorization":"Bearer "+RELAY_TOKEN},body:JSON.stringify(body)});
  if(!r.ok) throw new Error("relay "+r.status+" "+await r.text());
}
(async()=>{
  loadState();
  await authenticate();
  const found=await post("/api/Contract/search",{searchText:"MNQ",live:LIVE},authToken);
  const c=(found.contracts||[]).find(x=>x.activeContract)||found.contracts?.[0];
  if(!c) throw new Error("MNQ not found");
  console.log("CONTRACT",c.id,c.name,c.symbolId,"live="+LIVE);

  let q=0,t=0,d=0;
  const conn=new signalR.HubConnectionBuilder()
    .withUrl(HUB,{skipNegotiation:true,transport:signalR.HttpTransportType.WebSockets,accessTokenFactory:async()=>await getToken()})
    .withAutomaticReconnect()
    .build();

  conn.on("GatewayQuote",(id,x)=>{
    const rows=Array.isArray(x)?x:[x];
    q+=rows.length;
    lastQuoteReceivedAt=new Date().toISOString();
    for(const row of rows){
      latestQuote=row;
      if(Number.isFinite(+(row?.lastPrice??row?.price))) latestPrice=+(row.lastPrice??row.price);
    }
  });
  conn.on("GatewayTrade",(id,x)=>{
    const rows=Array.isArray(x)?x:[x];
    t+=rows.length;
    lastTradeReceivedAt=new Date().toISOString();
    for(const row of rows) addTrade(row);
  });
  conn.on("GatewayDepth",(id,x)=>{
    const rows=Array.isArray(x)?x:[x];
    d+=rows.length;
    lastDepthReceivedAt=new Date().toISOString();
    for(const row of rows) recordDepth(row);
  });

  conn.onreconnected(async()=>{
    recoveryCount++;
    console.log("AUTO_RECONNECTED",conn.connectionId);
    try{
      await conn.invoke("SubscribeContractQuotes",c.id);
      await conn.invoke("SubscribeContractTrades",c.id);
      try{await conn.invoke("SubscribeContractMarketDepth",c.id);}catch{}
    }catch(e){console.error("AUTO_RESUB_ERR",e.message);}
  });

  await conn.start();
  console.log("CONNECTED",conn.connectionId);
  console.log("QUOTE_SUB",await conn.invoke("SubscribeContractQuotes",c.id));
  console.log("TRADE_SUB",await conn.invoke("SubscribeContractTrades",c.id));
  try{console.log("DEPTH_SUB",await conn.invoke("SubscribeContractMarketDepth",c.id));}catch(e){console.log("DEPTH_SUB_ERR",e.message);}

  async function recoverRealtime(reason){
    if(Date.now()-lastRecoveryAt<30000) return;
    lastRecoveryAt=Date.now();
    recoveryCount++;
    console.log("WATCHDOG_RECOVERY",reason,new Date().toISOString());
    try{
      if(conn.state===signalR.HubConnectionState.Connected){
        // First try the least disruptive recovery: refresh subscriptions on the existing token/socket.
        console.log("WATCHDOG_TRADE_SUB",await conn.invoke("SubscribeContractTrades",c.id));
        console.log("WATCHDOG_QUOTE_SUB",await conn.invoke("SubscribeContractQuotes",c.id));
        try{console.log("WATCHDOG_DEPTH_SUB",await conn.invoke("SubscribeContractMarketDepth",c.id));}catch(e){console.log("WATCHDOG_DEPTH_ERR",e.message);}
        return;
      }
    }catch(e){
      console.log("WATCHDOG_RESUB_ERR",e.message);
    }
    try{
      if(conn.state!==signalR.HubConnectionState.Disconnected) await conn.stop();
    }catch{}
    try{
      await getToken();
      await conn.start();
      console.log("WATCHDOG_RECONNECTED",conn.connectionId);
      console.log("WATCHDOG_QUOTE_SUB",await conn.invoke("SubscribeContractQuotes",c.id));
      console.log("WATCHDOG_TRADE_SUB",await conn.invoke("SubscribeContractTrades",c.id));
      try{console.log("WATCHDOG_DEPTH_SUB",await conn.invoke("SubscribeContractMarketDepth",c.id));}catch(e){console.log("WATCHDOG_DEPTH_ERR",e.message);}
    }catch(e){
      console.error("WATCHDOG_RECONNECT_ERR",e.message);
    }
  }

  // Detect silent subscriptions: a websocket can remain "connected" while one or more
  // ProjectX event streams stop delivering. Re-subscribe before doing a full reconnect.
  setInterval(()=>{
    const now=Date.now();
    const tradeAge=lastTradeReceivedAt?now-Date.parse(lastTradeReceivedAt):Infinity;
    const quoteAge=lastQuoteReceivedAt?now-Date.parse(lastQuoteReceivedAt):Infinity;
    const depthAge=lastDepthReceivedAt?now-Date.parse(lastDepthReceivedAt):Infinity;
    const anyLive=Math.min(quoteAge,depthAge)<8000;
    if(tradeAge>15000 && anyLive) recoverRealtime("trade stream stale "+Math.round(tradeAge/1000)+"s").catch(()=>{});
    else if(Math.min(tradeAge,quoteAge,depthAge)>20000) recoverRealtime("all realtime streams stale").catch(()=>{});
  },5000);

  setInterval(()=>sendRelay().catch(e=>console.error("RELAY_ERR",e.message)),2000);
  setInterval(saveState,STATE_SAVE_MS);
  setInterval(()=>{pruneFlowStores();pruneDepthBook();},5*60*1000);
  setInterval(()=>console.log("COUNTS",{quotes:q,trades:t,depth:d,price:latestPrice,sessionCvd:sessionBuy-sessionSell,rthCvd:rthBuy-rthSell,tradeAgeSec:lastTradeReceivedAt?Math.round((Date.now()-Date.parse(lastTradeReceivedAt))/1000):null,recoveries:recoveryCount}),60000);
  process.on("SIGTERM",()=>{saveState();process.exit(0);});
  process.on("SIGINT",()=>{saveState();process.exit(0);});
})().catch(e=>{console.error(e);process.exit(1);});
