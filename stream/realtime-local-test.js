const signalR = require("@microsoft/signalr");
const { DateTime } = require("luxon");

const API_BASE = process.env.TOPSTEP_API_BASE || "https://api.topstepx.com";
const HUB = process.env.TOPSTEP_MARKET_HUB || "https://rtc.topstepx.com/hubs/market";
const USERNAME = process.env.TOPSTEP_USERNAME;
const API_KEY = process.env.TOPSTEP_API_KEY;
const LIVE = String(process.env.TOPSTEP_LIVE_DATA || "false").toLowerCase()==="true";
const RELAY_URL = process.env.RELAY_URL || "https://trade-data-production.up.railway.app/realtime-relay";
const RELAY_TOKEN = process.env.REALTIME_RELAY_TOKEN;
const ZONE="America/Los_Angeles";

if(!USERNAME||!API_KEY||!RELAY_TOKEN){console.error("Set TOPSTEP_USERNAME, TOPSTEP_API_KEY, REALTIME_RELAY_TOKEN");process.exit(1);}

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
let latestPrice=null,latestQuote=null;
let sessionBuy=0,sessionSell=0,rthBuy=0,rthSell=0;
let sessionPV=0,sessionVol=0,rthPV=0,rthVol=0;
let sessionKey=null,rthDate=null,lastTradeTs=null;
function pt(ts){return DateTime.fromISO(ts,{setZone:true}).setZone(ZONE);}
function currentSessionKey(d){return (d.hour>=15?d.plus({days:1}):d).toISODate();}
function ensureSessions(ts){
  const d=pt(ts),sk=currentSessionKey(d),rd=d.toISODate();
  if(sk!==sessionKey){sessionKey=sk;sessionBuy=sessionSell=sessionPV=sessionVol=0;}
  if(rd!==rthDate){rthDate=rd;rthBuy=rthSell=rthPV=rthVol=0;}
}
function updateBucket(store,ts,d,mins){
  const k=bucketStartIso(ts,mins),p=+d.price,v=+d.volume||0,t=+d.type;
  if(!Number.isFinite(p)||v<=0)return;
  let b=store[k]||{t:k,o:p,h:p,l:p,c:p,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};
  b.h=Math.max(b.h,p);b.l=Math.min(b.l,p);b.c=p;b.volume+=v;b.trades++;
  if(t===0){b.buyVolume+=v;b.delta+=v;} else if(t===1){b.sellVolume+=v;b.delta-=v;}
  store[k]=b;
}
function addTrade(d){
  const ts=d.timestamp||new Date().toISOString(),p=+d.price,v=+d.volume||0,t=+d.type;
  if(!Number.isFinite(p)||v<=0)return;
  ensureSessions(ts);
  latestPrice=p;lastTradeTs=ts;
  updateBucket(oneMin,ts,d,1);updateBucket(fiveMin,ts,d,5);
  if(t===0)sessionBuy+=v; else if(t===1)sessionSell+=v;
  sessionPV+=p*v;sessionVol+=v;
  const x=pt(ts),m=x.hour*60+x.minute;
  if(m>=390&&m<780){if(t===0)rthBuy+=v;else if(t===1)rthSell+=v;rthPV+=p*v;rthVol+=v;}
}
function arr(store,minutes){
  const cut=Date.now()-minutes*60000;
  return Object.values(store).filter(x=>Date.parse(x.t)>=cut).sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
}
async function sendRelay(){
  const body={
    receivedAt:new Date().toISOString(),
    lastTradeAt:lastTradeTs,
    currentPrice:latestPrice,
    quote:latestQuote,
    oneMin:arr(oneMin,360),
    fiveMin:arr(fiveMin,720),
    currentGlobexDelta:sessionBuy-sessionSell,
    currentRthDelta:rthBuy-rthSell,
    currentGlobexCvd:sessionBuy-sessionSell,
    currentRthCvd:rthBuy-rthSell,
    sessionVwap:sessionVol?sessionPV/sessionVol:null,
    rthVwap:rthVol?rthPV/rthVol:null
  };
  const r=await fetch(RELAY_URL,{method:"POST",headers:{"Content-Type":"application/json","Authorization":"Bearer "+RELAY_TOKEN},body:JSON.stringify(body)});
  if(!r.ok) throw new Error("relay "+r.status+" "+await r.text());
}
(async()=>{
  const auth=await post("/api/Auth/loginKey",{userName:USERNAME,apiKey:API_KEY});
  if(!auth.success||!auth.token) throw new Error("Auth failed");
  const found=await post("/api/Contract/search",{searchText:"MNQ",live:LIVE},auth.token);
  const c=(found.contracts||[]).find(x=>x.activeContract)||found.contracts?.[0];
  if(!c) throw new Error("MNQ not found");
  console.log("CONTRACT",c.id,c.name,c.symbolId,"live="+LIVE);

  let q=0,t=0,d=0;
  const conn=new signalR.HubConnectionBuilder()
    .withUrl(HUB,{skipNegotiation:true,transport:signalR.HttpTransportType.WebSockets,accessTokenFactory:()=>auth.token})
    .withAutomaticReconnect()
    .build();

  conn.on("GatewayQuote",(id,x)=>{q++;latestQuote=x;if(Number.isFinite(+(x?.lastPrice??x?.price)))latestPrice=+(x.lastPrice??x.price);});
  conn.on("GatewayTrade",(id,x)=>{t++;addTrade(x);});
  conn.on("GatewayDepth",()=>{d++;});

  await conn.start();
  console.log("CONNECTED",conn.connectionId);
  console.log("QUOTE_SUB",await conn.invoke("SubscribeContractQuotes",c.id));
  console.log("TRADE_SUB",await conn.invoke("SubscribeContractTrades",c.id));
  try{console.log("DEPTH_SUB",await conn.invoke("SubscribeContractMarketDepth",c.id));}catch(e){console.log("DEPTH_SUB_ERR",e.message);}

  setInterval(()=>sendRelay().catch(e=>console.error("RELAY_ERR",e.message)),2000);
  setInterval(()=>console.log("COUNTS",{quotes:q,trades:t,depth:d,price:latestPrice,sessionCvd:sessionBuy-sessionSell,rthCvd:rthBuy-rthSell}),10000);
})().catch(e=>{console.error(e);process.exit(1);});
