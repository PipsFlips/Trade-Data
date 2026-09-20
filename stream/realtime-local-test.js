const signalR = require("@microsoft/signalr");
const API_BASE = process.env.TOPSTEP_API_BASE || "https://api.topstepx.com";
const HUB = process.env.TOPSTEP_MARKET_HUB || "https://rtc.topstepx.com/hubs/market";
const USERNAME = process.env.TOPSTEP_USERNAME;
const API_KEY = process.env.TOPSTEP_API_KEY;
const LIVE = String(process.env.TOPSTEP_LIVE_DATA || "false").toLowerCase()==="true";

if(!USERNAME||!API_KEY){console.error("Set TOPSTEP_USERNAME and TOPSTEP_API_KEY");process.exit(1);}

async function post(path,payload,token){
  const r=await fetch(API_BASE+path,{method:"POST",headers:{"Content-Type":"application/json","Accept":"text/plain",...(token?{"Authorization":"Bearer "+token}:{})},body:JSON.stringify(payload)});
  if(!r.ok) throw new Error(path+" "+r.status+" "+await r.text());
  return r.json();
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

  conn.on("GatewayQuote",(id,x)=>{q++;console.log("QUOTE",q,id,x);});
  conn.on("GatewayTrade",(id,x)=>{t++;console.log("TRADE",t,id,x);});
  conn.on("GatewayDepth",(id,x)=>{d++;console.log("DEPTH",d,id,x);});

  await conn.start();
  console.log("CONNECTED",conn.connectionId);
  for(const [name,method] of [["QUOTE_SUB","SubscribeContractQuotes"],["TRADE_SUB","SubscribeContractTrades"],["DEPTH_SUB","SubscribeContractMarketDepth"]]){
    try{console.log(name,await conn.invoke(method,c.id));}catch(e){console.log(name+"_ERR",e.message);}
  }
  setInterval(()=>console.log("COUNTS",{quotes:q,trades:t,depth:d,connectionState:conn.state}),10000);
})().catch(e=>{console.error(e);process.exit(1);});
