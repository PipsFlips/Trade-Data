const signalR=require("@microsoft/signalr");
const {DateTime}=require("luxon");
const fs=require("fs");
const path=require("path");
const os=require("os");

const API_BASE=process.env.TOPSTEP_API_BASE||"https://api.topstepx.com";
const HUB=process.env.TOPSTEP_MARKET_HUB||"https://rtc.topstepx.com/hubs/market";
const USERNAME=process.env.TOPSTEP_USERNAME;
const API_KEY=process.env.TOPSTEP_API_KEY;
const LIVE=String(process.env.TOPSTEP_LIVE_DATA||"false").toLowerCase()==="true";
const RELAY_TOKEN=process.env.REALTIME_RELAY_TOKEN;
const MNQ_RELAY_URL=process.env.MNQ_RELAY_URL||"https://trade-data-production.up.railway.app/realtime-relay";
const MES_RELAY_URL=process.env.MES_RELAY_URL||"";
const ZONE="America/Los_Angeles";
const STATE_FILE=process.env.COLLECTOR_STATE_FILE||path.join(os.homedir(),".mnq-mes-collector-state.json");
const STATE_SAVE_MS=30000;
const TOKEN_REFRESH_MS=18*60*60*1000;

if(!USERNAME||!API_KEY||!RELAY_TOKEN){console.error("Missing TOPSTEP_USERNAME / TOPSTEP_API_KEY / REALTIME_RELAY_TOKEN");process.exit(1);}
if(!MES_RELAY_URL){console.error("Missing MES_RELAY_URL");process.exit(1);}

let authToken=null,authIssuedAt=0,lastRecoveryAt=0,recoveryCount=0;
let contracts={};

function freshState(symbol){
  return {
    symbol,
    oneMin:{},fiveMin:{},
    sessionProfile:{},rthProfile:{},
    latestPrice:null,latestQuote:null,
    sessionBuy:0,sessionSell:0,rthBuy:0,rthSell:0,
    sessionPV:0,sessionVol:0,rthPV:0,rthVol:0,
    sessionKey:null,rthDate:null,lastTradeTs:null,
    lastTradeReceivedAt:null,lastQuoteReceivedAt:null,lastDepthReceivedAt:null,
    icebergTape:{},depthAsk:{},depthBid:{}
  };
}
const states={MNQ:freshState("MNQ"),MES:freshState("MES")};

async function post(apiPath,payload,token){
  const r=await fetch(API_BASE+apiPath,{method:"POST",headers:{"Content-Type":"application/json","Accept":"text/plain",...(token?{"Authorization":"Bearer "+token}:{})},body:JSON.stringify(payload)});
  if(!r.ok) throw new Error(apiPath+" "+r.status+" "+await r.text());
  return r.json();
}
async function authenticate(){
  const a=await post("/api/Auth/loginKey",{userName:USERNAME,apiKey:API_KEY});
  if(!a.success||!a.token) throw new Error("Auth failed");
  authToken=a.token;authIssuedAt=Date.now();return authToken;
}
async function getToken(){if(!authToken||Date.now()-authIssuedAt>TOKEN_REFRESH_MS) await authenticate();return authToken;}
function pt(ts){return DateTime.fromISO(ts,{setZone:true}).setZone(ZONE);}
function currentSessionKey(d){return (d.hour>=15?d.plus({days:1}):d).toISODate();}
function bucketStartIso(ts,mins){const ms=Date.parse(ts),bm=mins*60000;return new Date(Math.floor(ms/bm)*bm).toISOString();}
function pxKey(p){return (Math.round((+p)*4)/4).toFixed(2);}

function ensureSessions(st,ts){
  const d=pt(ts),sk=currentSessionKey(d),rd=d.toISODate(),m=d.hour*60+d.minute;
  if(sk!==st.sessionKey){
    st.sessionKey=sk;st.sessionBuy=st.sessionSell=st.sessionPV=st.sessionVol=0;st.sessionProfile={};
  }
  if(m>=390&&m<780&&rd!==st.rthDate){
    st.rthDate=rd;st.rthBuy=st.rthSell=st.rthPV=st.rthVol=0;st.rthProfile={};
  }
}
function updateBucket(store,ts,d,mins){
  const k=bucketStartIso(ts,mins),p=+d.price,v=+d.volume||0,t=+d.type;
  if(!Number.isFinite(p)||v<=0)return;
  let b=store[k]||{t:k,o:p,h:p,l:p,c:p,volume:0,buyVolume:0,sellVolume:0,delta:0,trades:0};
  b.h=Math.max(b.h,p);b.l=Math.min(b.l,p);b.c=p;b.volume+=v;b.trades++;
  if(t===0){b.buyVolume+=v;b.delta+=v;}else if(t===1){b.sellVolume+=v;b.delta-=v;}
  store[k]=b;
}
function updateProfile(store,p,v,t){
  const k=pxKey(p),x=store[k]||{price:+k,volume:0,buyVolume:0,sellVolume:0,delta:0};
  x.volume+=v;
  if(t===0){x.buyVolume+=v;x.delta+=v;}else if(t===1){x.sellVolume+=v;x.delta-=v;}
  store[k]=x;
}
function finalizeProfile(store){
  const a=Object.values(store||{}).sort((x,y)=>x.price-y.price);
  if(!a.length)return null;
  const total=a.reduce((s,x)=>s+x.volume,0),poc=a.reduce((x,y)=>y.volume>x.volume?y:x),idx=a.indexOf(poc);
  let lo=idx-1,hi=idx+1,cum=poc.volume;const selected=new Set([idx]);
  while(cum<total*.70&&(lo>=0||hi<a.length)){
    const lv=lo>=0?a[lo].volume:-1,hv=hi<a.length?a[hi].volume:-1;
    const i=(hv>=lv&&hi<a.length)?hi++:lo--;
    if(i>=0&&i<a.length&&!selected.has(i)){selected.add(i);cum+=a[i].volume;}
  }
  const ps=[...selected].map(i=>a[i].price);
  return {method:"exact local GatewayTrade volume-at-price",totalVolume:total,poc:poc.price,vah:Math.max(...ps),val:Math.min(...ps),
    strongestVolumeNodes:[...a].sort((x,y)=>y.volume-x.volume).slice(0,12),
    strongestDeltaPrices:[...a].sort((x,y)=>Math.abs(y.delta)-Math.abs(x.delta)).slice(0,12)};
}
function recordIcebergTrade(st,d){
  const p=+d.price,v=+d.volume||0,t=+d.type;if(!Number.isFinite(p)||v<=0||(t!==0&&t!==1))return;
  const k=pxKey(p),now=Date.now();let x=st.icebergTape[k]||{price:+k,buyVol:0,sellVol:0,buyTrades:0,sellTrades:0,askRefresh:0,bidRefresh:0,firstAt:now,lastAt:now};
  if(now-x.firstAt>30000)x={price:+k,buyVol:0,sellVol:0,buyTrades:0,sellTrades:0,askRefresh:0,bidRefresh:0,firstAt:now,lastAt:now};
  if(t===0){x.buyVol+=v;x.buyTrades++;}else{x.sellVol+=v;x.sellTrades++;}
  x.lastAt=now;st.icebergTape[k]=x;
}
function recordDepth(st,row){
  const p=+row?.price;if(!Number.isFinite(p))return;
  const type=+row.type,side=(type===1||type===3||type===10)?"ask":(type===2||type===4||type===9)?"bid":null;
  if(!side)return;
  const cur=Number.isFinite(+row.currentVolume)?+row.currentVolume:(Number.isFinite(+row.volume)?+row.volume:null);if(!Number.isFinite(cur))return;
  const k=pxKey(p),book=side==="ask"?st.depthAsk:st.depthBid,prev=book[k];book[k]={currentVolume:cur,at:Date.now()};
  if(prev&&cur>prev.currentVolume){
    const x=st.icebergTape[k];
    if(x&&Date.now()-x.lastAt<=15000){if(side==="ask")x.askRefresh++;else x.bidRefresh++;st.icebergTape[k]=x;}
  }
}
function detectedIcebergs(st){
  const now=Date.now(),out=[];
  for(const [k,x] of Object.entries(st.icebergTape)){
    if(now-x.lastAt>30000){delete st.icebergTape[k];continue;}
    const age=(now-x.lastAt)/1000;
    if(x.buyVol>=12&&x.buyTrades>=4&&x.askRefresh>=2){
      const score=Math.min(100,35+Math.min(30,x.buyVol)+Math.min(20,x.askRefresh*5)+Math.min(15,x.buyTrades*2));
      out.push({side:"SELL",type:"sell-iceberg",price:x.price,score,aggressorVolume:x.buyVol,refreshes:x.askRefresh,trades:x.buyTrades,ageSec:+age.toFixed(1)});
    }
    if(x.sellVol>=12&&x.sellTrades>=4&&x.bidRefresh>=2){
      const score=Math.min(100,35+Math.min(30,x.sellVol)+Math.min(20,x.bidRefresh*5)+Math.min(15,x.sellTrades*2));
      out.push({side:"BUY",type:"buy-iceberg",price:x.price,score,aggressorVolume:x.sellVol,refreshes:x.bidRefresh,trades:x.sellTrades,ageSec:+age.toFixed(1)});
    }
  }
  return out.sort((a,b)=>b.score-a.score||a.ageSec-b.ageSec).slice(0,4);
}
function addTrade(st,d){
  const ts=d.timestamp||new Date().toISOString(),p=+d.price,v=+d.volume||0,t=+d.type;if(!Number.isFinite(p)||v<=0)return;
  ensureSessions(st,ts);st.latestPrice=p;st.lastTradeTs=ts;st.lastTradeReceivedAt=new Date().toISOString();recordIcebergTrade(st,d);
  updateBucket(st.oneMin,ts,d,1);updateBucket(st.fiveMin,ts,d,5);
  if(t===0)st.sessionBuy+=v;else if(t===1)st.sessionSell+=v;
  st.sessionPV+=p*v;st.sessionVol+=v;updateProfile(st.sessionProfile,p,v,t);
  const x=pt(ts),m=x.hour*60+x.minute;
  if(m>=390&&m<780){if(t===0)st.rthBuy+=v;else if(t===1)st.rthSell+=v;st.rthPV+=p*v;st.rthVol+=v;updateProfile(st.rthProfile,p,v,t);}
}
function arr(store,minutes){const cut=Date.now()-minutes*60000;return Object.values(store).filter(x=>Date.parse(x.t)>=cut).sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));}
function prune(st){
  const now=Date.now(),cut1=now-8*3600000,cut5=now-18*3600000,cutD=now-60000;
  for(const k of Object.keys(st.oneMin))if(Date.parse(st.oneMin[k]?.t||k)<cut1)delete st.oneMin[k];
  for(const k of Object.keys(st.fiveMin))if(Date.parse(st.fiveMin[k]?.t||k)<cut5)delete st.fiveMin[k];
  for(const b of [st.depthAsk,st.depthBid])for(const [k,x] of Object.entries(b))if((x?.at||0)<cutD)delete b[k];
}
function serializable(st){const o={...st};delete o.icebergTape;delete o.depthAsk;delete o.depthBid;return o;}
function saveState(){
  try{for(const st of Object.values(states))prune(st);const tmp=STATE_FILE+".tmp";fs.writeFileSync(tmp,JSON.stringify({savedAt:new Date().toISOString(),states:{MNQ:serializable(states.MNQ),MES:serializable(states.MES)}}));fs.renameSync(tmp,STATE_FILE);}catch(e){console.error("STATE_SAVE_ERR",e.message);}
}
function loadState(){
  try{
    if(fs.existsSync(STATE_FILE)){
      const x=JSON.parse(fs.readFileSync(STATE_FILE,"utf8"));
      for(const sym of ["MNQ","MES"]){if(x.states?.[sym])Object.assign(states[sym],x.states[sym]);}
      console.log("STATE_LOADED",STATE_FILE);
      return;
    }

    // One-time migration from the prior single-symbol MNQ collector.
    // Prefer the hardened home-directory state, then fall back to the legacy repo-local file.
    const legacyCandidates=[
      path.join(os.homedir(),".mnq-collector-state.json"),
      path.join(os.homedir(),"Trade-Data-main","stream","collector-state.json")
    ];
    const legacyPath=legacyCandidates.find(p=>fs.existsSync(p));
    if(!legacyPath) return;

    const x=JSON.parse(fs.readFileSync(legacyPath,"utf8"));
    const mnq=states.MNQ;
    for(const k of ["sessionKey","rthDate","sessionBuy","sessionSell","rthBuy","rthSell","sessionPV","sessionVol","rthPV","rthVol","lastTradeTs","latestPrice"]){
      if(x[k]!==undefined&&x[k]!==null) mnq[k]=x[k];
    }
    if(x.sessionProfile) mnq.sessionProfile=x.sessionProfile;
    if(x.rthProfile) mnq.rthProfile=x.rthProfile;
    if(x.oneMin) mnq.oneMin=x.oneMin;
    if(x.fiveMin) mnq.fiveMin=x.fiveMin;

    console.log("LEGACY_MNQ_STATE_LOADED",legacyPath);
    saveState();
    console.log("DUAL_STATE_MIGRATED",STATE_FILE);
  }catch(e){console.error("STATE_LOAD_ERR",e.message);}
}
async function findContract(symbol){
  const r=await post("/api/Contract/search",{searchText:symbol,live:LIVE},authToken);
  const cs=(r.contracts||[]).filter(c=>JSON.stringify(c).toUpperCase().includes(symbol));
  const c=cs.find(x=>x.activeContract)||cs[0];if(!c)throw new Error(symbol+" contract not found");return c;
}
function symbolForEvent(id,row){
  for(const sym of ["MNQ","MES"]){const c=contracts[sym];if(!c)continue;if(id===c.id||id===c.symbolId||row?.symbolId===c.symbolId||row?.contractId===c.id)return sym;}
  return null;
}
async function relayOne(sym){
  const st=states[sym],nowPt=DateTime.now().setZone(ZONE),m=nowPt.hour*60+nowPt.minute,rthActive=m>=390&&m<780&&st.rthDate===nowPt.toISODate();
  const body={receivedAt:new Date().toISOString(),lastTradeAt:st.lastTradeTs,lastTradeReceivedAt:st.lastTradeReceivedAt,lastQuoteReceivedAt:st.lastQuoteReceivedAt,lastDepthReceivedAt:st.lastDepthReceivedAt,
    currentPrice:st.latestPrice,quote:st.latestQuote,oneMin:arr(st.oneMin,360),fiveMin:arr(st.fiveMin,720),
    currentGlobexDelta:st.sessionBuy-st.sessionSell,currentRthDelta:rthActive?(st.rthBuy-st.rthSell):null,currentGlobexCvd:st.sessionBuy-st.sessionSell,currentRthCvd:rthActive?(st.rthBuy-st.rthSell):null,
    sessionVwap:st.sessionVol?st.sessionPV/st.sessionVol:null,rthVwap:rthActive&&st.rthVol?st.rthPV/st.rthVol:null,rthActive,
    profiles:{session:finalizeProfile(st.sessionProfile),rth:rthActive?finalizeProfile(st.rthProfile):null},
    collector:{startedAt:collectorStartedAt,recoveryCount},icebergs:detectedIcebergs(st)};
  const url=sym==="MNQ"?MNQ_RELAY_URL:MES_RELAY_URL;
  const r=await fetch(url,{method:"POST",headers:{"Content-Type":"application/json","Authorization":"Bearer "+RELAY_TOKEN},body:JSON.stringify(body)});
  if(!r.ok)throw new Error(sym+" relay "+r.status+" "+await r.text());
}
const collectorStartedAt=new Date().toISOString();

(async()=>{
  loadState();await authenticate();
  contracts.MNQ=await findContract("MNQ");contracts.MES=await findContract("MES");
  console.log("CONTRACTS",{MNQ:contracts.MNQ.name,MES:contracts.MES.name,live:LIVE});
  const conn=new signalR.HubConnectionBuilder().withUrl(HUB,{skipNegotiation:true,transport:signalR.HttpTransportType.WebSockets,accessTokenFactory:async()=>await getToken()}).withAutomaticReconnect().build();

  async function subscribeAll(){
    for(const sym of ["MNQ","MES"]){const c=contracts[sym];
      console.log(sym,"QUOTE_SUB",await conn.invoke("SubscribeContractQuotes",c.id));
      console.log(sym,"TRADE_SUB",await conn.invoke("SubscribeContractTrades",c.id));
      try{console.log(sym,"DEPTH_SUB",await conn.invoke("SubscribeContractMarketDepth",c.id));}catch(e){console.log(sym,"DEPTH_SUB_ERR",e.message);}
    }
  }
  conn.on("GatewayQuote",(id,x)=>{for(const row of (Array.isArray(x)?x:[x])){const sym=symbolForEvent(id,row);if(!sym)continue;const st=states[sym];st.lastQuoteReceivedAt=new Date().toISOString();st.latestQuote=row;if(Number.isFinite(+(row?.lastPrice??row?.price)))st.latestPrice=+(row.lastPrice??row.price);}});
  conn.on("GatewayTrade",(id,x)=>{for(const row of (Array.isArray(x)?x:[x])){const sym=symbolForEvent(id,row);if(sym)addTrade(states[sym],row);}});
  conn.on("GatewayDepth",(id,x)=>{for(const row of (Array.isArray(x)?x:[x])){const sym=symbolForEvent(id,row);if(!sym)continue;states[sym].lastDepthReceivedAt=new Date().toISOString();recordDepth(states[sym],row);}});
  conn.onreconnected(async()=>{recoveryCount++;console.log("AUTO_RECONNECTED",conn.connectionId);try{await subscribeAll();}catch(e){console.error("AUTO_RESUB_ERR",e.message);}});
  await conn.start();console.log("CONNECTED",conn.connectionId);await subscribeAll();

  async function recover(reason){
    if(Date.now()-lastRecoveryAt<30000)return;lastRecoveryAt=Date.now();recoveryCount++;console.log("WATCHDOG_RECOVERY",reason);
    try{if(conn.state===signalR.HubConnectionState.Connected){await subscribeAll();return;}}catch(e){console.log("WATCHDOG_RESUB_ERR",e.message);}
    try{if(conn.state!==signalR.HubConnectionState.Disconnected)await conn.stop();}catch{}
    await getToken();await conn.start();await subscribeAll();
  }
  setInterval(()=>{
    const now=Date.now();
    for(const sym of ["MNQ","MES"]){
      const st=states[sym];
      const ta=st.lastTradeReceivedAt?now-Date.parse(st.lastTradeReceivedAt):Infinity;
      const qa=st.lastQuoteReceivedAt?now-Date.parse(st.lastQuoteReceivedAt):Infinity;
      const da=st.lastDepthReceivedAt?now-Date.parse(st.lastDepthReceivedAt):Infinity;
      const tradeStaleMs=sym==="MES"?45000:15000;
      if(ta>tradeStaleMs&&Math.min(qa,da)<8000){
        recover(sym+" trade stale "+Math.round(ta/1000)+"s").catch(()=>{});
        break;
      }
    }
  },5000);
  setInterval(()=>Promise.all([relayOne("MNQ"),relayOne("MES")]).catch(e=>console.error("RELAY_ERR",e.message)),2000);
  setInterval(saveState,STATE_SAVE_MS);
  setInterval(()=>{for(const st of Object.values(states))prune(st);},5*60*1000);
  setInterval(()=>console.log("COUNTS",{MNQ:{price:states.MNQ.latestPrice,sessionCvd:states.MNQ.sessionBuy-states.MNQ.sessionSell},MES:{price:states.MES.latestPrice,sessionCvd:states.MES.sessionBuy-states.MES.sessionSell},recoveries:recoveryCount}),60000);
  process.on("SIGTERM",()=>{saveState();process.exit(0);});process.on("SIGINT",()=>{saveState();process.exit(0);});
})().catch(e=>{console.error(e);process.exit(1);});
