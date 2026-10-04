// Collector-bar research signals. A price touch is never a verified broker fill.
const {etParts}=require('./ib-strategy');
function buildVWAPStrategy({bars=[],now=Date.now(),market='MNQ',fresh=false,currentPrice=null}){
  const today=etParts(now),mes=market==='MES',pointValue=mes?5:2,buffer=mes?.5:2;
  const base={date:today.date,market,strategy:'VWAP',rangeLabel:'ORB',state:'WAIT — OPENING 15 MINUTES',reason:'NY VWAP starts at 09:30 ET; setup candles start at 09:45 ET.',high:null,low:null,sizePct:null,eligible:false,plan:null,trades:[],side:null,breakout:false,retest:false,confirmation:false,vwap:null,structure:'VWAP slope',entryCutoffET:'11:30',exitTimeET:'12:00',cutoffMinutes:Math.max(0,690-today.minutes)};
  const result=(state,reason)=>({...base,state,reason});
  if(!['MNQ','MES'].includes(market))return result('UNAVAILABLE — MARKET','MNQ and MES only.');
  const valid=bars.filter(b=>Number.isFinite(Date.parse(b.t))&&['o','h','l','c','v'].every(k=>Number.isFinite(b[k]))&&b.v>=0&&b.h>=Math.max(b.o,b.c)&&b.l<=Math.min(b.o,b.c)&&b.h>=b.l);
  const day=[...new Map(valid.map(b=>[Date.parse(b.t),b])).values()].sort((a,b)=>Date.parse(a.t)-Date.parse(b.t)).filter(b=>{const p=etParts(Date.parse(b.t));return p.date===today.date&&p.minutes>=570&&p.minutes<960;});
  const closed=day.filter(b=>Date.parse(b.t)+300000<=now),opening=closed.filter(b=>etParts(Date.parse(b.t)).minutes<585);
  if(opening.length){base.high=Math.max(...opening.map(b=>b.h));base.low=Math.min(...opening.map(b=>b.l));base.sizePct=base.low>0?(base.high-base.low)/base.low*100:null;}
  if(today.minutes<585)return base;
  if(opening.length!==3||!opening.every((b,i)=>etParts(Date.parse(b.t)).minutes===570+i*5))return result('UNAVAILABLE — OPENING DATA','Three complete 5-minute opening bars are required.');
  base.eligible=true;
  let pv=0,volume=0,priorVWAP=null,sessionHigh=-Infinity,sessionLow=Infinity,armed=null,active=null,lastTime=null,lastReason='Wait for a fresh session extreme after 09:45 ET.';
  for(const b of closed){
    const t=Date.parse(b.t),minute=etParts(t).minutes,end=t+300000;
    if(minute>=720)break;
    if(lastTime!==null&&t-lastTime!==300000){base.plan=null;return result('UNAVAILABLE — BAR GAP','A missing 5-minute bar prevents reliable setup tracking.');}
    lastTime=t;priorVWAP=base.vwap;pv+=(b.h+b.l+b.c)/3*b.v;volume+=b.v;base.vwap=volume?pv/volume:null;
    const highBefore=sessionHigh,lowBefore=sessionLow;sessionHigh=Math.max(sessionHigh,b.h);sessionLow=Math.min(sessionLow,b.l);
    if(active){
      const long=active.side==='LONG';
      if(active.status==='PENDING'){
        if(long?b.h>=active.entry:b.l<=active.entry){active.status='ENTRY LEVEL REACHED';active.observedAt=b.t;}
        else{active.status='EXPIRED';active.endedAt=new Date(end).toISOString();active=null;}
      }
      if(active&&active.status==='ENTRY LEVEL REACHED'){
        // OHLC cannot establish sequencing. Stop takes precedence if both are touched.
        const stop=long?b.l<=active.stop:b.h>=active.stop,target=long?b.h>=active.target:b.l<=active.target;
        if(stop||target){active.status=stop?'STOP LEVEL TOUCHED':'TARGET LEVEL TOUCHED';active.endedAt=new Date(end).toISOString();active=null;}
      }
      armed=null;continue;
    }
    if(etParts(end).minutes>=690)continue;
    if(!Number.isFinite(base.vwap)||!Number.isFinite(priorVWAP))continue;
    if(minute<585){
      if(b.h>highBefore&&b.c>base.vwap&&base.vwap>priorVWAP)armed={side:'LONG',extreme:b.h};
      else if(b.l<lowBefore&&b.c<base.vwap&&base.vwap<priorVWAP)armed={side:'SHORT',extreme:b.l};
      continue;
    }
    if(armed){
      const long=armed.side==='LONG',touch=b.l<=base.vwap&&b.h>=base.vwap;
      const aligned=long?b.c>base.vwap&&b.c>b.o&&base.vwap>priorVWAP:b.c<base.vwap&&b.c<b.o&&base.vwap<priorVWAP;
      base.side=armed.side;base.breakout=true;
      if(touch){
        base.retest=true;
        if(aligned){
          const entry=long?b.h+.25:b.l-.25,stop=long?b.l-buffer:b.h+buffer,distance=Math.abs(entry-stop);
          const target=long?Math.ceil((entry+distance*1.5)*4)/4:Math.floor((entry-distance*1.5)*4)/4;
          const contracts=Math.min(5,Math.floor(150/(distance*pointValue)));
          const room=long?armed.extreme-entry:entry-armed.extreme;
          if(distance>0&&contracts>=1&&room>=Math.abs(target-entry)){
            active={side:armed.side,entry,stop,distance,target,contracts,risk:distance*pointValue*contracts,reward:Math.abs(target-entry)*pointValue*contracts,rr:Math.abs(target-entry)/distance,priorExtreme:armed.extreme,confirmedAt:new Date(end).toISOString(),expiresAt:new Date(end+300000).toISOString(),exitTimeET:'12:00',status:'PENDING'};
            base.trades.push(active);base.confirmation=true;lastReason='Confirmed VWAP rejection. Stop-entry is valid for the next 5-minute candle only.';
          }else lastReason='Last pullback skipped: structural stop exceeds the $150 budget or prior extreme lacks 1.5R target room.';
        }else lastReason='First VWAP touch failed candle direction, VWAP side or slope confirmation.';
        armed=null;continue;
      }
      if(long?b.c<=base.vwap:b.c>=base.vwap){armed=null;lastReason='Pullback lost the required side of NY VWAP.';}
    }
    // New extremes arm later candles; the extreme candle cannot confirm its own pullback.
    if(b.h>highBefore&&b.c>base.vwap&&base.vwap>priorVWAP)armed={side:'LONG',extreme:b.h};
    else if(b.l<lowBefore&&b.c<base.vwap&&base.vwap<priorVWAP)armed={side:'SHORT',extreme:b.l};
  }
  base.plan=active;
  if(today.minutes>=720){if(active){active.status='TIME EXIT';active.endedAt=new Date(now).toISOString();}return result('TIME EXIT — 12:00 ET','Noon time exit reached. Check any remaining position at your broker.');}
  if(!fresh){base.plan=null;return result('UNAVAILABLE — COLLECTOR STALE','Fresh collector data is required for an actionable entry. Historical labels are research signals only.');}
  if(active){
    if(active.status==='PENDING'){
      const live=day.find(b=>Date.parse(b.t)===Date.parse(active.confirmedAt)),long=active.side==='LONG';
      if((live&&(long?live.h>=active.entry:live.l<=active.entry))||(Number.isFinite(currentPrice)&&(long?currentPrice>=active.entry:currentPrice<=active.entry)))active.status='ENTRY LEVEL REACHED';
      else if(now>=Date.parse(active.expiresAt)){active.status='EXPIRED';base.plan=null;return result('WAIT — NEW EXTREME','Unfilled entry expired. Wait for a new session extreme and pullback.');}
    }
    return result(active.status==='PENDING'?active.side+' ENTRY':active.status,active.status==='PENDING'?lastReason:'Entry level was reached. Verify your broker fill and manage the bracket; no overlapping setup.');
  }
  if(today.minutes>=690)return result('ENTRY WINDOW CLOSED','11:30 ET entry cutoff passed. Manage any existing position until noon.');
  base.side=armed?.side||null;base.breakout=Boolean(armed);
  return result(armed?'WAIT — '+armed.side+' VWAP PULLBACK':'WAIT — NEW EXTREME',armed?'Wait for a later candle to touch NY VWAP and close with direction and slope confirmation.':lastReason);
}
module.exports={buildVWAPStrategy};
