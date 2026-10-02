// Opening-hour candidate. This is a plan calculator, not a broker/order connection.
const {etParts}=require('./ib-strategy');
function buildORBStrategy({bars=[],now=Date.now(),market='MNQ',fresh=false,currentPrice=null,structureAt}){
  const mes=market==='MES',pointValue=mes?5:2,stopBuffer=mes?.5:2,maxStop=mes?8:20,costReserve=mes?4:0;
  const today=etParts(now),base={date:today.date,market,strategy:'ORB',rangeLabel:'ORB',entryCutoffET:'10:15',exitTimeET:'10:30',state:'WAIT — ORB FORMING',reason:'Range forms 09:30–09:45 ET. No entries before 09:45 ET.',high:null,low:null,sizePct:null,eligible:false,plan:null,side:null,breakout:false,retest:false,confirmation:false,vwap:null,structure:'TRANSITION',cutoffMinutes:Math.max(0,615-today.minutes)};
  const result=(state,reason,extra={})=>({...base,state,reason,...extra});
  const completed=(reason)=>result('DONE FOR DAY',reason);
  if(!['MNQ','MES'].includes(market))return result('UNAVAILABLE — MARKET','ORB supports MNQ and MES only.');
  const valid=bars.filter(b=>Number.isFinite(Date.parse(b.t))&&['o','h','l','c','v'].every(k=>Number.isFinite(b[k]))&&b.v>=0&&b.h>=Math.max(b.o,b.c)&&b.l<=Math.min(b.o,b.c)&&b.h>=b.l);
  const unique=[...new Map(valid.map(b=>[Date.parse(b.t),b])).values()].sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
  const day=unique.filter(b=>{const p=etParts(Date.parse(b.t));return p.date===today.date&&p.minutes>=570&&p.minutes<960;});
  const closed=day.filter(b=>Date.parse(b.t)+300000<=now);
  const opening=closed.filter(b=>etParts(Date.parse(b.t)).minutes<585);
  if(opening.length){base.high=Math.max(...opening.map(b=>b.h));base.low=Math.min(...opening.map(b=>b.l));base.sizePct=(base.high-base.low)/base.low*100;}
  if(today.minutes<585)return base;
  if(opening.length!==3||!opening.every((b,i)=>etParts(Date.parse(b.t)).minutes===570+i*5))return result('UNAVAILABLE — ORB DATA INCOMPLETE','All three completed 5-minute opening-range bars are required.');
  if(base.high<=base.low||base.low<=0)return result('NO TRADE — ORB RANGE INVALID','A valid nonzero opening range is required.');
  base.eligible=true;
  let pv=0,vol=0;
  for(const b of opening){pv+=(b.h+b.l+b.c)/3*b.v;vol+=b.v;}
  base.vwap=vol?pv/vol:null;
  let side=null,breakTime=null,plan=null,triggered=null,priorTime=Date.parse(opening.at(-1).t);
  const align=(b)=>Number.isFinite(base.vwap)&&(side==='LONG'?b.c>base.vwap&&base.structure==='TREND UP':b.c<base.vwap&&base.structure==='TREND DOWN');
  const outside=(b)=>side==='LONG'?b.c>base.high:b.c<base.low;
  const reached=(b)=>side==='LONG'?b.h>=plan.entry:b.l<=plan.entry;
  for(const b of closed.filter(b=>etParts(Date.parse(b.t)).minutes>=585)){
    const t=Date.parse(b.t),end=t+300000;
    if(etParts(t).minutes>=615)break;
    if(t-priorTime!==300000)return result('UNAVAILABLE — BAR GAP','A missing 5-minute bar prevents reliable break/retest tracking.');
    priorTime=t;
    pv+=(b.h+b.l+b.c)/3*b.v;vol+=b.v;base.vwap=vol?pv/vol:null;
    base.structure=structureAt?structureAt(unique.filter(x=>Date.parse(x.t)+300000<=end),end):'TRANSITION';
    if(plan){
      // The order has only the next 5-minute candle to reach entry, never past 10:15.
      if(reached(b)){triggered={...plan,observedAt:b.t};break;}
      if(!outside(b)||!align(b))return result('INVALIDATED','Unfilled entry lost range, NY VWAP or structure alignment. No new attempt.');
      return completed('Unfilled entry expired after its next 5-minute candle. Cancel the entry order; no new attempt.');
    }
    if(etParts(end).minutes>=615)break;
    if(!side){
      side=b.c>base.high?'LONG':b.c<base.low?'SHORT':null;
      if(!side)continue;
      base.side=side;base.breakout=true;breakTime=t;
      if(!align(b))return result('INVALIDATED','First breakout did not align with NY VWAP and trend structure. No opposite-side retry.');
      continue; // A breakout cannot also be its own retest.
    }
    base.side=side;base.breakout=true;
    if((t-breakTime)/300000>3)return completed('The first retest did not occur within three 5-minute candles. No new attempt.');
    const touch=side==='LONG'?b.l<=base.high:b.h>=base.low;
    if(!touch){
      if((t-breakTime)/300000===3)return completed('The first retest did not occur within three 5-minute candles. No new attempt.');
      continue;
    }
    base.retest=true;
    if(!outside(b))return result('INVALIDATED','The first retest candle closed inside the opening range. No second retest or opposite-side retry.');
    if(!align(b))return result('INVALIDATED','First retest confirmation did not align with NY VWAP and trend structure.');
    const entry=side==='LONG'?b.h+.25:b.l-.25,stop=side==='LONG'?b.l-stopBuffer:b.h+stopBuffer;
    const distance=Math.abs(entry-stop),contracts=Math.min(5,Math.floor(100/(distance*pointValue+costReserve)));
    base.confirmation=true;
    if(distance>maxStop||distance<=0||contracts<1)return result('NO TRADE — STOP TOO WIDE',`Structural stop exceeds the ${maxStop}-point maximum. Do not tighten it to force a trade.`);
    const target=side==='LONG'?Math.ceil((entry+distance*1.5)*4)/4:Math.floor((entry-distance*1.5)*4)/4;
    const expiresAt=Math.min(end+300000,t+((615-etParts(t).minutes)*60000));
    plan={side,entry,stop,distance,target,contracts,risk:distance*pointValue*contracts,costReserve:costReserve*contracts,reward:Math.abs(target-entry)*pointValue*contracts,rr:Math.abs(target-entry)/distance,confirmedAt:new Date(end).toISOString(),expiresAt:new Date(expiresAt).toISOString(),exitTimeET:'10:30'};
  }
  if(triggered)return result(today.minutes>=630?'TIME EXIT — 10:30 ET':'ENTRY LEVEL REACHED',today.minutes>=630?'10:30 ET time exit reached. Close any remaining ORB position at your broker. No new setup today.':'Entry price was reached after confirmation. Check your broker; a price touch is not a verified fill. No new setup today.',{plan:triggered});
  if(today.minutes>=615)return completed('10:15 ET entry cutoff passed. Cancel unfilled entries. Close any open ORB position at 10:30 ET.');
  if(!fresh)return result('UNAVAILABLE — COLLECTOR STALE','Fresh collector data is required before an entry plan can be acted on.');
  if(plan){
    if(now>=Date.parse(plan.expiresAt))return completed('Unfilled entry order expired. Cancel it; no new attempt.');
    const live=day.find(b=>Date.parse(b.t)===Date.parse(plan.confirmedAt));
    if((live&&reached(live))||(Number.isFinite(currentPrice)&&(side==='LONG'?currentPrice>=plan.entry:currentPrice<=plan.entry)))return result('ENTRY LEVEL REACHED','Entry price reached. Verify any fill at your broker; no new setup today.',{plan});
    return result(`${side} ENTRY`,'Confirmed first retest. Entry expires at the next 5-minute close or 10:15 ET, whichever comes first.',{plan});
  }
  return result(side?`ARMED ${side}`:'WAIT — NO BREAK',side?'Wait for the first boundary touch within three candles, then an aligned 5-minute rejection close outside ORB.':'Wait for the first completed 5-minute close outside the 15-minute opening range.');
}
module.exports={buildORBStrategy};
