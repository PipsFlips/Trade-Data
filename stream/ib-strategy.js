// Deterministic MNQ plan from completed collector bars. No orders or fills are inferred.
const etFormatter=new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
function etParts(ms){
  const p={};for(const x of etFormatter.formatToParts(new Date(ms)))p[x.type]=x.value;
  return {date:`${p.year}-${p.month}-${p.day}`,minutes:+p.hour*60+(+p.minute)};
}
function buildIBStrategy({bars=[],now=Date.now(),market='MNQ',fresh=false,currentPrice=null,structureAt}){
  const today=etParts(now),base={date:today.date,market:'MNQ',state:'WAIT — IB FORMING',reason:'Range forms 09:30–10:30 ET. No entries before 10:30 ET.',high:null,low:null,sizePct:null,eligible:false,plan:null,side:null,breakout:false,retest:false,confirmation:false,vwap:null,structure:'TRANSITION',cutoffMinutes:Math.max(0,690-today.minutes)};
  const result=(state,reason,extra={})=>({...base,state,reason,...extra});
  if(market!=='MNQ')return result('MNQ ONLY','IB Strategy is configured for MNQ. MES tools remain available in Advanced Indicators & Market Analysis.');
  const valid=bars.filter(b=>Number.isFinite(Date.parse(b.t))&&['o','h','l','c','v'].every(k=>Number.isFinite(b[k]))&&b.v>=0&&b.h>=b.l&&b.h>=Math.max(b.o,b.c)&&b.l<=Math.min(b.o,b.c));
  const unique=[...new Map(valid.map(b=>[Date.parse(b.t),b])).values()].sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
  const day=unique.filter(b=>{const p=etParts(Date.parse(b.t));return p.date===today.date&&p.minutes>=570&&p.minutes<960;});
  const closed=day.filter(b=>Date.parse(b.t)+300000<=now);
  const ib=closed.filter(b=>etParts(Date.parse(b.t)).minutes<630);
  if(ib.length){base.high=Math.max(...ib.map(b=>b.h));base.low=Math.min(...ib.map(b=>b.l));base.sizePct=(base.high-base.low)/base.low*100;}
  if(today.minutes<630)return base;
  const complete=ib.length===12&&ib.every((b,i)=>etParts(Date.parse(b.t)).minutes===570+i*5);
  if(!complete)return result('UNAVAILABLE — IB DATA INCOMPLETE','All twelve completed 5-minute IB bars are required.');
  const range=base.high-base.low;
  base.eligible=base.sizePct>=0.60&&base.sizePct<0.90;
  if(!base.eligible)return result('NO TRADE — IB SIZE INVALID','IB must be at least 0.60% and below 0.90% (the 0.60–0.89% bucket).');
  let pv=0,vol=0;
  for(const b of ib){pv+=(b.h+b.l+b.c)/3*b.v;vol+=b.v;}
  let side=null,extreme=null,plan=null,priorTime=Date.parse(ib.at(-1).t),triggered=null;
  const align=(b,vw,ms,s)=>Number.isFinite(vw)&&(s==='LONG'?b.c>vw&&ms==='TREND UP':b.c<vw&&ms==='TREND DOWN');
  const depth=(b,s)=>s==='LONG'?b.c<base.high-range*.25:b.c>base.low+range*.25;
  for(const b of closed.filter(b=>etParts(Date.parse(b.t)).minutes>=630)){
    const t=Date.parse(b.t),end=t+300000;
    if(t-priorTime!==300000)return result('UNAVAILABLE — BAR GAP','A missing 5-minute bar prevents reliable break/retest tracking.');
    priorTime=t;
    if(etParts(end).minutes>=690){
      if(plan&&etParts(t).minutes<690&&(side==='LONG'?b.h>=plan.entry:b.l<=plan.entry))triggered={...plan,observedAt:b.t};
      break;
    }
    pv+=(b.h+b.l+b.c)/3*b.v;vol+=b.v;
    base.vwap=vol?pv/vol:null;
    // The callback receives only bars completed at this candle close: no look-ahead.
    base.structure=structureAt?structureAt(unique.filter(x=>Date.parse(x.t)+300000<=end),end):'TRANSITION';
    if(plan){
      const hit=side==='LONG'?b.h>=plan.entry:b.l<=plan.entry;
      if(hit){triggered={...plan,observedAt:b.t};break;}
      if(depth(b,side)||!align(b,base.vwap,base.structure,side))return result('INVALIDATED','Pending entry lost VWAP/structure alignment or closed too deeply inside IB.',{side,breakout:true,retest:true,confirmation:true});
      continue;
    }
    if(!side){
      side=b.c>base.high?'LONG':b.c<base.low?'SHORT':null;
      if(!side)continue;
      base.side=side;base.breakout=true;
      if(!align(b,base.vwap,base.structure,side))return result('INVALIDATED','First breakout did not align with NY VWAP and market structure. No opposite-side retry.');
      continue; // Never use the breakout candle as its own retest.
    }
    base.side=side;base.breakout=true;
    if(depth(b,side))return result('INVALIDATED','5-minute close penetrated more than 25% back into IB. No second attempt.');
    const touch=side==='LONG'?b.l<=base.high:b.h>=base.low;
    if(touch||extreme!==null){
      extreme=side==='LONG'?Math.min(extreme??Infinity,b.l):Math.max(extreme??-Infinity,b.h);
      base.retest=true;
    }
    const outside=side==='LONG'?b.c>base.high:b.c<base.low;
    if(extreme===null||!outside)continue;
    if(!align(b,base.vwap,base.structure,side))return result('INVALIDATED','First retest confirmation did not align with NY VWAP and structure.');
    const entry=side==='LONG'?b.h+.25:b.l-.25,stop=side==='LONG'?extreme-2:extreme+2;
    const distance=Math.abs(entry-stop),contracts=Math.min(5,Math.floor(100/(distance*2)));
    if(distance>20||distance<=0||contracts<1)return result('NO TRADE — STOP TOO WIDE','Structural stop exceeds the 20-point maximum. Do not tighten it to force a trade.',{side,breakout:true,retest:true,confirmation:true});
    // Round targets away from entry to the exchange tick, keeping at least 1.5R.
    const target=side==='LONG'?Math.ceil((entry+distance*1.5)*4)/4:Math.floor((entry-distance*1.5)*4)/4;
    plan={side,entry,stop,distance,target,contracts,risk:distance*2*contracts,reward:Math.abs(target-entry)*2*contracts,rr:Math.abs(target-entry)/distance,confirmedAt:new Date(end).toISOString()};
    base.confirmation=true;
  }
  if(triggered)return result('ENTRY LEVEL REACHED','Entry price was reached after confirmation. Check your broker; a price touch is not a verified fill. No new setup today.',{side,breakout:true,retest:true,confirmation:true,plan:triggered});
  if(today.minutes>=690)return result('DONE FOR DAY','11:30 ET entry cutoff passed. Cancel any unfilled entry order.');
  if(!fresh)return result('UNAVAILABLE — COLLECTOR STALE','Fresh collector data is required before an entry plan can be acted on.',{side,breakout:!!side,retest:extreme!==null});
  if(plan){
    // Only post-confirmation bars may trigger the stop-entry level.
    const live=day.find(b=>Date.parse(b.t)===Date.parse(plan.confirmedAt));
    const hit=live&&(side==='LONG'?live.h>=plan.entry:live.l<=plan.entry);
    if(hit||(Number.isFinite(currentPrice)&&(side==='LONG'?currentPrice>=plan.entry:currentPrice<=plan.entry)))return result('ENTRY LEVEL REACHED','Entry price reached. Confirm any fill with your broker; no new setup today.',{side,breakout:true,retest:true,confirmation:true,plan});
    return result(`${side} ENTRY`,'Confirmed first retest. Stop-entry plan valid only before 11:30 ET; use the stated bracket.',{side,breakout:true,retest:true,confirmation:true,plan});
  }
  return result(side?`ARMED ${side}`:'WAIT — NO BREAK',side?'Wait for the first touch of the IB boundary and a completed 5-minute rejection close.':'Wait for a completed 5-minute close outside IB.',{side,breakout:!!side,retest:extreme!==null});
}
module.exports={buildIBStrategy,etParts};
