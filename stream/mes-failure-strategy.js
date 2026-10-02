// Collector-only research plan. A price touch never establishes a broker fill.
const {etParts}=require('./ib-strategy');
function buildMESFailureStrategy({bars=[],now=Date.now(),market='MES',fresh=false,currentPrice=null}){
  const today=etParts(now),base={date:today.date,market,strategy:'FAILURE',rangeLabel:'ORB',entryCutoffET:'10:30',exitTimeET:'10:30',state:'WAIT — ORB FORMING',reason:'Opening range forms 09:30–09:45 ET.',high:null,low:null,sizePct:null,eligible:false,plan:null,side:null,breakout:false,retest:false,confirmation:false,vwap:null,structure:'Not required',cutoffMinutes:Math.max(0,630-today.minutes)};
  const result=(state,reason,extra={})=>({...base,state,reason,...extra});
  if(market!=='MES')return result('MES ONLY','Opening Range Failure is configured for MES.');
  const valid=bars.filter(b=>Number.isFinite(Date.parse(b.t))&&['o','h','l','c','v'].every(k=>Number.isFinite(b[k]))&&b.v>=0&&b.h>=Math.max(b.o,b.c)&&b.l<=Math.min(b.o,b.c)&&b.h>=b.l);
  const unique=[...new Map(valid.map(b=>[Date.parse(b.t),b])).values()].sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));
  const day=unique.filter(b=>{const p=etParts(Date.parse(b.t));return p.date===today.date&&p.minutes>=570&&p.minutes<960;});
  const closed=day.filter(b=>Date.parse(b.t)+300000<=now),opening=closed.filter(b=>etParts(Date.parse(b.t)).minutes<585);
  if(opening.length){base.high=Math.max(...opening.map(b=>b.h));base.low=Math.min(...opening.map(b=>b.l));base.sizePct=(base.high-base.low)/base.low*100;}
  if(today.minutes<585)return base;
  if(opening.length!==3||!opening.every((b,i)=>etParts(Date.parse(b.t)).minutes===570+i*5))return result('UNAVAILABLE — ORB DATA INCOMPLETE','All three completed opening-range bars are required.');
  if(base.high<=base.low||base.low<=0)return result('NO TRADE — ORB RANGE INVALID','A positive opening range is required.');
  base.eligible=true;
  let pv=0,vol=0,breakout=null,failed=false,plan=null,triggered=null,previous=Date.parse(opening.at(-1).t);
  const add=b=>{pv+=(b.h+b.l+b.c)/3*b.v;vol+=b.v;base.vwap=vol?pv/vol:null;};
  opening.forEach(add);
  const reached=b=>plan.side==='LONG'?b.h>=plan.entry:b.l<=plan.entry;
  for(const b of closed.filter(b=>etParts(Date.parse(b.t)).minutes>=585)){
    const t=Date.parse(b.t),end=t+300000;
    if(etParts(t).minutes>=630)break;
    if(t-previous!==300000)return result('UNAVAILABLE — BAR GAP','Missing bars prevent reliable failure/retest tracking.');
    previous=t;add(b);
    if(plan){if(reached(b)){triggered=plan;break;}return result('DONE FOR DAY','Unfilled entry expired after the next 5-minute candle. No retry.');}
    if(etParts(end).minutes>=630)break;
    if(!breakout){breakout=b.c>base.high?'UP':b.c<base.low?'DOWN':null;if(breakout){base.breakout=true;base.side=breakout==='UP'?'SHORT':'LONG';}continue;}
    const inside=b.c>base.low&&b.c<base.high;
    if(!failed){if(inside)failed=true;continue;}
    if(!inside)return result('INVALIDATED','Price closed outside or on the opening-range boundary after the failure. No retry.');
    const short=breakout==='UP',touch=short?b.h>=base.high:b.l<=base.low;
    if(!touch)continue;
    base.retest=true;
    if(!Number.isFinite(base.vwap)||(short?b.c>=base.vwap:b.c<=base.vwap))return result('INVALIDATED','First boundary retest failed NY VWAP confirmation.');
    base.confirmation=true;
    const side=short?'SHORT':'LONG',entry=short?b.l-.25:b.h+.25,stop=short?b.h+.5:b.l-.5,distance=Math.abs(stop-entry);
    const choices=[base.vwap,short?base.low:base.high].filter(x=>short?x<entry:x>entry);
    if(!choices.length)return result('NO TRADE — NO TARGET ROOM','No profitable VWAP or opposite-boundary target.');
    let target=short?Math.max(...choices):Math.min(...choices);target=short?Math.ceil(target*4)/4:Math.floor(target*4)/4;
    const contracts=Math.min(5,Math.floor(100/(distance*5+4))),rr=Math.abs(target-entry)/distance;
    if(distance<=0||distance>8||contracts<1||rr<1.5)return result('NO TRADE — STOP OR TARGET ROOM','Requires an 8-point maximum stop and at least 1.5R to the nearest target.');
    plan={side,entry,stop,target,distance,contracts,risk:distance*5*contracts,costReserve:4*contracts,reward:Math.abs(target-entry)*5*contracts,rr,confirmedAt:new Date(end).toISOString(),expiresAt:new Date(end+300000).toISOString(),exitTimeET:'10:30'};
  }
  if(triggered)return result(today.minutes>=630?'TIME EXIT — 10:30 ET':'ENTRY LEVEL REACHED',today.minutes>=630?'10:30 ET time exit reached. Manage any remaining position at your broker.':'Entry price reached after confirmation. Verify any fill at your broker; no retry.',{plan:triggered});
  if(today.minutes>=630)return result('DONE FOR DAY','10:30 ET cutoff and time exit reached. Cancel unfilled orders; close any remaining failure position.');
  if(!fresh)return result('UNAVAILABLE — COLLECTOR STALE','Fresh collector data is required for an actionable entry plan.');
  if(plan){
    if(now>=Date.parse(plan.expiresAt))return result('DONE FOR DAY','Unfilled entry expired. No retry.');
    const live=day.find(b=>Date.parse(b.t)===Date.parse(plan.confirmedAt));
    if((live&&reached(live))||(Number.isFinite(currentPrice)&&(plan.side==='LONG'?currentPrice>=plan.entry:currentPrice<=plan.entry)))return result('ENTRY LEVEL REACHED','Price touched entry; verify any broker fill. No retry.',{plan});
    return result(plan.side+' ENTRY','Confirmed failed-breakout retest. Entry expires after the next candle; time exit 10:30 ET.',{plan});
  }
  return result(failed?'WAIT — FAILURE RETEST':breakout?'WAIT — RETURN INSIDE':'WAIT — NO BREAK',failed?'Wait for a later retest of the failed boundary, closing inside with NY VWAP confirmation.':breakout?'Wait for a later close strictly inside the opening range.':'Wait for the first completed close outside the opening range.');
}
module.exports={buildMESFailureStrategy};
