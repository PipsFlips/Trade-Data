// Pure execution reconstruction and discipline checks. No network or order transmission.
const {DateTime}=require('luxon');
const ZONE='America/New_York';
function sessionDate(at){const t=DateTime.fromMillis(typeof at==='number'?at:Date.parse(at),{zone:ZONE});return (t.hour>=18?t.plus({days:1}):t).toISODate();}
function sessionBounds(date){const end=DateTime.fromISO(date,{zone:ZONE}).set({hour:17});return {start:end.minus({days:1}).set({hour:18}).toUTC().toISO(),end:end.toUTC().toISO()};}
function symbolOf(id){return String(id||'').split('.')[3]||'UNKNOWN';}
function cash(n){return Math.round(n*100)/100;}
function normalizeFill(row){
  const at=Date.parse(row.creationTimestamp),size=Number(row.size),price=Number(row.price);
  if(row.id==null||row.accountId==null||!Number.isFinite(at)||!Number.isInteger(size)||size<=0||!Number.isFinite(price)||![0,1].includes(row.side))return null;
  return {...row,at,time:new Date(at).toISOString(),symbol:symbolOf(row.contractId),size,price,fees:Number(row.fees)||0,profitAndLoss:row.profitAndLoss==null?null:Number(row.profitAndLoss)};
}
function reconstruct(rows){
  const unique=new Map();let invalid=0;
  for(const r of rows){const f=normalizeFill(r);if(!f){invalid++;continue;}unique.set(`${f.accountId}:${f.id}`,f);}
  const fills=[...unique.values()].filter(f=>!f.voided).sort((a,b)=>a.at-b.at||Number(a.id)-Number(b.id));
  const books=new Map(),trades=[],entries=[],exposure=[],warnings=[];
  function newTrade(f,qty,fee){return {id:`${f.accountId}:${f.contractId}:${f.id}`,accountId:f.accountId,contractId:f.contractId,symbol:f.symbol,side:f.side===0?'LONG':'SHORT',entryAt:f.time,entryMs:f.at,exitAt:null,exitMs:null,entryPrice:f.price,maxSize:qty,quantity:qty,entryQuantity:qty,entryValue:f.price*qty,exitQuantity:0,exitValue:0,gross:0,fees:fee,net:null,fills:[],entryOrders:[],addsToLosing:[],complete:false};}
  for(const f of fills){
    const key=`${f.accountId}:${f.contractId}`,sign=f.side===0?1:-1;let b=books.get(key),remaining=f.size;
    if(!b&&f.profitAndLoss!==null){warnings.push({accountId:f.accountId,at:f.time,code:'MISSING_OPENING_FILL',fillId:f.id});continue;}
    if(b&&b.sign!==sign){
      const close=Math.min(b.qty,remaining),fee=f.fees*(close/f.size);
      b.trade.gross+=f.profitAndLoss==null?0:f.profitAndLoss;
      if(f.profitAndLoss==null)warnings.push({accountId:f.accountId,at:f.time,code:'CLOSING_PNL_MISSING',fillId:f.id});
      b.trade.fees+=fee;b.trade.exitQuantity+=close;b.trade.exitValue+=f.price*close;
      b.trade.fills.push({...f,role:'EXIT',allocatedSize:close});b.qty-=close;remaining-=close;
      if(b.qty===0){Object.assign(b.trade,{exitAt:f.time,exitMs:f.at,complete:true,exitPrice:b.trade.exitValue/b.trade.exitQuantity,entryPrice:b.trade.entryValue/b.trade.entryQuantity,net:cash(b.trade.gross-b.trade.fees),gross:cash(b.trade.gross),fees:cash(b.trade.fees)});trades.push(b.trade);books.delete(key);b=null;}
    }
    if(remaining>0){
      const fee=f.fees*(remaining/f.size),adding=Boolean(b);
      if(!b){b={sign,qty:remaining,average:f.price,trade:newTrade(f,remaining,fee)};books.set(key,b);}
      else {if((f.price-b.average)*sign<0)b.trade.addsToLosing.push({at:f.time,orderId:f.orderId,price:f.price});b.average=(b.average*b.qty+f.price*remaining)/(b.qty+remaining);b.qty+=remaining;b.trade.entryQuantity+=remaining;b.trade.entryValue+=f.price*remaining;b.trade.fees+=fee;}
      const orderKey=f.orderId!=null?String(f.orderId):`fill-${f.id}`;
      const priorEntry=entries.find(e=>e.tradeId===b.trade.id&&e.orderKey===orderKey);
      if(priorEntry){priorEntry.size+=remaining;priorEntry.positionSize=b.qty;}
      else entries.push({tradeId:b.trade.id,accountId:f.accountId,symbol:f.symbol,contractId:f.contractId,at:f.at,time:f.time,orderKey,size:remaining,positionSize:b.qty,adding,price:f.price});
      b.trade.maxSize=Math.max(b.trade.maxSize,b.qty);b.trade.fills.push({...f,role:adding?'ADD':'ENTRY',allocatedSize:remaining});b.trade.entryOrders=[...new Set([...b.trade.entryOrders,orderKey])];
    }
    const current=[...books.values()].filter(x=>x.trade.accountId===f.accountId);
    exposure.push({accountId:f.accountId,at:f.at,time:f.time,totalContracts:current.reduce((n,x)=>n+x.qty,0),positions:current.map(x=>({symbol:x.trade.symbol,size:x.qty}))});
  }
  for(const b of books.values()){b.trade.quantity=b.qty;b.trade.entryPrice=b.trade.entryValue/b.trade.entryQuantity;b.trade.net= cash(b.trade.gross-b.trade.fees);trades.push(b.trade);}
  return {fills,trades:trades.sort((a,b)=>a.entryMs-b.entryMs),entries,exposure,warnings,invalid};
}
const DEFAULT_POLICY={id:'discipline-reset-v1',effectiveAt:null,allowedAccountIds:[],maxEntries:3,maxRisk:150,dailyLoss:300,dailyProfit:300,maxContracts:5,allowedSymbols:['MNQ'],cooldownMinutes:15,maxLosses:2,spendingLimit:0,targetSessions:20};
function auditDay({date,accountId,book,policy=DEFAULT_POLICY,attestations=[],spending=[],coverage={},now=Date.now(),riskObservations=[]}){
  const bounds=sessionBounds(date),start=Date.parse(bounds.start),end=Date.parse(bounds.end),effective=Date.parse(policy.effectiveAt),active=Number.isFinite(effective)&&end>effective;
  const rows=book.fills.filter(f=>f.accountId===accountId&&sessionDate(f.at)===date),entries=book.entries.filter(e=>e.accountId===accountId&&sessionDate(e.at)===date);
  const closed=book.trades.filter(t=>t.accountId===accountId&&t.complete&&sessionDate(t.exitMs)===date);
  const open=book.trades.filter(t=>t.accountId===accountId&&!t.complete&&t.entryMs<end);
  const checks=[];const add=(key,label,status,detail)=>checks.push({key,label,status,detail});
  const first=entries[0];let running=0,peak=0,dd=0,min=0;
  for(const f of rows){running+=(f.profitAndLoss??0)-f.fees;peak=Math.max(peak,running);dd=Math.max(dd,peak-running);min=Math.min(min,running);}
  const entryAfter=(pred)=>entries.filter(e=>pred(e)).map(e=>e.time);
  const priorLoss=e=>closed.filter(t=>t.exitMs<e.at&&t.net<0).at(-1);
  const cooldown=entryAfter(e=>{const l=priorLoss(e);return l&&e.at-l.exitMs<policy.cooldownMinutes*60000;});
  const increased=entryAfter(e=>{const l=priorLoss(e);return l&&e.symbol===l.symbol&&e.positionSize>l.maxSize;});
  const afterLosses=entryAfter(e=>closed.filter(t=>t.exitMs<e.at&&t.net<0).length>=policy.maxLosses);
  const afterProfit=entryAfter(e=>rows.filter(f=>f.at<e.at).reduce((n,f)=>n+(f.profitAndLoss??0)-f.fees,0)>=policy.dailyProfit);
  const afterDailyLoss=entryAfter(e=>rows.filter(f=>f.at<e.at).reduce((n,f)=>n+(f.profitAndLoss??0)-f.fees,0)<=-policy.dailyLoss);
  const exposures=book.exposure.filter(e=>e.accountId===accountId&&sessionDate(e.at)===date);
  const maxSize=Math.max(0,...exposures.map(e=>e.totalContracts));
  const notes=attestations.filter(a=>a.date===date&&a.accountId===accountId);
  const lock=notes.find(a=>a.kind==='risk-lock'&&a.recordedAt&&Date.parse(a.recordedAt)<(first?.at??end));
  const purchases=spending.filter(x=>sessionDate(x.recordedAt)===date);
  const complete=coverage.complete===true&&!book.warnings.some(w=>w.accountId===accountId&&sessionDate(w.at)===date)&&book.invalid===0;
  const known=(violation)=>violation?'FAIL':complete?'PASS':'UNKNOWN';
  add('entries','Entry orders',known(entries.length>policy.maxEntries),`${entries.length} / ${policy.maxEntries}; additions count, partial fills of one order count once.`);
  add('contracts','Maximum total contracts',known(maxSize>policy.maxContracts),`${maxSize} / ${policy.maxContracts}; concurrent positions included.`);
  add('symbols','Permitted instruments',known(entries.some(e=>!policy.allowedSymbols.includes(e.symbol))),policy.allowedSymbols.join(', '));
  add('account','Authorized account',policy.allowedAccountIds.length?known(!policy.allowedAccountIds.includes(accountId)):'UNKNOWN',policy.allowedAccountIds.length?'Funded-only reset.':'Select the funded account to establish the allowlist.');
  add('cooldown','15-minute cooldown',known(cooldown.length>0),cooldown.length?cooldown.join(', '):'No observed entry inside cooldown after a completed net losing position.');
  add('size-increase','Size increase after loss',known(increased.length>0),increased.length?increased.join(', '):'Compared with the last losing position in the same instrument.');
  add('second-loss','No entry after two losses',known(afterLosses.length>0),afterLosses.length?afterLosses.join(', '):`${closed.filter(t=>t.net<0).length} completed losing positions.`);
  add('profit','No entry after profit ceiling',known(afterProfit.length>0),afterProfit.length?afterProfit.join(', '):`Net realized ceiling +$${policy.dailyProfit}.`);
  add('daily-loss-entry','No entry after daily loss limit',known(afterDailyLoss.length>0),afterDailyLoss.length?afterDailyLoss.join(', '):`No new entry after net realized P&L reaches -$${policy.dailyLoss}.`);
  add('realized-loss','Realized daily loss',known(min<-policy.dailyLoss),`Lowest net realized P&L $${cash(min)}; limit -$${policy.dailyLoss}. Peak-to-trough realized drawdown $${cash(dd)}.`);
  add('intraday-risk','Full intraday loss / drawdown','UNKNOWN','Fills do not establish every unrealized P&L high/low. Periodic balance/position observations are sampled, not an exact equity curve.');
  const risks=riskObservations.filter(x=>x.accountId===accountId&&sessionDate(x.at)===date);
  add('stop-risk','Maximum defined position risk',risks.some(x=>x.risk>policy.maxRisk)?'FAIL':'UNKNOWN',risks.length?`Observed maximum $${Math.max(...risks.map(x=>x.risk))}; stop coverage still requires entry-time evidence.`:'Final order history cannot prove the stop that existed at entry. Capturing open-order observations prospectively.');
  add('adding-loser','No adding to a losing position',known(book.trades.some(t=>t.accountId===accountId&&t.addsToLosing.some(a=>sessionDate(a.at)===date))),'Evaluated at addition price versus then-current average entry.');
  add('risk-lock','Risk Lock before first entry','UNKNOWN',lock?'Self-confirmed before entry; not independently verified through the API.':'No pre-entry confirmation recorded. Platform Risk Lock is not exposed by the documented API.');
  add('spending','No account purchases / resets',purchases.some(x=>x.amount>0)?'FAIL':'UNKNOWN',purchases.length?`Logged spending $${cash(purchases.reduce((n,x)=>n+x.amount,0))}.`:'No logged purchase; trade data does not verify outside spending.');
  add('coverage','Execution history coverage',complete?'PASS':'UNKNOWN',coverage.error||'Successful bounded account history fetch required; missing opening fills prevent a pass.');
  const failures=checks.filter(c=>c.status==='FAIL');
  const automated=checks.filter(c=>!['risk-lock','spending','intraday-risk','stop-risk'].includes(c.key));
  const automaticResult=failures.length?'FAIL':automated.every(c=>c.status==='PASS')?'PASS':'UNKNOWN';
  const result=!rows.length?'NO TRADES':!active?'BASELINE':failures.length?'FAIL':checks.some(c=>c.status==='UNKNOWN')?'UNVERIFIED':'PASS';
  return {date,accountId,policyId:policy.id,active,result,automaticResult,finalized:now>=end,checks,failures,entries:entries.length,closedTrades:closed.length,openTrades:open.length,maxContracts:maxSize,net:cash(running),gross:cash(rows.reduce((n,f)=>n+(f.profitAndLoss??0),0)),fees:cash(rows.reduce((n,f)=>n+f.fees,0)),realizedDrawdown:cash(dd),minRealized:cash(min),firstEntry:first?.time??null,bounds};
}
function complianceSummary(audits){
  const dates=[...new Set(audits.filter(a=>a.active&&a.result!=='NO TRADES').map(a=>a.date))].sort();let streak=0;const days=[];
  for(const date of dates){const rows=audits.filter(a=>a.date===date&&a.active&&a.result!=='NO TRADES');const result=rows.some(a=>a.result==='FAIL')?'FAIL':rows.every(a=>a.result==='PASS'&&a.finalized)?'PASS':'UNVERIFIED';streak=result==='PASS'?streak+1:0;days.push({date,result,finalized:rows.every(a=>a.finalized)});}
  const passed=days.filter(d=>d.result==='PASS').length;
  const violations=days.filter(d=>d.result==='FAIL'),recent=days.slice(-10).filter(d=>d.result==='FAIL').length;
  return {streak,passed,total:days.length,rate:days.length?passed/days.length:null,days,violations:violations.length,recommendedShutdownDays:recent>=2?5:days.at(-1)?.result==='FAIL'?1:0,enforcement:'Recommendation only. Apply native Topstep lockout; this service cannot lock or trade accounts.'};
}
module.exports={sessionDate,sessionBounds,symbolOf,cash,normalizeFill,reconstruct,auditDay,complianceSummary,DEFAULT_POLICY};
