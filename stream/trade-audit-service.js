// Private, read-only ProjectX collector. Trading endpoints are deliberately absent.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {DateTime}=require('luxon');
const {reconstruct,auditDay,sessionDate,sessionBounds,complianceSummary,DEFAULT_POLICY,cash,symbolOf}=require('./trade-audit-engine');
const {buildIBStrategy}=require('./ib-strategy');
const {buildORBStrategy}=require('./orb-strategy');
const {buildMESFailureStrategy}=require('./mes-failure-strategy');
const READ_PATHS=new Set(['/api/Account/search','/api/Trade/search','/api/Order/search','/api/Order/searchOpen','/api/Position/searchOpen','/api/Contract/searchById']);
const read=file=>{try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch{return null;}};
const lines=file=>{try{return fs.readFileSync(file,'utf8').split('\n').filter(Boolean).map(x=>JSON.parse(x));}catch(e){if(e.code==='ENOENT')return [];throw new Error('Persisted audit log requires recovery; collection stopped.');}};
function atomic(file,value){fs.mkdirSync(path.dirname(file),{recursive:true});fs.writeFileSync(file+'.tmp',JSON.stringify(value));fs.renameSync(file+'.tmp',file);}
const hash=value=>crypto.createHash('sha256').update(JSON.stringify(value)??'null').digest('hex');
function append(file,row){fs.mkdirSync(path.dirname(file),{recursive:true});fs.appendFileSync(file,JSON.stringify(row)+'\n',{mode:0o600});}
const validDate=d=>/^\d{4}-\d{2}-\d{2}$/.test(String(d))&&DateTime.fromISO(d).isValid;
function compact(p,recordedAt){
  const at=Date.parse(recordedAt),closed=(rows,ms)=>(rows||[]).filter(b=>Date.parse(b.t)+ms<=at);
  const profiles=Object.fromEntries(Object.entries(p.profiles||{}).map(([k,v])=>[k,v?Object.fromEntries(Object.entries(v).filter(([key])=>!['byPrice','rows','bins','volumeAtPrice'].includes(key))):null]));
  return {recordedAt,sourceGeneratedAt:p.generatedUtc,market:p.marketSymbol,contract:p.contract,sourceFresh:Boolean(p.diagnostics?.relayFresh),currentPrice:p.currentPrice,
    flow:{globexDelta:p.flow?.currentGlobexDelta,globexCvd:p.flow?.currentGlobexCvd,rthDelta:p.flow?.currentRthDelta,rthCvd:p.flow?.currentRthCvd,oneMin:p.flow?.oneMin?.slice(-10),fiveMin:p.flow?.fiveMin?.slice(-10)},
    vwap:p.vwap,levels:p.levels,profiles,volatility:p.volatility,marketStructure:p.marketStructure,signal:p.signal,analysis:p.analysis,edgefulContext:p.edgefulContext,
    orderBlocks:p.orderBlocks,trapCandidates:p.trapCandidates,confirmedTraps:p.confirmedTraps,absorptionZones:p.absorptionZones,icebergs:p.icebergs,restingLiquidity:p.restingLiquidity,gaps:p.gaps,orb:p.orb,middayOrb:p.middayOrb,closePressure:p.closePressure,
    ibStrategy:p.ibStrategy,orbStrategy:p.orbStrategy,failureStrategy:p.failureStrategy,
    // Complete price history is archived separately once per timestamp.
    // Snapshots only need a bounded trailing window for entry-time direction.
    bars5m:closed(p.bars5m,300000).slice(-12),bars1h:closed(p.bars1h,3600000).slice(-12),bars4h:closed(p.bars4h,14400000).slice(-12),bars1d:closed(p.bars1d,86400000).slice(-12)};
}
function trend(rows,ms,asOf){const b=(rows||[]).filter(x=>Date.parse(x.t)+ms<=asOf).slice(-6);if(b.length<6)return {direction:'UNAVAILABLE',bars:b.length};const up=b.at(-1).c>b[0].c&&b.at(-1).h>b[0].h&&b.at(-1).l>b[0].l,down=b.at(-1).c<b[0].c&&b.at(-1).h<b[0].h&&b.at(-1).l<b[0].l;return {direction:up?'BULLISH':down?'BEARISH':'MIXED',bars:b.length,method:'Six completed bars: close, high and low progression.'};}
function compareIndicators(snap,trade){
  if(!snap)return [];
  const sign=trade.side==='LONG'?1:-1,wanted=sign===1?'BUY':'SELL',items=[];
  const add=(key,label,value,aligned,detail='')=>items.push({key,label,value,status:aligned==null?'CONTEXT':aligned?'ALIGNED':'OPPOSED',detail});
  for(const key of ['globex','rth']){const v=snap.vwap?.[key];if(Number.isFinite(v))add('vwap-'+key,key==='globex'?'Session VWAP':'NY VWAP',v,(trade.entryPrice-v)*sign>=0,`Entry ${trade.entryPrice} vs recorded VWAP; snapshot precedes fill.`);}
  const structure=snap.marketStructure?.state;add('structure','Indicator market structure',structure||'Unavailable',structure==='TREND UP'?sign===1:structure==='TREND DOWN'?sign===-1:null);
  const age=(trade.entryMs-Date.parse(snap.analysis?.asOf))/1000,bias=snap.analysis?.bias;
  add('analysis','Indicator analysis bias',bias||'Unavailable',Number.isFinite(age)&&age<=120?(bias==='BULLISH'?sign===1:bias==='BEARISH'?sign===-1:null):null,Number.isFinite(age)?`Analysis timestamp is ${Math.round(age)} seconds before entry.${age>120?' STALE: excluded from alignment.':''}`:'Analysis timestamp unavailable.');
  const delta=(snap.flow?.fiveMin||[]).filter(b=>Date.parse(b.t)+300000<=trade.entryMs).at(-1)?.delta;
  if(Number.isFinite(delta))add('delta5','Completed 5m delta',delta,delta===0?null:Math.sign(delta)===sign,'Aggressive volume context; not a standalone entry signal.');
  for(const [key,label] of [['globexCvd','Session CVD'],['rthCvd','NY CVD']]){const v=snap.flow?.[key];if(Number.isFinite(v))add(key,label,v,null,'Cumulative sign alone does not prove entry quality.');}
  const near=(snap.levels||[]).filter(l=>Number.isFinite(l.price)).sort((a,b)=>Math.abs(a.price-trade.entryPrice)-Math.abs(b.price-trade.entryPrice)).slice(0,4);
  add('levels','Nearest recorded levels',near.map(l=>`${l.label}: ${l.price} (${cash(Math.abs(l.price-trade.entryPrice))} points)`).join('; ')||'Unavailable',null);
  for(const [key,label] of [['orderBlocks','Order blocks'],['confirmedTraps','Confirmed traps'],['absorptionZones','Absorption zones'],['icebergs','Iceberg candidates']]){
    const rows=snap[key]||[];add(key,label,`${rows.length} recorded`,null,rows.map(z=>`${z.side||z.type||'Zone'} ${z.low??z.zoneLow??z.level??z.price??''}${z.high??z.zoneHigh?'–'+(z.high??z.zoneHigh):''}${z.score!=null?' · score '+z.score:''}`).slice(0,6).join('; ')||'No recorded candidate. Absence is not proof of no market activity.');
  }
  const edge=snap.analysis?.edgeful;add('edgeful','Edgeful qualifiers',edge?.bias||'Unavailable',null,(edge?.checks||[]).map(x=>`${x.label}: ${x.side} · ${x.text}`).join('; ')||'Historical frequencies are not this trade’s win probability.');
  add('signal','Recorded actionable signal',snap.signal?.side||'Unavailable',snap.signal?.side===wanted?true:snap.signal?.side&&snap.signal.side!=='NEUTRAL'?false:null,`Recorded score ${snap.signal?.score??'unavailable'}; signal direction alone does not confirm an entry.`);
  return items;
}
function createAuditService({dir,post,getMarket,getSnapshot,structureAt,mesUrl='',accessToken='',enabled=true,now=()=>Date.now(),pollMs=60000}){
  fs.mkdirSync(dir,{recursive:true});
  const stateFile=path.join(dir,'state.json');
  const state=read(stateFile)||{schema:1,createdAt:new Date(now()).toISOString(),policies:[],accounts:[],coverage:{},lastSync:null,backfillCursor:0};
  const config=()=>state.policies.at(-1)||{...DEFAULT_POLICY};
  let fills=new Map(),orders=new Map(),journals=[],attestations=[],spending=[],plans=[],reviews=[],hypotheses=[],observations=[],marketCache=new Map(),book=null,busy=false,marketBusy=false,backfillBusy=false,historyBusy=false,status={state:'starting',errors:[],lastMarketCapture:null};
  for(const entry of fs.readdirSync(dir)){if(entry.startsWith('executions-')&&entry.endsWith('.jsonl'))for(const e of lines(path.join(dir,entry)))fills.set(`${e.row.accountId}:${e.row.id}`,e.row);if(entry.startsWith('orders-')&&entry.endsWith('.jsonl'))for(const e of lines(path.join(dir,entry)))orders.set(`${e.row.accountId}:${e.row.id}`,e.row);}
  journals=lines(path.join(dir,'journals.jsonl'));attestations=lines(path.join(dir,'attestations.jsonl'));spending=lines(path.join(dir,'spending.jsonl'));plans=lines(path.join(dir,'plans.jsonl'));reviews=lines(path.join(dir,'reviews.jsonl'));hypotheses=lines(path.join(dir,'hypotheses.jsonl'));
  // Corrections are append-only: originals remain available in exports.
  const spendingCorrections=lines(path.join(dir,'spending-corrections.jsonl'));
  function applySpendingCorrections(){
    const original=lines(path.join(dir,'spending.jsonl'));
    spending=original.map(x=>{const edits=spendingCorrections.filter(c=>c.id===x.id);return edits.reduce((v,c)=>({...v,...(c.purchaseDate?{purchaseDate:c.purchaseDate}:{}),...(c.deleted?{deleted:true,correctionReason:c.reason}:{}),...(c.sheetSync?{sheetSync:c.sheetSync}:{})}),x);}).filter(x=>!x.deleted);
  }
  applySpendingCorrections();
  const recentObservationFiles=fs.readdirSync(dir).filter(x=>/^observations-.*\.jsonl$/.test(x)).sort().slice(-3);
  observations=recentObservationFiles.flatMap(file=>lines(path.join(dir,file))).slice(-10000);
  const versions={};for(const file of ['server.js','ib-strategy.js','orb-strategy.js','mes-failure-strategy.js','edgeful-baselines.json'])versions[file]=crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,file))).digest('hex');
  const revision=hash(versions).slice(0,16);atomic(path.join(dir,'strategy-revisions',revision+'.json'),{revision,versions,registeredAt:new Date(now()).toISOString()});
  for(const file of Object.keys(versions)){const dest=path.join(dir,'strategy-revisions',revision+'-'+file);if(!fs.existsSync(dest))fs.copyFileSync(path.join(__dirname,file),dest);}
  const rebuild=()=>book=reconstruct([...fills.values()]);rebuild();
  function observedAccounts(){return state.accounts.filter(x=>x.canTrade||x.isVisible||config().allowedAccountIds.includes(x.id));}
  const save=()=>atomic(stateFile,state);
  async function readApi(route,payload){if(!READ_PATHS.has(route))throw new Error('Endpoint not permitted by read-only audit collector');const j=await post(route,payload);if(j.success!==true)throw new Error('ProjectX read failed');return j;}
  function mergeRows(kind,rows){const target=kind==='executions'?fills:orders;for(const row of rows||[]){if(row.accountId==null||row.id==null)continue;const key=`${row.accountId}:${row.id}`;if(hash(target.get(key))===hash(row))continue;append(path.join(dir,`${kind}-${sessionDate(row.creationTimestamp)}.jsonl`),{receivedAt:new Date(now()).toISOString(),revision:target.has(key)?'correction':'initial',row});target.set(key,row);}rebuild();}
  async function fetchDay(account,date){
    const b=sessionBounds(date),q={accountId:account.id,startTimestamp:b.start,endTimestamp:new Date(Math.min(Date.parse(b.end),now())).toISOString()};
    if(Date.parse(q.endTimestamp)<=Date.parse(q.startTimestamp))return;
    const key=`${account.id}:${date}`;
    try{const a=await readApi('/api/Trade/search',q),o=await readApi('/api/Order/search',q);
      mergeRows('executions',a.trades);mergeRows('orders',o.orders);
      // The API documents no pagination or count cap. Saturated responses remain unverified.
      state.coverage[key]={complete:(a.trades||[]).length<1000&&(o.orders||[]).length<1000,checkedAt:new Date(now()).toISOString(),tradeRows:a.trades?.length||0,orderRows:o.orders?.length||0,error:(a.trades||[]).length>=1000||(o.orders||[]).length>=1000?'Possibly truncated history response; do not grade complete.':null};
    }catch{state.coverage[key]={complete:false,checkedAt:new Date(now()).toISOString(),error:'Account history unavailable; retrying.'};}save();
  }
  async function poll(){
    if(!enabled||busy)return;busy=true;
    try{
      const a=await readApi('/api/Account/search',{onlyActiveAccounts:false});
      // No balances or account IDs are exposed in public routes or logs.
      state.accounts=(a.accounts||[]).map(x=>({id:x.id,name:x.name,balance:x.balance,canTrade:x.canTrade,isVisible:x.isVisible,simulated:x.simulated}));
      if(!state.policies.length){
        const funded=state.accounts.filter(x=>x.isVisible&&/^EXPRESS[-_]|^XFA[-_]/i.test(x.name));
        // User authorized a funded-only reset. Resolve only an unambiguous visible
        // Express account; never guess among multiple funded accounts.
        if(funded.length===1)state.policies.push({...DEFAULT_POLICY,allowedAccountIds:[funded[0].id],effectiveAt:new Date(now()).toISOString(),selectionSource:'Single visible Express Funded account, matched to user-funded-only instruction'});
      }
      const date=sessionDate(now());
      for(const account of observedAccounts()){
        await fetchDay(account,date);
        try{const p=await readApi('/api/Position/searchOpen',{accountId:account.id}),o=await readApi('/api/Order/searchOpen',{accountId:account.id});const observation={recordedAt:new Date(now()).toISOString(),accountId:account.id,balance:account.balance,positions:p.positions||[],openOrders:o.orders||[]};append(path.join(dir,`observations-${sessionDate(now())}.jsonl`),observation);observations.push(observation);observations=observations.slice(-10000);}catch{}
      }
      state.lastSync=new Date(now()).toISOString();status.state='collecting';status.errors=[];save();
    }catch{status.state='unavailable';status.errors=['Execution collector could not synchronize. No compliance pass is issued while data is unavailable.'];}
    finally{busy=false;}
  }
  async function backfill(){
    if(!enabled||backfillBusy||busy||!state.accounts.length)return;backfillBusy=true;
    try{
      const today=DateTime.fromISO(sessionDate(now()));const jobs=[];
      // Prioritize recent days across accounts; continue over restarts, 30 days initially.
      for(let d=1;d<=30;d++)for(const a of observedAccounts())jobs.push([a,today.minus({days:d}).toISODate()]);
      for(const [a,date] of jobs){if(state.coverage[`${a.id}:${date}`]?.complete)continue;await fetchDay(a,date);break;}
    }finally{backfillBusy=false;}
  }
  function marketFile(market,date){return path.join(dir,'market',`${market}-${date}.jsonl`);}
  function contexts(market,date){const key=`${market}:${date}`;if(!marketCache.has(key)){marketCache.set(key,lines(marketFile(market,date)));if(marketCache.size>6)marketCache.delete(marketCache.keys().next().value);}return marketCache.get(key);}
  function archiveBars(market,contractId,rows,minutes=5){
    const buckets=new Map();for(const b of rows||[]){if(Date.parse(b.t)+minutes*60000>now()||!['o','h','l','c','v'].every(k=>Number.isFinite(+b[k])))continue;const date=sessionDate(b.t);if(!buckets.has(date))buckets.set(date,[]);buckets.get(date).push(b);}
    for(const [date,bars] of buckets){const file=path.join(dir,'bars',`${market}-${date}-${minutes}m.json`),existing=read(file)||{market,contractId,minutes,bars:[],source:'Topstep collector',retrievedAt:new Date(now()).toISOString()};const map=new Map(existing.bars.map(b=>[b.t,b]));let changed=false;for(const b of bars){if(!map.has(b.t)){map.set(b.t,b);changed=true;}else if(hash(map.get(b.t))!==hash(b)){append(path.join(dir,'bar-corrections.jsonl'),{observedAt:new Date(now()).toISOString(),market,contractId,old:map.get(b.t),bar:b});map.set(b.t,b);changed=true;}}if(changed){existing.bars=[...map.values()].sort((a,b)=>Date.parse(a.t)-Date.parse(b.t));existing.lastRetrievedAt=new Date(now()).toISOString();atomic(file,existing);}}
  }
  async function capture(){
    if(!enabled||marketBusy)return;marketBusy=true;
    try{
      const inputs=[getMarket()];if(mesUrl){try{const r=await fetch(mesUrl.replace(/\/$/,'')+'/indicator.json',{signal:AbortSignal.timeout(10000)});if(r.ok)inputs.push(await r.json());}catch{}}
      for(const p of inputs){if(!p?.marketSymbol||!p.contract?.id)continue;const at=new Date(now()).toISOString(),row={...compact(p,at),revision};const date=sessionDate(at);append(marketFile(p.marketSymbol,date),row);const key=`${p.marketSymbol}:${date}`;if(marketCache.has(key))marketCache.get(key).push(row);
        archiveBars(p.marketSymbol,p.contract.id,p.bars5m,5);archiveBars(p.marketSymbol,p.contract.id,p.bars1h,60);archiveBars(p.marketSymbol,p.contract.id,p.bars4h,240);archiveBars(p.marketSymbol,p.contract.id,p.bars1d,1440);
        if(p.marketSymbol===getMarket()?.marketSymbol){const s=getSnapshot();archiveBars(p.marketSymbol,p.contract.id,s?.bars?.oneMinRecent,1);archiveBars(p.marketSymbol,p.contract.id,s?.bars?.fiveMinRecent,5);}
        // Frozen automatic morning context is the last capture at/before 08:35 ET,
        // selected only later; no post-cutoff data is ever substituted.
      }
      status.lastMarketCapture=new Date(now()).toISOString();
    }catch{status.errors=[...new Set([...status.errors,'Market-context archive unavailable.'])];}finally{marketBusy=false;}
  }
  function loadBars(market,date,minutes=5){return read(path.join(dir,'bars',`${market}-${date}-${minutes}m.json`))?.bars||[];}
  async function importMarketHistory(){
    if(historyBusy)return;historyBusy=true;
    try{
      const sourceDir=path.join(path.dirname(dir),'backtest-archive-MNQ');
      const manifest=read(path.join(sourceDir,'manifest.json'));
      for(const chunk of manifest?.chunks||[]){if(!chunk.success||!chunk.count)continue;const file=path.join(sourceDir,chunk.filename),record=read(file);if(!record?.success)continue;const key=hash({file,retrievedAt:record.retrievedAt,count:record.bars.length});state.importedMarketChunks??={};if(state.importedMarketChunks[key])continue;archiveBars('MNQ',record.contractId,record.bars,record.unitNumber);state.importedMarketChunks[key]=true;}
      if(mesUrl){
        const response=await fetch(mesUrl.replace(/\/$/,'')+'/backtest-history.json?export=bars',{signal:AbortSignal.timeout(20000)});
        if(response.ok){const data=await response.json();for(const record of data.chunks||[]){if(!record.success||!record.bars?.length)continue;const key=hash({market:'MES',file:record.filename,count:record.bars.length,last:record.last});state.importedMarketChunks??={};if(state.importedMarketChunks[key])continue;archiveBars('MES',record.contractId,record.bars,record.unitNumber);state.importedMarketChunks[key]=true;}}
        const response2=await fetch(mesUrl.replace(/\/$/,'')+'/snapshot.json',{signal:AbortSignal.timeout(15000)});
        if(response2.ok){const p=await response2.json();archiveBars('MES',p.contract?.id,p.bars?.oneMinRecent,1);archiveBars('MES',p.contract?.id,p.bars?.fiveMinRecent,5);}
      }save();
    }catch{status.errors=[...new Set([...status.errors,'Historical market import incomplete; retrying.'])];}finally{historyBusy=false;}
  }
  function preEntry(trade){
    const date=sessionDate(trade.entryMs),rows=contexts(trade.symbol,date),snap=rows.filter(x=>Date.parse(x.recordedAt)<=trade.entryMs).at(-1),age=snap?(trade.entryMs-Date.parse(snap.recordedAt))/1000:null;
    const currentDate=DateTime.fromMillis(trade.entryMs,{zone:'America/New_York'}).toISODate();const cutoff=DateTime.fromISO(currentDate,{zone:'America/New_York'}).set({hour:8,minute:35}).toMillis();
    const morning=rows.filter(x=>Date.parse(x.recordedAt)<=Math.min(cutoff,trade.entryMs)&&Date.parse(x.recordedAt)>=cutoff-5*60000).at(-1)||null;
    const plan=plans.filter(x=>x.date===date&&x.market===trade.symbol&&Date.parse(x.recordedAt)<=trade.entryMs).at(-1)||null;
    const bars=loadBars(trade.symbol,date).filter(b=>Date.parse(b.t)+300000<=trade.entryMs);
    const structure=(bs,at)=>structureAt(bs,at);
    // Replaying price rules uses completed bars only and the SAME strategy code.
    const priceFresh=bars.length>0&&trade.entryMs-(Date.parse(bars.at(-1).t)+300000)<=300000;
    const args={bars,now:trade.entryMs,market:trade.symbol,currentPrice:trade.entryPrice,fresh:priceFresh,structureAt:structure};
    const replay={ib:buildIBStrategy(args),orb:buildORBStrategy(args),failure:buildMESFailureStrategy(args)};
    const tf={m5:trend(bars,300000,trade.entryMs),m15:trend(aggregate(bars,15),900000,trade.entryMs),h1:trend(snap?.bars1h||loadBars(trade.symbol,date,60),3600000,trade.entryMs),h4:trend(snap?.bars4h||loadBars(trade.symbol,date,240),14400000,trade.entryMs),daily:trend(snap?.bars1d||loadBars(trade.symbol,date,1440),86400000,trade.entryMs)};
    return {snapshot:snap||null,snapshotAgeSeconds:age,usableSnapshot:Boolean(snap&&age<=120&&snap.sourceFresh),morning,plan,replayed:replay,timeframes:tf,barCount:bars.length,provenance:snap?'Prospectively recorded indicator state; capture strictly precedes entry.':'Historical bar reconstruction only; missing historical order flow and indicator state stay unavailable.',revision:snap?.revision||revision,replayRevision:revision};
  }
  function aggregate(bars,minutes){const buckets=new Map();for(const b of bars){const t=Math.floor(Date.parse(b.t)/(minutes*60000))*minutes*60000;if(!buckets.has(t))buckets.set(t,[]);buckets.get(t).push(b);}return [...buckets].filter(([,x])=>x.length===minutes/5).map(([t,x])=>({t:new Date(t).toISOString(),o:x[0].o,h:Math.max(...x.map(b=>b.h)),l:Math.min(...x.map(b=>b.l)),c:x.at(-1).c,v:x.reduce((n,b)=>n+b.v,0)}));}
  function analyzeTrade(t){
    const context=preEntry(t),snap=context.usableSnapshot?context.snapshot:null,side=t.side==='LONG'?'BULLISH':'BEARISH',matching=Object.entries(context.timeframes).filter(([,v])=>v.direction===side).map(([k])=>k),opposing=Object.entries(context.timeframes).filter(([,v])=>!['UNAVAILABLE','MIXED',side].includes(v.direction)).map(([k])=>k);
    const postBars=loadBars(t.symbol,sessionDate(t.entryMs)).filter(b=>Date.parse(b.t)>=t.entryMs&&Date.parse(b.t)+300000<=(t.exitMs||now())),sign=t.side==='LONG'?1:-1;
    const mfe=postBars.length?Math.max(0,...postBars.map(b=>(sign===1?b.h-t.entryPrice:t.entryPrice-b.l))):null,mae=postBars.length?Math.max(0,...postBars.map(b=>(sign===1?t.entryPrice-b.l:b.h-t.entryPrice))):null;
    const comparison=Object.entries(context.replayed).filter(([key])=>key!=='failure'||t.symbol==='MES').filter(([key])=>key!=='ib'||t.symbol==='MNQ').map(([key,x])=>({strategy:key,state:x.state,side:x.side,eligible:x.eligible,plan:x.plan,reason:x.reason,classification:x.side&&x.side!==t.side?'DIRECTION CONFLICT':x.plan&&x.side===t.side?'CANDIDATE — confirm entry timing / price':'NO CONFIRMED MATCH'}));
    const notes=[];notes.push(matching.length?`Direction aligned with completed-bar progression on ${matching.join(', ')}.`:'No completed-bar directional alignment was established.');if(opposing.length)notes.push(`Direction opposed ${opposing.join(', ')}; review whether this was a deliberate countertrend trade.`);
    if(snap?.analysis?.bias)notes.push(`Recorded analysis bias: ${typeof snap.analysis.bias==='string'?snap.analysis.bias:JSON.stringify(snap.analysis.bias)}.`);
    if(!snap)notes.push('Exact pre-entry flow, VWAP, traps, order blocks, iceberg and Edgeful signal state cannot be verified; later indicator values are excluded.');
    if(t.addsToLosing.length)notes.push('Added while price was adverse to average entry; discipline review takes priority over setup quality.');
    if(mfe!==null)notes.push(`Full 5-minute bars entirely inside the holding period show at least ${cash(mfe)} favorable and ${cash(mae)} adverse points. Entry/exit boundary bars are excluded; these are lower bounds.`);
    const journal=journals.filter(j=>j.tradeId===t.id).at(-1)||null;
    return {...t,context,comparison,indicatorChecks:compareIndicators(snap,t),analysis:{notes,mfePointsLowerBound:mfe,maePointsLowerBound:mae,holdingSeconds:t.complete?(t.exitMs-t.entryMs)/1000:null,method:'Rules-based evidence review; no claim of causal edge or guaranteed outcome.'},journal,journalRequired:needsJournal(t),review:reviews.filter(r=>r.tradeId===t.id).at(-1)||null};
  }
  function policyFor(date){const end=Date.parse(sessionBounds(date).end);return [...state.policies].reverse().find(p=>Date.parse(p.effectiveAt)<end)||{...DEFAULT_POLICY};}
  function allAudits(){const dates=[...new Set(book.fills.map(f=>sessionDate(f.at)))].sort().slice(-90);return dates.flatMap(date=>[...new Set(book.fills.filter(f=>sessionDate(f.at)===date).map(f=>f.accountId))].map(accountId=>auditDay({date,accountId,book,policy:policyFor(date),attestations,spending,coverage:state.coverage[`${accountId}:${date}`]||{},now:now()})));}
  function needsJournal(t){return t.complete&&t.exitMs>=Date.parse(state.createdAt)&&!journals.some(j=>j.tradeId===t.id);}
  function data(){const audits=allAudits();return {generatedAt:new Date(now()).toISOString(),status,accounts:observedAccounts(),policy:config(),policyHistory:state.policies,collector:{lastSync:state.lastSync,startedAt:state.createdAt,historyDays:30,coverage:state.coverage,fillCount:fills.size,orderCount:orders.size,revision},audits,compliance:complianceSummary(audits),trades:book.trades.slice(-500).reverse().map(t=>({...t,fills:undefined,journal:journals.filter(j=>j.tradeId===t.id).at(-1)||null,journalRequired:needsJournal(t)})),journalsPending:book.trades.filter(needsJournal).length,attestations,spending,plans,reviews,strategies:strategyLibrary(),warnings:book.warnings,privacy:'Owner-private Site. This API requires a dedicated server credential. No trading records appear on public indicator routes.'};}
  function strategyLibrary(){const latest=new Map();for(const j of journals)latest.set(j.tradeId,j);return [{id:'orb',name:'15-minute ORB break / retest',markets:['MNQ','MES'],rules:'09:30–09:45 ET range; close beyond boundary, first retest and completed rejection; VWAP and structure checks; entry cutoff 10:15, time exit 10:30 ET.',file:'orb-strategy.js'}, {id:'ib',name:'MNQ Initial Balance break / retest',markets:['MNQ'],rules:'09:30–10:30 ET IB; 0.60–0.89% size bucket; VWAP and structure alignment; breakout then distinct retest; no opposite retry.',file:'ib-strategy.js'}, {id:'failure',name:'MES Opening Range Failure',markets:['MES'],rules:'09:30–09:45 ET ORB; failed breakout and retest into range; next-bar entry expiry; 10:30 ET cutoff / exit.',file:'mes-failure-strategy.js'}, {id:'indicator-context',name:'Indicator confluence and Edgeful context',markets:['MNQ','MES'],rules:'Recorded market structure, analysis setups, levels, VWAPs, profiles, deltas/CVD, order blocks, traps, absorption, iceberg, liquidity, gaps and Edgeful context. Evidence dimensions, not independent validated strategies.',file:'server.js'},...hypotheses.map(h=>({...h,name:h.title,file:null}))].map(s=>{const r=s.revision||revision,tags=[...latest.values()].filter(j=>j.strategy===s.id&&j.revision===r),closed=book.trades.filter(t=>t.complete&&tags.some(j=>j.tradeId===t.id));const wins=closed.filter(t=>t.net>0).length;return {...s,revision:r,sourceHash:s.file?versions[s.file]:hash(s.rules),status:'RESEARCH — NOT VALIDATED',taggedTrades:closed.length,winRate:closed.length?wins/closed.length:null,net:cash(closed.reduce((n,t)=>n+t.net,0)),sampleRule:'Latest journal tag only, current rule revision only. A label is self-reported; a candidate is not proof of compliance. Separate market, regime and discipline. Reserve later sessions for out-of-sample validation.'};});}
  function requireToken(req,res,next){res.set('Cache-Control','no-store');res.set('X-Robots-Tag','noindex, nofollow');const supplied=String(req.headers.authorization||'');const expected='Bearer '+accessToken;const a=Buffer.from(supplied),b=Buffer.from(expected);if(!accessToken||a.length!==b.length||!crypto.timingSafeEqual(a,b))return res.status(401).json({error:'Unauthorized'});next();}
  const text=(value,max=4000)=>typeof value==='string'?value.trim().slice(0,max):'';
  function attach(app){
    app.use('/audit',requireToken);
    app.get('/audit/status',(req,res)=>res.json({status,lastSync:state.lastSync,fillCount:fills.size,accountCount:state.accounts.length}));
    app.get('/audit/data',(req,res)=>{try{res.json(data());}catch{res.status(503).json({error:'Audit data unavailable'});}});
    app.get('/audit/trade/:id',(req,res)=>{const t=book.trades.find(t=>t.id===req.params.id);if(!t)return res.status(404).json({error:'Trade not found'});try{res.json({...analyzeTrade(t),chartBars:loadBars(t.symbol,sessionDate(t.entryMs)).filter(b=>Date.parse(b.t)>=t.entryMs-3600000&&Date.parse(b.t)<=Math.min(now(),(t.exitMs||t.entryMs)+3600000)).slice(0,300)});}catch{res.status(503).json({error:'Trade context unavailable'});}});
    app.post('/audit/config',(req,res)=>{
      const id=Number(req.body.accountId);if(!observedAccounts().some(a=>a.id===id))return res.status(400).json({error:'Select your current funded account.'});
      if(config().allowedAccountIds.length&&config().allowedAccountIds[0]!==id)return res.status(409).json({error:'Funded account already selected. Account changes require a reviewed policy revision; they cannot erase the reset.'});
      const primary=observedAccounts().find(a=>a.id===id);
      if(!/^EXPRESS[-_]|^XFA[-_]/i.test(primary.name))return res.status(400).json({error:'The primary account must be your funded account.'});
      const raw=req.body.copyAccountIds;
      if(raw!==undefined&&(!Array.isArray(raw)||raw.length>4||raw.some(x=>!Number.isSafeInteger(x))||new Set(raw).size!==raw.length))return res.status(400).json({error:'Choose up to four distinct recorded combine accounts.'});
      const copies=raw===undefined?(config().copyAccountIds||[]):raw;
      if(copies.some(copy=>copy===id||!observedAccounts().some(a=>a.id===copy&&a.isVisible&&!/^EXPRESS[-_]|^XFA[-_]/i.test(a.name))))return res.status(400).json({error:'Copy accounts must be visible combines, separate from the funded account.'});
      const allowed=[id,...[...copies].sort((a,b)=>a-b)],previous=config();
      if(!state.policies.length||JSON.stringify(allowed)!==JSON.stringify(previous.allowedAccountIds)){
        const recordedAt=new Date(now()).toISOString();
        state.policies.push({...previous,id:'discipline-reset-'+crypto.randomUUID(),effectiveAt:recordedAt,resetStartedAt:previous.resetStartedAt||previous.effectiveAt||recordedAt,allowedAccountIds:allowed,primaryAccountId:id,copyAccountIds:allowed.slice(1),selectionSource:copies.length?'Owner-authorized funded account with copy-traded combines':'Owner-authorized funded account',revisionReason:text(req.body.reason,500)||'Authorized account selection revised'});save();
      }res.json({policy:config()});
    });
    app.post('/audit/journal',(req,res)=>{
      const b=req.body,t=book.trades.find(t=>t.id===b.tradeId);if(!t||!t.complete)return res.status(400).json({error:'A completed recorded trade is required.'});
      const required=['entryReason','exitReason','emotion','ruleFollowed','lesson'];if(required.some(k=>text(b[k]).length<3))return res.status(400).json({error:'Complete every journal prompt.'});
      const j={id:crypto.randomUUID(),tradeId:t.id,recordedAt:new Date(now()).toISOString(),strategy:text(b.strategy,100)||'unclassified',revision:text(b.revision,100)||revision,...Object.fromEntries(required.map(k=>[k,text(b[k])]))};append(path.join(dir,'journals.jsonl'),j);journals.push(j);res.json({ok:true,journal:j});
    });
    app.post('/audit/attestation',(req,res)=>{const b=req.body;if(!validDate(b.date)||!state.accounts.some(a=>a.id===Number(b.accountId))||!['risk-lock','no-spending','session-close'].includes(b.kind))return res.status(400).json({error:'Invalid confirmation'});if(b.kind==='session-close'&&book.trades.some(t=>needsJournal(t)&&t.accountId===Number(b.accountId)&&sessionDate(t.exitMs)===b.date))return res.status(409).json({error:'Complete required trade journals before closing the review.'});const a={date:b.date,accountId:Number(b.accountId),kind:b.kind,recordedAt:new Date(now()).toISOString(),source:'SELF-REPORTED — NOT API VERIFIED'};append(path.join(dir,'attestations.jsonl'),a);attestations.push(a);res.json({ok:true,attestation:a});});
    app.post('/audit/spending',(req,res)=>{
      const b=req.body,amount=Number(b.amount),description=text(b.description,500),requestId=text(b.requestId,100),purchaseDate=b.purchaseDate;
      if(!Number.isFinite(amount)||amount<0||!description||(purchaseDate&&!validDate(purchaseDate)))return res.status(400).json({error:'Enter a valid amount, description and purchase date.'});
      const existing=requestId&&spending.find(x=>x.requestId===requestId);
      if(existing){if(existing.amount!==amount||existing.description!==description||existing.purchaseDate!==purchaseDate)return res.status(409).json({error:'Submission ID already used for a different purchase.'});return res.json({ok:true,spending:existing,duplicate:true});}
      const x={id:crypto.randomUUID(),amount,description,recordedAt:new Date(now()).toISOString(),source:'self-reported',...(requestId?{requestId}:{}),...(purchaseDate?{purchaseDate}:{})};
      append(path.join(dir,'spending.jsonl'),x);spending.push(x);res.json({ok:true,spending:x});
    });
    app.post('/audit/spending/correct',(req,res)=>{
      const b=req.body,x=spending.find(x=>x.id===b.id),reason=text(b.reason,500);
      if(!x)return res.status(404).json({error:'Active spending entry not found.'});
      if(reason.length<3||(!b.deleted&&!b.purchaseDate&&!b.sheetSync)||(b.purchaseDate&&!validDate(b.purchaseDate)))return res.status(400).json({error:'Specify a correction and reason.'});
      const c={id:x.id,reason,recordedAt:new Date(now()).toISOString(),...(b.deleted===true?{deleted:true}:{}),...(b.purchaseDate?{purchaseDate:b.purchaseDate}:{}),...(b.sheetSync?{sheetSync:b.sheetSync}:{})};
      append(path.join(dir,'spending-corrections.jsonl'),c);spendingCorrections.push(c);applySpendingCorrections();res.json({ok:true,spending:spending.find(v=>v.id===x.id)||null});
    });
    app.post('/audit/plan',(req,res)=>{const b=req.body;if(!validDate(b.date)||!['MNQ','MES'].includes(b.market)||['bias','setups','invalidation','mentalState'].some(k=>text(b[k]).length<3))return res.status(400).json({error:'Complete the pre-session plan and readiness prompts.'});const p={id:crypto.randomUUID(),date:b.date,market:b.market,recordedAt:new Date(now()).toISOString(),revision,...Object.fromEntries(['bias','setups','invalidation','mentalState'].map(k=>[k,text(b[k])]))};append(path.join(dir,'plans.jsonl'),p);plans.push(p);res.json({ok:true});});
    app.post('/audit/review',(req,res)=>{const b=req.body;if(!book.trades.some(t=>t.id===b.tradeId)||text(b.analysis).length<20)return res.status(400).json({error:'A recorded trade and substantive analysis are required.'});const r={id:crypto.randomUUID(),tradeId:b.tradeId,analysis:text(b.analysis,20000),author:text(b.author,100)||'review',recordedAt:new Date(now()).toISOString()};append(path.join(dir,'reviews.jsonl'),r);reviews.push(r);res.json({ok:true});});
    app.post('/audit/strategy',(req,res)=>{const b=req.body,title=text(b.title,150),rules=text(b.rules,5000),invalidation=text(b.invalidation,3000);if(title.length<3||rules.length<20||invalidation.length<10||!['MNQ','MES','BOTH'].includes(b.market))return res.status(400).json({error:'Specify the strategy name, repeatable rules, invalidation and market.'});const r=hash({title,rules,invalidation,market:b.market}).slice(0,16),h={id:'research-'+r,revision:r,title,rules,invalidation,markets:b.market==='BOTH'?['MNQ','MES']:[b.market],recordedAt:new Date(now()).toISOString(),status:'RESEARCH — NOT VALIDATED'};if(!hypotheses.some(x=>x.id===h.id)){append(path.join(dir,'hypotheses.jsonl'),h);hypotheses.push(h);}res.json({ok:true,strategy:h});});
    app.get('/audit/export',(req,res)=>{res.set('Content-Disposition','attachment; filename="trader-pip-audit-export.json"');res.json({...data(),spendingOriginals:lines(path.join(dir,'spending.jsonl')),spendingCorrections,executions:[...fills.values()],orders:[...orders.values()],journals,observations:observations.slice(-1000)});});
  }
  function start(){if(!enabled)return;setTimeout(()=>poll().catch(()=>{}),1000);setTimeout(()=>capture().catch(()=>{}),2000);setTimeout(()=>importMarketHistory().catch(()=>{}),5000);const timers=[setInterval(()=>poll().catch(()=>{}),pollMs),setInterval(()=>capture().catch(()=>{}),60000),setInterval(()=>backfill().catch(()=>{}),10000),setInterval(()=>importMarketHistory().catch(()=>{}),300000)];timers.forEach(t=>t.unref());}
  return {attach,start,poll,capture,data,analyzeTrade,readApi,backfill};
}
module.exports={createAuditService,compact,trend,compareIndicators,READ_PATHS};
