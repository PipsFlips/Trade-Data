const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const express=require('express');
const {reconstruct,auditDay,sessionDate,sessionBounds,DEFAULT_POLICY}=require('./trade-audit-engine');
const {createAuditService,compact,READ_PATHS}=require('./trade-audit-service');
const mk=(id,minutes,side,size,price,pnl=null,extra={})=>({id,accountId:1,contractId:'CON.F.US.MNQ.Z26',creationTimestamp:new Date(Date.parse('2026-10-02T14:00:00Z')+minutes*60000).toISOString(),side,size,price,profitAndLoss:pnl,fees:1,orderId:id,voided:false,...extra});
const policy={...DEFAULT_POLICY,effectiveAt:'2026-10-01T00:00:00Z',allowedAccountIds:[1]};
function audit(rows){return auditDay({date:'2026-10-02',accountId:1,book:reconstruct(rows),policy,coverage:{complete:true},now:Date.parse('2026-10-03T00:00:00Z')});}
test('partials of one entry order count once; adds count and fees reconcile',()=>{
 const b=reconstruct([mk(1,0,0,2,100,null,{orderId:7}),mk(2,1,0,3,101,null,{orderId:7}),mk(3,2,1,2,103,10),mk(4,3,1,3,104,18)]);
 assert.equal(b.entries.length,1);assert.equal(b.trades[0].maxSize,5);assert.equal(b.trades[0].gross,28);assert.equal(b.trades[0].fees,4);assert.equal(b.trades[0].net,24);assert.equal(b.trades[0].complete,true);
});
test('reversal splits closing and new position, without double counting fees/P&L',()=>{
 const b=reconstruct([mk(1,0,0,2,100),mk(2,1,1,3,102,8),mk(3,2,0,1,101,2)]);
 assert.equal(b.trades.length,2);assert.deepEqual(b.trades.map(t=>t.side),['LONG','SHORT']);assert.equal(b.trades.reduce((n,t)=>n+t.gross,0),10);assert.equal(b.trades.reduce((n,t)=>n+t.fees,0),3);assert.equal(b.entries.length,2);
});
test('deduplicates corrected fills; voided records do not inflate P&L',()=>{const b=reconstruct([mk(1,0,0,1,100),mk(2,1,1,1,101,2),mk(2,1,1,1,102,4),mk(3,2,0,1,100,null,{voided:true})]);assert.equal(b.fills.length,2);assert.equal(b.trades[0].net,2);});
test('missing opening fill prevents an audit pass and does not invent entries',()=>{const a=audit([mk(2,1,1,1,100,-50)]);assert.equal(a.entries,0);assert.equal(a.automaticResult,'UNKNOWN');assert.equal(a.checks.find(x=>x.key==='coverage').status,'UNKNOWN');});
test('cooldown, increased size, adding to loser and second-loss entry are independently caught',()=>{
 const a=audit([mk(1,0,0,1,100),mk(2,1,1,1,95,-10),mk(3,2,0,2,100),mk(4,3,0,1,99),mk(5,4,1,3,95,-25),mk(6,30,0,1,100),mk(7,31,1,1,101,2)]);
 for(const key of ['cooldown','size-increase','adding-loser','second-loss'])assert.equal(a.checks.find(c=>c.key===key).status,'FAIL',key);
});
test('profit ceiling and daily loss ceiling detect subsequent entry, including exact boundary',()=>{
 const a=audit([mk(1,0,0,1,100),mk(2,1,1,1,251,302),mk(3,20,0,1,100),mk(4,21,1,1,100,0)]);assert.equal(a.checks.find(c=>c.key==='profit').status,'FAIL');
 const b=audit([mk(1,0,0,1,100),mk(2,1,1,1,0,-298),mk(3,20,0,1,100)]);assert.equal(b.checks.find(c=>c.key==='daily-loss-entry').status,'FAIL');
});
test('simultaneous instruments are included in total contract cap',()=>{const b=audit([mk(1,0,0,3,100),mk(2,1,0,3,100,null,{contractId:'CON.F.US.MES.Z26'}),mk(3,2,1,3,101,6),mk(4,3,1,3,101,15,{contractId:'CON.F.US.MES.Z26'})]);assert.equal(b.maxContracts,6);assert.equal(b.checks.find(c=>c.key==='contracts').status,'FAIL');assert.equal(b.checks.find(c=>c.key==='symbols').status,'FAIL');});
test('self-confirmations and final order history cannot turn unknown risk checks into PASS',()=>{const a=audit([mk(1,0,0,1,100),mk(2,20,1,1,110,20)]);assert.equal(a.automaticResult,'PASS');assert.equal(a.result,'UNVERIFIED');for(const key of ['risk-lock','spending','stop-risk','intraday-risk'])assert.equal(a.checks.find(c=>c.key===key).status,'UNKNOWN');});
test('ET session boundaries follow DST and 18:00 daily rollover',()=>{assert.equal(sessionDate('2026-10-01T21:59:59Z'),'2026-10-01');assert.equal(sessionDate('2026-10-01T22:00:00Z'),'2026-10-02');assert.equal(sessionBounds('2026-11-02').start,'2026-11-01T23:00:00.000Z');assert.equal(sessionBounds('2026-10-02').start,'2026-10-01T22:00:00.000Z');});
test('archive excludes unfinished and future bars; no private data in indicator evidence shape',()=>{const now='2026-10-02T14:05:00Z';const c=compact({marketSymbol:'MNQ',bars5m:[{t:'2026-10-02T14:00:00Z'},{t:'2026-10-02T14:05:00Z'}],bars1h:[],accounts:[{id:1}],executions:[mk(1,0,0,1,100)]},now);assert.equal(c.bars5m.length,1);assert.equal(c.accounts,undefined);assert.equal(c.executions,undefined);});
test('collector strictly refuses trading API paths',async()=>{const dir=fs.mkdtempSync(path.join(os.tmpdir(),'audit-'));const s=createAuditService({dir,post:async()=>({success:true}),getMarket:()=>({}),getSnapshot:()=>({}),structureAt:()=>'',enabled:false});await assert.rejects(s.readApi('/api/Order/place',{}));assert.equal([...READ_PATHS].some(x=>/place|modify|cancel/.test(x)),false);fs.rmSync(dir,{recursive:true});});
test('token protection, durable idempotent polling, journal requirements and anti-backdating',async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'audit-'));let at=Date.parse('2026-10-02T13:59:00Z'),rows=[];
 const post=async route=>route==='/api/Account/search'?{success:true,accounts:[{id:1,name:'XFA Test',canTrade:true,isVisible:true}]}:route==='/api/Trade/search'?{success:true,trades:rows}:route==='/api/Position/searchOpen'?{success:true,positions:[]}:{success:true,orders:[]};
 const opts={dir,post,getMarket:()=>({}),getSnapshot:()=>({}),structureAt:()=>'',accessToken:'test-secret',now:()=>at};const s=createAuditService(opts);await s.poll();rows=[mk(1,0,0,1,100),mk(2,20,1,1,110,20)];at=Date.parse('2026-10-02T15:00:00Z');await s.poll();await s.poll();assert.equal(s.data().collector.fillCount,2);assert.equal(s.data().journalsPending,1);
 const app=express();app.use(express.json());s.attach(app);const server=app.listen(0);await new Promise(r=>server.once('listening',r));const url='http://127.0.0.1:'+server.address().port;
 try{assert.equal((await fetch(url+'/audit/data')).status,401);assert.equal((await fetch(url+'/audit/data',{headers:{Authorization:'Bearer wrong'}})).status,401);
 const request=(route,body)=>fetch(url+'/audit/'+route,{method:'POST',headers:{Authorization:'Bearer test-secret','Content-Type':'application/json'},body:JSON.stringify(body)});
 assert.equal((await request('attestation',{date:'2026-10-02',accountId:1,kind:'session-close'})).status,409);
 assert.equal((await request('journal',{tradeId:s.data().trades[0].id,entryReason:'abc'})).status,400);
 const body={tradeId:s.data().trades[0].id,strategy:'orb',entryReason:'Confirmed retest',exitReason:'Target hit',emotion:'Calm and patient',ruleFollowed:'Risk respected',lesson:'Wait for confirmation',recordedAt:'2020-01-01'};
 assert.equal((await request('journal',body)).status,200);assert.equal(s.data().journalsPending,0);assert.equal((await request('attestation',{date:'2026-10-02',accountId:1,kind:'session-close'})).status,200);
 assert.equal(s.data().trades[0].journal.recordedAt,new Date(at).toISOString());const restored=createAuditService(opts);assert.equal(restored.data().collector.fillCount,2);assert.equal(restored.data().journalsPending,0);
 }finally{server.close();fs.rmSync(dir,{recursive:true});}
});
