const test=require('node:test'),assert=require('node:assert/strict');
const {buildVWAPStrategy:build}=require('./vwap-strategy');
const t=m=>Date.parse('2026-10-01T13:30:00Z')+m*60000;
const b=(m,o,h,l,c,v=10)=>({t:new Date(t(m)).toISOString(),o,h,l,c,v});
const long=()=>[b(0,100,101,99,100),b(5,105,115,105,114),b(10,116,130,116,129,1),b(15,107,109,105,108)];
const run=(rows=long(),min=20,extra={})=>build({bars:rows,now:t(min),fresh:true,...extra});
test('NY open anchor and 09:45 setup candle: earliest close 09:50',()=>{
  assert.equal(run(long(),14).state,'WAIT — OPENING 15 MINUTES');
  assert.equal(run(long(),19).trades.length,0);
  const s=run();assert.equal(s.state,'LONG ENTRY');assert.equal(s.plan.confirmedAt,new Date(t(20)).toISOString());assert.equal(s.plan.entry,109.25);assert.equal(s.plan.stop,103);assert.equal(s.plan.target,118.75);assert.equal(s.plan.priorExtreme,130);assert.equal(s.plan.contracts,5);assert.ok(s.plan.risk<=150);assert.ok(s.plan.rr>=1.5);
});
test('short mirrors long and MES uses its own point value and stop buffer',()=>{
  const rows=long().map(x=>({...x,o:200-x.o,h:200-x.l,l:200-x.h,c:200-x.c}));
  assert.equal(run(rows).state,'SHORT ENTRY');assert.equal(run(rows).plan.target,81.25);
  const mes=run(long(),20,{market:'MES'});assert.equal(mes.state,'LONG ENTRY');assert.equal(mes.plan.stop,104.5);assert.ok(mes.plan.risk<=150);assert.ok(mes.plan.contracts<=5);
});
test('no partial-bar signal, no own-candle confirmation, no malformed or missing session data',()=>{
  const rows=long();rows[2]={...rows[2],h:110,o:108,l:107,c:109};
  assert.equal(run(rows).trades.length,0);
  assert.equal(run(long().slice(1)).state,'UNAVAILABLE — OPENING DATA');
  const gap=[...long(),b(25,108,110,107,109)];assert.equal(run(gap,30).state,'UNAVAILABLE — BAR GAP');
  assert.equal(run(long().map(x=>({...x,v:0}))).trades.length,0);
});
test('VWAP direction and frozen prior extreme room both required',()=>{
  const bearish=long();bearish[3]=b(15,109,109,105,108);assert.equal(run(bearish).trades.length,0);
  const tooWide=long();tooWide[3]=b(15,107,129,90,128);assert.equal(run(tooWide).trades.length,0);
});
test('stale collector disables actionable plans and next-bar expiry does not repeat entries',()=>{
  const stale=run(long(),20,{fresh:false});assert.equal(stale.state,'UNAVAILABLE — COLLECTOR STALE');assert.equal(stale.plan,null);
  const expired=run([...long(),b(20,108,109,107,108)],25);assert.equal(expired.trades.length,1);assert.equal(expired.trades[0].status,'EXPIRED');assert.equal(expired.plan,null);
  const reached=run([...long(),b(20,108,111,107,110)],25);assert.equal(reached.state,'ENTRY LEVEL REACHED');assert.equal(reached.trades.length,1);
});
test('same-bar stop/target conflict takes stop first; cutoff and noon exit enforced',()=>{
  const stopped=run([...long(),b(20,108,120,102,110)],25);assert.equal(stopped.trades[0].status,'STOP LEVEL TOUCHED');assert.equal(stopped.plan,null);
  const reached=[...long(),b(20,108,111,107,110)];assert.equal(run(reached,150).state,'TIME EXIT — 12:00 ET');
  assert.equal(run(long().slice(0,3),120).state,'ENTRY WINDOW CLOSED');
});
