const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {buildORBStrategy}=require('./orb-strategy');
const t=m=>Date.parse('2026-10-01T13:30:00Z')+m*60000;
const bar=(m,o,h,l,c)=>({t:new Date(t(m)).toISOString(),o,h,l,c,v:10});
const opening=()=>Array.from({length:3},(_,i)=>bar(i*5,30020,30050,30000,30020));
const long=()=>[...opening(),bar(15,30050,30058,30050,30055),bar(20,30055,30057,30049,30054)];
const run=(bars,minute,extra={})=>buildORBStrategy({bars,now:t(minute),fresh:true,structureAt:()=> 'TREND UP',...extra});
test('ORB freezes three completed formation bars at 09:45, not the IB hour',()=>{
  assert.equal(run(opening(),14).state,'WAIT — ORB FORMING');
  assert.equal(run(opening(),15).state,'WAIT — NO BREAK');
  assert.equal(run(opening().slice(1),15).state,'UNAVAILABLE — ORB DATA INCOMPLETE');
  const s=run(long(),25);assert.equal(s.high,30050);assert.equal(s.low,30000);assert.equal(s.eligible,true);
});
test('long entry uses first later retest, structural stop, capped size and tick-rounded target',()=>{
  assert.equal(run(long(),20).state,'ARMED LONG');
  const s=run(long(),25);assert.equal(s.state,'LONG ENTRY');assert.equal(s.plan.entry,30057.25);
  assert.equal(s.plan.stop,30047);assert.equal(s.plan.contracts,4);assert.ok(s.plan.risk<=100);
  assert.ok(s.plan.rr>=1.5);assert.equal(s.plan.target*4%1,0);assert.equal(s.plan.expiresAt,new Date(t(30)).toISOString());
});
test('short is symmetric and remains capped at five MNQ',()=>{
  const s=run([...opening(),bar(15,30000,30000,29993,29995),bar(20,29995,30001,29994,29996)],25,{structureAt:()=> 'TREND DOWN'});
  assert.equal(s.state,'SHORT ENTRY');assert.equal(s.plan.entry,29993.75);assert.equal(s.plan.stop,30003);assert.equal(s.plan.contracts,5);
  assert.ok(s.plan.risk<=100);assert.ok(s.plan.rr>=1.5);
});
test('failed first retest cancels rather than seeking another or opposite breakout',()=>{
  const rows=[...opening(),long()[3],bar(20,30055,30057,30045,30049),bar(25,30049,30059,30049,30057),bar(30,30050,30050,29990,29995)];
  assert.equal(run(rows,35).state,'INVALIDATED');
});
test('first retest deadline is exactly the next three completed candles',()=>{
  const rows=[...opening(),long()[3],bar(20,30055,30059,30051,30055),bar(25,30055,30059,30051,30055),bar(30,30055,30059,30051,30055)];
  assert.equal(run(rows,35).state,'DONE FOR DAY');
  rows[6]=bar(30,30055,30058,30049,30054);assert.equal(run(rows,35).state,'LONG ENTRY');
});
test('unfilled stop-entry lasts only its next candle; no new attempt after expiry',()=>{
  const rows=[...long(),bar(25,30054,30056,30051,30055)];
  assert.equal(run(rows,29).state,'LONG ENTRY');assert.equal(run(rows,30).state,'DONE FOR DAY');
});
test('entry touch is observational, and 10:30 time exit remains visible',()=>{
  const rows=[...long(),bar(25,30054,30059,30051,30057)];
  const s=run(rows,30);assert.equal(s.state,'ENTRY LEVEL REACHED');assert.match(s.reason,/not a verified fill/);
  const end=run(rows,60);assert.equal(end.state,'TIME EXIT — 10:30 ET');assert.equal(end.plan.exitTimeET,'10:30');
});
test('10:15 cutoff allows a previously armed order touch but excludes a new confirmation',()=>{
  const rows=[...opening(),...Array.from({length:3},(_,i)=>bar(15+i*5,30020,30049,30001,30020)),bar(30,30050,30058,30050,30055),bar(35,30055,30057,30049,30054),bar(40,30054,30059,30051,30057)];
  assert.equal(run(rows,45).state,'ENTRY LEVEL REACHED');
  rows[5]=bar(25,30020,30049,30001,30020);rows[6]=bar(30,30020,30049,30001,30020);rows[7]=long()[3];rows[7]={...rows[7],t:new Date(t(35)).toISOString()};rows[8]=bar(40,30055,30057,30049,30054);
  assert.equal(run(rows,45).state,'DONE FOR DAY');
});
test('stale data, invalid structure, missing bars and wide structural stop block entries',()=>{
  assert.equal(run(long(),25,{fresh:false}).state,'UNAVAILABLE — COLLECTOR STALE');
  assert.equal(run(long(),25,{structureAt:()=> 'RANGE'}).state,'INVALIDATED');
  assert.equal(run([...opening(),bar(20,30050,30058,30050,30055)],25).state,'UNAVAILABLE — BAR GAP');
  assert.equal(run([...opening(),long()[3],bar(20,30055,30059,30030,30054)],25).state,'NO TRADE — STOP TOO WIDE');
});
test('open candle cannot confirm and structure callback cannot see future bars',()=>{
  const rows=[...long(),bar(40,30054,30100,30049,30070)];
  const s=run(rows,24,{structureAt:(bars,asOf)=>{assert.ok(bars.every(b=>Date.parse(b.t)+300000<=asOf));return 'TREND UP';}});
  assert.equal(s.state,'ARMED LONG');
});
test('MES cannot use MNQ sizing and ORB daily range resets in Eastern Time',()=>{
  assert.equal(run(long(),25,{market:'MES'}).state,'NO TRADE — STOP TOO WIDE');
  const rows=[...opening(),long()[3],bar(20,30055,30056,30050,30054)];
  const mes=run(rows,25,{market:'MES'});assert.equal(mes.state,'LONG ENTRY');assert.equal(mes.market,'MES');
  assert.equal(mes.plan.stop,30049.5);assert.equal(mes.plan.contracts,2);assert.ok(mes.plan.risk+mes.plan.costReserve<=100);
  const tomorrow=buildORBStrategy({bars:long(),now:t(24*60+15),fresh:true});assert.equal(tomorrow.state,'UNAVAILABLE — ORB DATA INCOMPLETE');
});
test('deployment explicitly packages both strategy modules and payload keeps both fields',()=>{
  const docker=fs.readFileSync(__dirname+'/Dockerfile','utf8');assert.match(docker,/COPY .*ib-strategy\.js .*orb-strategy\.js/);
  const server=fs.readFileSync(__dirname+'/server.js','utf8');assert.match(server,/\n    ibStrategy,\n    orbStrategy,/);
});
