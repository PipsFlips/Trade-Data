const {test}=require('node:test');
const assert=require('node:assert/strict');
const {buildIBStrategy,etParts}=require('./ib-strategy');
const t=m=>Date.parse('2026-10-01T13:30:00Z')+m*60000;
const bar=(m,o,h,l,c)=>({t:new Date(t(m)).toISOString(),o,h,l,c,v:10});
const ib=()=>Array.from({length:12},(_,i)=>bar(i*5,30050,30180,30000,30070));
const run=(rows,minute,extra={})=>buildIBStrategy({bars:rows,now:t(minute),fresh:true,structureAt:()=> 'TREND UP',...extra});
const long=()=>[...ib(),bar(60,30180,30187,30180,30185),bar(65,30185,30190,30178,30186)];
test('forming range cannot issue entries, and missing IB bars fail closed',()=>{
  assert.equal(run(ib(),55).state,'WAIT — IB FORMING');
  assert.equal(run(ib().slice(1),65).state,'UNAVAILABLE — IB DATA INCOMPLETE');
});
test('eligible first break arms; confirmation needs a later first retest',()=>{
  const rows=long();assert.equal(run(rows,65).state,'ARMED LONG');
  const s=run(rows,70);assert.equal(s.state,'LONG ENTRY');assert.equal(s.plan.entry,30190.25);
  assert.equal(s.plan.stop,30176);assert.equal(s.plan.contracts,3);assert.ok(s.plan.rr>=1.5);
  assert.ok(s.plan.risk<=100);assert.equal(s.plan.target*4%1,0);
});
test('short rules mirror long rules',()=>{
  const s=run([...ib(),bar(60,30000,30000,29994,29996),bar(65,29996,30002,29990,29995)],70,{structureAt:()=> 'TREND DOWN'});
  assert.equal(s.state,'SHORT ENTRY');assert.equal(s.plan.entry,29989.75);assert.equal(s.plan.stop,30004);assert.ok(s.plan.rr>=1.5);
});
test('25% invalidation locks first breakout even if opposite side later breaks',()=>{
  const rows=[...ib(),bar(60,30180,30187,30180,30185),bar(65,30185,30186,30120,30130),bar(70,30130,30130,29990,29995)];
  assert.equal(run(rows,75).state,'INVALIDATED');
});
test('unconfirmed candle and future data cannot confirm or influence structure checks',()=>{
  let maxSeen=0;
  const s=run([...long(),bar(100,30180,30200,30150,30190)],69,{structureAt:(rows,asOf)=>{maxSeen=Math.max(maxSeen,...rows.map(b=>Date.parse(b.t)+300000));assert.ok(maxSeen<=asOf);return 'TREND UP';}});
  assert.equal(s.state,'ARMED LONG');
});
test('ineligible size, stale relay, range structure, missing bar and wide stop block entries',()=>{
  assert.equal(run(ib().map(b=>({...b,h:30100})),65).state,'NO TRADE — IB SIZE INVALID');
  assert.equal(run(long(),70,{fresh:false}).state,'UNAVAILABLE — COLLECTOR STALE');
  assert.equal(run(long(),70,{structureAt:()=> 'RANGE'}).state,'INVALIDATED');
  assert.equal(run([...ib(),bar(65,30180,30187,30180,30185)],70).state,'UNAVAILABLE — BAR GAP');
  const wide=[...ib(),long()[12],bar(65,30185,30190,30160,30186)];
  assert.equal(run(wide,70).state,'NO TRADE — STOP TOO WIDE');
});
test('entry touch consumes the signal but never reports a broker fill',()=>{
  const s=run([...long(),bar(70,30186,30192,30184,30191)],75);
  assert.equal(s.state,'ENTRY LEVEL REACHED');assert.match(s.reason,/not a verified fill/);
});
test('unfilled orders expire at 11:30 ET and confirmations at cutoff are excluded',()=>{
  const rows=[...long(),...Array.from({length:10},(_,i)=>bar(70+i*5,30186,30189,30182,30186))];
  assert.equal(run(rows,120).state,'DONE FOR DAY');
  const late=[...ib(),...Array.from({length:10},(_,i)=>bar(60+i*5,30070,30179,30030,30070)),bar(110,30180,30187,30180,30185),bar(115,30185,30190,30178,30186)];
  assert.equal(run(late,120).state,'DONE FOR DAY');
});
test('pending entry touch in the final five-minute bar is retained',()=>{
  const rows=[...long(),...Array.from({length:9},(_,i)=>bar(70+i*5,30186,30189,30182,30186)),bar(115,30186,30192,30182,30191)];
  assert.equal(run(rows,120).state,'ENTRY LEVEL REACHED');
});
test('MES cannot inherit MNQ sizing and ET time follows DST',()=>{
  assert.equal(run(long(),70,{market:'MES'}).state,'MNQ ONLY');
  assert.equal(etParts(Date.parse('2026-11-02T14:30:00Z')).minutes,570);
  assert.equal(etParts(Date.parse('2026-10-01T13:30:00Z')).minutes,570);
});
