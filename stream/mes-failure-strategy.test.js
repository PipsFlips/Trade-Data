const test=require('node:test'),assert=require('node:assert/strict');
const {buildMESFailureStrategy:build}=require('./mes-failure-strategy');
const t=m=>Date.parse('2026-10-01T13:30:00Z')+m*60000;
const b=(m,o,h,l,c,v=10)=>({t:new Date(t(m)).toISOString(),o,h,l,c,v});
const opening=()=>[0,5,10].map(m=>b(m,119,120,100,119));
const short=()=>[...opening(),b(15,121,125,121,124,1000),b(20,119,119,117,118,1),b(25,119,120,117,118,1)];
const run=(bars,min=30,extra={})=>build({bars,now:t(min),fresh:true,...extra});
test('failure requires separate breakout, return and retest candles',()=>{
 assert.equal(run(short(),20).state,'WAIT — RETURN INSIDE');assert.equal(run(short(),25).state,'WAIT — FAILURE RETEST');
 assert.equal(run(short(),29).state,'WAIT — FAILURE RETEST');
 const s=run(short());assert.equal(s.state,'SHORT ENTRY');assert.equal(s.plan.entry,116.75);assert.equal(s.plan.stop,120.5);assert.equal(s.plan.target,100);assert.equal(s.plan.contracts,4);assert.ok(s.plan.risk+s.plan.costReserve<=100);assert.ok(s.plan.rr>=1.5);
});
test('long failure mirrors short and never requires structure',()=>{
 const rows=[0,5,10].map(m=>b(m,101,120,100,101));rows.push(b(15,99,99,95,96,1000),b(20,101,103,100,102,1),b(25,101,103,100,102,1));
 const s=run(rows);assert.equal(s.state,'LONG ENTRY');assert.equal(s.plan.entry,103.25);assert.equal(s.plan.stop,99.5);assert.equal(s.plan.target,120);assert.equal(s.structure,'Not required');
});
test('freshness, gaps and invalid opening fail closed',()=>{
 assert.equal(run(short(),30,{fresh:false}).state,'UNAVAILABLE — COLLECTOR STALE');
 assert.equal(run(short().filter((_,i)=>i!==4)).state,'UNAVAILABLE — BAR GAP');assert.equal(run(short().slice(1)).state,'UNAVAILABLE — ORB DATA INCOMPLETE');
 assert.equal(run(short(),30,{market:'MNQ'}).state,'MES ONLY');
});
test('unfilled order expires after one candle; entry touch is observational and time exit retained',()=>{
 const rows=[...short(),b(30,118,119,117,118)];assert.equal(run(rows,35).state,'DONE FOR DAY');
 rows[6]=b(30,118,119,116,117);assert.equal(run(rows,35).state,'ENTRY LEVEL REACHED');assert.equal(run(rows,60).state,'TIME EXIT — 10:30 ET');
});
test('first retest with failed VWAP or outside close invalidates without retry',()=>{
 const rows=short();rows[4]=b(20,119,121,118,120,1);assert.equal(run(rows).state,'WAIT — FAILURE RETEST');
 const outside=[...short().slice(0,5),b(25,119,121,119,120,1)];assert.equal(run(outside).state,'INVALIDATED');
 const noVwap=short().map(x=>({...x,v:0}));assert.equal(run(noVwap).state,'INVALIDATED');
});
test('nearest target with insufficient reward rejects rather than choosing farther boundary',()=>{
 const rows=short();rows[3]={...rows[3],v:10};rows[5]=b(25,119,120,111,112,1);
 assert.equal(run(rows).state,'NO TRADE — STOP OR TARGET ROOM');
});
