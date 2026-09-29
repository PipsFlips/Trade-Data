const {test}=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const {DateTime}=require('luxon');
const source=fs.readFileSync(`${__dirname}/server.js`,'utf8');
const baseline=require('./edgeful-baselines.json');
const et=s=>DateTime.fromISO(s,{zone:'America/New_York'});
const rows=(a,b,price=100)=>{const out=[];for(let t=et(a);t<et(b);t=t.plus({minutes:5}))out.push({t:t.toISO(),o:price,h:price+2,l:price-2,c:price+1,v:10});return out;};
function build(now,bars,symbol='MNQ'){
  const c=vm.createContext({DateTime,nowPT:()=>et(now),MARKET_SYMBOL:symbol,EDGEFUL_12M:baseline});
  vm.runInContext(source.slice(source.indexOf('function dt('),source.indexOf('function vwap('))+source.slice(source.indexOf('function buildEdgefulChartContext('),source.indexOf('function buildEdgefulContext(')),c);
  return c.buildEdgefulChartContext(bars);
}
const get=(result,key)=>result.items.find(x=>x.key===key);
test('complete ranges activate; open candle cannot trigger a close-confirmed break',()=>{
  const b=rows('2026-09-29T09:30','2026-09-29T10:40');b.at(-1).c=200;
  const e=build('2026-09-29T10:37',b);
  assert.equal(get(e,'ib60').state,'No break yet');
  assert.equal(get(e,'orb15').geometry.high,102);
  assert.equal(get(e,'opening15').state,'Green opening 15m');
  assert.equal(get(e,'ib60').geometry.end,et('2026-09-29T10:30').toISO());
  assert.equal(get(build('2026-09-29T10:40',b),'ib60').state,'High broken');
});
test('missing formation bar suppresses geometry; doji has no directional probability',()=>{
  const b=rows('2026-09-29T09:30','2026-09-29T10:30');b.splice(1,1);
  assert.equal(get(build('2026-09-29T10:30',b),'ib60').geometry,null);
  const d=rows('2026-09-29T09:30','2026-09-29T09:45');d.at(-1).c=100;
  assert.match(get(build('2026-09-29T09:45',d),'opening15').stat,/No doji/);
});
test('evening reset cannot reuse today RTH; MNQ and MES select separate baselines',()=>{
  const b=rows('2026-09-28T09:30','2026-09-28T16:00');
  const e=build('2026-09-28T20:00',b);
  assert.equal(e.sessionDate,'2026-09-29');assert.equal(e.phase,'PRE-RTH');
  assert.equal(get(e,'orb15').geometry,null);assert.equal(get(e,'midday').geometry,null);
  assert.match(get(e,'gap').state,/Waiting/);
  assert.equal(build('2026-09-28T20:00',b,'MES').ticker,'ES');
  assert.notEqual(get(e,'overnight').stat,get(build('2026-09-28T20:00',b,'MES'),'overnight').stat);
});
test('Friday prior close maps to Monday gap and records completed fill',()=>{
  const b=[...rows('2026-09-25T09:30','2026-09-25T16:00'),...rows('2026-09-28T09:30','2026-09-28T09:45',110)];
  let e=build('2026-09-28T09:45',b);assert.equal(get(e,'gap').state,'Gap up · unfilled');
  assert.equal(get(e,'gap').geometry.low,101);assert.match(get(e,'pdh').stat,/80.0%/);
  b.at(-1).l=100;e=build('2026-09-28T09:45',b);assert.equal(get(e,'gap').state,'Filled during RTH');
});
test('midday monitoring ignores after-close bars and keeps explicit sample',()=>{
  const b=rows('2026-09-29T14:00','2026-09-29T16:05');b.at(-1).c=200;
  const e=build('2026-09-29T16:10',b);assert.equal(get(e,'midday').state,'No break yet');
  assert.match(get(e,'midday').detail,/248 sessions/);
  assert.match(get(e,'midday').stat,/73.0%/);
});
test('Edgeful chart content includes continuation extension and retracement statistics',()=>{
  const b=rows('2026-09-29T09:30','2026-09-29T16:05');
  const e=build('2026-09-29T16:10',b);
  assert.match(get(e,'overnight').stat,/continuation green→green 54.0% · red→red 48.0%/);
  assert.match(get(e,'orb15').stat,/avg extension up \+0.40% \/ down -0.47%/);
  assert.match(get(e,'orb15').stat,/0.5x retrace up 37.8% \/ down 41.9%/);
  assert.match(get(e,'ib60').detail,/maximum observed extension up \+2.17% \/ down -4.17%/);
  assert.match(get(e,'midday').detail,/0.5x retracement up 23.7% \/ down 19.3%/);
});
