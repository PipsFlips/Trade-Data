const {test}=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const {DateTime}=require('luxon');
const source=fs.readFileSync(`${__dirname}/server.js`,'utf8');
const baseline=require('./edgeful-baselines.json');
const pt=s=>DateTime.fromISO(s,{zone:'America/Los_Angeles'});
const bar=(t,o,h,l,c)=>({t:t.toUTC().toISO(),o,h,l,c,v:100,delta:10});
function makeBars({priorOpen=100.5,priorClose=100.2}={}){
  const out=[];
  const p0=pt('2026-09-25T06:30');
  for(let i=0;i<78;i++){
    const t=p0.plus({minutes:i*5});
    out.push(bar(t,i?100.4:priorOpen,i===0?101.5:101.3,i===0?100:100.1,i===77?priorClose:100.4));
  }
  const c0=pt('2026-09-28T06:30');
  for(let i=0;i<12;i++){
    const t=c0.plus({minutes:i*5});
    out.push(bar(t,100.6,100.85,100.15,100.6));
  }
  out.push(bar(pt('2026-09-28T07:30'),100.7,102.2,100.7,102));
  out.push(bar(pt('2026-09-28T09:55'),102,102.2,101.8,102));
  return out;
}
function run(symbol,bars,gaps=[]){
  const c=vm.createContext({
    Date,
    EDGEFUL_12M:baseline,
    MARKET_SYMBOL:symbol,
    MARKET_PARAMS:{majorCap:30,majorMin:12,obCap:20,obMin:8,orbCap:25,orbMin:10,gapMin:12,clusterMin:2},
    nowPT:()=>pt('2026-09-28T10:00'),
    lastClosedBar:xs=>xs.at(-1)||null,
    median:xs=>xs.length?xs.slice().sort((a,b)=>a-b)[Math.floor(xs.length/2)]:null,
    nearestLevelDistance:(levels,price,side)=>{
      const xs=levels.map(x=>+x.price).filter(Number.isFinite).filter(x=>side==='BUY'?x>price:x<price);
      return xs.length?(side==='BUY'?Math.min(...xs)-price:price-Math.max(...xs)):null;
    },
    between:(xs,start,end)=>xs.filter(x=>Date.parse(x.t)>=start.toMillis()&&Date.parse(x.t)<end.toMillis()),
    summary:xs=>({open:+xs[0].o,high:Math.max(...xs.map(x=>+x.h)),low:Math.min(...xs.map(x=>+x.l)),close:+xs.at(-1).c})
  });
  vm.runInContext(source.slice(source.indexOf('function buildMarketAnalysis('),source.indexOf('async function sendSmsAlert')),c);
  const orb={formed:true,start:pt('2026-09-28T06:30').toISO(),end:pt('2026-09-28T06:45').toISO(),open:100.6,close:100.65,high:100.85,low:100.15};
  return c.buildMarketAnalysis({
    currentPrice:102,
    levels:[{id:'pdh',label:'PDH',price:101.5,priority:100},{id:'pdl',label:'PDL',price:100,priority:100},{id:'pwh',label:'PWH',price:104,priority:80}],
    f1:[],f5:bars,signal:null,traps:[],orderBlocks:[],globexVwap:null,rthVwap:101,sessionCvd:100,atr5:1,
    orb,middayOrb:null,bars5m:bars,delta15:{direction:'BULLISH'},profiles:{},icebergs:[],gaps
  });
}
test('selected Edgeful qualifier baselines are pinned to the verified 1y scan',()=>{
  assert.equal(baseline.NQ.selectedQualifiers.dataThrough,'2026-09-28');
  assert.deepEqual(baseline.NQ.selectedQualifiers.smallGapFill.up,{rate:90.3,sample:31});
  assert.deepEqual(baseline.NQ.selectedQualifiers.ib60Size,{bucketMinPct:0.6,bucketMaxPct:0.89,singleBreak:85.7,sample:98});
  assert.deepEqual(baseline.ES.selectedQualifiers.insideDayBreakout,{bucketMinPct:0,bucketMaxPct:0.99,rate:87,sample:92});
  assert.deepEqual(baseline.ES.selectedQualifiers.priorDayColorLevel.previousRedHighBreakGreenClose,{rate:84.6,sample:52});
});
test('NQ selected inside-day and 60m IB qualifiers activate only after live close-confirmed direction',()=>{
  const a=run('MNQ',makeBars());
  assert.ok(a.edgeful.checks.some(x=>x.label==='inside-day breakout bucket'&&x.side==='BUY'&&/86.1%/.test(x.text)));
  assert.ok(a.edgeful.checks.some(x=>x.label==='60m IB size bucket'&&x.side==='BUY'&&/85.7%/.test(x.text)));
});
test('ES prior red day plus confirmed PDH break uses the color-conditioned continuation stat',()=>{
  const a=run('MES',makeBars({priorOpen:100.8,priorClose:100.2}));
  assert.ok(a.edgeful.checks.some(x=>x.label==='prior-red + PDH break'&&x.side==='BUY'&&/84.6%/.test(x.text)));
  assert.ok(!a.edgeful.checks.some(x=>x.label==='prior-high follow-through'));
});
test('small RTH gaps use the size-conditioned fill rate rather than the generic gap rate',()=>{
  const gaps=[{kind:'RTH',label:'RTH',near:true,filled:false,fillDirection:'SELL',direction:'UP',size:.15,priorClose:100,open:100.15,distance:.1,fillTarget:100}];
  const a=run('MNQ',makeBars(),gaps);
  assert.ok(a.edgeful.checks.some(x=>x.label==='size-conditioned gap fill'&&x.side==='SELL'&&/90.3%/.test(x.text)&&/n=31/.test(x.text)));
  assert.ok(!a.edgeful.checks.some(x=>x.label==='gap-fill tendency'));
});
