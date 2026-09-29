const {test}=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const {DateTime}=require('luxon');
const source=fs.readFileSync(`${__dirname}/server.js`,'utf8');
const baseline=require('./edgeful-baselines.json');
const pt=s=>DateTime.fromISO(s,{zone:'America/Los_Angeles'});
const rows=(start,count,price=100)=>{
  const out=[];
  for(let i=0;i<count;i++){
    const t=pt(start).plus({minutes:i*5});
    const p=price+i*.2;
    out.push({t:t.toUTC().toISO(),o:p,h:p+1,l:p-1,c:p+.5,v:100,delta:10});
  }
  return out;
};
function build(){
  const c=vm.createContext({
    Date,
    EDGEFUL_12M:baseline,
    MARKET_SYMBOL:'MNQ',
    MARKET_PARAMS:{majorCap:30,majorMin:12,obCap:20,obMin:8,orbCap:25,orbMin:10,gapMin:12,clusterMin:2},
    nowPT:()=>pt('2026-09-28T10:00'),
    lastClosedBar:(xs)=>xs.at(-1)||null,
    median:xs=>xs.length?xs.slice().sort((a,b)=>a-b)[Math.floor(xs.length/2)]:null,
    nearestLevelDistance:(levels,price,side)=>{
      const xs=levels.map(x=>+x.price).filter(Number.isFinite).filter(x=>side==='BUY'?x>price:x<price);
      return xs.length?(side==='BUY'?Math.min(...xs)-price:price-Math.max(...xs)):null;
    },
    between:(xs,start,end)=>xs.filter(x=>Date.parse(x.t)>=start.toMillis()&&Date.parse(x.t)<end.toMillis()),
    summary:xs=>({open:+xs[0].o,high:Math.max(...xs.map(x=>+x.h)),low:Math.min(...xs.map(x=>+x.l)),close:+xs.at(-1).c})
  });
  vm.runInContext(source.slice(source.indexOf('function buildMarketAnalysis('),source.indexOf('async function sendSmsAlert')),c);
  const bars=rows('2026-09-28T06:30',48);
  const orb={formed:true,start:pt('2026-09-28T06:30').toISO(),end:pt('2026-09-28T06:45').toISO(),open:100,close:100.5,high:101,low:99};
  return c.buildMarketAnalysis({
    currentPrice:104,
    levels:[{id:'pdh',label:'PDH',price:103,priority:100},{id:'pwh',label:'PWH',price:108,priority:80}],
    f1:[],f5:bars,signal:null,traps:[],orderBlocks:[],globexVwap:null,rthVwap:102,sessionCvd:100,atr5:1,
    orb,middayOrb:null,bars5m:bars,delta15:{direction:'BULLISH'},profiles:{},icebergs:[],gaps:[]
  });
}

test('live analysis exposes Edgeful checks and applies them to a directional setup',()=>{
  const analysis=build();
  assert.equal(analysis.edgeful.bias,'BUY');
  assert.ok(analysis.edgeful.checks.some(x=>x.label==='15m ORB continuation'));
  assert.ok(analysis.edgeful.checks.some(x=>x.label==='prior-high follow-through'));
  const setup=analysis.setups.find(x=>x.side==='BUY');
  assert.ok(setup);
  assert.match(setup.context,/Edgeful qualifier supports BUY/);
  assert.match(analysis.note,/low-weight qualifiers/);
});
