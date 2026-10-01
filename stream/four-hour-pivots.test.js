const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const {DateTime,Settings}=require('luxon');
const source=fs.readFileSync(`${__dirname}/server.js`,'utf8');
const context=vm.createContext({DateTime});
vm.runInContext(source.slice(source.indexOf('function aggregateFourHourBars('),source.indexOf('function median(')),context);
const et=s=>DateTime.fromISO(s,{zone:'America/New_York'});
const bar=(t,p)=>({t:t.toUTC().toISO(),o:p,h:p+1,l:p-1,c:p+.25,v:10});
test('4H aggregation follows CME session alignment and keeps the shortened pre-maintenance bar',()=>{
  const originalNow=Settings.now;
  const frozenNow=et('2026-09-29T18:30').toMillis();
  try{
    Settings.now=()=>frozenNow;
    const starts=[
      '2026-09-28T18:00','2026-09-28T19:00','2026-09-28T20:00','2026-09-28T21:00',
      '2026-09-28T22:00','2026-09-28T23:00','2026-09-29T00:00','2026-09-29T01:00',
      '2026-09-29T02:00','2026-09-29T03:00','2026-09-29T04:00','2026-09-29T05:00',
      '2026-09-29T06:00','2026-09-29T07:00','2026-09-29T08:00','2026-09-29T09:00',
      '2026-09-29T10:00','2026-09-29T11:00','2026-09-29T12:00','2026-09-29T13:00',
      '2026-09-29T14:00','2026-09-29T15:00','2026-09-29T16:00'
    ];
    const rows=starts.map((s,i)=>bar(et(s),100+i));
    const four=context.aggregateFourHourBars(rows);
    assert.equal(four.length,6);
    assert.equal(DateTime.fromISO(four[0].t).setZone('America/New_York').toFormat('HH:mm'),'18:00');
    assert.equal(DateTime.fromISO(four[1].t).setZone('America/New_York').toFormat('HH:mm'),'22:00');
    assert.equal(DateTime.fromISO(four[5].t).setZone('America/New_York').toFormat('HH:mm'),'14:00');
    assert.equal(four[5].o,120);
    assert.equal(four[5].c,122.25);
  } finally { Settings.now=originalNow; }
});
