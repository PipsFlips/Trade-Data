const test=require('node:test'),assert=require('node:assert/strict');
const {windows,normalize}=require('./backtest-history');
test('weekly windows partition each roll span and stay below request bar limit',()=>{
  const qs=windows('MES');
  assert.equal(qs[0].startTime,'2026-04-01T00:00:00.000Z');
  assert.equal(qs.at(-1).endTime,'2026-10-02T00:00:00.000Z');
  qs.forEach((q,i)=>{assert.ok((Date.parse(q.endTime)-Date.parse(q.startTime))/60000<20000);if(i)assert.equal(q.startTime,qs[i-1].endTime);});
});
test('invalid and out-of-window bars cannot leak into data; duplicates are counted',()=>{
  const q=windows('MNQ')[0],b={t:q.startTime,o:1,h:2,l:0,c:1,v:10};
  const x=normalize([b,b,{...b,t:q.endTime},{...b,h:0}],q);
  assert.equal(x.bars.length,1);assert.equal(x.invalid,1);assert.equal(x.duplicates,1);assert.equal(x.outOfWindow,1);
});
