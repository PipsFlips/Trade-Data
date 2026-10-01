const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const {cases,summarize,runAudit}=require('./history-coverage');
test('probes are fixed, bounded, historical windows across both product roots',()=>{
  for(const market of ['MNQ','MES'])for(const q of cases(market)){
    assert.ok(q.contractId.startsWith(`CON.F.US.${market}.`));
    assert.ok(Date.parse(q.endTime)<Date.parse('2026-10-01T00:00:00Z'));
    assert.ok(Date.parse(q.endTime)-Date.parse(q.startTime)<=150*60000);
    assert.equal(q.includePartialBar,false);assert.ok(q.limit<=1000);
  }
});
test('summary detects duplicates, missing data, out-of-window rows and invalid prices',()=>{
  const q={startTime:'2026-09-30T13:30:00Z',endTime:'2026-09-30T13:33:00Z',unit:2,unitNumber:1};
  const b={t:q.startTime,o:1,h:2,l:0,c:1,v:10};
  const s=summarize({success:true,errorCode:0,bars:[b,b,{...b,t:'2026-09-30T13:29:00Z',h:-1}]},q);
  assert.equal(s.duplicateTimestamps,1);assert.equal(s.outOfWindow,1);
  assert.equal(s.missingTimestamps,2);assert.equal(s.invalidOHLCV,1);
});
test('audit caches once, calls only read APIs and excludes response secrets/error text',async()=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'history-audit-'));
  try{
    const file=path.join(dir,'report.json'),calls=[];
    const post=async(p,q)=>{calls.push(p);if(p.includes('Contract'))return {success:true,contract:{id:q.contractId,name:'contract',activeContract:false,secret:'SECRET'}};throw new Error('HTTP 403 SECRET');};
    await runAudit({market:'MNQ',live:false,post,file,pause:async()=>{}});
    const count=calls.length;
    const result=await runAudit({market:'MNQ',live:false,post,file,pause:async()=>{}});
    assert.equal(calls.length,count);assert.equal(result.status,'complete');
    assert.ok(calls.every(p=>['/api/Contract/searchById','/api/History/retrieveBars'].includes(p)));
    assert.ok(!fs.readFileSync(file,'utf8').includes('SECRET'));
    assert.equal(result.results[0].httpStatus,403);
  }finally{fs.rmSync(dir,{recursive:true,force:true});}
});
