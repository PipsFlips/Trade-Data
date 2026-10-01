// Bounded, one-time subscription audit. Uses the service's existing API session.
// Persists coverage metadata only; never persists prices, credentials, or errors' text.
const fs = require('node:fs');
const AUDIT_ID = 'history-coverage-2026-10-01-v1';
function cases(market) {
  const out = [];
  const add = (expiry, date, utcHour, unit, minutes = 150) => out.push({
    contractId: `CON.F.US.${market}.${expiry}`, date, unit,
    unitNumber: 1,
    startTime: `${date}T${utcHour}:30:00Z`,
    endTime: new Date(Date.parse(`${date}T${utcHour}:30:00Z`) + minutes * 60000).toISOString(),
    limit: 1000, includePartialBar: false
  });
  add('Z26', '2026-09-30', '13', 2);
  add('Z26', '2026-09-30', '13', 1, 10);
  add('Z26', '2026-08-03', '13', 2);
  for (const [expiry,date,hour] of [
    ['U26','2026-09-10','13'], ['M26','2026-06-10','13'],
    ['H26','2026-03-11','13'], ['Z25','2025-12-10','14'],
    ['U25','2025-09-10','13'], ['Z24','2024-12-11','14']
  ]) {
    add(expiry,date,hour,2);
    add(expiry,date,hour,3);
  }
  add('U26','2026-09-10','13',1,10);
  return out;
}
function summarize(response, query) {
  const rows = Array.isArray(response?.bars) ? response.bars : [];
  const times = rows.map(b => Date.parse(b.t ?? b.time ?? b.timestamp)).filter(Number.isFinite).sort((a,b)=>a-b);
  const start = Date.parse(query.startTime), end = Date.parse(query.endTime);
  const inWindow = times.filter(t=>t>=start&&t<end);
  const step = (query.unit===1?1000:query.unit===2?60000:3600000)*query.unitNumber;
  const expected = query.unit===3 ? null : Math.round((end-start)/step);
  return {
    success: response?.success === true, errorCode: response?.errorCode ?? null,
    count: rows.length, inWindowCount: inWindow.length,
    first: times.length ? new Date(times[0]).toISOString() : null,
    last: times.length ? new Date(times.at(-1)).toISOString() : null,
    duplicateTimestamps: times.length-new Set(times).size,
    outOfWindow: times.length-inWindow.length, expected,
    missingTimestamps: expected===null?null:Math.max(0,expected-new Set(inWindow).size),
    invalidOHLCV: rows.filter(b=>![b.o,b.h,b.l,b.c,b.v].every(v=>v!==null&&v!==undefined&&Number.isFinite(+v)) || +b.h<Math.max(+b.o,+b.l,+b.c) || +b.l>Math.min(+b.o,+b.h,+b.c) || +b.v<0).length
  };
}
function readReport(file) {
  try { return JSON.parse(fs.readFileSync(file,'utf8')); } catch { return null; }
}
async function runAudit({market,live,post,file,pause=(ms)=>new Promise(r=>setTimeout(r,ms))}) {
  const previous=readReport(file);
  if(previous?.auditId===AUDIT_ID) return previous;
  const report={auditId:AUDIT_ID,market,liveSubscription:live,startedAt:new Date().toISOString(),status:'running',contracts:[],results:[]};
  const save=()=>{fs.writeFileSync(file+'.tmp',JSON.stringify(report));fs.renameSync(file+'.tmp',file);};
  save();
  const queries=cases(market);
  for(const contractId of [...new Set(queries.map(q=>q.contractId))]) {
    try {
      const j=await post('/api/Contract/searchById',{contractId});
      report.contracts.push({contractId,success:j.success===true,errorCode:j.errorCode??null,found:Boolean(j.contract),name:j.contract?.name??null,active:j.contract?.activeContract??null});
    } catch { report.contracts.push({contractId,success:false,transportError:true}); }
    save(); await pause(2000);
  }
  for(const q of queries) {
    try { const {date,...payload}=q; const j=await post('/api/History/retrieveBars',{...payload,live});report.results.push({...q,...summarize(j,q)}); }
    catch(e) {
      const status=String(e?.message||'').match(/HTTP (\d{3})/);
      report.results.push({...q,success:false,transportError:true,httpStatus:status?+status[1]:null});
    }
    save(); await pause(2000);
  }
  report.status='complete';report.finishedAt=new Date().toISOString();save();return report;
}
module.exports={cases,summarize,readReport,runAudit,AUDIT_ID};
