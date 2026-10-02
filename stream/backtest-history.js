// Fixed collector-only backfill. No account/order API calls. Successful chunks
// are immutable on the Railway volume and survive deployment/restarts.
const fs=require('node:fs');
const path=require('node:path');
const ID='backfill-2026-04-01-to-2026-10-01-v1';
function windows(market){
  const out=[];
  for(const [expiry,start,end] of [['M26','2026-04-01','2026-06-15'],['U26','2026-06-15','2026-09-14'],['Z26','2026-09-14','2026-10-02']]){
    const finish=Date.parse(end+'T00:00:00Z');
    for(let t=Date.parse(start+'T00:00:00Z');t<finish;t+=7*86400000){
      const startTime=new Date(t).toISOString(),endTime=new Date(Math.min(t+7*86400000,finish)).toISOString();
      out.push({contractId:`CON.F.US.${market}.${expiry}`,startTime,endTime});
    }
  }
  return out;
}
function normalize(rows,query){
  const lo=Date.parse(query.startTime),hi=Date.parse(query.endTime),map=new Map();
  let invalid=0,duplicates=0,outOfWindow=0;
  for(const b of rows||[]){
    const t=Date.parse(b.t??b.time??b.timestamp);
    if(!Number.isFinite(t)||!['o','h','l','c','v'].every(k=>b[k]!=null&&Number.isFinite(+b[k]))||+b.h<Math.max(+b.o,+b.l,+b.c)||+b.l>Math.min(+b.o,+b.h,+b.c)||+b.v<0){invalid++;continue;}
    if(t<lo||t>=hi){outOfWindow++;continue;}
    if(map.has(t))duplicates++;
    map.set(t,{t:new Date(t).toISOString(),o:+b.o,h:+b.h,l:+b.l,c:+b.c,v:+b.v});
  }
  return {bars:[...map.values()].sort((a,b)=>Date.parse(a.t)-Date.parse(b.t)),invalid,duplicates,outOfWindow};
}
function read(file){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch{return null;}}
function write(file,j){fs.writeFileSync(file+'.tmp',JSON.stringify(j));fs.renameSync(file+'.tmp',file);}
async function runBackfill({market,live,post,dir,pause=ms=>new Promise(r=>setTimeout(r,ms))}){
  fs.mkdirSync(dir,{recursive:true});
  const manifestFile=path.join(dir,'manifest.json');
  let manifest=read(manifestFile);
  if(manifest?.id!==ID)manifest={id:ID,market,liveSubscription:live,status:'running',startedAt:new Date().toISOString(),rollRule:'Calendar Monday of quarterly expiration week; raw individual contracts, no price adjustment',chunks:[]};
  if(manifest.status==='complete')return manifest;
  manifest.status='running';write(manifestFile,manifest);
  for(const q of windows(market))for(const unitNumber of [1,5]){
    const filename=`${q.contractId.split('.').at(-1)}-${q.startTime.slice(0,10)}-${unitNumber}m.json`;
    const existing=read(path.join(dir,filename));
    if(existing?.success===true)continue;
    const metadata={filename,...q,unitNumber};
    try{
      const response=await post('/api/History/retrieveBars',{...q,live,unit:2,unitNumber,limit:20000,includePartialBar:false});
      const cleaned=normalize(response.bars,q);
      const chunk={...metadata,success:response.success===true,errorCode:response.errorCode??null,retrievedAt:new Date().toISOString(),...cleaned};
      // 7-day requests contain fewer than 20k minutes, so cannot need pagination.
      if((response.bars||[]).length>=20000)chunk.possiblyTruncated=true;
      write(path.join(dir,filename),chunk);
      Object.assign(metadata,{success:chunk.success,errorCode:chunk.errorCode,count:cleaned.bars.length,invalid:cleaned.invalid,duplicates:cleaned.duplicates,outOfWindow:cleaned.outOfWindow,first:cleaned.bars[0]?.t??null,last:cleaned.bars.at(-1)?.t??null,possiblyTruncated:Boolean(chunk.possiblyTruncated)});
    }catch(e){const m=String(e?.message||'').match(/HTTP (\d{3})/);Object.assign(metadata,{success:false,transportError:true,httpStatus:m?+m[1]:null});}
    const index=manifest.chunks.findIndex(c=>c.filename===filename);
    if(index<0)manifest.chunks.push(metadata);else manifest.chunks[index]=metadata;
    write(manifestFile,manifest);await pause(2000);
  }
  manifest.status=manifest.chunks.some(c=>!c.success)?'complete-with-errors':'complete';manifest.finishedAt=new Date().toISOString();write(manifestFile,manifest);return manifest;
}
function exportData(dir){
  const manifest=read(path.join(dir,'manifest.json'));
  if(!manifest)return {status:'pending'};
  return {...manifest,chunks:manifest.chunks.map(c=>({...c,bars:read(path.join(dir,c.filename))?.bars||[]}))};
}
module.exports={ID,windows,normalize,read,runBackfill,exportData};
