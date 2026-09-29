// Run with: node --test session-windows.test.js
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const {DateTime,Settings}=require('luxon');
const server=fs.readFileSync(`${__dirname}/server.js`,'utf8');
const html=fs.readFileSync(`${__dirname}/indicator.html`,'utf8');
const context=vm.createContext({DateTime,ZONE:'America/Los_Angeles'});
// Exercise production functions without starting the collector or HTTP server.
vm.runInContext(server.slice(server.indexOf('function dt('),server.indexOf('function vwap('))+server.slice(server.indexOf('function nowPT('),server.indexOf('function previousRTH('))+server.slice(server.indexOf('function currentSessions('),server.indexOf('function previousCMEWeek('))+html.slice(html.indexOf('function etSessionMinutes('),html.indexOf('function ptParts(')),context);
const et=s=>DateTime.fromISO(s,{zone:'America/New_York'});
const bar=(t,h=100,l=90)=>({t:t.toISO(),o:95,h,l,c:95,v:1});
test('ET windows, exclusive boundaries, freeze, and DST',()=>{
  const originalNow=Settings.now;
  try {
    for(const day of ['2026-01-13','2026-07-14','2026-03-08','2026-03-09','2026-11-01','2026-11-02']){
      const end=et(`${day}T08:30`),start=et(`${day}T03:00`);
      const asia=start.minus({days:1}).set({hour:20});
      const rows=[bar(asia.minus({minutes:5}),999,-999),bar(asia,101,89),bar(start.minus({minutes:5}),102,88),bar(start,103,87),bar(end.minus({minutes:10}),104,86),bar(end.minus({minutes:5}),105,85),bar(end,999,-999)];
      Settings.now=()=>end.toMillis();
      const result=context.currentSessions(rows);
      assert.equal(result.asia.high,102,day);
      assert.equal(result.asia.low,88,day);
      assert.equal(result.london.high,105,day);
      assert.equal(result.london.low,85,day);
      assert.equal(result.london.bars,3,day);
      assert.equal(result.bounds.asiaStartEastern,asia.toISO());
      assert.equal(result.bounds.londonEndEastern,end.toISO());
      assert.equal(DateTime.fromISO(result.bounds.londonEndPacific).toMillis(),end.toMillis());
      assert.equal(context.etSessionMinutes(asia.toISO()),1200);
      assert.equal(context.etSessionMinutes(start.toISO()),180);
      assert.equal(context.etSessionMinutes(end.set({hour:9,minute:30}).toISO()),570);
      Settings.now=()=>end.plus({hours:2}).toMillis();
      assert.equal(context.currentSessions(rows).london.high,105,'London freezes');
      Settings.now=()=>start.toMillis();
      assert.equal(context.currentSessions(rows).london,null,'No London bars at start');
      Settings.now=()=>asia.minus({minutes:1}).toMillis();
      assert.equal(context.currentSessions(rows).asia,null,'Asia has not opened');
    }
    for(const [day,hours] of [['2026-03-08',6],['2026-11-01',8]]){
      const stamp=et(`${day}T08:30`).toMillis();
      Settings.now=()=>stamp;
      const b=context.currentSessions([]).bounds;
      assert.equal(DateTime.fromISO(b.asiaEndEastern).diff(DateTime.fromISO(b.asiaStartEastern),'hours').hours,hours);
    }
    const resetStamp=et('2026-09-29T18:00').toMillis();
    Settings.now=()=>resetStamp;
    const reset=context.currentSessions([]);
    assert.equal(reset.asia,null);
    assert.equal(reset.london,null);
    assert.equal(reset.bounds.asiaStartEastern,et('2026-09-29T20:00').toISO());
  } finally { Settings.now=originalNow; }
});
test('browser script parses and session labels match canonical ET windows',()=>{
  for(const script of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(script[1]);
  for(const label of ['Asia 20:00 ET','London 03:00 ET','NY 09:30 ET','03:00–08:30 ET']) assert.ok(html.includes(label));
  assert.ok(!html.includes('05:20 PT'));
});


test('clean-start indicator UI defaults overlays and panels closed',()=>{
  assert.ok(html.includes('const defaultChartLayers={sessions:false,prior:false,profiles:false,pivots:false,ob:false,orb:false,middayOrb:false,vwap:false,traps:false,absorption:false,liquidity:false,structure:false,edgeful:false}'));
  assert.equal((html.match(/class="layerbtn active"/g)||[]).length,0);
  assert.equal((html.match(/<details class="section collapsibleSection" open>/g)||[]).length,0);
  assert.ok(html.includes("const notesOpen=host.querySelector('.edgeNotes')?.open===true"));
  assert.ok(html.includes('edgeNotes.open=notesOpen'));
});
