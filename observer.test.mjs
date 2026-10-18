import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {runObserver,deliver,sshReachable,telegramNotifier} from './observer.mjs';

class D1 {
  constructor(file=':memory:',initialize=true) {this.db=new DatabaseSync(file);if(initialize)this.db.exec(readFileSync(new URL('./schema.sql',import.meta.url),'utf8'));}
  prepare(sql) {
    const root=this;
    function statement(params=[]) {
      return {sql,params,bind:(...args)=>statement(args),first:async()=>root.db.prepare(sql).get(...params)||null,run:async()=>root.execute(sql,params)};
    }
    return statement();
  }
  execute(sql,params) {return {meta:{changes:Number(this.db.prepare(sql).run(...params).changes)}};}
  async batch(statements) {
    this.db.exec('BEGIN');
    try {const result=statements.map(s=>this.execute(s.sql,s.params));this.db.exec('COMMIT');return result;}
    catch(error) {this.db.exec('ROLLBACK');throw error;}
  }
}
function fixture(t) {
  const db=new D1();t.after(()=>db.db.close());const notices=[];
  const notify=async notice=>{notices.push(notice);return 'sent';};
  const tick=(slot,ok,overrides={})=>runObserver({db,probe:async()=>ok,notify,scheduledTime:slot*300000,now:slot*300000,...overrides});
  return {db,notices,notify,tick};
}

test('healthy baseline sends nothing; two failures send once, recovery sends once',async t=>{
  const {tick,notices}=fixture(t);
  await tick(0,true);await tick(1,false);assert.equal(notices.length,0);
  await tick(2,false);await tick(3,false);await tick(4,true);await tick(5,true);
  assert.deepEqual(notices.map(n=>n.kind),['down','recovery']);
});
test('closing and reopening the SQLite file retains state and notice receipts',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'observer-synthetic-'));const file=join(directory,'state.sqlite');let db=new D1(file);let sends=0;
  const tick=(slot)=>runObserver({db,probe:async()=>false,notify:async()=>{sends++;return 'sent';},scheduledTime:slot*300000,now:slot*300000});
  try {await tick(0);await tick(1);db.db.close();db=new D1(file,false);await tick(2);assert.equal(sends,1);}
  finally {db.db.close();rmSync(directory,{recursive:true});}
});
test('duplicate and old scheduled invocations do not probe or send again',async t=>{
  const f=fixture(t);await f.tick(5,true);let probes=0;
  for(const slot of [5,4])assert.equal((await f.tick(slot,false,{probe:async()=>{probes++;return false;}})).status,'duplicate_or_old_tick');
  assert.equal(probes,0);assert.equal(f.notices.length,0);
});
test('missed slots reset the consecutive failure count',async t=>{
  const f=fixture(t);await f.tick(0,false);await f.tick(3,false);assert.equal(f.notices.length,0);
  await f.tick(4,false);assert.equal(f.notices.length,1);
});
test('a subsequent outage is a new episode',async t=>{
  const f=fixture(t);for(const [slot,ok] of [[0,false],[1,false],[2,true],[3,false],[4,false]])await f.tick(slot,ok);
  assert.deepEqual(f.notices.map(n=>n.id),['1:down','1:recovery','2:down']);
});
test('competing contradictory probes cannot insert an uncommitted alert',async t=>{
  const f=fixture(t);await f.tick(0,false);
  let release;const barrier=new Promise(resolve=>{release=resolve;});let entered=0;
  const probe=ok=>async()=>{if(++entered===2)release();await barrier;return ok;};
  const results=await Promise.all([f.tick(1,true,{probe:probe(true)}),f.tick(1,false,{probe:probe(false)})]);
  assert.equal(results.filter(r=>r.status==='concurrent_tick').length,1);
  const state=await f.db.prepare('SELECT * FROM monitor').first();
  assert.equal(f.notices.length,state.phase==='down'?1:0);
});
test('storage transaction failure produces neither state commit nor notification',async t=>{
  const f=fixture(t);await f.tick(0,false);
  f.db.db.exec("CREATE TRIGGER reject_notice BEFORE INSERT ON notices BEGIN SELECT RAISE(ABORT,'synthetic full'); END");
  await assert.rejects(f.tick(1,false));
  assert.equal((await f.db.prepare('SELECT * FROM monitor').first()).last_slot,0);assert.equal(f.notices.length,0);
});
test('lost remote acknowledgement is durable and is never resent',async t=>{
  const f=fixture(t);await f.tick(0,false);let sends=0;
  await f.tick(1,false,{notify:async()=>{sends++;throw new Error('synthetic lost ack');}});
  await f.tick(2,false,{notify:async()=>{sends++;return 'sent';}});
  assert.equal(sends,1);assert.equal((await f.db.prepare('SELECT state FROM notices').first()).state,'uncertain');
});
test('receipt write failure after remote send leaves a durable claim and blocks resend',async t=>{
  const f=fixture(t);await f.tick(0,false);let sends=0;
  f.db.db.exec("CREATE TRIGGER reject_finish BEFORE UPDATE OF state ON notices WHEN NEW.state='sent' BEGIN SELECT RAISE(ABORT,'synthetic full'); END");
  await assert.rejects(f.tick(1,false,{notify:async()=>{sends++;return 'sent';}}));
  f.db.db.exec('DROP TRIGGER reject_finish');await deliver(f.db,async()=>{sends++;return 'sent';},600000);
  assert.equal(sends,1);assert.equal((await f.db.prepare('SELECT state FROM notices').first()).state,'claimed');
});
test('late and future ticks never probe',async t=>{
  const f=fixture(t);let calls=0;
  for(const now of [-1,300001])assert.equal((await f.tick(0,true,{now,probe:async()=>{calls++;return true;}})).status,'late_or_invalid_tick');
  assert.equal(calls,0);
});
test('SSH probe checks only the fixed VDS and a valid SSH2 banner',async()=>{
  let closed=0;
  const socket={opened:Promise.resolve(),readable:new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('SSH-2.0-Synthetic\r\n'));c.close();}}),close:async()=>{closed++;}};
  assert.equal(await sshReachable(target=>{assert.deepEqual(target,{hostname:'203.0.113.42',port:22});return socket;},{hostname:'203.0.113.42'}),true);
  assert.equal(closed,1);
});
test('invalid banners and refused connections do not confirm reachability',async()=>{
  const invalid={opened:Promise.resolve(),readable:new ReadableStream({start(c){c.enqueue(new TextEncoder().encode('HTTP/1.1 200 OK\r\n'));c.close();}}),close:async()=>{}};
  assert.equal(await sshReachable(()=>invalid),false);
  assert.equal(await sshReachable(()=>{throw new Error('synthetic refused');}),false);
});
test('stalled connect and cleanup are both bounded',async()=>{
  const hold=setTimeout(()=>{},2000);const start=Date.now();
  try {assert.equal(await sshReachable(()=>({opened:new Promise(()=>{}),close:()=>new Promise(()=>{})}),{timeoutMs:10}),false);assert.ok(Date.now()-start<1500);}
  finally {clearTimeout(hold);}
});
test('Telegram uses one POST, no redirects, and no retry after lost response',async()=>{
  let calls=0;
  const notifier=telegramNotifier({OWNER_ID:'123',BOT_TOKEN:'123456:SYNTHETIC_NOT_A_REAL_CREDENTIAL'},async(url,request)=>{calls++;assert.equal(request.method,'POST');assert.equal(request.redirect,'error');assert.equal(JSON.parse(request.body).chat_id,'123');throw new Error('synthetic transport loss');});
  assert.equal(await notifier({kind:'down'}),'uncertain');assert.equal(calls,1);
});
