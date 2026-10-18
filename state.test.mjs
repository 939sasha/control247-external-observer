import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {seal,unseal,keyFromHex,openState,issueStorage} from './state.mjs';
import {initialBody} from './init.mjs';
import {runObserver,telegramNotifier} from './observer.mjs';
import {validTarget,maintainSchedule,main} from './run.mjs';
const hex='ab'.repeat(32),key=keyFromHex(hex);

test('state survives authenticated remote roundtrip; ciphertext hides SQLite and content',async()=>{
 let body=await initialBody(hex);assert.ok(!body.includes('SQLite'));assert.ok(!body.includes('unknown'));
 const db=openState({body,key,persist:async value=>{body=value;}});
 await runObserver({db,probe:async()=>false,notify:async()=>{throw Error('not expected');},scheduledTime:300000,now:300001});db.close();
 const restored=openState({body,key,persist:async()=>{}});
 assert.equal((await restored.prepare('SELECT failures FROM monitor').first()).failures,1);restored.close();
});
test('tampering and wrong keys never reinitialize state',async()=>{
 const body=await initialBody(hex);
 assert.throws(()=>unseal(body,keyFromHex('cd'.repeat(32))),/authentication/);
 const buffer=Buffer.from(body.split('\n')[1],'base64');buffer[buffer.length-1]^=1;
 assert.throws(()=>unseal(body.split('\n')[0]+'\n'+buffer.toString('base64'),key),/authentication/);
 assert.throws(()=>openState({body:'',key,persist:async()=>{}}),/envelope/);
});
test('claimed-before-send survives close/reopen and suppresses uncertain resend',async()=>{
 let body=await initialBody(hex),sent=0;
 for(let tick=1;tick<=3;tick++){
  const db=openState({body,key,persist:async value=>{body=value;}});
  await runObserver({db,probe:async()=>false,notify:async()=>{sent++;return 'uncertain';},scheduledTime:tick*300000,now:tick*300000+1});db.close();
 }
 assert.equal(sent,1);
 const db=openState({body,key,persist:async()=>{}});assert.equal((await db.prepare('SELECT state FROM notices').first()).state,'uncertain');db.close();
});
test('lost state-save acknowledgement before Telegram fails closed across restart',async()=>{
 let body=await initialBody(hex),saves=0,sends=0;
 let db=openState({body,key,persist:async value=>{body=value;}});
 await runObserver({db,probe:async()=>false,notify:async()=>{sends++;return 'sent';},scheduledTime:300000,now:300001});db.close();
 db=openState({body,key,persist:async value=>{body=value;if(++saves===2)throw Error('lost acknowledgement');}});
 await assert.rejects(runObserver({db,probe:async()=>false,notify:async()=>{sends++;return 'sent';},scheduledTime:600000,now:600001}));db.close();
 db=openState({body,key,persist:async value=>{body=value;}});
 await runObserver({db,probe:async()=>false,notify:async()=>{sends++;return 'sent';},scheduledTime:900000,now:900001});
 assert.equal(sends,0);assert.equal((await db.prepare('SELECT state FROM notices').first()).state,'claimed');db.close();
});
test('GitHub state API validates destination and exact encrypted readback',async()=>{
 const body=await initialBody(hex);let saved;
 const store=issueStorage({repository:'example/observer',number:'1',token:'SYNTHETIC',request:async(url,options)=>{
  assert.equal(url,'https://api.github.com/repos/example/observer/issues/1');assert.equal(options.redirect,'error');
  if(options.method==='PATCH')saved=JSON.parse(options.body).body;
  return {ok:true,json:async()=>({number:1,body:saved||body})};
 }});
 assert.equal(await store.read(),body);await store.save(body);assert.equal(saved,body);
 const broken=issueStorage({repository:'example/observer',number:'1',token:'SYNTHETIC',request:async()=>({ok:true,json:async()=>({number:2,body})})});
 await assert.rejects(broken.read(),/api_failed/);
});
test('API failures do not expose request credentials or response bodies',async()=>{
 const store=issueStorage({repository:'example/observer',number:'1',token:'SYNTHETIC_SECRET',request:async()=>{throw Error('SYNTHETIC_SECRET');}});
 await assert.rejects(store.read(),e=>!e.message.includes('SYNTHETIC_SECRET'));
});
test('Telegram confirmation requires exact recipient bot and message id',async()=>{
 const env={OWNER_ID:'123',BOT_TOKEN:'123456:SYNTHETIC_NOT_A_REAL_CREDENTIAL'};
 for(const result of [{message_id:1,chat:{id:123},from:{id:123456}},{message_id:1,chat:{id:456},from:{id:123456}},{message_id:true,chat:{id:123},from:{id:123456}}]){
  const notify=telegramNotifier(env,async()=>({ok:true,text:async()=>JSON.stringify({ok:true,result})}));
  assert.equal(await notify({kind:'down'}),result.chat.id===123&&result.message_id===1?'sent':'uncertain');
 }
});
test('workflow forbids private runners, serializes jobs and exposes no target or token',()=>{
 const text=readFileSync(new URL('./.github/workflows/observer.yml',import.meta.url),'utf8');
 assert.match(text,/github.event.repository.private == false/);assert.match(text,/runs-on: ubuntu-latest/);
 assert.match(text,/cancel-in-progress: false/);assert.match(text,/timeout-minutes: 3/);
 assert.doesNotMatch(text,/upload-artifact|actions\/cache|pull_request|95\.81\.99\.130/);
});
test('weekly activity marker is skipped while fresh and updates only its own file',async()=>{
 let writes=0;const env={GITHUB_REPOSITORY:'example/observer',GITHUB_TOKEN:'SYNTHETIC'};
 const request=async(url,options)=>{
  assert.equal(url,'https://api.github.com/repos/example/observer/contents/observer-heartbeat.txt');
  if(options.method==='PUT'){writes++;return {ok:true};}
  return {ok:true,json:async()=>({sha:'a'.repeat(40),content:Buffer.from('2026-10-01T00:00:00Z').toString('base64')})};
 };
 assert.equal(await maintainSchedule(env,request,Date.parse('2026-10-02T00:00:00Z')),false);
 assert.equal(await maintainSchedule(env,request,Date.parse('2026-10-09T00:00:00Z')),true);assert.equal(writes,1);
});
test('non-workflow execution and private target are rejected before network use',async()=>{
 await assert.rejects(main({}),/workflow_only/);
 await assert.rejects(main({GITHUB_ACTIONS:'true',MONITOR_HOST:'127.0.0.1'}),/public_target/);
 assert.equal(validTarget('203.0.113.42'),true);
 for(const host of ['10.0.0.1','192.168.1.1','169.254.169.254','bad.example','224.0.0.1'])assert.equal(validTarget(host),false);
});
