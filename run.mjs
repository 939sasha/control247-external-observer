import { createConnection,isIP } from 'node:net';
import { Readable } from 'node:stream';
import { pathToFileURL } from 'node:url';
import { runObserver,sshReachable,telegramNotifier,deliver } from './observer.mjs';
import { issueStorage,keyFromHex,openState } from './state.mjs';

export function validTarget(host){
  if(isIP(host)!==4)return false;
  const [a,b]=host.split('.').map(Number);
  return a>0&&a<224&&a!==10&&a!==127&&!(a===169&&b===254)&&!(a===172&&b>=16&&b<=31)&&!(a===192&&b===168)&&!(a===100&&b>=64&&b<=127);
}
function connect({hostname,port}){
  const socket=createConnection({host:hostname,port});
  const opened=new Promise((resolve,reject)=>{socket.once('connect',resolve);socket.once('error',reject);});
  return {opened,get readable(){return Readable.toWeb(socket);},close(){socket.destroy();return Promise.resolve();}};
}
export async function maintainSchedule(env,request=fetch,now=Date.now()){
  if(!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(env.GITHUB_REPOSITORY||''))throw new Error('github_configuration_invalid');
  const url=`https://api.github.com/repos/${env.GITHUB_REPOSITORY}/contents/observer-heartbeat.txt`;
  const headers={Authorization:`Bearer ${env.GITHUB_TOKEN}`,Accept:'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28','Content-Type':'application/json'};
  const previous=await request(url,{headers,redirect:'error',signal:AbortSignal.timeout(15000)});
  let sha;
  if(previous.ok){
    const file=await previous.json();sha=file.sha;
    const stamp=Date.parse(Buffer.from(file.content||'','base64').toString('utf8').trim());
    if(!Number.isFinite(stamp)||stamp>now+300000)throw new Error('heartbeat_marker_invalid');
    if(now-stamp<7*86400000)return false;
  }else if(previous.status!==404)throw new Error('heartbeat_read_failed');
  const result=await request(url,{method:'PUT',headers,redirect:'error',signal:AbortSignal.timeout(15000),body:JSON.stringify({message:'Maintain scheduled external observation',content:Buffer.from(new Date(now).toISOString()+'\n').toString('base64'),...(sha?{sha}:{})})});
  if(!result.ok)throw new Error('heartbeat_commit_failed');
  return true;
}
export async function assertDeliveryReceipts(db){
  const unresolved=await db.prepare("SELECT id FROM notices WHERE state IN ('claimed','uncertain','failed') LIMIT 1").first();
  if(unresolved)throw new Error('delivery_manual_review_required');
}
export async function testChannel(db,notify,now){
  await db.prepare("INSERT OR IGNORE INTO notices(id,kind,episode,state,created) VALUES('channel-test-v1','channel_test',0,'pending',?)").bind(now).run();
  const previous=await db.prepare("SELECT state FROM notices WHERE id='channel-test-v1'").first();
  if(previous.state==='pending')await deliver(db,notify,now);
  const receipt=await db.prepare("SELECT state FROM notices WHERE id='channel-test-v1'").first();
  if(receipt.state!=='sent')throw new Error('channel_test_manual_review_required');
}
export async function main(env=process.env){
  if(env.GITHUB_ACTIONS!=='true')throw new Error('github_workflow_only');
  if(!validTarget(env.MONITOR_HOST))throw new Error('public_target_required');
  const key=keyFromHex(env.OBSERVER_STATE_KEY),storage=issueStorage({repository:env.GITHUB_REPOSITORY,number:env.STATE_ISSUE,token:env.GITHUB_TOKEN});
  const db=openState({body:await storage.read(),key,persist:storage.save});
  try{
    const now=Date.now();
    const result=await runObserver({db,probe:()=>sshReachable(connect,{hostname:env.MONITOR_HOST}),notify:telegramNotifier(env),scheduledTime:now,now});
    if(env.OBSERVER_TEST_CHANNEL==='true'){
      if(env.GITHUB_EVENT_NAME!=='workflow_dispatch')throw new Error('manual_test_only');
      await testChannel(db,telegramNotifier(env),now);
    }
    await db.prune();
    await assertDeliveryReceipts(db);
    await maintainSchedule(env);
    if(['uncertain','claimed'].includes(result.delivery))throw new Error('delivery_manual_review_required');
  }finally{db.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  main().then(()=>console.log('observer_tick_completed')).catch(()=>{console.error('observer_tick_failed_review_required');process.exitCode=1;});
}
