const PERIOD = 300_000;

export async function deliver(db, notify, now) {
  const notice = await db.prepare("SELECT id,kind,episode FROM notices WHERE state='pending' ORDER BY created,id LIMIT 1").first();
  if (!notice) return 'none';
  const claim = await db.prepare("UPDATE notices SET state='claimed',claimed=? WHERE id=? AND state='pending'").bind(now,notice.id).run();
  if (claim.meta.changes !== 1) return 'concurrent';
  // An uncertain remote result is never automatically sent again.
  let state;
  try { state = await notify(notice); } catch { state = 'uncertain'; }
  if (!['sent','failed','uncertain'].includes(state)) state = 'uncertain';
  await db.prepare('UPDATE notices SET state=?,finished=? WHERE id=? AND state=\'claimed\'').bind(state,now,notice.id).run();
  return state;
}

export async function runObserver({db,probe,notify,scheduledTime,now,receipt=crypto.randomUUID()}) {
  if (!Number.isSafeInteger(now) || !Number.isSafeInteger(scheduledTime) || scheduledTime>now || now-scheduledTime>PERIOD) return {status:'late_or_invalid_tick'};
  const slot = Math.floor(scheduledTime/PERIOD);
  const before = await db.prepare('SELECT * FROM monitor WHERE id=1').first();
  if (!before) throw new Error('observer_not_initialized');
  if (slot<=before.last_slot) return {status:'duplicate_or_old_tick'};
  let ok=false;
  try { ok=(await probe())===true; } catch { /* retain no transport exception */ }
  const failures=ok ? 0 : Math.min((slot===before.last_slot+1 ? before.failures : 0)+1,2);
  let phase=before.phase, episode=before.episode, kind=null;
  if (ok) {
    if (phase==='down') kind='recovery';
    phase='up';
  } else if (failures===2 && phase!=='down') {
    phase='down'; episode++; kind='down';
  }
  const statements=[db.prepare('UPDATE monitor SET version=version+1,last_slot=?,failures=?,phase=?,episode=?,checked_at=?,receipt=? WHERE id=1 AND version=? AND last_slot<?').bind(slot,failures,phase,episode,now,receipt,before.version,slot)];
  if (kind) statements.push(db.prepare("INSERT OR IGNORE INTO notices(id,kind,episode,state,created) SELECT ?,?,?,'pending',? WHERE EXISTS(SELECT 1 FROM monitor WHERE id=1 AND receipt=?)").bind(`${episode}:${kind}`,kind,episode,now,receipt));
  // Unique receipt binds notice creation to this exact successful CAS, even
  // when competing scheduled invocations observe different probe results.
  const committed=await db.batch(statements);
  if (committed[0].meta.changes!==1) return {status:'concurrent_tick'};
  return {status:phase,failures,delivery:await deliver(db,notify,now)};
}

export async function sshReachable(connect,{timeoutMs=8000,hostname}={}) {
  let socket,reader;
  const timeout=AbortSignal.timeout(timeoutMs);
  const failed=new Promise((_,reject)=>timeout.addEventListener('abort',()=>reject(new Error('probe_timeout')),{once:true}));
  try {
    socket=connect({hostname,port:22});
    await Promise.race([socket.opened,failed]);
    reader=socket.readable.getReader();let banner='';
    while (banner.length<255) {
      const part=await Promise.race([reader.read(),failed]);
      if (part.done) break;
      banner+=new TextDecoder().decode(part.value).slice(0,255-banner.length);
      if (banner.includes('\n')) break;
    }
    return /^SSH-2\.0-[^\r\n]{1,200}\r?\n/.test(banner);
  } catch { return false; }
  finally {
    const closing=[];
    if (reader) {try {closing.push(reader.cancel());} catch {}}
    if (socket) {try {closing.push(socket.close());} catch {}}
    let cleanupTimer;
    await Promise.race([Promise.allSettled(closing),new Promise(resolve=>{cleanupTimer=setTimeout(resolve,500);})]);
    clearTimeout(cleanupTimer);
    if (reader) {try {reader.releaseLock();} catch {}}
  }
}

export function telegramNotifier(env,request=fetch) {
  if (!/^\d{1,20}$/.test(env.OWNER_ID||'') || !/^\d{6,15}:[A-Za-z0-9_-]{20,}$/.test(env.BOT_TOKEN||'')) throw new Error('observer_secret_configuration_invalid');
  return async notice=>{
    const text=notice.kind==='channel_test'
      ? '🛰 Внешний наблюдатель подключён.\n✅ Проверка из GitHub и доставка в этот бот работают.\nЭто тест связи, а не сообщение о сбое.'
      : notice.kind==='down'
      ? '⚠️ Внешняя связь с VDS недоступна: две последовательные проверки SSH не прошли. Возможна проблема сервера, сети или SSH. Полное выключение сервера не установлено.'
      : '✅ Внешняя связь с VDS восстановилась: SSH снова отвечает. Работа продуктов требует отдельных проверок.';
    try {
      const response=await request(`https://api.telegram.org/bot${env.BOT_TOKEN}/sendMessage`,{method:'POST',redirect:'error',signal:AbortSignal.timeout(10000),headers:{'Content-Type':'application/json'},body:JSON.stringify({chat_id:env.OWNER_ID,text})});
      if (!response.ok) return 'failed';
      const body=await response.text();
      if (body.length>8192) return 'uncertain';
      const parsed=JSON.parse(body),result=parsed.result;
      return parsed.ok===true && Number.isSafeInteger(result?.message_id) && result.message_id>0 && String(result.chat?.id)===env.OWNER_ID && String(result.from?.id)===env.BOT_TOKEN.split(':')[0] ? 'sent' : 'uncertain';
    } catch {return 'uncertain';}
  };
}
