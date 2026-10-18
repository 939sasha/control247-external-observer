import { createCipheriv,createDecipheriv,randomBytes } from 'node:crypto';
import { deflateSync,inflateSync } from 'node:zlib';
import { mkdtempSync,readFileSync,writeFileSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const AAD=Buffer.from('CONTROL247_GITHUB_STATE_V1');
export function keyFromHex(value) {
  if (!/^[a-f0-9]{64}$/i.test(value||'')) throw new Error('state_key_invalid');
  return Buffer.from(value,'hex');
}
export function seal(raw,key) {
  if (raw.length>262144) throw new Error('state_size_limit');
  const nonce=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,nonce);cipher.setAAD(AAD);
  const encrypted=Buffer.concat([cipher.update(deflateSync(raw)),cipher.final()]);
  return AAD.toString()+'\n'+Buffer.concat([nonce,cipher.getAuthTag(),encrypted]).toString('base64');
}
export function unseal(body,key) {
  if (typeof body!=='string' || body.length>60000 || !body.startsWith(AAD.toString()+'\n')) throw new Error('state_envelope_invalid');
  const text=body.slice(AAD.length+1);
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(text)) throw new Error('state_encoding_invalid');
  const raw=Buffer.from(text,'base64');if(raw.length<29)throw new Error('state_encoding_invalid');
  const cipher=createDecipheriv('aes-256-gcm',key,raw.subarray(0,12));cipher.setAAD(AAD);cipher.setAuthTag(raw.subarray(12,28));
  let decoded;
  try { decoded=inflateSync(Buffer.concat([cipher.update(raw.subarray(28)),cipher.final()]),{maxOutputLength:262144}); }
  catch { throw new Error('state_authentication_failed'); }
  if (decoded.subarray(0,16).toString()!=='SQLite format 3\0') throw new Error('state_database_invalid');
  return decoded;
}
export function openState({body,key,persist,initialize=false}) {
  const directory=mkdtempSync(join(tmpdir(),'control247-observer-')),file=join(directory,'state.sqlite');
  let sql;
  try {
    if (!initialize) writeFileSync(file,unseal(body,key),{mode:0o600});
    sql=new DatabaseSync(file);
    if (initialize) sql.exec(readFileSync(new URL('./schema.sql',import.meta.url),'utf8'));
    if (sql.prepare('PRAGMA integrity_check').get().integrity_check!=='ok') throw new Error('state_database_invalid');
  } catch(error) { sql?.close();rmSync(directory,{recursive:true,force:true});throw error; }
  const save=async()=>{await persist(seal(readFileSync(file),key));};
  const prepare=(query)=>{
    const statement={query,params:[],bind(...params){this.params=params;return this;},
      async first(){return sql.prepare(this.query).get(...this.params)||null;},
      async run(){const result=sql.prepare(this.query).run(...this.params);await save();return {meta:{changes:Number(result.changes)}};}};
    return statement;
  };
  return {prepare,async batch(statements){
    const results=[];sql.exec('BEGIN IMMEDIATE');
    try {for(const s of statements){const r=sql.prepare(s.query).run(...s.params);results.push({meta:{changes:Number(r.changes)}});}sql.exec('COMMIT');}
    catch(error){sql.exec('ROLLBACK');throw error;}
    await save();return results;
  },save,async prune(){
    const count=sql.prepare("SELECT count(*) AS n FROM notices WHERE state IN ('pending','claimed','uncertain','failed')").get().n;
    if(count>128)throw new Error('manual_delivery_review_required');
    sql.exec("DELETE FROM notices WHERE state='sent' AND id!='channel-test-v1' AND id NOT IN (SELECT id FROM notices ORDER BY created DESC,id DESC LIMIT 32)");
    await save();
  },close(){sql.close();rmSync(directory,{recursive:true,force:true});}};
}
export function issueStorage({repository,number,token,request=fetch}) {
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository||'') || !/^[1-9]\d{0,9}$/.test(String(number||'')) || !token) throw new Error('github_configuration_invalid');
  const url=`https://api.github.com/repos/${repository}/issues/${number}`;
  async function call(method,body){
    try {
      const response=await request(url,{method,redirect:'error',signal:AbortSignal.timeout(15000),headers:{Authorization:`Bearer ${token}`,Accept:'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28','Content-Type':'application/json'},...(body===undefined?{}:{body:JSON.stringify({body})})});
      if(!response.ok)throw new Error();
      const result=await response.json();
      if(String(result.number)!==String(number)||typeof result.body!=='string')throw new Error();
      return result.body;
    } catch {throw new Error('github_state_api_failed');}
  }
  return {read:()=>call('GET'),save:async body=>{const readback=await call('PATCH',body);if(readback!==body)throw new Error('github_state_readback_mismatch');}};
}
