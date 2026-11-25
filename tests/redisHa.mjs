// Disposable loopback-only processes; no Docker, system services or production.
import assert from 'node:assert/strict';
import {createServer} from 'node:net';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdirSync,mkdtempSync,writeFileSync,existsSync,rmSync} from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {randomUUID} from 'node:crypto';
import {Redis} from 'ioredis';
import {createRedisAccess} from '../adapter/redis.mjs';
import {createCacheBus} from '../adapter/cacheBus.mjs';
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
export async function waitRedis(check,label,timeout=30000){
 const deadline=Date.now()+timeout;let last;
 while(Date.now()<deadline){try{const value=await check();if(value)return value;}catch(error){last=error.code||error.name;}await delay(150);}
 throw Error('Redis HA timeout: '+label+(last?' ('+last+')':''));
}
export async function startRedisHa(){
 const localBinary=path.resolve('.work/runtime/redis-7.2.17/src/redis-server');
 const binary=process.env.HA_TEST_REDIS_BIN || (existsSync(localBinary)?localBinary:'redis-server');
 const directory=mkdtempSync(path.resolve('.work/redis-ha-')),nodes=[],reservations=[];
 const password='synthetic-data-'+randomUUID(),sentinelPassword='synthetic-sentinel-'+randomUUID(),name='omni-ha-test';
 for(let i=0;i<6;i++){const socket=createServer();await new Promise((resolve,reject)=>{socket.once('error',reject);socket.listen(0,'127.0.0.1',resolve);});reservations.push(socket);nodes.push({port:socket.address().port,sentinel:i>2,index:i});}
 await Promise.all(reservations.map(socket=>new Promise(resolve=>socket.close(resolve))));
 const env={HA_REDIS_MODE:'sentinel',HA_REDIS_MASTER:name,HA_REDIS_NODES:JSON.stringify(nodes.slice(3).map(({port})=>({host:'127.0.0.1',port}))),HA_REDIS_PASSWORD:password,HA_REDIS_SENTINEL_PASSWORD:sentinelPassword,REDIS_KEY_PREFIX:'ha_test_'+randomUUID()+':',HA_REDIS_TLS:'false',HA_REDIS_SENTINEL_TLS:'false'};
 async function inspect(node,command){
  const client=new Redis({host:'127.0.0.1',port:node.port,password:node.sentinel?sentinelPassword:password,lazyConnect:true,connectTimeout:500,commandTimeout:1000,maxRetriesPerRequest:0,enableOfflineQueue:false,retryStrategy:null});client.on('error',()=>{});
  try{await client.connect();const info=await client.info('server');assert.ok(info.includes('process_id:'+node.child.pid+'\r\n'),'Refusing an unowned Redis process');return await command(client);}finally{client.disconnect();}
 }
 async function start(node){
  if(node.child&&node.child.exitCode===null&&!node.child.signalCode)return;
  const local=path.join(directory,'node-'+node.index);mkdirSync(local,{recursive:true});
  const file=path.join(local,'redis.conf');
  if(!existsSync(file))writeFileSync(file,[`bind 127.0.0.1`,`protected-mode yes`,`port ${node.port}`,`daemonize no`,`dir ${local}`,`logfile ""`,`loglevel warning`,`maxclients 64`,
   ...(node.sentinel?[`requirepass ${sentinelPassword}`,`sentinel monitor ${name} 127.0.0.1 ${nodes[0].port} 2`,`sentinel auth-pass ${name} ${password}`,`sentinel down-after-milliseconds ${name} 1000`,`sentinel failover-timeout ${name} 10000`,`sentinel parallel-syncs ${name} 1`]:[`requirepass ${password}`,`masterauth ${password}`,`save ""`,`appendonly no`,`maxmemory 16mb`,`maxmemory-policy allkeys-lru`,`repl-diskless-sync-delay 0`,...(node.index?[`replicaof 127.0.0.1 ${nodes[0].port}`]:[])])].join('\n')+'\n',{mode:0o600});
  node.log='';node.child=spawn(binary,[file,...(node.sentinel?['--sentinel']:[])],{cwd:local,env:{...process.env,TMPDIR:local},stdio:['ignore','pipe','pipe']});
  node.child.on('error',error=>{node.error=error;});
  for(const output of [node.child.stdout,node.child.stderr])output.on('data',data=>{node.log=(node.log+data).slice(-4000);});
  await waitRedis(async()=>{
   if(node.error||node.child.exitCode!==null||node.child.signalCode)throw Error('Owned Redis did not start: '+(node.error?.code||node.log));
   return inspect(node,client=>client.ping());
  },'owned process '+node.index,10000);
 }
 async function stop(node){
  if(!node.child||node.child.exitCode!==null||node.child.signalCode)return;
  const exited=once(node.child,'exit');node.child.kill('SIGTERM');const timer=setTimeout(()=>node.child.kill('SIGKILL'),3000);try{await exited;}finally{clearTimeout(timer);}
 }
 async function master(){
  const address=await inspect(nodes[3],client=>client.sentinel('get-master-addr-by-name',name));assert.equal(address?.[0],'127.0.0.1');
  const node=nodes.slice(0,3).find(node=>node.port===Number(address[1]));assert.ok(node,'Sentinel returned an unowned data server');return node;
 }
 async function healthy(){
  const roles=await Promise.all(nodes.slice(0,3).map(node=>inspect(node,client=>client.info('replication'))));
  return roles.filter(info=>info.includes('role:master\r\n')).length===1&&roles.filter(info=>info.includes('role:slave\r\n')&&info.includes('master_link_status:up\r\n')).length===2;
 }
 const fixture={env,nodes,directory,inspect,master,
  async failover(){const old=await master();await stop(old);const promoted=await waitRedis(async()=>{const node=await master();if(node===old)return false;return(await inspect(node,client=>client.info('replication'))).includes('role:master\r\n')?node:false;},'automatic Sentinel promotion');return{old:old.port,promoted:promoted.port};},
  async outage(){for(const node of nodes.slice(0,3))await stop(node);},
  async recover(){for(const node of nodes.slice(0,3))await start(node);await waitRedis(healthy,'one primary/two connected replicas after recovery',45000);},
  async close(){for(const node of [...nodes].reverse())await stop(node);rmSync(directory,{recursive:true});},
 };
 try{for(const node of nodes)await start(node);await waitRedis(healthy,'initial replication');await waitRedis(async()=>Number(await inspect(nodes[3],client=>client.sentinel('ckquorum',name)).then(value=>/^OK/.test(value)))===1,'Sentinel quorum');return fixture;}
 catch(error){await fixture.close();throw error;}
}
async function test(){
 const fixture=await startRedisHa(),access=createRedisAccess(fixture.env);let refreshed=0;
 const bus=createCacheBus({env:fixture.env,access,identity:'synthetic-live-redis-test',epoch:()=>1,owns:()=>true,invalidate:()=>{refreshed++;}});
 try{
  await bus.start();await waitRedis(()=>bus.status().connected&&refreshed>0,'initial subscription');
  let before=refreshed;await bus.publish(['api_keys']);await waitRedis(()=>refreshed>before,'real invalidation delivery');
  const result=await fixture.failover();await waitRedis(async()=>(await(await access.get()).ping())==='PONG'&&bus.status().connected,'client/subscriber reconnect');
  before=refreshed;await bus.publish(['provider_connections']);await waitRedis(()=>refreshed>before,'invalidation after election');
  console.log('PASS live isolated Redis Sentinel: primary '+result.old+' -> '+result.promoted+', authenticated three-data/three-Sentinel quorum, real Pub/Sub and client/subscriber reconnection');
  await fixture.outage();await waitRedis(()=>!bus.status().connected,'outage observed');await assert.rejects(async()=>{await(await access.get()).ping();});
  before=refreshed;await fixture.recover();await waitRedis(()=>bus.status().connected&&refreshed>before,'outage recovery invalidation',45000);
  await waitRedis(async()=>(await(await access.get()).ping())==='PONG','publisher recovery');
  console.log('PASS live isolated Redis outage: bounded command failure, original processes restart, one primary/two replicas recover and reconnect invalidates missed cache state; no production/Cluster/TLS claim');
 }finally{bus.close();access.close();await fixture.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href)await test();
