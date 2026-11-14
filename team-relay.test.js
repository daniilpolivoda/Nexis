const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const attach = require('./party-relay');
class Socket extends EventEmitter {
  readyState=1; bufferedAmount=0; sent=[];
  send(text){this.sent.push(JSON.parse(text));} ping(){this.emit('pong');}
  close(){this.readyState=3;this.emit('close');} terminate(){this.close();}
  message(data){this.emit('message',Buffer.from(JSON.stringify(data)));}
}
test('team updates are fast, bounded and confined to confirmed party members',t=>{
  let now=1000;const server=new EventEmitter();const service=attach(server,{now:()=>now});t.after(()=>service.close());
  function client(name){const ws=new Socket();server.emit('connection',ws,{url:'/party'});ws.message({type:'hello',id:name,name});return ws;}
  const a=client('Alice'),b=client('Bob'),outside=client('Outside');
  a.message({type:'create',name:'Team'});b.message({type:'join',name:'Team'});
  assert(a.sent[0].features.includes('team'));
  const state={id:'12345678-1234-1234-1234-123456789012',server:'server',dimension:'minecraft:overworld',x:1,y:64,z:2,health:18,maxHealth:20,absorption:4,food:20,armor:10,level:5,xp:.5,ping:50,slot:2};
  for(let i=0;i<150;i++){a.message({type:'team',kind:'pose',state});now+=100;}
  assert.equal(b.sent.filter(m=>m.type==='team').length,150);
  assert.equal(outside.sent.filter(m=>m.type==='team').length,0);
  assert.equal(a.readyState,1,'position packets do not consume the chat command rate budget');
  const data=Buffer.alloc(20000,1).toString('base64');
  a.message({type:'team',kind:'inventory',id:state.id,server:'server',data});
  assert.equal(b.sent.at(-1).data,data);
  const count=b.sent.length;
  a.message({type:'team',kind:'pose',state:{...state,x:NaN}});
  outside.message({type:'team',kind:'pose',state});
  assert.equal(b.sent.length,count);
  b.message({type:'leave'});const after=b.sent.length;now+=100;
  a.message({type:'team',kind:'pose',state});assert.equal(b.sent.length,after);
});
