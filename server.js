// 픽셀 배틀 매칭 서버: 게임 페이지 서빙 + 대기열 + 입력 중계 (락스텝 방식)
const http=require('http'),fs=require('fs'),path=require('path'),zlib=require('zlib'),{randomInt}=require('crypto'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000;
const raw=fs.readFileSync(path.join(__dirname,'index.html')),gz=zlib.gzipSync(raw,{level:9});
const srv=http.createServer((req,res)=>{
  if(req.url==='/healthz'){res.end('ok');return}
  const g=/gzip/.test(req.headers['accept-encoding']||'');
  res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache',...(g?{'Content-Encoding':'gzip'}:{})});
  res.end(g?gz:raw);
});
const wss=new WebSocketServer({server:srv,maxPayload:512});
const IDS=['magnet','root','mercy','volt','sera','speedy'],KEYS=['U','j','k','l'];
let wait=null;
const send=(w,o)=>{if(w&&w.readyState===1)w.send(JSON.stringify(o))};
const drop=w=>{if(wait===w)wait=null;if(w.peer){send(w.peer,{t:'left'});w.peer.peer=null;w.peer=null}};
wss.on('connection',ws=>{
  ws.alive=true;ws.peer=null;ws.on('pong',()=>{ws.alive=true});
  ws.on('message',d=>{
    let m;try{m=JSON.parse(d)}catch(e){return}
    if(m.t==='p'){send(ws,{t:'p',ts:Number(m.ts)||Date.now()});return}
    if(m.t==='q'&&!ws.peer&&wait!==ws){
      ws.name=String(m.n||'게스트').slice(0,12);ws.ch=IDS.includes(m.c)?m.c:'magnet';
      if(wait&&wait.readyState===1){
        const a=wait,b=ws;wait=null;a.peer=b;b.peer=a;
        const base={t:'match',seed:randomInt(2**31),map:randomInt(6),names:[a.name,b.name],ids:[a.ch,b.ch]};
        send(a,{...base,slot:0});send(b,{...base,slot:1});
      }else wait=ws;
    }else if(m.t==='in'&&ws.peer){
      send(ws.peer,{t:'in',f:m.f|0,L:m.L?1:0,R:m.R?1:0,D:m.D?1:0,U:m.U?1:0,q:KEYS.includes(m.q)?m.q:0});
    }else if(m.t==='x')drop(ws);
  });
  ws.on('close',()=>drop(ws));ws.on('error',()=>{});
});
setInterval(()=>wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()}),15000);
srv.listen(PORT,()=>console.log('listening',PORT));
