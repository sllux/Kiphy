/**
 * Kick Chat & Alerts OBS Overlay Server (Centrifugo WebSocket Relay)
 * Suporta canais dinâmicos via URL:
 * https://kiphy.onrender.com/nomedocanal
 * ou https://kiphy.onrender.com/?channel=nomedocanal
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

const WebSocketClient = globalThis.WebSocket || (() => {
  try { return require('ws'); } catch (e) { return null; }
})();

if (!WebSocketClient) {
  console.error('[ERRO] WebSocket não encontrado. Execute com: node --experimental-websocket server.js');
  process.exit(1);
}

const PORT = process.env.PORT || 3000;

function sanitizeSlug(raw) {
  if (!raw) return 'renanbalinha';
  let str = raw.toString().trim();
  str = str.replace(/^https?:\/\/(www\.)?kick\.com\//i, '');
  str = str.split('?')[0].split('&')[0].split('/')[0].split('#')[0];
  str = str.replace(/[^a-zA-Z0-9_\-]/g, '').toLowerCase().trim();
  return str || 'renanbalinha';
}

const DEFAULT_CHANNEL = sanitizeSlug(process.argv[2] || process.env.KICK_CHANNEL || 'renanbalinha');

const channelCache = new Map();
const sseClients = new Set();

let kickWs = null;
let currentToken = null;
let subscribedTopics = new Set();
let messageIdCounter = 1;

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// 1. Obter Chatroom ID e Channel ID
async function getChannelInfo(rawSlug) {
  const cleanSlug = sanitizeSlug(rawSlug);
  if (channelCache.has(cleanSlug)) {
    return channelCache.get(cleanSlug);
  }

  console.log(`[Kick] Buscando informações do canal: ${cleanSlug}...`);
  const res = await fetch(`https://kick.com/api/v2/channels/${encodeURIComponent(cleanSlug)}`, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
  });

  if (!res.ok) {
    throw new Error(`Falha ao buscar canal "${cleanSlug}" (HTTP ${res.status})`);
  }

  const data = await res.json();
  const info = {
    slug: cleanSlug,
    channelId: data.id,
    chatroomId: data.chatroom ? data.chatroom.id : data.id
  };

  channelCache.set(cleanSlug, info);
  console.log(`[Kick] Canal "${cleanSlug}" conectado! (Chatroom ID: ${info.chatroomId})`);
  return info;
}

// 2. Obter Token Guest JWT para o Centrifugo
async function getKickRealtimeToken() {
  const clientId = generateUUID();

  const res = await fetch('https://web.kick.com/api/v1/realtime/auth/connection', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'User-Agent': 'Mozilla/5.0'
    },
    body: JSON.stringify({ client_id: clientId })
  });

  if (!res.ok) {
    throw new Error(`Erro ao obter token da Kick (HTTP ${res.status})`);
  }

  const json = await res.json();
  if (!json.data || !json.data.token) {
    throw new Error('Resposta de autenticação inválida da Kick');
  }

  return json.data.token;
}

// 3. Conectar ao WebSocket Centrifugo
async function connectToKick() {
  if (kickWs && (kickWs.readyState === WebSocketClient.OPEN || kickWs.readyState === WebSocketClient.CONNECTING)) {
    return;
  }

  try {
    currentToken = await getKickRealtimeToken();
    const wsUrl = 'wss://realtime.us-west-2.platform.kick.com/connection/websocket';

    const wsOptions = {
      headers: {
        'Origin': 'https://kick.com',
        'User-Agent': 'Mozilla/5.0'
      }
    };

    try {
      kickWs = new WebSocketClient(wsUrl, wsOptions);
    } catch (e) {
      kickWs = new WebSocketClient(wsUrl);
    }

    kickWs.onopen = () => {
      console.log('[Kick WS] Conexão estabelecida! Autenticando com Centrifugo...');
      const connectCommand = {
        connect: {
          token: currentToken,
          name: 'js'
        },
        id: messageIdCounter++
      };
      kickWs.send(JSON.stringify(connectCommand));
    };

    kickWs.onmessage = (event) => {
      handleKickMessage(event.data);
    };

    kickWs.onerror = (err) => {
      console.error('[Kick WS] Erro:', err.message || err);
    };

    kickWs.onclose = (event) => {
      console.warn(`[Kick WS] Conexão encerrada (${event.code}). Reconectando em 5s...`);
      subscribedTopics.clear();
      setTimeout(connectToKick, 5000);
    };
  } catch (err) {
    console.error('[Kick] Falha ao conectar:', err.message);
    setTimeout(connectToKick, 5000);
  }
}

// Processar mensagens recebidas do Centrifugo
function handleKickMessage(raw) {
  const lines = raw.toString().split('\n');

  for (const line of lines) {
    if (!line.trim()) continue;

    if (line.trim() === '{}') {
      if (kickWs && kickWs.readyState === WebSocketClient.OPEN) {
        kickWs.send('{}');
      }
      continue;
    }

    try {
      const msg = JSON.parse(line);

      if (msg.connect) {
        console.log(`[Kick WS] ✅ Autenticado com sucesso! Pronto para receber eventos.`);
        for (const [slug, info] of channelCache.entries()) {
          subscribeToAllChannelTopics(info);
        }
      }

      if (msg.push && msg.push.pub && msg.push.pub.data) {
        const pub = msg.push.pub;
        const topicName = msg.push.channel;
        const eventName = pub.data.event;

        let eventPayload = pub.data.data;
        if (typeof eventPayload === 'string') {
          try {
            eventPayload = JSON.parse(eventPayload);
          } catch (e) {}
        }

        if (eventName.includes("ChatMessage")) {
          console.log(`💬 [Chat] ${eventPayload.sender?.username || "Anon"}: ${eventPayload.content || ""}`);
        } else {
          console.log(`[Evento] ${eventName} (${topicName})`);
        }

        broadcastEvent({
          type: eventName,
          channel: topicName,
          data: eventPayload
        });
      }
    } catch (e) {
      console.error('[Kick WS] Erro ao processar frame:', e.message);
    }
  }
}

function subscribeTopic(topic) {
  if (subscribedTopics.has(topic)) return;
  if (!kickWs || kickWs.readyState !== WebSocketClient.OPEN) return;

  console.log(`[Kick WS] Assinando tópico: ${topic}...`);
  const subCommand = {
    subscribe: { channel: topic },
    id: messageIdCounter++
  };
  kickWs.send(JSON.stringify(subCommand));
  subscribedTopics.add(topic);
}

function subscribeToAllChannelTopics(info) {
  subscribeTopic(`chatrooms.${info.chatroomId}.v2`);
  subscribeTopic(`channel.${info.channelId}`);
}

// Transmitir eventos filtrados apenas para quem está assistindo o canal correspondente
function broadcastEvent(eventData) {
  const formatted = `data: ${JSON.stringify(eventData)}\n\n`;
  const eventTopic = eventData.channel; // Ex: 'chatrooms.14039724.v2', 'channel.14239844' ou 'test'

  for (const client of sseClients) {
    // Se for mensagem de teste (/test), envia para todas as telas
    if (eventTopic === 'test') {
      try {
        client.res.write(formatted);
      } catch (e) {
        sseClients.delete(client);
      }
      continue;
    }

    // Identifica o canal deste cliente
    const clientInfo = channelCache.get(client.channelSlug);
    if (!clientInfo) continue;

    const chatroomTopic = `chatrooms.${clientInfo.chatroomId}.v2`;
    const channelTopic = `channel.${clientInfo.channelId}`;

    // Só envia a mensagem se o tópico for exatamente o canal que o usuário abriu!
    if (eventTopic === chatroomTopic || eventTopic === channelTopic) {
      try {
        client.res.write(formatted);
      } catch (e) {
        sseClients.delete(client);
      }
    }
  }
}

// Servidor HTTP usando WHATWG URL API
const server = http.createServer(async (req, res) => {
  const reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = reqUrl.pathname;
  const searchParams = reqUrl.searchParams;

  // 1. Rota SSE para o OBS Studio (/events)
  if (pathname === '/events') {
    const rawChannel = searchParams.get('channel') || DEFAULT_CHANNEL;
    const channelSlug = sanitizeSlug(rawChannel);

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });

    res.write(': connected\n\n');

    const clientObj = { res, channelSlug };
    sseClients.add(clientObj);

    getChannelInfo(channelSlug).then((info) => {
      subscribeToAllChannelTopics(info);
    }).catch(err => {
      console.error('[SSE] Erro:', err.message);
    });

    req.on('close', () => {
      sseClients.delete(clientObj);
    });
    return;
  }

  // 2. Rota para simular testes no OBS (/test)
  if (pathname === '/test' || pathname === '/api/test') {
    const eventType = searchParams.get('type') || 'all';

    console.log(`[Teste] Disparando simulação: ${eventType}`);

    if (eventType === 'chat' || eventType === 'all') {
      broadcastEvent({
        type: 'App\\Events\\ChatMessageEvent',
        channel: 'test',
        data: {
          id: 'test-msg-' + Date.now(),
          content: 'Chat da Kick funcionando com balão de fala! [emote:1730755:emojiBubbly]',
          sender: {
            username: 'ViewerLegal',
            identity: { badges: [{ type: 'subscriber' }] }
          }
        }
      });
    }

    if (eventType === 'sub' || eventType === 'all') {
      setTimeout(() => {
        broadcastEvent({
          type: 'App\\Events\\SubscriptionEvent',
          channel: 'test',
          data: {
            username: 'InscritoFiel',
            months_subscribed: 6
          }
        });
      }, 500);
    }

    if (eventType === 'gift' || eventType === 'all') {
      setTimeout(() => {
        broadcastEvent({
          type: 'App\\Events\\GiftedSubscriptionsEvent',
          channel: 'test',
          data: {
            gifter_username: 'SuperDoador',
            gifted_usernames: ['user1', 'user2', 'user3', 'user4', 'user5']
          }
        });
      }, 1000);
    }

    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
      status: 'success',
      message: `Simulação de "${eventType}" enviada para o OBS!`
    }));
    return;
  }

  // 3. Favicon (evita 404 nos logs)
  if (pathname === '/favicon.ico') {
    res.writeHead(204);
    res.end();
    return;
  }

  // 4. Qualquer outra rota (ex: '/', '/xqc', '/coringa', '/renanbalinha') serve o overlay!
  const htmlPath = path.join(__dirname, 'public', 'index.html');
  if (fs.existsSync(htmlPath)) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    fs.createReadStream(htmlPath).pipe(res);
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Página não encontrada');
});

server.listen(PORT, async () => {
  console.log('====================================================');
  console.log(`🚀 Kick Chat Overlay Server rodando na porta ${PORT}`);
  console.log(`📺 Exemplo: http://localhost:${PORT}/renanbalinha`);
  console.log(`📺 Ou outro: http://localhost:${PORT}/xqc`);
  console.log('====================================================');

  try {
    const info = await getChannelInfo(DEFAULT_CHANNEL);
    await connectToKick();
    subscribeToAllChannelTopics(info);
  } catch (err) {
    console.error('[Inicialização] Erro:', err.message);
  }
});
