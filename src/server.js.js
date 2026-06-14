const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fetch = require('node-fetch');
const path = require('path');
const pino = require('pino');
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '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';
const PORT = process.env.PORT || 3000;
const COURIERS = ['Servientrega', 'Gintracom'];

const MENSAJE_CONFIRMACION = (nombre, numeroPedido, producto, cantidad, total, ciudad) =>
`✅ ${nombre}, tu pedido #${numeroPedido} ha sido CONFIRMADO 🎉

🛍️ ${producto}
📦 Cantidad: ${cantidad}
💰 Total: $${total}
📍 Entrega en: ${ciudad}

🚚 Tu pedido llegará a su destino en un plazo de 24 a 72 horas hábiles.
¡Gracias por tu compra! 💪`;

const MENSAJE_EN_CAMINO = (nombre, numeroPedido, courier) =>
`📦 ${nombre}, tu pedido #${numeroPedido} ya está en camino con *${courier}*.

🚚 Llegará en 24 a 72 horas hábiles.
¡Gracias por tu paciencia! 💪`;

const MENSAJE_ENTREGADO = (nombre, numeroPedido) =>
`✅ ${nombre}, tu pedido #${numeroPedido} fue entregado exitosamente.

¡Gracias por tu compra! Si tienes alguna consulta, escríbenos. 😊`;

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

const pedidos = [];
let qrDataUrl = null;
let estadoConexion = 'desconectado';
let sock = null;

const esperandoConfirmacion = new Map();
const esperandoDireccion = new Map();

async function conectarWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger: pino({ level: 'silent' }),
    printQRInTerminal: true,
    auth: state,
  });

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      const QRCode = require('qrcode');
      qrDataUrl = await QRCode.toDataURL(qr);
      estadoConexion = 'esperando_qr';
      io.emit('qr', qrDataUrl);
      io.emit('estado', { estado: 'esperando_qr', mensaje: 'Escanea el código QR con tu WhatsApp' });
      console.log('\n📱 Escanea el QR con tu WhatsApp\n');
    }

    if (connection === 'close') {
      const shouldReconnect = lastDisconnect?.error instanceof Boom &&
        lastDisconnect.error.output?.statusCode !== DisconnectReason.loggedOut;
      estadoConexion = 'desconectado';
      io.emit('estado', { estado: 'desconectado', mensaje: 'WhatsApp desconectado' });
      if (shouldReconnect) {
        console.log('Reconectando...');
        conectarWhatsApp();
      }
    }

    if (connection === 'open') {
      estadoConexion = 'conectado';
      qrDataUrl = null;
      io.emit('estado', { estado: 'conectado', mensaje: 'WhatsApp conectado ✓' });
      console.log('✅ WhatsApp conectado y listo!');
    }
  });

  sock.ev.on('creds.update', saveCreds);

const mensajesProcesados = new Set();

sock.ev.on('messages.upsert', async ({ messages }) => {
    for (const msg of messages) {
      if (msg.key.fromMe) continue;
      
      // DEDUPLICADOR
      const msgId = msg.key.id;
      if (mensajesProcesados.has(msgId)) continue;
      mensajesProcesados.add(msgId);
      
      const texto = msg.message?.conversation || msg.message?.extendedTextMessage?.text || '';
      if (!texto) continue;
      const de = msg.key.remoteJid;
      await procesarMensaje(texto.trim(), de, msg);
    }
  });
}

async function enviarMensaje(jid, texto) {
  if (sock && estadoConexion === 'conectado') {
    await sock.sendMessage(jid, { text: texto });
  }
}

async function procesarMensaje(texto, de, msg) {
  if (esperandoDireccion.has(de)) {
    const pedido = esperandoDireccion.get(de);
    pedido.datos.ciudad = texto;
    pedido.datos.completo = true;
    esperandoDireccion.delete(de);
    const msgConfirmar = `✅ Gracias por tu dirección.\n\nResumen de tu pedido #${pedido.datos.numeroPedido}:\n🛍️ ${pedido.datos.producto}\n📦 Cantidad: ${pedido.datos.cantidad}\n💰 Total: $${pedido.datos.total}\n📍 Entrega en: ${texto}\n\nResponde *CONFIRMO* para confirmar tu pedido.`;
    await enviarMensaje(de, msgConfirmar);
    esperandoConfirmacion.set(de, pedido);
    pedido.estado = 'pendiente_confirmacion';
    pedido.decision_ia = 'Dirección recibida, esperando confirmación del cliente';
    io.emit('pedido_actualizado', pedido);
    return;
  }

  if (esperandoConfirmacion.has(de) && texto.toUpperCase().includes('CONFIRMO')) {
    const pedido = esperandoConfirmacion.get(de);
    esperandoConfirmacion.delete(de);
    await confirmarPedido(pedido.id, true, de);
    return;
  }

  if (!texto.includes('pedido #') && !texto.includes('Pedido #')) return;

  console.log(`\n📨 Nuevo pedido recibido de ${de}`);
  const datosPedido = await analizarPedidoConIA(texto);

  const pedido = {
    id: Date.now(),
    telefono: de,
    texto,
    datos: datosPedido,
    timestamp: new Date().toISOString(),
    estado: 'revision',
    decision_ia: datosPedido.decision,
    courier: null,
    respuesta_bot: null,
    confirmado_en: null
  };

  pedidos.unshift(pedido);
  io.emit('nuevo_pedido', pedido);

  if (!datosPedido.ciudad || datosPedido.ciudad === 'null' || datosPedido.ciudad === null) {
    const msgDireccion = `Hola ${datosPedido.nombre || 'cliente'} 👋\n\nHemos recibido tu pedido #${datosPedido.numeroPedido} 🎉\n\nPara proceder con el envío necesitamos tu dirección completa.\n\n📍 Por favor responde con tu dirección de entrega detallada.\n\n¡Gracias!`;
    await enviarMensaje(de, msgDireccion);
    pedido.respuesta_bot = msgDireccion;
    pedido.decision_ia = 'Dirección incompleta — esperando que el cliente la complete';
    esperandoDireccion.set(de, pedido);
    io.emit('pedido_actualizado', pedido);
    return;
  }

  const msgConfirmar = `Hola ${datosPedido.nombre || 'cliente'} 👋\n\nHemos recibido tu pedido #${datosPedido.numeroPedido} 🎉\n\nResumen:\n🛍️ ${datosPedido.producto}\n📦 Cantidad: ${datosPedido.cantidad}\n💰 Total: $${datosPedido.total}\n📍 Entrega en: ${datosPedido.ciudad}\n\nResponde *CONFIRMO* para confirmar tu pedido. ✅`;
  await enviarMensaje(de, msgConfirmar);
  pedido.respuesta_bot = msgConfirmar;
  pedido.estado = 'pendiente_confirmacion';
  pedido.decision_ia = 'Pedido completo — esperando confirmación del cliente';
  esperandoConfirmacion.set(de, pedido);
  io.emit('pedido_actualizado', pedido);
}

async function analizarPedidoConIA(texto) {
  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 1000,
        messages: [{
          role: 'user',
          content: `Analiza este mensaje de pedido de WhatsApp y extrae los datos. Responde SOLO con JSON válido, sin texto adicional.

Mensaje:
${texto}

Responde con este formato exacto:
{
  "nombre": "nombre del cliente o null",
  "numeroPedido": "número del pedido o null",
  "producto": "descripción del producto o null",
  "cantidad": "cantidad o null",
  "total": "monto total sin símbolo $ o null",
"ciudad": "ciudad y provincia o null. Una dirección COMPLETA debe tener calle con número o intersección (ej: '39 y La A', 'Av. Principal 234'). Una dirección INCOMPLETA es solo nombre de calle sin número (ej: '9 de octubre', 'Av. Amazonas'). Una intersección como 'Sarar y Magnolia' SÍ es una dirección completa. Solo pon null si es únicamente el nombre de una calle sin intersección ni número",  "faltante": "lista de campos que faltan o vacío",
  "decision": "texto breve explicando la decisión de la IA"
}`
        }]
      })
    });
    const data = await response.json();
    if (!data.content || !data.content[0]) throw new Error('API error');
    const limpio = data.content[0].text.trim().replace(/```json|```/g, '').trim();
    return JSON.parse(limpio);
  } catch (error) {
    console.error('Error al analizar con IA:', error);
    return { nombre: null, numeroPedido: null, producto: null, cantidad: null, total: null, ciudad: null, completo: false, faltante: 'Error', decision: 'Error en análisis IA — revisión manual requerida' };
  }
}

async function confirmarPedido(pedidoId, automatico = false, jidOverride = null) {
  const pedido = pedidos.find(p => p.id === pedidoId);
  if (!pedido) return { error: 'Pedido no encontrado' };
  const { nombre, numeroPedido, producto, cantidad, total, ciudad } = pedido.datos;
  const mensaje = MENSAJE_CONFIRMACION(nombre || 'Cliente', numeroPedido || pedidoId, producto || 'Tu producto', cantidad || '1', total || '0', ciudad || 'tu dirección');
  try {
    const jid = jidOverride || pedido.telefono;
    if (jid !== 'simulado@test') await enviarMensaje(jid, mensaje);
    pedido.estado = 'confirmado';
    pedido.respuesta_bot = mensaje;
    pedido.confirmado_en = new Date().toISOString();
    pedido.confirmado_por = automatico ? 'IA automática' : 'Manual';
    io.emit('pedido_actualizado', pedido);
    console.log(`✅ Pedido #${numeroPedido} confirmado`);
    return { ok: true, pedido };
  } catch (error) {
    console.error('Error:', error);
    return { error: error.message };
  }
}

app.get('/api/pedidos', (req, res) => res.json(pedidos));
app.get('/api/couriers', (req, res) => res.json(COURIERS));

app.get('/api/estado', (req, res) => {
  res.json({
    estado: estadoConexion, qr: qrDataUrl,
    totalPedidos: pedidos.length,
    confirmados: pedidos.filter(p => p.estado === 'confirmado').length,
    enCamino: pedidos.filter(p => p.estado === 'en_camino').length,
    entregados: pedidos.filter(p => p.estado === 'entregado').length,
    pendientes: pedidos.filter(p => p.estado === 'pendiente_confirmacion').length,
    totalDinero: pedidos.filter(p => ['confirmado','en_camino','entregado'].includes(p.estado) && p.datos.total).reduce((acc, p) => acc + parseFloat(p.datos.total || 0), 0).toFixed(2)
  });
});

app.post('/api/confirmar/:id', async (req, res) => {
  const resultado = await confirmarPedido(parseInt(req.params.id), false);
  res.json(resultado);
});

app.post('/api/rechazar/:id', (req, res) => {
  const pedido = pedidos.find(p => p.id === parseInt(req.params.id));
  if (!pedido) return res.json({ error: 'No encontrado' });
  pedido.estado = 'rechazado';
  io.emit('pedido_actualizado', pedido);
  res.json({ ok: true });
});

app.post('/api/en-camino/:id', async (req, res) => {
  const pedido = pedidos.find(p => p.id === parseInt(req.params.id));
  if (!pedido) return res.json({ error: 'No encontrado' });
  const { courier } = req.body;
  pedido.estado = 'en_camino';
  pedido.courier = courier;
  pedido.en_camino_en = new Date().toISOString();
  if (pedido.telefono !== 'simulado@test') {
    await enviarMensaje(pedido.telefono, MENSAJE_EN_CAMINO(pedido.datos.nombre || 'Cliente', pedido.datos.numeroPedido, courier));
  }
  io.emit('pedido_actualizado', pedido);
  res.json({ ok: true });
});

app.post('/api/entregado/:id', async (req, res) => {
  const pedido = pedidos.find(p => p.id === parseInt(req.params.id));
  if (!pedido) return res.json({ error: 'No encontrado' });
  pedido.estado = 'entregado';
  pedido.entregado_en = new Date().toISOString();
  if (pedido.telefono !== 'simulado@test') {
    await enviarMensaje(pedido.telefono, MENSAJE_ENTREGADO(pedido.datos.nombre || 'Cliente', pedido.datos.numeroPedido));
  }
  io.emit('pedido_actualizado', pedido);
  res.json({ ok: true });
});

app.post('/api/simular', async (req, res) => {
  const mensajePrueba = `Hola Ana Ullón,\nHemos recibido tu pedido #1237 🎉\nActualmente se encuentra PENDIENTE DE CONFIRMACIÓN\n🛍️ Soporte de Sentadillas y Abdominales\n📦 Cantidad: 1 Soporte de Sentadillas y Abdominales - Rosa\n💰 Total a pagar: $29.99\n📍 Dirección de entrega: Sarar y Magnolia, CUENCA, AZUAY\n📞 Teléfono: +593989153620`;
  const datosPedido = await analizarPedidoConIA(mensajePrueba);
  const pedido = { id: Date.now(), telefono: 'simulado@test', texto: mensajePrueba, datos: datosPedido, timestamp: new Date().toISOString(), estado: datosPedido.completo ? 'pendiente_confirmacion' : 'revision', decision_ia: datosPedido.decision, courier: null, respuesta_bot: null, confirmado_en: null };
  pedidos.unshift(pedido);
  io.emit('nuevo_pedido', pedido);
  res.json({ ok: true, pedido });
});

server.listen(PORT, () => {
  console.log(`\n🚀 Panel iniciado en http://localhost:${PORT}`);
  console.log('📱 Iniciando conexión con WhatsApp...\n');
});

conectarWhatsApp();
