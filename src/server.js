const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fetch = require('node-fetch');
const path = require('path');
const pino = require('pino');

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
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

// MAPAS PARA RASTREAR ESTADO DE CONVERSACIÓN
const clientesEnCurso = new Map(); // { numero: { estado: 'esperando_direccion'|'esperando_confirmacion', pedido: {...} } }

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
  console.log(`\n📨 Mensaje de ${de}: ${texto.substring(0, 50)}...`);

  // ============================================
  // ESTADO 1: CLIENTE RESPONDIENDO CON DIRECCIÓN COMPLETA
  // ============================================
  if (clientesEnCurso.has(de) && clientesEnCurso.get(de).estado === 'esperando_direccion') {
    const cursoCliente = clientesEnCurso.get(de);
    const pedido = cursoCliente.pedido;
    
    // ACTUALIZA la dirección que el cliente acaba de enviar
    pedido.datos.ciudad = texto;
    pedido.datos.completo = true;
    
    console.log(`✅ Dirección recibida, pidiendo confirmación...`);
    
    // ENVÍA: Verifica la dirección y pide CONFIRMO
    const msgVerificar = `✅ Gracias por tu dirección.\n\nResumen de tu pedido #${pedido.datos.numeroPedido}:\n🛍️ ${pedido.datos.producto}\n📦 Cantidad: ${pedido.datos.cantidad}\n💰 Total: $${pedido.datos.total}\n📍 Entrega en: ${texto}\n\nResponde *CONFIRMO* para confirmar tu pedido. ✅`;
    
    await enviarMensaje(de, msgVerificar);
    pedido.respuesta_bot = msgVerificar;
    pedido.estado = 'pendiente_confirmacion';
    pedido.decision_ia = 'Dirección completada — esperando confirmación del cliente';
    
    // CAMBIA estado: ahora espera que diga CONFIRMO
    clientesEnCurso.set(de, { estado: 'esperando_confirmacion', pedido });
    io.emit('pedido_actualizado', pedido);
    return;
  }

  // ============================================
  // ESTADO 2: CLIENTE CONFIRMANDO CON "CONFIRMO"
  // ============================================
  if (clientesEnCurso.has(de) && clientesEnCurso.get(de).estado === 'esperando_confirmacion' && texto.toUpperCase().includes('CONFIRMO')) {
    const cursoCliente = clientesEnCurso.get(de);
    const pedido = cursoCliente.pedido;
    
    console.log(`✅ Cliente confirmó, enviando confirmación final...`);
    
    // CONFIRMA PEDIDO (envía mensaje de confirmación)
    await confirmarPedido(pedido.id, true, de);
    
    // LIMPIA el estado del cliente
    clientesEnCurso.delete(de);
    console.log(`✅ Pedido completamente confirmado`);
    return;
  }

  // ============================================
  // ESTADO 3: NUEVO PEDIDO (del formulario Shopify)
  // ============================================
  if (!texto.includes('pedido #') && !texto.includes('Pedido #')) {
    console.log(`⏭️ Ignorando: mensaje sin número de pedido`);
    return;
  }

  console.log(`\n🔍 Analizando nuevo pedido de Shopify...`);
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

  // ============================================
  // DECISIÓN: ¿Dirección completa o incompleta?
  // ============================================
  
  if (!datosPedido.ciudad || datosPedido.ciudad === 'null' || datosPedido.ciudad === null) {
    // 🔴 DIRECCIÓN INCOMPLETA - Pedir que la complete
    console.log(`📍 Dirección incompleta, pidiendo al cliente...`);
    
    const msgSolicitarDireccion = `Hola ${datosPedido.nombre || 'cliente'} 👋\n\nHemos recibido tu pedido #${datosPedido.numeroPedido} 🎉\n\nPara proceder con el envío necesitamos tu dirección completa.\n\n📍 Por favor responde con tu dirección de entrega detallada.\n\n¡Gracias!`;
    
    await enviarMensaje(de, msgSolicitarDireccion);
    pedido.respuesta_bot = msgSolicitarDireccion;
    pedido.decision_ia = 'Dirección incompleta — esperando que el cliente la complete';
    pedido.estado = 'esperando_direccion';
    
    // REGISTRA que este cliente está esperando dirección
    clientesEnCurso.set(de, { estado: 'esperando_direccion', pedido });
    io.emit('pedido_actualizado', pedido);
    return;
  }

  // ✅ DIRECCIÓN COMPLETA - Pedir confirmación directamente
  console.log(`✅ Dirección completa desde el inicio, pidiendo confirmación...`);
  
  const msgPedirConfirmacion = `Hola ${datosPedido.nombre || 'cliente'} 👋\n\nHemos recibido tu pedido #${datosPedido.numeroPedido} 🎉\n\nResumen:\n🛍️ ${datosPedido.producto}\n📦 Cantidad: ${datosPedido.cantidad}\n💰 Total: $${datosPedido.total}\n📍 Entrega en: ${datosPedido.ciudad}\n\nResponde *CONFIRMO* para confirmar tu pedido. ✅`;
  
  await enviarMensaje(de, msgPedirConfirmacion);
  pedido.respuesta_bot = msgPedirConfirmacion;
  pedido.estado = 'pendiente_confirmacion';
  pedido.decision_ia = 'Pedido completo — esperando confirmación del cliente';
  
  // REGISTRA que este cliente está esperando que diga CONFIRMO
  clientesEnCurso.set(de, { estado: 'esperando_confirmacion', pedido });
  io.emit('pedido_actualizado', pedido);
}

async function analizarPedidoConIA(texto) {
  try {
    console.log(`\n🤖 Analizando con Claude Sonnet 4.6...`);
    
    const prompt = `You are an expert at extracting order information from WhatsApp messages in Spanish.

Your task: Extract order details from this WhatsApp message and respond ONLY with valid JSON.

MESSAGE TO ANALYZE:
${texto}

INSTRUCTIONS:
1. Extract EXACTLY these fields:
   - nombre: Customer name (null if not found)
   - numeroPedido: Order number/ID (look for #XXXX or "pedido #XXXX")
   - producto: Product description
   - cantidad: Quantity
   - total: Total price WITHOUT currency symbol (just the number like "29.99")
   - ciudad: Full delivery address with street + number OR intersection (e.g., "Sarar y Magnolia, Cuenca, Azuay")
   - telefono: Phone number
   - completo: true if all required fields present, false otherwise
   - faltante: List of missing fields (empty string if none)
   - decision: Brief explanation in Spanish

2. RULES FOR "ciudad":
   - COMPLETE address: "Sarar y Magnolia, Cuenca" OR "Calle 5 #234, Ciudad" OR "Av. Principal 100"
   - INCOMPLETE address: "Calle 5" OR "Av. Principal" (no number/intersection)
   - If address is incomplete or missing, set to null

3. Return ONLY valid JSON, no markdown, no backticks, no extra text.

RESPONSE FORMAT (valid JSON only):
{
  "nombre": "string or null",
  "numeroPedido": "string or null",
  "producto": "string or null",
  "cantidad": "string or null",
  "total": "number string or null",
  "ciudad": "string or null",
  "telefono": "string or null",
  "completo": boolean,
  "faltante": "string or empty",
  "decision": "string in Spanish"
}`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 500,
        messages: [{
          role: 'user',
          content: prompt
        }]
      })
    });

    if (!response.ok) {
      const errorData = await response.json();
      console.error(`❌ Error de API Anthropic (${response.status}):`, errorData);
      throw new Error(`API Error ${response.status}: ${errorData.error?.message || 'Unknown'}`);
    }

    const data = await response.json();
    
    if (!data.content || !data.content[0] || !data.content[0].text) {
      console.error('❌ Respuesta inesperada de API:', JSON.stringify(data));
      throw new Error('Invalid API response structure');
    }

    let rawText = data.content[0].text.trim();
    console.log(`📦 Respuesta bruta de Claude:\n${rawText.substring(0, 200)}...`);

    rawText = rawText.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();

    let datosParsed;
    try {
      datosParsed = JSON.parse(rawText);
    } catch (parseError) {
      console.error('❌ Error parseando JSON:', parseError.message);
      console.error('   Texto recibido:', rawText);
      throw new Error(`JSON parse error: ${parseError.message}`);
    }

    const camposRequeridos = ['nombre', 'numeroPedido', 'producto', 'cantidad', 'total', 'ciudad', 'telefono', 'completo', 'faltante', 'decision'];
    const camposFaltantes = camposRequeridos.filter(campo => !(campo in datosParsed));
    
    if (camposFaltantes.length > 0) {
      console.warn(`⚠️ Campos faltantes en respuesta:`, camposFaltantes);
      camposRequeridos.forEach(campo => {
        if (!(campo in datosParsed)) {
          datosParsed[campo] = null;
        }
      });
      datosParsed.completo = false;
    }

    console.log(`✅ Análisis exitoso:`, {
      nombre: datosParsed.nombre,
      numeroPedido: datosParsed.numeroPedido,
      ciudad: datosParsed.ciudad,
      completo: datosParsed.completo
    });

    return datosParsed;

  } catch (error) {
    console.error('❌ Error en analizarPedidoConIA:', error.message);
    console.error('   Stack:', error.stack);
    
    return {
      nombre: null,
      numeroPedido: null,
      producto: null,
      cantidad: null,
      total: null,
      ciudad: null,
      telefono: null,
      completo: false,
      faltante: 'Error en análisis',
      decision: `❌ Error: ${error.message}. Requiere revisión manual.`
    };
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
