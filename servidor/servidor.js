/*
 * Servidor de mensajes (broker) basado en MQTT.
 *
 * Esquema de canales: los clientes se suscriben a un canal y cada mensaje
 * publicado en ese canal se retransmite a todos los suscriptores.
 *
 * Uso:  node servidor/servidor.js [puerto]
 */

const net = require('net');
const protocolo = require('../compartido/protocolo');
const { TIPO, CODIGO, crear } = protocolo;

const PUERTO = Number(process.argv[2] || process.env.PUERTO || protocolo.PUERTO_DEFECTO);
const HOST = process.env.HOST || '0.0.0.0';

const clientes = new Map(); // id -> { socket, suscripciones (Set de filtros) }
const registrar = (mensaje) => console.log(`[MQTT] ${mensaje}`);

function conectar(socket, estado, paquete) {
    if (estado.id !== null) return socket.destroy(); // CONNECT repetido
    if (!paquete.idCliente) return socket.end(crear.connack(CODIGO.RECHAZADO));

    // Si el id ya esta en uso, se expulsa la sesion anterior (como MQTT)
    const anterior = clientes.get(paquete.idCliente);
    if (anterior) {
        registrar(`Cliente "${paquete.idCliente}" reconectado, expulsando sesion anterior`);
        anterior.socket.destroy();
    }
    estado.id = paquete.idCliente;
    clientes.set(estado.id, estado);
    socket.write(crear.connack(CODIGO.OK));
    registrar(`Cliente conectado: "${estado.id}" desde ${socket.remoteAddress}:${socket.remotePort}`);
}

// Nucleo del esquema publicar/suscribir: reenviar el mensaje a todos los
// clientes que tengan al menos un filtro que coincida con el canal.
function difundir(estado, paquete) {
    if (!protocolo.esCanalValido(paquete.canal)) return;
    const trama = crear.publish(paquete.canal, paquete.mensaje);
    let receptores = 0;
    for (const cliente of clientes.values()) {
        if ([...cliente.suscripciones].some((filtro) => protocolo.coincideCanal(filtro, paquete.canal))) {
            cliente.socket.write(trama);
            receptores++;
        }
    }
    registrar(`"${estado.id}" publico en "${paquete.canal}" (${paquete.mensaje.length} bytes) -> ${receptores} suscriptor(es)`);
}

function procesar(socket, estado, paquete) {
    if (estado.id === null && paquete.tipo !== TIPO.CONNECT) return socket.destroy();
    switch (paquete.tipo) {
        case TIPO.CONNECT: return conectar(socket, estado, paquete);
        case TIPO.PUBLISH: return difundir(estado, paquete);
        case TIPO.SUBSCRIBE:
            if (!protocolo.esFiltroValido(paquete.canal)) return socket.write(crear.suback(CODIGO.RECHAZADO));
            estado.suscripciones.add(paquete.canal);
            socket.write(crear.suback(CODIGO.OK));
            return registrar(`"${estado.id}" suscrito al canal "${paquete.canal}"`);
        case TIPO.UNSUBSCRIBE:
            estado.suscripciones.delete(paquete.canal);
            socket.write(crear.unsuback());
            return registrar(`"${estado.id}" cancelo la suscripcion a "${paquete.canal}"`);
        case TIPO.PINGREQ: return socket.write(crear.pingresp());
        case TIPO.DISCONNECT: return socket.end();
    }
}

const servidor = net.createServer((socket) => {
    const estado = { id: null, socket, buffer: Buffer.alloc(0), suscripciones: new Set() };

    socket.on('data', (datos) => {
        estado.buffer = Buffer.concat([estado.buffer, datos]);
        try {
            const { paquetes, resto } = protocolo.extraerPaquetes(estado.buffer);
            estado.buffer = resto;
            paquetes.forEach((paquete) => procesar(socket, estado, paquete));
        } catch (error) {
            registrar(`Paquete mal formado de "${estado.id ?? socket.remoteAddress}": ${error.message}`);
            socket.destroy();
        }
    });

    socket.on('close', () => {
        if (estado.id !== null && clientes.get(estado.id) === estado) {
            clientes.delete(estado.id);
            registrar(`Cliente desconectado: "${estado.id}"`);
        }
    });

    socket.on('error', () => {}); // un reset de un cliente no debe tumbar el servidor
});

servidor.listen(PUERTO, HOST, () => registrar(`Servidor escuchando en ${HOST}:${PUERTO}`));
