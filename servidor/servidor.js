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

// Clientes conectados: id -> { socket, suscripciones (Set de filtros) }
const clientes = new Map();

function registrar(mensaje) {
    console.log(`[MQTT] ${mensaje}`);
}

/* ------------------------- logica del broker -------------------------- */

function conectarCliente(socket, estado, paquete) {
    if (estado.id !== null) {
        registrar(`Error: "${estado.id}" envio CONNECT dos veces, cerrando`);
        socket.destroy();
        return;
    }
    if (!paquete.idCliente) {
        socket.write(crear.connack(CODIGO.RECHAZADO));
        socket.end();
        return;
    }

    // Si ya existe una sesion con el mismo id, se expulsa la anterior
    // (mismo comportamiento que MQTT).
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

function suscribirCliente(socket, estado, paquete) {
    if (!protocolo.esFiltroValido(paquete.canal)) {
        socket.write(crear.suback(CODIGO.RECHAZADO));
        registrar(`Suscripcion rechazada de "${estado.id}": filtro invalido "${paquete.canal}"`);
        return;
    }
    estado.suscripciones.add(paquete.canal);
    socket.write(crear.suback(CODIGO.OK));
    registrar(`"${estado.id}" suscrito al canal "${paquete.canal}"`);
}

function desuscribirCliente(socket, estado, paquete) {
    estado.suscripciones.delete(paquete.canal);
    socket.write(crear.unsuback());
    registrar(`"${estado.id}" cancelo la suscripcion al canal "${paquete.canal}"`);
}

// Nucleo del esquema publicar/suscribir: reenviar el mensaje a todos los
// clientes cuyo filtro de suscripcion coincida con el canal.
function difundir(estado, paquete) {
    if (!protocolo.esCanalValido(paquete.canal)) {
        registrar(`Publicacion descartada de "${estado.id}": canal invalido "${paquete.canal}"`);
        return;
    }
    const trama = crear.publish(paquete.canal, paquete.mensaje);
    let receptores = 0;
    for (const cliente of clientes.values()) {
        for (const filtro of cliente.suscripciones) {
            if (protocolo.coincideCanal(filtro, paquete.canal)) {
                cliente.socket.write(trama);
                receptores++;
                break; // no duplicar si coincide con varios filtros
            }
        }
    }
    registrar(`"${estado.id}" publico en "${paquete.canal}" (${paquete.mensaje.length} bytes) -> ${receptores} suscriptor(es)`);
}

function procesarPaquete(socket, estado, paquete) {
    if (estado.id === null && paquete.tipo !== TIPO.CONNECT) {
        registrar(`Error: paquete tipo ${paquete.tipo} antes de CONNECT, cerrando`);
        socket.destroy();
        return;
    }

    switch (paquete.tipo) {
        case TIPO.CONNECT:     conectarCliente(socket, estado, paquete); break;
        case TIPO.SUBSCRIBE:   suscribirCliente(socket, estado, paquete); break;
        case TIPO.UNSUBSCRIBE: desuscribirCliente(socket, estado, paquete); break;
        case TIPO.PUBLISH:     difundir(estado, paquete); break;
        case TIPO.PINGREQ:     socket.write(crear.pingresp()); break;
        case TIPO.DISCONNECT:  socket.end(); break;
        default:
            registrar(`Paquete no soportado (tipo ${paquete.tipo}) de "${estado.id}"`);
    }
}

/* ------------------------- servidor TCP ------------------------------- */

const servidor = net.createServer((socket) => {
    const estado = {
        id: null,
        socket,
        buffer: Buffer.alloc(0),
        suscripciones: new Set()
    };

    socket.on('data', (datos) => {
        estado.buffer = Buffer.concat([estado.buffer, datos]);
        try {
            const { paquetes, resto } = protocolo.extraerPaquetes(estado.buffer);
            estado.buffer = resto;
            for (const paquete of paquetes) {
                procesarPaquete(socket, estado, paquete);
            }
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

    socket.on('error', (error) => {
        registrar(`Error de socket (${estado.id ?? 'sin identificar'}): ${error.message}`);
    });
});

servidor.listen(PUERTO, HOST, () => {
    registrar(`Servidor escuchando en ${HOST}:${PUERTO}`);
});
