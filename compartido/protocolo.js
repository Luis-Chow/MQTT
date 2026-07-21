/*
 * Protocolo de mensajeria basado en MQTT (QoS 0). Formato de paquete:
 *
 *   [ tipo<<4 ] [ longitud del cuerpo: varint base 128, 1-4 bytes ] [ cuerpo ]
 *
 * Las cadenas del cuerpo (id, canal) van como en MQTT:
 * 2 bytes de longitud (uint16 big-endian) + texto UTF-8.
 */

const TIPO = {
    CONNECT: 1, CONNACK: 2, PUBLISH: 3, SUBSCRIBE: 8, SUBACK: 9,
    UNSUBSCRIBE: 10, UNSUBACK: 11, PINGREQ: 12, PINGRESP: 13, DISCONNECT: 14
};
const CODIGO = { OK: 0, RECHAZADO: 1 };

function codificarLongitud(valor) {
    const bytes = [];
    do {
        const byte = valor % 128;
        valor = Math.floor(valor / 128);
        bytes.push(valor > 0 ? byte | 0x80 : byte); // bit alto = continua
    } while (valor > 0);
    return Buffer.from(bytes);
}

// Devuelve { valor, bytesLeidos } o null si el varint aun esta incompleto
function decodificarLongitud(buffer, offset) {
    let valor = 0;
    for (let i = 0; i < 4; i++) {
        if (offset + i >= buffer.length) return null;
        valor += (buffer[offset + i] & 0x7F) * 128 ** i;
        if (!(buffer[offset + i] & 0x80)) return { valor, bytesLeidos: i + 1 };
    }
    throw new Error('Longitud restante mal formada');
}

function cadena(texto) {
    const datos = Buffer.from(String(texto), 'utf8');
    const longitud = Buffer.alloc(2);
    longitud.writeUInt16BE(datos.length);
    return Buffer.concat([longitud, datos]);
}

function leerCadena(buffer) {
    const fin = 2 + buffer.readUInt16BE(0);
    return { valor: buffer.toString('utf8', 2, fin), resto: buffer.subarray(fin) };
}

function empaquetar(tipo, cuerpo = Buffer.alloc(0)) {
    return Buffer.concat([Buffer.from([tipo << 4]), codificarLongitud(cuerpo.length), cuerpo]);
}

const crear = {
    connect:     (id)             => empaquetar(TIPO.CONNECT, cadena(id)),
    connack:     (codigo)         => empaquetar(TIPO.CONNACK, Buffer.from([codigo])),
    publish:     (canal, mensaje) => empaquetar(TIPO.PUBLISH, Buffer.concat([cadena(canal),
                                        Buffer.isBuffer(mensaje) ? mensaje : Buffer.from(String(mensaje), 'utf8')])),
    subscribe:   (canal)          => empaquetar(TIPO.SUBSCRIBE, cadena(canal)),
    suback:      (codigo)         => empaquetar(TIPO.SUBACK, Buffer.from([codigo])),
    unsubscribe: (canal)          => empaquetar(TIPO.UNSUBSCRIBE, cadena(canal)),
    unsuback:    ()               => empaquetar(TIPO.UNSUBACK),
    pingreq:     ()               => empaquetar(TIPO.PINGREQ),
    pingresp:    ()               => empaquetar(TIPO.PINGRESP),
    disconnect:  ()               => empaquetar(TIPO.DISCONNECT)
};

// Paquete completo (cabecera + cuerpo) -> objeto con sus campos
function analizar(trama) {
    const tipo = trama[0] >> 4;
    const cuerpo = trama.subarray(1 + decodificarLongitud(trama, 1).bytesLeidos);
    const paquete = { tipo };
    if (tipo === TIPO.CONNECT) paquete.idCliente = leerCadena(cuerpo).valor;
    if (tipo === TIPO.CONNACK || tipo === TIPO.SUBACK) paquete.codigo = cuerpo[0] ?? CODIGO.OK;
    if (tipo === TIPO.SUBSCRIBE || tipo === TIPO.UNSUBSCRIBE) paquete.canal = leerCadena(cuerpo).valor;
    if (tipo === TIPO.PUBLISH) {
        const { valor, resto } = leerCadena(cuerpo);
        paquete.canal = valor;
        paquete.mensaje = resto;
    }
    return paquete;
}

// TCP entrega un flujo continuo: separa los paquetes completos del buffer
// y devuelve el sobrante para pegarlo con la siguiente lectura.
function extraerPaquetes(buffer) {
    const paquetes = [];
    while (buffer.length >= 2) {
        const longitud = decodificarLongitud(buffer, 1);
        if (longitud === null) break;
        const total = 1 + longitud.bytesLeidos + longitud.valor;
        if (buffer.length < total) break;
        paquetes.push(analizar(buffer.subarray(0, total)));
        buffer = buffer.subarray(total);
    }
    return { paquetes, resto: buffer };
}

// Comodines de suscripcion:  +  un solo nivel   |   #  todos los niveles (al final)
function coincideCanal(filtro, canal) {
    const f = filtro.split('/'), c = canal.split('/');
    for (let i = 0; i < f.length; i++) {
        if (f[i] === '#') return true;
        if (i >= c.length || (f[i] !== '+' && f[i] !== c[i])) return false;
    }
    return f.length === c.length;
}

const esFiltroValido = (filtro) => typeof filtro === 'string' && filtro.length > 0 &&
    filtro.split('/').every((n, i, todos) => n === '+' || (n === '#' ? i === todos.length - 1 : !/[#+]/.test(n)));

const esCanalValido = (canal) => typeof canal === 'string' && canal.length > 0 && !/[#+]/.test(canal);

module.exports = {
    TIPO, CODIGO, PUERTO_DEFECTO: 1883,
    crear, extraerPaquetes, coincideCanal, esFiltroValido, esCanalValido
};
