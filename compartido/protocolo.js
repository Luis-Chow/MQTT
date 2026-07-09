/*
 * Protocolo de mensajeria basado en MQTT (QoS 0). Formato de paquete:
 *
 *   [ tipo<<4 ] [ longitud restante: varint base 128, 1-4 bytes ] [ cuerpo ]
 *
 * Las cadenas del cuerpo (id, canal) van como en MQTT:
 * 2 bytes de longitud (uint16 big-endian) + texto UTF-8.
 */

const TIPO = {
    CONNECT: 1, CONNACK: 2, PUBLISH: 3, SUBSCRIBE: 8, SUBACK: 9,
    UNSUBSCRIBE: 10, UNSUBACK: 11, PINGREQ: 12, PINGRESP: 13, DISCONNECT: 14
};

const CODIGO = { OK: 0, RECHAZADO: 1 };

/* ------------------------- codificacion ------------------------------- */

function codificarLongitud(valor) {
    const bytes = [];
    do {
        const byte = valor % 128;
        valor = Math.floor(valor / 128);
        bytes.push(valor > 0 ? byte | 0x80 : byte); // bit alto = continua
    } while (valor > 0);
    return Buffer.from(bytes);
}

// Devuelve { valor, bytesLeidos } o null si el varint aun esta incompleto.
function decodificarLongitud(buffer, offset) {
    let valor = 0;
    for (let i = 0; i < 4; i++) {
        if (offset + i >= buffer.length) return null;
        valor += (buffer[offset + i] & 0x7F) * 128 ** i;
        if ((buffer[offset + i] & 0x80) === 0) return { valor, bytesLeidos: i + 1 };
    }
    throw new Error('Longitud restante mal formada');
}

function codificarCadena(texto) {
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
    connect:     (idCliente)      => empaquetar(TIPO.CONNECT, codificarCadena(idCliente)),
    connack:     (codigo)         => empaquetar(TIPO.CONNACK, Buffer.from([codigo])),
    publish:     (canal, mensaje) => empaquetar(TIPO.PUBLISH, Buffer.concat([
                                         codificarCadena(canal),
                                         Buffer.isBuffer(mensaje) ? mensaje : Buffer.from(String(mensaje), 'utf8')
                                     ])),
    subscribe:   (canal)          => empaquetar(TIPO.SUBSCRIBE, codificarCadena(canal)),
    suback:      (codigo)         => empaquetar(TIPO.SUBACK, Buffer.from([codigo])),
    unsubscribe: (canal)          => empaquetar(TIPO.UNSUBSCRIBE, codificarCadena(canal)),
    unsuback:    ()               => empaquetar(TIPO.UNSUBACK),
    pingreq:     ()               => empaquetar(TIPO.PINGREQ),
    pingresp:    ()               => empaquetar(TIPO.PINGRESP),
    disconnect:  ()               => empaquetar(TIPO.DISCONNECT)
};

/* ------------------------- decodificacion ----------------------------- */

// Recibe un paquete completo (cabecera + cuerpo) y devuelve sus campos.
function analizar(trama) {
    const tipo = trama[0] >> 4;
    const longitud = decodificarLongitud(trama, 1);
    const cuerpo = trama.subarray(1 + longitud.bytesLeidos);
    const paquete = { tipo };

    if (tipo === TIPO.CONNECT) paquete.idCliente = leerCadena(cuerpo).valor;
    if (tipo === TIPO.CONNACK || tipo === TIPO.SUBACK) paquete.codigo = cuerpo[0] ?? CODIGO.OK;
    if (tipo === TIPO.SUBSCRIBE || tipo === TIPO.UNSUBSCRIBE) paquete.canal = leerCadena(cuerpo).valor;
    if (tipo === TIPO.PUBLISH) {
        const canal = leerCadena(cuerpo);
        paquete.canal = canal.valor;
        paquete.mensaje = canal.resto;
    }
    return paquete;
}

/*
 * TCP entrega los datos como flujo continuo: un "data" puede traer medio
 * paquete o varios juntos. Esta funcion separa los paquetes completos y
 * devuelve el sobrante para la siguiente lectura.
 */
function extraerPaquetes(buffer) {
    const paquetes = [];
    while (buffer.length >= 2) {
        const longitud = decodificarLongitud(buffer, 1);
        if (longitud === null) break; // varint incompleto, esperar mas datos
        const total = 1 + longitud.bytesLeidos + longitud.valor;
        if (buffer.length < total) break; // cuerpo incompleto
        paquetes.push(analizar(buffer.subarray(0, total)));
        buffer = buffer.subarray(total);
    }
    return { paquetes, resto: buffer };
}

/* ------------------------- canales y comodines ------------------------ */

/*
 * Canales jerarquicos separados por "/", como en MQTT. Las suscripciones
 * aceptan comodines:  +  un solo nivel   |   #  todos los niveles (al final)
 */
function coincideCanal(filtro, canal) {
    const nivelesFiltro = filtro.split('/');
    const nivelesCanal = canal.split('/');
    for (let i = 0; i < nivelesFiltro.length; i++) {
        if (nivelesFiltro[i] === '#') return true;
        if (i >= nivelesCanal.length) return false;
        if (nivelesFiltro[i] !== '+' && nivelesFiltro[i] !== nivelesCanal[i]) return false;
    }
    return nivelesFiltro.length === nivelesCanal.length;
}

function esFiltroValido(filtro) {
    return typeof filtro === 'string' && filtro.length > 0 &&
        filtro.split('/').every((nivel, i, niveles) =>
            nivel === '+' || (nivel === '#' ? i === niveles.length - 1 : !/[#+]/.test(nivel)));
}

function esCanalValido(canal) {
    return typeof canal === 'string' && canal.length > 0 && !/[#+]/.test(canal);
}

module.exports = {
    TIPO, CODIGO, PUERTO_DEFECTO: 1883,
    crear, extraerPaquetes, coincideCanal, esFiltroValido, esCanalValido
};
