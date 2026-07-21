/*
 * Libreria cliente para el servidor de mensajes basado en MQTT.
 *
 *   const cliente = new ClienteMQTT({ id: 'sensor-01' });
 *   await cliente.conectar();
 *   await cliente.suscribir('sensores/#', (mensaje, canal) => { ... });
 *   cliente.publicar('sensores/temperatura', '25.4');
 *
 * Eventos: 'conectado', 'mensaje' (canal, mensaje), 'desconectado', 'error'.
 */

const net = require('net');
const { EventEmitter } = require('events');
const protocolo = require('../compartido/protocolo');
const { TIPO, CODIGO, crear } = protocolo;

class ClienteMQTT extends EventEmitter {
    constructor(opciones = {}) {
        super();
        this.host = opciones.host || '127.0.0.1';
        this.puerto = opciones.puerto || protocolo.PUERTO_DEFECTO;
        this.id = opciones.id || 'cliente-' + Math.random().toString(16).slice(2, 8);
        this.keepalive = opciones.keepalive ?? 30; // segundos entre pings (0 = sin ping)
        this.conectado = false;
        this.socket = null;
        this.buffer = Buffer.alloc(0);
        this.manejadores = new Map(); // filtro -> funcion(mensaje, canal)
        this.esperas = []; // promesas pendientes de respuesta; TCP conserva el orden (FIFO)
    }

    conectar() {
        return this._esperar(() => {
            this.socket = net.createConnection(this.puerto, this.host, () =>
                this.socket.write(crear.connect(this.id)));
            this.socket.on('data', (datos) => this._recibir(datos));
            this.socket.on('error', (error) => {
                const espera = this.esperas.shift();
                espera ? espera.reject(error) : this.emit('error', error);
            });
            this.socket.on('close', () => {
                clearInterval(this.timerPing);
                this.socket = null;
                if (this.conectado) { this.conectado = false; this.emit('desconectado'); }
            });
        });
    }

    desconectar() {
        clearInterval(this.timerPing);
        this.socket?.end(crear.disconnect());
    }

    publicar(canal, mensaje) {
        if (!this.conectado) throw new Error('El cliente no esta conectado');
        if (!protocolo.esCanalValido(canal)) throw new Error(`Canal invalido para publicar: "${canal}"`);
        this.socket.write(crear.publish(canal, mensaje));
    }

    suscribir(canal, manejador) {
        return this._esperar(() => this.socket.write(crear.subscribe(canal)), { canal, manejador });
    }

    desuscribir(canal) {
        return this._esperar(() => this.socket.write(crear.unsubscribe(canal)), { canal });
    }

    // Encola una promesa pendiente y ejecuta la accion que pide su respuesta
    _esperar(accion, extra = {}) {
        return new Promise((resolve, reject) => {
            this.esperas.push({ resolve, reject, ...extra });
            accion();
        });
    }

    _recibir(datos) {
        this.buffer = Buffer.concat([this.buffer, datos]);
        try {
            const { paquetes, resto } = protocolo.extraerPaquetes(this.buffer);
            this.buffer = resto;
            paquetes.forEach((paquete) => this._procesar(paquete));
        } catch (error) {
            this.emit('error', error);
            this.socket.destroy();
        }
    }

    _procesar(paquete) {
        if (paquete.tipo === TIPO.PUBLISH) {
            this.emit('mensaje', paquete.canal, paquete.mensaje);
            for (const [filtro, manejador] of this.manejadores) {
                if (protocolo.coincideCanal(filtro, paquete.canal)) manejador(paquete.mensaje, paquete.canal);
            }
            return;
        }
        if (paquete.tipo === TIPO.PINGRESP) return; // el servidor sigue vivo

        // CONNACK / SUBACK / UNSUBACK: responder la peticion mas antigua
        const espera = this.esperas.shift();
        if (!espera) return;
        if (paquete.codigo > CODIGO.OK) {
            if (paquete.tipo === TIPO.CONNACK) this.socket.destroy();
            return espera.reject(new Error('Peticion rechazada por el servidor'));
        }
        if (paquete.tipo === TIPO.CONNACK) {
            this.conectado = true;
            this.emit('conectado');
            if (this.keepalive > 0) {
                this.timerPing = setInterval(() => this.socket?.write(crear.pingreq()), this.keepalive * 1000);
                this.timerPing.unref?.();
            }
            return espera.resolve(this);
        }
        if (paquete.tipo === TIPO.SUBACK && espera.manejador) this.manejadores.set(espera.canal, espera.manejador);
        if (paquete.tipo === TIPO.UNSUBACK) this.manejadores.delete(espera.canal);
        espera.resolve(espera.canal);
    }
}

module.exports = ClienteMQTT;
