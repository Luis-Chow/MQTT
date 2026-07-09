/*
 * Ejemplo: cliente suscriptor.
 *
 * Se suscribe a uno o varios canales e imprime cada mensaje recibido.
 * Acepta comodines: "+" (un nivel) y "#" (todos los niveles).
 *
 * Uso:  node ejemplos/suscriptor.js [canal ...]
 *       node ejemplos/suscriptor.js sensores/#
 *       node ejemplos/suscriptor.js sensores/+/temperatura alertas
 */

const ClienteMQTT = require('../cliente/cliente-mqtt');

const canales = process.argv.slice(2);
if (canales.length === 0) canales.push('demo/#');

const cliente = new ClienteMQTT({ id: 'suscriptor-' + process.pid });

async function principal() {
    await cliente.conectar();
    console.log(`[SUB] Conectado al servidor como "${cliente.id}"`);

    for (const canal of canales) {
        await cliente.suscribir(canal, (mensaje, canalReal) => {
            console.log(`[SUB] ${canalReal}: ${mensaje}`);
        });
        console.log(`[SUB] Suscrito al canal "${canal}"`);
    }
    console.log('[SUB] Esperando mensajes... (Ctrl+C para salir)');
}

cliente.on('desconectado', () => {
    console.log('[SUB] Desconectado del servidor');
    process.exit(0);
});

principal().catch((error) => {
    console.error(`[SUB] Error: ${error.message}`);
    process.exit(1);
});
