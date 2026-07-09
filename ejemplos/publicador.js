/*
 * Ejemplo: cliente publicador (proveedor).
 *
 * Con canal y mensaje publica una sola vez y termina.
 * Solo con canal (o sin argumentos) entra en modo demostracion y publica
 * una lectura simulada de temperatura cada 2 segundos.
 *
 * Uso:  node ejemplos/publicador.js [canal] [mensaje]
 *       node ejemplos/publicador.js alertas "se abrio la puerta"
 *       node ejemplos/publicador.js demo/temperatura
 */

const ClienteMQTT = require('../cliente/cliente-mqtt');

const canal = process.argv[2] || 'demo/temperatura';
const mensaje = process.argv.slice(3).join(' ');

const cliente = new ClienteMQTT({ id: 'publicador-' + process.pid });

async function principal() {
    await cliente.conectar();
    console.log(`[PUB] Conectado al servidor como "${cliente.id}"`);

    if (mensaje) {
        cliente.publicar(canal, mensaje);
        console.log(`[PUB] Publicado en "${canal}": ${mensaje}`);
        cliente.desconectar();
        return;
    }

    console.log(`[PUB] Modo demostracion: publicando en "${canal}" cada 2 s (Ctrl+C para salir)`);
    setInterval(() => {
        const temperatura = (20 + Math.random() * 10).toFixed(1);
        cliente.publicar(canal, temperatura);
        console.log(`[PUB] Publicado en "${canal}": ${temperatura}`);
    }, 2000);
}

principal().catch((error) => {
    console.error(`[PUB] Error: ${error.message}`);
    process.exit(1);
});
