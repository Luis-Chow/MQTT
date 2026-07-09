/*
 * Ejemplo: chat por canales.
 *
 * Cada sala de chat es un canal ("chat/<sala>"). Todos los participantes
 * son a la vez publicadores (lo que escriben) y suscriptores (lo que leen),
 * demostrando el esquema publicar/suscribir en ambos sentidos.
 *
 * Uso:  node ejemplos/chat.js [sala] [nombre]
 *       node ejemplos/chat.js general ana
 */

const readline = require('readline');
const ClienteMQTT = require('../cliente/cliente-mqtt');

const sala = process.argv[2] || 'general';
const nombre = process.argv[3] || 'anonimo-' + process.pid;
const canal = `chat/${sala}`;

const cliente = new ClienteMQTT({ id: `chat-${nombre}-${process.pid}` });
const consola = readline.createInterface({ input: process.stdin, output: process.stdout });

async function principal() {
    await cliente.conectar();
    await cliente.suscribir(canal, (mensaje) => {
        const { autor, texto } = JSON.parse(mensaje.toString());
        if (autor !== nombre) console.log(`\r${autor}: ${texto}`);
        consola.prompt(true);
    });

    console.log(`Conectado a la sala "${sala}" como "${nombre}" (Ctrl+C para salir)`);
    consola.setPrompt('> ');
    consola.prompt();

    consola.on('line', (linea) => {
        const texto = linea.trim();
        if (texto) cliente.publicar(canal, JSON.stringify({ autor: nombre, texto }));
        consola.prompt();
    });

    consola.on('close', () => {
        cliente.desconectar();
        process.exit(0);
    });
}

principal().catch((error) => {
    console.error(`Error: ${error.message}`);
    process.exit(1);
});
