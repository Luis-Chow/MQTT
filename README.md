# Servidor de Mensajes basado en MQTT

Servidor de mensajes (broker) y libreria cliente que implementan un
protocolo de mensajeria **basado en MQTT** sobre TCP, con esquema de
**canales**: los clientes se suscriben a un canal y cada mensaje enviado
por un publicador a ese canal se retransmite a **todos los suscriptores**.

Trabajo de la materia *Programacion de Protocolos de Red*.
Entrega: 14/07/2026.

## Arquitectura

```
+--------------+                                    +--------------+
| Publicador   | --PUBLISH("sensores/temp","25")--> |              |
| (cliente)    |                                    |   Servidor   |
+--------------+                                    |   (broker)   |
                                                    |              |
+--------------+  <----PUBLISH("sensores/temp")---- |  canal ->    |
| Suscriptor A |  --SUBSCRIBE("sensores/temp")----> |  suscritos   |
+--------------+                                    |              |
+--------------+  <----PUBLISH("sensores/temp")---- |              |
| Suscriptor B |  --SUBSCRIBE("sensores/#")-------> |              |
+--------------+                                    +--------------+
```

- **Servidor / broker** (`servidor/servidor.js`) -> puerto **1883** (el
  estandar de MQTT). Acepta conexiones TCP, registra suscripciones por
  canal y difunde cada publicacion a los suscriptores del canal.
- **Libreria cliente** (`cliente/cliente-mqtt.js`) -> clase `ClienteMQTT`
  con la que se construye cualquier aplicacion (publicador, suscriptor
  o ambas cosas a la vez).
- **Protocolo compartido** (`compartido/protocolo.js`) -> contrato del
  protocolo: codificacion y decodificacion de paquetes, usado por el
  servidor y por el cliente.

## Protocolo de aplicacion (sobre TCP)

Cada paquete tiene el mismo formato que MQTT:

| Parte              | Tamano    | Descripcion                                      |
|--------------------|-----------|--------------------------------------------------|
| Tipo + flags       | 1 byte    | 4 bits altos: tipo de paquete; 4 bajos: flags    |
| Longitud restante  | 1-4 bytes | Bytes del cuerpo, varint base 128 (como MQTT)    |
| Cuerpo             | N bytes   | Depende del tipo de paquete                      |

Las cadenas del cuerpo (id, canal) se codifican como en MQTT: 2 bytes de
longitud (uint16 big-endian) + texto UTF-8.

### Tipos de paquete

| Tipo        | Valor | Direccion           | Cuerpo                    |
|-------------|------:|---------------------|---------------------------|
| CONNECT     |  1    | cliente -> servidor | id del cliente            |
| CONNACK     |  2    | servidor -> cliente | codigo (0 = aceptado)     |
| PUBLISH     |  3    | ambos sentidos      | canal + mensaje (binario) |
| SUBSCRIBE   |  8    | cliente -> servidor | canal (admite comodines)  |
| SUBACK      |  9    | servidor -> cliente | codigo (0 = aceptado)     |
| UNSUBSCRIBE | 10    | cliente -> servidor | canal                     |
| UNSUBACK    | 11    | servidor -> cliente | (vacio)                   |
| PINGREQ     | 12    | cliente -> servidor | (vacio, keepalive)        |
| PINGRESP    | 13    | servidor -> cliente | (vacio)                   |
| DISCONNECT  | 14    | cliente -> servidor | (vacio, cierre ordenado)  |

Los valores de tipo son los del estandar MQTT 3.1.1. Simplificaciones
respecto al estandar: solo QoS 0 (entrega sin confirmacion), sin sesiones
persistentes, sin mensajes retenidos y sin autenticacion.

### Canales y comodines

Los canales son jerarquicos y se separan con `/`, por ejemplo
`sensores/sala/temperatura`. Las **suscripciones** admiten comodines:

| Comodin | Significado        | Ejemplo                  | Coincide con                           |
|---------|--------------------|--------------------------|----------------------------------------|
| `+`     | un solo nivel      | `sensores/+/temperatura` | `sensores/sala/temperatura`            |
| `#`     | todos los niveles  | `sensores/#`             | `sensores/patio/humedad`, `sensores/x` |

Para **publicar** se usa siempre un canal exacto (sin comodines).

### Secuencia tipica

```
Cliente                          Servidor
  |-- CONNECT("sensor-01") -------->|
  |<-- CONNACK(0) ------------------|
  |-- SUBSCRIBE("sensores/#") ----->|
  |<-- SUBACK(0) -------------------|
  |                                 |   otro cliente publica en
  |<-- PUBLISH("sensores/t", "25") -|   "sensores/t"
  |-- PINGREQ --------------------->|   (cada 30 s)
  |<-- PINGRESP --------------------|
  |-- DISCONNECT ------------------>|
```

## Libreria cliente (API)

```js
const ClienteMQTT = require('./cliente/cliente-mqtt');

const cliente = new ClienteMQTT({
    host: '127.0.0.1',   // opcional (por defecto 127.0.0.1)
    puerto: 1883,        // opcional (por defecto 1883)
    id: 'sensor-01',     // opcional (por defecto aleatorio)
    keepalive: 30        // opcional, segundos entre pings (0 = sin ping)
});

await cliente.conectar();                    // resuelve al recibir CONNACK

await cliente.suscribir('sensores/#', (mensaje, canal) => {
    console.log(`${canal}: ${mensaje}`);     // mensaje es un Buffer
});

cliente.publicar('sensores/temperatura', '25.4');

await cliente.desuscribir('sensores/#');
cliente.desconectar();
```

Eventos disponibles: `conectado`, `mensaje (canal, mensaje)`,
`desconectado` y `error`.

## Estructura del proyecto

```
MQTT/
+- package.json
+- README.md
+- Imagen/
|  +- #1.jpg                 (enunciado del proyecto)
+- compartido/
|  +- protocolo.js           (contrato del protocolo, compartido por todos)
+- servidor/
|  +- servidor.js            (servidor de mensajes / broker)
+- cliente/
|  +- cliente-mqtt.js        (libreria cliente: clase ClienteMQTT)
+- ejemplos/
   +- suscriptor.js          (imprime lo que llega a uno o varios canales)
   +- publicador.js          (publica mensajes en un canal)
   +- chat.js                (chat por salas usando canales)
```

## Como ejecutarlo

### 1) Requisitos

- [Node.js](https://nodejs.org/) 18 o superior. Sin dependencias externas
  (`npm install` no es necesario).

### 2) Iniciar el servidor

Desde la carpeta `MQTT/`:

```
npm start
```

Salida esperada:

```
[MQTT] Servidor escuchando en 0.0.0.0:1883
```

(Se puede cambiar el puerto con `node servidor/servidor.js 2883`.)

### 3) Abrir un suscriptor (en otra terminal)

```
npm run suscriptor                                # se suscribe a demo/#
npm run suscriptor -- sensores/+/temperatura      # con comodines
```

### 4) Publicar mensajes (en otra terminal)

```
npm run publicador                                # demo: temperatura cada 2 s
npm run publicador -- alertas "se abrio la puerta"  # un solo mensaje
```

Todo lo publicado aparece al instante en cada suscriptor cuyo canal
coincida.

### 5) Chat por canales (opcional)

Abrir dos o mas terminales:

```
npm run chat -- general ana
npm run chat -- general luis
```

Cada sala de chat es un canal (`chat/general`); todo lo que escribe un
participante lo reciben los demas suscriptores de la sala.

## Notas sobre el diseno

- **TCP como transporte**: igual que MQTT real, se necesita entrega
  fiable y en orden. Como TCP es un flujo continuo, un `data` puede traer
  paquetes incompletos o varios juntos; `extraerPaquetes()` separa los
  paquetes completos usando el campo de longitud y guarda el sobrante.
- **Broadcast por canal**: el servidor mantiene por cada cliente el
  conjunto de filtros a los que esta suscrito. Al recibir un PUBLISH,
  recorre los clientes y reenvia la trama a los que tengan al menos un
  filtro que coincida (sin duplicar si coinciden varios).
- **Sesiones unicas por id**: si un cliente se conecta con un id ya en
  uso, el servidor expulsa la sesion anterior (comportamiento de MQTT).
- **Keepalive**: el cliente envia PINGREQ periodicos para mantener viva
  la conexion y detectar caidas del servidor.
