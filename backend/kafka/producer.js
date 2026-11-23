const kafka = require('./client')

const producer = kafka.producer()

async function conectarProducer() {
    await producer.connect()
    console.log('> kafka producer conectado')
}

// Publica um evento num topic.
// topic  → nome do canal  (ex: 'pedido.criado')
// evento → objeto JS      (será serializado para JSON)
async function publicar(topic, evento) {
    await producer.send({
        topic,
        messages: [
            {
                // key agrupa mensagens relacionadas na mesma partition
                // → pedidos do mesmo usuário chegam na ordem certa
                key: String(evento.usuario_id ?? evento.pedido_id ?? 'global'),
                value: JSON.stringify(evento),
            }
        ]
    })
}

async function desconectarProducer() {
    await producer.disconnect()
}

module.exports = { conectarProducer, publicar, desconectarProducer }
