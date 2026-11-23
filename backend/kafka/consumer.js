require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') })
const kafka = require('./client')

// groupId identifica este consumer group.
// Se você rodar dois processos com o mesmo groupId, o Kafka
// divide as partitions entre eles (paralelismo automático).
const consumer = kafka.consumer({ groupId: 'diybello-notificacoes' })

async function iniciarConsumer() {
    await consumer.connect()
    console.log('> kafka consumer conectado')

    // subscribe registra interesse num topic.
    // fromBeginning: true → lê desde o início (útil para dev/replay)
    await consumer.subscribe({ topic: 'pedido.criado', fromBeginning: false })

    console.log('> aguardando eventos no topic [pedido.criado]...\n')

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const evento = JSON.parse(message.value.toString())

            console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
            console.log(`[${topic}] partition=${partition} offset=${message.offset}`)
            console.log(`  pedido_id   : #${evento.pedido_id}`)
            console.log(`  cliente     : ${evento.usuario_nome} <${evento.usuario_email}>`)
            console.log(`  total       : R$ ${evento.total.toFixed(2)}`)
            console.log(`  pagamento   : ${evento.forma_pagamento}`)
            console.log(`  itens       : ${evento.quantidade_itens}`)
            console.log(`  criado_em   : ${evento.criado_em}`)
            console.log()

            // Aqui você implementaria ações reais, ex:
            // await enviarEmailConfirmacao(evento)
            // await notificarAdminNoSlack(evento)
            // await registrarNoSistemaDeRelatorios(evento)
        }
    })
}

iniciarConsumer().catch(err => {
    console.error('Erro no consumer:', err)
    process.exit(1)
})
