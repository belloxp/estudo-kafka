require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') })
const kafka = require('./client')

const admin    = kafka.admin()
const consumer = kafka.consumer({ groupId: 'diybello-notificacoes' })

async function garantirTopics(topics) {
    await admin.connect()
    const existentes = await admin.listTopics()
    const faltando = topics.filter(t => !existentes.includes(t))

    if (faltando.length > 0) {
        await admin.createTopics({
            topics: faltando.map(topic => ({
                topic,
                numPartitions: 1,
                replicationFactor: 1
            }))
        })
        console.log(`> topics criados: ${faltando.join(', ')}`)
    }

    await admin.disconnect()
}

async function iniciarConsumer() {
    // Garante que o topic existe antes de assinar
    await garantirTopics(['pedido.criado'])

    await consumer.connect()
    console.log('> kafka consumer conectado')

    // fromBeginning: false → só mensagens novas (chegaram depois que ele conectou)
    // fromBeginning: true  → relê tudo desde o começo (útil pra debug/replay)
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
