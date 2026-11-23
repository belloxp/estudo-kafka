require('dotenv').config({ path: require('path').resolve(__dirname, '../.env') })
const kafka = require('./client')

const admin    = kafka.admin()
const consumer = kafka.consumer({ groupId: 'diybello-estoque' })

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
    await garantirTopics(['estoque.atualizado'])

    await consumer.connect()
    console.log('> kafka consumer [estoque] conectado')

    await consumer.subscribe({ topic: 'estoque.atualizado', fromBeginning: false })

    console.log('> aguardando eventos no topic [estoque.atualizado]...\n')

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            // message.value é um Buffer — toString() vira string — JSON.parse vira objeto
            // esse objeto é EXATAMENTE o que o publicar() enviou no estoque.service.js
            const evento = JSON.parse(message.value.toString())

            console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
            console.log(`[${topic}] partition=${partition} offset=${message.offset}`)
            console.log(`  movimento_id  : #${evento.movimento_id}`)
            console.log(`  produto       : ${evento.produto_nome} (id=${evento.produto_id})`)
            console.log(`  tipo          : ${evento.tipo}`)
            console.log(`  quantidade    : ${evento.quantidade}`)
            console.log(`  saldo_anterior: ${evento.saldo_anterior}`)
            console.log(`  saldo_atual   : ${evento.saldo_atual}`)
            console.log(`  registrado_em : ${evento.registrado_em}`)

            // alerta_baixo é uma flag que o producer calculou e embutiu no evento
            // o consumer só decide o que fazer com ela — sem precisar reconsultar o banco
            if (evento.alerta_baixo) {
                console.log(`  ⚠️  ALERTA: estoque baixo! ${evento.produto_nome} com apenas ${evento.saldo_atual} unidade(s)`)
                // Ex de ação real:
                // await notificarAdminEstoqueBaixo(evento)
            }

            console.log()
        }
    })
}

iniciarConsumer().catch(err => {
    console.error('Erro no consumer de estoque:', err)
    process.exit(1)
})
