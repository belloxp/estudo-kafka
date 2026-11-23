const { Kafka } = require('kafkajs')

// Um único cliente Kafka reutilizado por todo o backend.
// clientId identifica essa aplicação no broker (aparece nos logs do Kafka).
// brokers lista os endereços dos brokers — em produção teríamos vários.
const kafka = new Kafka({
    clientId: 'diybello-backend',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
})

module.exports = kafka
