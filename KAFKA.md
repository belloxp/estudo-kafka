# Kafka no DIYBELLO — Guia Completo do que foi feito e por quê

> Estudo aplicado: Apache Kafka + Node.js + KafkaJS  
> Projeto base: DIYBELLO (e-commerce de kits DIY — Express + Sequelize + MySQL)

---

## O que você precisa ter instalado

| Ferramenta | Pra quê | Como verificar |
|---|---|---|
| **Docker Desktop** | Rodar o Kafka localmente sem instalar nada na máquina | `docker --version` |
| **Node.js** | Já estava no projeto | `node --version` |
| **KafkaJS** | Client Kafka pra Node.js — foi instalado via npm | já está no `package.json` |

Você **não precisa instalar o Kafka na sua máquina**. Ele roda 100% dentro do container Docker.

---

## Por que Kafka? O problema que ele resolve

Antes da integração, quando um pedido era criado (`POST /pedidos`), o `pedido.service.js` fazia **tudo de forma síncrona e acoplada**:

```
POST /pedidos
  → valida carrinho
  → confere estoque
  → salva pedido no banco
  → salva itens
  → deduz estoque
  → cria entrega
  → responde ao cliente  ✅
```

Isso funciona, mas tem um problema: **e se você quiser mandar um e-mail de confirmação? Notificar o admin? Gerar um relatório?** Você teria que adicionar tudo dentro do mesmo service, e ele ficaria gigante e frágil — uma falha num sistema externo (ex: servidor de e-mail fora do ar) poderia derrubar o pedido inteiro.

**Kafka resolve isso com o padrão de eventos:**

```
POST /pedidos
  → [banco] salva tudo numa transação SQL  ✅ pedido garantido
  → [kafka] publica evento "pedido.criado"
              │
              ├── [consumer A] manda e-mail de confirmação
              ├── [consumer B] notifica admin no Slack
              └── [consumer C] atualiza painel de relatórios
```

Cada consumer é **independente**. Se o e-mail falhar, o pedido continua salvo. Se você quiser adicionar um novo sistema, só cria mais um consumer — sem tocar no código original.

---

## Conceitos fundamentais do Kafka

### Broker
O servidor Kafka em si. Recebe mensagens, armazena, distribui para consumers.  
No projeto: roda no container `diybello-kafka` na porta `9092`.

### Topic
Um canal nomeado onde mensagens são publicadas.  
No projeto: `pedido.criado` — toda vez que um pedido é finalizado, uma mensagem vai pra cá.  
Você pode ter quantos topics quiser: `estoque.atualizado`, `entrega.despachada`, etc.

### Partition
Cada topic é dividido em partitions (divisões internas). Mensagens dentro de uma partition chegam **em ordem**. Partitions permitem paralelismo — vários consumers processando ao mesmo tempo.  
No projeto: 1 partition (suficiente pra dev).

### Offset
É a posição de uma mensagem dentro de uma partition (0, 1, 2...).  
O consumer guarda qual offset já leu — se reiniciar, continua de onde parou.

### Producer
Quem **publica** mensagens num topic.  
No projeto: o backend Express, via `kafka/producer.js`.

### Consumer
Quem **lê e processa** mensagens de um topic.  
No projeto: o script `kafka/consumer.js`.

### Consumer Group
Um grupo de consumers identificado por um `groupId`.  
Se você rodar dois processos com o mesmo `groupId`, o Kafka divide as partitions entre eles automaticamente — paralelismo sem duplicação.  
Se você rodar dois processos com `groupId` **diferente**, cada um recebe **todas** as mensagens — útil quando sistemas independentes precisam do mesmo evento.

### Mensagem
Tem três partes:
- **key** → usado pra roteamento de partitions (mensagens com a mesma key vão pra mesma partition → ordem garantida)
- **value** → o conteúdo (no projeto: JSON com dados do pedido)
- **headers** → metadados opcionais (versão do schema, trace ID, etc.)

### KRaft (modo sem Zookeeper)
Kafka moderno (3.x+) roda em modo KRaft: o próprio broker gerencia o cluster sem precisar do Zookeeper. Mais simples, menos container. É o modo que configuramos no `docker-compose.yml`.

---

## Arquivos criados

### `docker-compose.yml` — na raiz do projeto

```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: diybello-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller   # KRaft: um processo faz os dois papéis
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"  # cria o topic automaticamente ao publicar
      ...
```

**Por que essa imagem?** `apache/kafka` é a imagem oficial do projeto Apache. `bitnami/kafka` é mais popular mas estava indisponível no momento.

**Por que `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true`?** Em dev é conveniente — o topic `pedido.criado` é criado automaticamente quando o producer publica pela primeira vez, sem precisar rodar um comando manual.

**Por que a porta 9093?** O protocolo CONTROLLER (comunicação interna KRaft) usa uma porta separada da porta do broker (9092). A 9093 não é exposta pro host — só o Kafka usa internamente.

---

### `backend/kafka/client.js` — conexão centralizada

```js
const kafka = new Kafka({
    clientId: 'diybello-backend',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
})
```

**Por que existe esse arquivo?** Para ter **um único ponto de configuração** da conexão. Tanto o producer quanto o consumer importam daqui. Se o endereço do broker mudar (ex: em produção), você muda num lugar só.

**`clientId`**: nome que identifica essa aplicação nos logs do broker. Não afeta o funcionamento, mas ajuda muito no debug — você vê exatamente qual app está publicando/consumindo.

**`KAFKA_BROKER`**: vem do `.env`. Em produção seria o endereço do Kafka na nuvem (ex: Confluent Cloud, MSK da AWS).

---

### `backend/kafka/producer.js` — publica eventos

```js
const producer = kafka.producer()

async function conectarProducer() {
    await producer.connect()
}

async function publicar(topic, evento) {
    await producer.send({
        topic,
        messages: [{
            key: String(evento.usuario_id ?? evento.pedido_id ?? 'global'),
            value: JSON.stringify(evento),
        }]
    })
}
```

**Por que `connect()` explícito?** O KafkaJS não conecta automaticamente. A conexão é feita uma vez quando o servidor sobe (`index.js`) e fica aberta pra sempre — reusar a conexão é muito mais eficiente do que conectar e desconectar a cada mensagem.

**Por que a `key` é o `usuario_id`?** A key determina em qual partition a mensagem cai. Mensagens com a mesma key sempre vão pra mesma partition, e dentro de uma partition a **ordem é garantida**. Usando `usuario_id` como key, todos os pedidos de um mesmo cliente chegam em ordem cronológica para o consumer.

**Por que `JSON.stringify`?** O Kafka trafega bytes. Você serializa o objeto JS pra string JSON antes de publicar, e o consumer faz `JSON.parse` ao receber.

---

### `backend/kafka/consumer.js` — consome e processa

```js
const consumer = kafka.consumer({ groupId: 'diybello-notificacoes' })

await consumer.subscribe({ topic: 'pedido.criado', fromBeginning: false })

await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        const evento = JSON.parse(message.value.toString())
        // processa o evento...
    }
})
```

**Por que é um script separado do Express?** O consumer é um processo independente. Ele pode rodar numa máquina diferente, escalar separado, cair e reiniciar sem afetar a API. É a separação de responsabilidades do Kafka em prática.

**`fromBeginning: false`**: o consumer só lê mensagens novas (chegaram depois que ele conectou). `fromBeginning: true` releria todas as mensagens desde o começo — útil pra replay de eventos ou debug.

**`groupId: 'diybello-notificacoes'`**: identifica o grupo. Se você quiser um segundo consumer independente (ex: relatórios), crie com um `groupId` diferente — ele receberá as mesmas mensagens sem interferir neste.

**`eachMessage`**: função chamada uma vez pra cada mensagem. O Kafka só avança o offset depois que ela resolve (sem erro) — se jogar uma exception, a mensagem pode ser reprocessada.

---

## Arquivos modificados

### `backend/index.js` — entrypoint do servidor

**O que mudou:**
```js
// linha adicionada no import
const { conectarProducer } = require('./kafka/producer')

// linha adicionada dentro de startServer()
await conectarProducer()
```

**Por que aqui?** O producer precisa estar conectado antes do servidor começar a aceitar requisições. Se o `app.listen` chamasse antes do `conectarProducer`, um pedido poderia chegar antes da conexão estar pronta e dar erro.

**E se o Kafka estiver fora do ar quando o servidor subir?** O `conectarProducer()` lançaria uma exception, e o `catch` do `startServer()` encerraria o processo com `process.exit(1)`. Em produção você pode querer um comportamento mais tolerante (retry com backoff), mas pra dev isso é suficiente.

---

### `backend/service/pedido.service.js` — lógica de negócio do pedido

**O que mudou:**

1. Import do producer no topo:
```js
const { publicar } = require('../kafka/producer')
```

2. Publicação do evento após o `transacao.commit()`:
```js
await publicar('pedido.criado', {
    pedido_id: pedido.id,
    usuario_id: usuario.id,
    usuario_email: usuario.email,
    usuario_nome: usuario.nome,
    total,
    desconto: resultadoDesconto.desconto,
    frete,
    forma_pagamento: pedido.forma_pagamento,
    quantidade_itens: itensValidados.length,
    itens: itensValidados.map(i => ({
        produto_id: i.produto_id,
        nome: i.nome_produto,
        quantidade: i.quantidade,
        preco_unitario: i.preco_unitario
    })),
    criado_em: new Date().toISOString()
})
```

**Por que DEPOIS do `commit()` e fora da transação?**  
Esta é a decisão mais importante de design aqui. O banco de dados já confirmou o pedido com `commit()`. O Kafka é um sistema externo — se publicarmos dentro da transação SQL e o Kafka falhar, o Sequelize faria rollback de um pedido que, do ponto de vista do banco, estava correto. O resultado seria um pedido perdido.

Publicando após o commit:
- Se o Kafka falhar → o pedido está salvo no banco ✅, apenas o evento não foi enviado (pode ser reprocessado depois)
- Se o banco falhar → `catch` faz rollback, `publicar` nunca é chamado ✅

**O que o evento carrega?** Tudo que um consumer precisaria saber pra agir sem consultar o banco de novo: dados do pedido, dados do usuário (nome, e-mail), itens comprados, valor total. Isso é chamado de **evento rico** (fat event) — o consumer é autossuficiente.

---

## Como rodar

### 1. Subir o Kafka (uma vez)
```bash
# na raiz do projeto
docker compose up -d
```

Verificar se está saudável:
```bash
docker logs diybello-kafka --tail 10
# deve aparecer: Kafka Server started
```

### 2. Terminal 1 — Consumer (fica escutando)
```bash
cd backend
npm run consumer
```

### 3. Terminal 2 — Servidor Express
```bash
cd backend
npm run dev
```

### 4. Criar um pedido via REST
```http
POST http://localhost:3000/pedidos
Authorization: Bearer <seu_token>
Content-Type: application/json

{
  "itens": [{ "produto_id": 1, "quantidade": 2 }],
  "forma_pagamento": "PIX"
}
```

No terminal do consumer você verá:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[pedido.criado] partition=0 offset=0
  pedido_id   : #42
  cliente     : Guilherme Bello <bello@email.com>
  total       : R$ 150.00
  pagamento   : PIX
  itens       : 1
  criado_em   : 2026-11-23T20:00:00.000Z
```

---

## Parar o Kafka
```bash
docker compose down          # para o container (mantém os dados)
docker compose down -v       # para e apaga os dados (recomeça do zero)
```

---

## Próximos passos naturais desse estudo

| O que fazer | Por que é relevante |
|---|---|
| Adicionar topic `estoque.baixo` quando `estoque_atual < 5` | Aprende a ter múltiplos topics e producers |
| Criar um segundo consumer com `groupId` diferente | Vê múltiplos sistemas consumindo o mesmo evento |
| Testar `fromBeginning: true` no consumer | Entende replay de eventos |
| Simular o Kafka fora do ar e ver o que acontece | Aprende resiliência e retry |
| Usar **Avro ou JSON Schema** pra validar o formato das mensagens | Versionamento de eventos em produção |
| Kafka UI (Kafdrop ou Redpanda Console) | Visualizar topics, mensagens e consumer groups numa interface web |

---

## Sobre o repositório separado `estudo-kafka`

Faz total sentido separar. O que vai pra esse repo:

```
estudo-kafka/
├── docker-compose.yml          ← o que criamos aqui
├── backend/
│   ├── kafka/
│   │   ├── client.js
│   │   ├── producer.js
│   │   └── consumer.js
│   ├── index.js                ← versão modificada
│   └── service/
│       └── pedido.service.js  ← versão modificada
└── KAFKA.md                    ← este arquivo
```

O projeto original (`prototipo-diybello`) pode ficar limpo sem o Kafka, e o `estudo-kafka` vira sua referência pra quando quiser aplicar isso em outros projetos.
