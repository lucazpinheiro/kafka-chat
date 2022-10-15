import { Consumer, ConsumerSubscribeTopics, EachBatchPayload, Kafka, EachMessagePayload } from 'kafkajs'

interface IncomingMessage extends EachMessagePayload {
  author?: string
  content?: string
}

type MessageHandler = (message: {
  author?: string;
  content?: string;
}) => void

export default class ConsumerFactory {
  private topic: string
  private clientId: string
  private kafkaConsumer: Consumer

  constructor(topic: string, clientId: string) {
    this.topic = topic
    this.clientId = clientId
    this.kafkaConsumer = this.createKafkaConsumer()
  }

  private createKafkaConsumer(): Consumer {
    const kafka = new Kafka({ 
      clientId: this.clientId,
      brokers: ['localhost:9092']
    })
    return kafka.consumer({ groupId: 'consumer-group' })
  }

  async startConsumer(cb: MessageHandler): Promise<void> {
    const subscribeTopic: ConsumerSubscribeTopics = {
      topics: [this.topic],
      fromBeginning: true
    }

    try {
      await this.kafkaConsumer.connect()
      await this.kafkaConsumer.subscribe(subscribeTopic)

      await this.kafkaConsumer.run({
        eachMessage: async (messagePayload: IncomingMessage) => {
          const { topic, partition, message } = messagePayload
          const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
          const rawMessage = JSON.parse(String(message.value?.toString()))
          cb(rawMessage)
        }
      })
      
    } catch (error) {
      console.log('Error: ', error)
    }
  }

  async shutdown(): Promise<void> {
    await this.kafkaConsumer.disconnect()
  }

}