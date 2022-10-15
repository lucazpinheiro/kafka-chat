import { Kafka, logCreator, logLevel, Message, Producer, ProducerBatch, TopicMessages } from 'kafkajs'

interface CustomMessageFormat {
  author: string
  content: string
}

export default class ProducerFactory {
  private topic: string
  private clientId: string
  private producer: Producer

  constructor(topic: string, clientId: string) {
    this.topic = topic
    this.clientId = clientId
    this.producer = this.createProducer()
  }

  private createProducer() : Producer {
    const kafka = new Kafka({
      clientId: this.clientId,
      brokers: ['localhost:9092'],
    })

    return kafka.producer()
  }

  async start(): Promise<void> {
    try {
      await this.producer.connect()
    } catch (error) {
      console.log('Error connecting the producer: ', error)
    }
  }

  async shutdown(): Promise<void> {
    await this.producer.disconnect()
  }

  async sendMessage(message: CustomMessageFormat): Promise<void> {
    const kafkaMessage = {
        value: JSON.stringify(message)
    }

    const topicMessages: TopicMessages = {
      topic: this.topic,
      messages: [kafkaMessage]
    }

    await this.producer.send(topicMessages)
  }

}