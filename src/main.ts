import * as readline from 'node:readline'
import ConsumerFactory from './consumer'
import ProducerFactory from './producer'

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})



async function main () {
  const consumer = new ConsumerFactory('chat', 'consumer-id')
  const producer = new ProducerFactory('chat', 'producer-id')
  
  const messageHandler = (message: { author?: string, content?: string }) => {
    if (message.author && message.content) {
      console.log(`[${message.author}]> ${message.content}`)
    }
  }

  await consumer.startConsumer(messageHandler)
  // await consumer.shutdown()
  // await producer.start()
  // await producer.sendMessage({
  //   author: 'l2',
  //   content: 'ablublublé'
  // })

  const msgPrompt = (msg: string): Promise<string> => {
    return new Promise((resolve, rejects) => {
      rl.question(msg, resolve)
    })
  }
  
  const chatMainLoop = async (): Promise<any> => {
    try {
      const message = await msgPrompt('>> ')
      if (message === ':q') {
        process.exit(0)
      }
      
      console.log('sended >>', message)
      await producer.sendMessage({
        author: 'l2',
        content: message
      })
      return chatMainLoop()
    } catch (err) {
      console.log('SOMETHING WENT WRONG', err)
      return chatMainLoop()
    }
  }

  // await chatMainLoop()
}


main()