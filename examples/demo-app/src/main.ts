import { Producer, Consumer } from '@platformatic/kafka'
import { createPlatformaticConsumerTransport, createPlatformaticProducerTransport, createPlatformaticRuntimeClient } from 'kafka-typegen/runtime/platformatic'
import { TopicName } from '../generated/kafka-client.js'

const main = async () => {
  // const typegenProducer = createPlatformaticProducerTransport()

  const runtimeClient = createPlatformaticRuntimeClient({
    producer: new Producer({
      bootstrapBrokers: ['localhost:9092'],
      clientId: 'my-runtime-producer'
    }),
    consumer: new Consumer({
      bootstrapBrokers: ['localhost:9092'],
      clientId: 'my-runtime-consumer',
      groupId: 'my-runtime-group'
    })
  })

  typegenProducer.send({
    topicName: TopicName.
  })
}

main().catch((err) => {
  console.error('Error in main:', err)
  process.exit(1)
})
