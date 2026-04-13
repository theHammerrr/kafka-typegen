import { Producer } from '@platformatic/kafka';
import { createPlatformaticRuntimeProducer, } from 'kafka-typegen/runtime';
import { createProducer } from './generated/kafka/kafka-client.js';


const producer = async () => {
    const producer = createProducer(
        createPlatformaticRuntimeProducer({
            producer: new Producer({
                bootstrapBrokers: ['localhost:19092'],
                clientId: 'demo-app',
            }),
            schemaRegistry: {
                url: 'http://localhost:18081',
            }
        })
    );

    await producer.userEvents.userCreated.send({
        id: '123',
        email: 'user@example.com',
        isAdmin: false,
    }, {
        idempotent: true,
    })

    await producer.userEvents.userDeleted.send({
        id: '123',
    })


    await producer.productEvents.productCreated.send({
        id: '456',
        name: 'Product 1',
        price: 9.99,
        createdAt: new Date().getTime(),
    })
    
    await producer.close()
}

producer().catch((err) => {
    console.error(err)
    process.exit(1)
})