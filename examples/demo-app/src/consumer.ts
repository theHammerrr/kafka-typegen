import { Consumer } from '@platformatic/kafka';
import { createPlatformaticRuntimeConsumer, } from 'kafka-typegen/runtime';
import { createConsumer } from './generated/kafka/kafka-client.js';


const consumer = async () => {
    const consumer = createConsumer(
        createPlatformaticRuntimeConsumer({
            consumer: new Consumer({
                bootstrapBrokers: ['localhost:19092'],
                clientId: 'demo-app',
                groupId: 'demo-app-group',
            }),
            schemaRegistry: {
                url: 'http://localhost:18081',
            }
        })
    )
    
    consumer.userEvents.userCreated.on(async (message) => {
        console.log(`event ${message.event}, payload: ${JSON.stringify(message.payload)}`);
    })

    consumer.userEvents.userDeleted.on(async (message) => {
        console.log(`event ${message.event}, payload: ${JSON.stringify(message.payload)}`);
    })

    consumer.productEvents.productCreated.on(async (message) => {
        const createdAt = new Date(message.payload.createdAt);
        const productId = message.payload.id;
        console.log(`event ${message.event}, payload: ${JSON.stringify(message.payload)}`);
        console.log(`product id: ${productId} created at: ${createdAt}`);
        
    })

    process.once('SIGINT', async () => {
        await consumer.close({force: true});
        process.exit(0);
    });

    process.once('SIGTERM', async () => {
        await consumer.close({force: true});
        process.exit(0);
    });
}

consumer().catch((err) => {
    console.error(err)
    process.exit(1)
})
