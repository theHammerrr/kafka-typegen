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

    consumer.events.userCreated.on(async (message) => {
        console.log(`event ${message.event}, payload: ${JSON.stringify(message.payload)}`);
    })

    consumer.events.userDeleted.on(async (message) => {
        console.log(`event ${message.event}, payload: ${JSON.stringify(message.payload)}`);
    })

    process.once('SIGINT', async () => {
        await consumer.close();
        process.exit(0);
    });

    process.once('SIGTERM', async () => {
        await consumer.close();
        process.exit(0);
    });
}

consumer().catch((err) => {
    console.error(err)
    process.exit(1)
})