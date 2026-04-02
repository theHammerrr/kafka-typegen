import { Consumer, jsonDeserializer, Producer } from '@platformatic/kafka';
import {
    createPlatformaticConsumerTransport,
    createPlatformaticProducerTransport,
    createPlatformaticRuntimeConsumer,
    createPlatformaticRuntimeProducer,
    createRuntimeProducer,
} from 'kafka-typegen/runtime';
import { createConsumer, createProducer, TopicNames } from './generated/kafka/kafka-client.js';
import { create } from 'domain';


const consumer = async () => {
    const consumer = createConsumer(
        createPlatformaticRuntimeConsumer({
            consumer: new Consumer({
                bootstrapBrokers: ['localhost:9092'],
                clientId: 'demo-app',
                groupId: 'demo-app-group',
            }),
            serialization: {
                async serialize(_metadata, payload) {
                    return {
                        value: new TextEncoder().encode(JSON.stringify(payload))
                    };
                },
                async deserialize() {
                    throw new Error('Not used');
                }

            }
        })
    )

    consumer.events.userCreated.on(async (message) => {
        console.log(message.payload.isAdmin);
    })
}

const producer = () => {
    const producer = createProducer(createPlatformaticRuntimeProducer({
        producer: new Producer({
            bootstrapBrokers: ['localhost:9092'],
            clientId: 'demo-app',
        }),
        serialization: {
            async serialize(_metadata, payload) {
                return {
                    value: new TextEncoder().encode(JSON.stringify(payload))
                };
            },
            async deserialize() {
                throw new Error('Not used');
            }

        }
    }))

    producer.events.userCreated.send({
        id: '123',
        email: 'user@example.com',
        isAdmin: false,
    })
}

consumer().catch((err) => {
    console.error(err)
    process.exit(1)
})