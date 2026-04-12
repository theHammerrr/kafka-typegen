import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import {
  GenericContainer,
  Network,
  Wait,
  type StartedNetwork,
  type StartedTestContainer
} from 'testcontainers';

const KAFKA_IMAGE = 'confluentinc/cp-kafka:7.6.1';
const SCHEMA_REGISTRY_IMAGE = 'confluentinc/cp-schema-registry:7.6.1';
const KAFKA_EXTERNAL_PORT = 9093;
const SCHEMA_REGISTRY_PORT = 8081;

export interface IntegrationEnvironment {
  readonly kafkaBroker: string;
  readonly schemaRegistryUrl: string;
  stop(): Promise<void>;
}

export async function startIntegrationEnvironment(): Promise<IntegrationEnvironment> {
  let network: StartedNetwork | undefined;
  let kafka: StartedKafkaContainer | undefined;
  let schemaRegistry: StartedTestContainer | undefined;

  try {
    network = await new Network().start();
    kafka = await new KafkaContainer(KAFKA_IMAGE)
      .withKraft()
      .withNetwork(network)
      .withHostname('kafka')
      .withNetworkAliases('kafka')
      .start();
    schemaRegistry = await new GenericContainer(SCHEMA_REGISTRY_IMAGE)
      .withNetwork(network)
      .withHostname('schema-registry')
      .withNetworkAliases('schema-registry')
      .withExposedPorts(SCHEMA_REGISTRY_PORT)
      .withEnvironment({
        SCHEMA_REGISTRY_HOST_NAME: 'schema-registry',
        SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'PLAINTEXT://kafka:9092',
        SCHEMA_REGISTRY_LISTENERS: `http://0.0.0.0:${SCHEMA_REGISTRY_PORT}`
      })
      .withWaitStrategy(
        Wait.forHttp('/subjects', SCHEMA_REGISTRY_PORT).forStatusCode(200)
      )
      .withStartupTimeout(180_000)
      .start();

    return {
      kafkaBroker: `${kafka.getHost()}:${kafka.getMappedPort(KAFKA_EXTERNAL_PORT)}`,
      schemaRegistryUrl: `http://${schemaRegistry.getHost()}:${schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT)}`,
      async stop() {
        await schemaRegistry?.stop();
        await kafka?.stop();
        await network?.stop();
      }
    };
  } catch (error) {
    await Promise.allSettled([
      schemaRegistry?.stop(),
      kafka?.stop(),
      network?.stop()
    ]);
    throw new Error(
      `Failed to start Kafka/Schema Registry Testcontainers environment. Ensure Docker is running and the images '${KAFKA_IMAGE}' and '${SCHEMA_REGISTRY_IMAGE}' are pullable. Cause: ${String(error)}`
    );
  }
}
