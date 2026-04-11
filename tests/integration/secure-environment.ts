import { Kafka } from 'kafkajs';
import {
  GenericContainer,
  Network,
  Wait,
  type StartedNetwork,
  type StartedTestContainer
} from 'testcontainers';

const KAFKA_IMAGE = 'confluentinc/cp-kafka:7.6.1';
const DEFAULT_CLUSTER_ID = '4L6g3nShT-eMCtK--X86sw';
const STARTER_SCRIPT = '/testcontainers_secure_start.sh';
const WAIT_FOR_SCRIPT_MESSAGE = 'Waiting for secure Kafka starter script...';
const KAFKA_EXTERNAL_PORT = 9093;
const KAFKA_INTERNAL_PORT = 9092;
const KAFKA_CONTROLLER_PORT = 9094;
const SASL_USERNAME = 'kafka-typegen';
const SASL_PASSWORD = 'kafka-typegen-secret';
const SASL_MECHANISM = 'scram-sha-256';

export interface SecureIntegrationEnvironment {
  readonly kafkaBroker: string;
  readonly sasl: {
    readonly mechanism: 'scram-sha-256';
    readonly password: string;
    readonly username: string;
  };
  stop(): Promise<void>;
}

export async function startSecureIntegrationEnvironment(): Promise<SecureIntegrationEnvironment> {
  let network: StartedNetwork | undefined;
  let kafka: StartedTestContainer | undefined;
  let kafkaLogs = '';

  try {
    network = await new Network().start();
    kafka = await new GenericContainer(KAFKA_IMAGE)
      .withNetwork(network)
      .withHostname('secure-kafka')
      .withNetworkAliases('secure-kafka')
      .withExposedPorts(KAFKA_EXTERNAL_PORT)
      .withStartupTimeout(180_000)
      .withEnvironment({
        CLUSTER_ID: DEFAULT_CLUSTER_ID,
        KAFKA_BROKER_ID: '1',
        KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE: 'false',
        KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
        KAFKA_CONTROLLER_QUORUM_VOTERS: `1@secure-kafka:${KAFKA_CONTROLLER_PORT}`,
        KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
        KAFKA_INTER_BROKER_LISTENER_NAME: 'BROKER',
        KAFKA_LISTENERS: [
          `BROKER://0.0.0.0:${KAFKA_INTERNAL_PORT}`,
          `SASL_PLAINTEXT://0.0.0.0:${KAFKA_EXTERNAL_PORT}`,
          `CONTROLLER://0.0.0.0:${KAFKA_CONTROLLER_PORT}`
        ].join(','),
        KAFKA_LISTENER_NAME_SASL_PLAINTEXT_SCRAM_SHA_256_SASL_JAAS_CONFIG:
          'org.apache.kafka.common.security.scram.ScramLoginModule required;',
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
          'BROKER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT',
        KAFKA_NODE_ID: '1',
        KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: '1',
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
        KAFKA_PROCESS_ROLES: 'broker,controller',
        KAFKA_SASL_ENABLED_MECHANISMS: 'SCRAM-SHA-256',
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1'
      })
      .withEntrypoint(['sh'])
      .withCommand([
        '-c',
        `echo '${WAIT_FOR_SCRIPT_MESSAGE}'; while [ ! -f ${STARTER_SCRIPT} ]; do sleep 0.1; done; ${STARTER_SCRIPT}`
      ])
      .withWaitStrategy(Wait.forLogMessage(WAIT_FOR_SCRIPT_MESSAGE))
      .start();

    const kafkaBroker = `${kafka.getHost()}:${kafka.getMappedPort(KAFKA_EXTERNAL_PORT)}`;
    await kafka.copyContentToContainer([
      {
        content: buildKafkaStartScript(kafkaBroker),
        mode: 0o777,
        target: STARTER_SCRIPT
      }
    ]);
    await waitForSecureKafka(kafkaBroker);

    return {
      kafkaBroker,
      sasl: {
        mechanism: SASL_MECHANISM,
        password: SASL_PASSWORD,
        username: SASL_USERNAME
      },
      async stop() {
        await kafka?.stop();
        await network?.stop();
      }
    };
  } catch (error) {
    kafkaLogs = await readContainerLogs(kafka);
    await Promise.allSettled([kafka?.stop(), network?.stop()]);
    throw new Error(
      `Failed to start secure Kafka Testcontainers environment. Ensure Docker is running and '${KAFKA_IMAGE}' is pullable. Cause: ${String(error)}${kafkaLogs === '' ? '' : `\nKafka logs:\n${kafkaLogs}`}`
    );
  }
}

function buildKafkaStartScript(kafkaBroker: string): string {
  return `#!/bin/bash
export KAFKA_ADVERTISED_LISTENERS="BROKER://secure-kafka:${KAFKA_INTERNAL_PORT},SASL_PLAINTEXT://${kafkaBroker}"
echo 'kafka-storage format --ignore-formatted -t "${DEFAULT_CLUSTER_ID}" -c /etc/kafka/kafka.properties --add-scram "SCRAM-SHA-256=[name=${SASL_USERNAME},password=${SASL_PASSWORD}]"' >> /etc/confluent/docker/configure
/etc/confluent/docker/run
`;
}

async function waitForSecureKafka(kafkaBroker: string): Promise<void> {
  const kafka = new Kafka({
    brokers: [kafkaBroker],
    clientId: 'kafka-typegen-secure-waiter',
    sasl: {
      mechanism: SASL_MECHANISM,
      password: SASL_PASSWORD,
      username: SASL_USERNAME
    }
  });

  const deadline = Date.now() + 180_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    const admin = kafka.admin();

    try {
      await admin.connect();
      await admin.fetchTopicMetadata();
      await admin.disconnect();
      return;
    } catch (error) {
      lastError = error;
      await admin.disconnect().catch(() => undefined);
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
  }

  throw new Error(
    `Timed out waiting for secure Kafka to accept SASL connections. Cause: ${String(lastError)}`
  );
}

async function readContainerLogs(container: StartedTestContainer | undefined): Promise<string> {
  if (container === undefined) {
    return '';
  }

  try {
    const stream = await container.logs({ tail: 200 });
    let logs = '';

    for await (const chunk of stream) {
      logs += chunk.toString();
    }

    return logs.trim();
  } catch {
    return '';
  }
}
