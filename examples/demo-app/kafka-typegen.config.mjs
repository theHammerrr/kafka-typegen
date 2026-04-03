import { defineConfig } from "kafka-typegen";

export default defineConfig({
  outputDir: "./src/generated/kafka",
  sources: {
    rootDir: "./schemas",
  },
  generation: {
    packageName: "@app/kafka",
  },
  runtime: {
    transport: "@platformatic/kafka",
  },
  sync: {
    kafka: {
      brokers: ["localhost:19092"],
    },
    schemaRegistry: {
      failOnDrift: false,
    },
  },
  schemaRegistry: {
    url: "http://localhost:18081",
  },
  topics: [
    {
      name: "user.events",
      events: [
        {
          name: "user.created",
          schemaPath: "./user-created.avsc",
        },
        {
          name: "user.deleted",
          schemaPath: "./user-deleted.avsc",
        },
      ],
      sync: {
        partitions: 3,
        replicationFactor: 1,
      },
    },
  ],
});
