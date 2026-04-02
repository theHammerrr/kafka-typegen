export default {
  outputDir: './src/generated/kafka',
  sources: {
    rootDir: './schemas'
  },
  generation: {
    packageName: '@app/kafka'
  },
  runtime: {
    transport: '@platformatic/kafka'
  },
  topics: [
    {
      name: 'user.events',
      events: [
        {
          name: 'user.created',
          schemaPath: './user-created.avsc'
        }
      ]
    }
  ]
};
