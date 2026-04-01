export default {
  outputDir: './generated',
  sources: {
    rootDir: '../schemas'
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
