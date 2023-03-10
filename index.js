const dotenv = require('dotenv');
const { Kafka } = require('kafkajs')
const {SchemaRegistry} = require('@kafkajs/confluent-schema-registry');


dotenv.config();

const config = {
  clientId: process.env.APPNAME,
  brokers: [process.env.KAFKABROKER],
  ssl: true,
  sasl: {
    mechanism: process.env.SASLMECHANISM, // scram-sha-256 or scram-sha-512
    username: process.env.SASLUSERNAME,
    password: process.env.SASLPASSWORD
  }
};

const kafka = new Kafka(config);
const registry = new SchemaRegistry({ 
  host: process.env.SCHEMA_REGISTRY_URL,
  auth: {
    username: process.env.SCHEMA_REGISTRY_KEY,
    password: process.env.SCHEMA_REGISTRY_SECRET,
  }
})

const producer = kafka.producer();

const run = async () => {
  const timestamp = `${new Date(Date.now()).toLocaleString("en-GB").replaceAll(/[/,: ]/g, "")}`;

  const personToEncode = {
    firstName: "Oluwaremi",
    lastName: "Oluwole",
    timestamp: timestamp,
    age: 33
  };

  const id = await registry.getRegistryId("test_topicPersonRecord", 3);
  console.log(`Id is ${id}`); 

  const encodedPerson = await registry.encode(process.env.SCHEMA_ID, personToEncode);

  const messages = [
    {value: encodedPerson}
  ];
  
  // Producing
  await producer.connect()

  await producer.send({
    topic: process.env.KAFKATOPIC,
    messages 
  });

  await producer.disconnect();

  // Consuming
  const consumer = kafka.consumer({ groupId: process.env.KAFKACONSUMERGROUP });
  await consumer.connect()
  await consumer.subscribe({ topic: process.env.KAFKATOPIC, fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: await registry.decode(message.value)
      })
    },
  });

  await consumer.disconnect();
}

run().catch(console.error)

