const dotenv = require('dotenv');
const { Kafka } = require('kafkajs')
const avro = require('./avrotypes');
const {SchemaRegistry} = require('@kafkajs/confluent-schema-registry');
// import * as avro from './avrotypes/person';


dotenv.config();

const config = {
  clientId: process.env.APPNAME,
  brokers: [process.env.KAFKABROKER],
  // authenticationTimeout: 10000,
  // reauthenticationThreshold: 10000,
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

//registry.register()

const run = async () => {
  let timestamp = `${new Date(Date.now()).toLocaleString("en-GB").replaceAll(/[/,: ]/g, "")}`;

  let personsToEncode = [{
    firstName: "Oluwaremi",
    lastName: "Oluwole",
    timestamp: timestamp,
    age: 34
  },
  {
    firstName: "Oluwole",
    lastName: "Ogundele",
    timestamp: timestamp,
    age: 33
  }]; 

  let encodedPersons = [];

  for (let personToEncode of personsToEncode){
    let encodedPerson = await registry.encode(process.env.SCHEMA_ID, personToEncode);
    encodedPersons.push(encodedPerson);
  }
  /* registry.getLatestSchemaId(process.env.SCHEMA_SUBJECT)
  .then(i => id = i);
  console.log(`Id is ${id}`); */

  /* encodedPersons = personsToEncode.map(p => async () => {
    let encodedPerson = await registry.encode(process.env.SCHEMA_ID, p);
    console.log(encodedPerson);
    return encodedPerson;
  }); */

  
  const messages = encodedPersons.reduce((acc, curr) => [...acc, {value: curr}], []);
  
  /* encodedPersons = encodedPersons.map(p => avro.person.toBuffer(p));
  const messages = encodedPersons.reduce((acc, curr) => [...acc, {value: curr}], []); */
  
  // Producing
  await producer.connect()
  /* await producer.send({
    topic: process.env.KAFKATOPIC,
    messages: [
      {value: 'Hi, my name is Oluwaremi'}
    ]
  }); */
  await producer.send({
    topic: process.env.KAFKATOPIC,
    messages 
  });

  await producer.disconnect();

  const consumer = kafka.consumer({ groupId: process.env.KAFKACONSUMERGROUP })
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: process.env.KAFKATOPIC, fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: await registry.decode(message.value)
        // value: avro.person.fromBuffer(message.value)
        // value: message.value.toString()
      })
    },
  })
}

run().catch(console.error)

