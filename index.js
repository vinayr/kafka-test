const kafka = require('kafka-node');
const Consumer = kafka.Consumer;

const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
const topics = [{ topic: 'test' }];
const options = {
  autoCommit: true,
  //fetchMaxWaitMs: 100,
  //fetchMaxBytes: 1024 * 1024,
};

const consumer = new Consumer(client, topics, options);

consumer.on('message', message => {
  console.log(message);
});

consumer.on('error', err => {
  console.log('error', err);
});
