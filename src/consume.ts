import { Kafka } from 'kafkajs';

export async function consume(): Promise<void> {
  try {
    const kafka = new Kafka({
      clientId: 'song',
      brokers: [
        'tricycle-01.srvs.cloudkafka.com:9094',
        'tricycle-02.srvs.cloudkafka.com:9094',
        'tricycle-03.srvs.cloudkafka.com:9094'
      ],
      ssl: true,
      sasl: {
        mechanism: 'scram-sha-256', // scram-sha-256 or scram-sha-512
        username: 'p8hxohu1',
        password: 'Cr4SGh92Gd3MJdKzKstv53j5oplq3F2O'
      },
    });
    const consumer = kafka.consumer({ groupId: 'song' });
    await consumer.connect();
    await consumer.subscribe({
      topic: 'p8hxohu1-tweets',
      fromBeginning: true
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(result);
        const { message } = result;
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        //@ts-ignore
        console.log(JSON.parse(message.value.toString()));
      }
    });
  } catch (err) {
    console.error(err);
  }
}

(async function run() {
  await consume();
})();
