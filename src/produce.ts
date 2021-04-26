import { Kafka } from 'kafkajs';

export async function produce(): Promise<void> {
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

    const randomUser: Array<string> = ['song', 'minghui', 'haoran'];
    const randomText: Array<string> = ['random text', 'random text1', 'random text2', 'random text3'];

    // producer => cluster [brokers/servers, brokers, brokers] => topic <= consumer
    const producer = kafka.producer();
    await producer.connect();
    for (let i = 0; i < 100; i++) {
      const result = await producer.send({
        topic: 'p8hxohu1-tweets',
        messages: [
          {
            value: JSON.stringify({
              tweetOwner: randomUser[Math.floor(Math.random() * randomUser.length)],
              tweetInfo: randomText[Math.floor(Math.random() * randomText.length)]
            }),
          }
        ]
      });
      console.log('send successfully');
      console.log(result);
    }
    producer.disconnect();
  } catch (err) {
    console.error(err);
  }
}

(async function run() {
  await produce();
})();
