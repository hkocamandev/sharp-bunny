const amqp = require("amqplib");
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");

const QUEUE = "image_jobs";
const WORKER_ID = process.env.WORKER_ID || Math.floor(Math.random() * 1000);

async function startWorker() {
  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();
  await channel.assertQueue(QUEUE);

  console.log(`Worker ${WORKER_ID} listening for messages...`);

  channel.consume(QUEUE, async (msg) => {
    if (msg) {
      const { path: imagePath } = JSON.parse(msg.content.toString());
      const filename = path.basename(imagePath);
      const outputPath = `processed/${filename}`;

      try {
        await sharp(imagePath).resize(800).toFile(outputPath);

        console.log(`Worker ${WORKER_ID} processed: ${filename}`);

        // Saving logs of the workers (producer will be reading from here)
        const log = {
          worker: WORKER_ID,
          filename,
          timestamp: new Date().toISOString(),
        };

        fs.appendFileSync("worker-logs.json", JSON.stringify(log) + "\n");

        channel.ack(msg);
      } catch (err) {
        console.error(` Worker ${WORKER_ID} error:`, err);
      }
    }
  });
}

startWorker();
