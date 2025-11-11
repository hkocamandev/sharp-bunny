const amqp = require("amqplib");
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");

const QUEUE = "image_jobs";

async function startWorker() {
  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();
  await channel.assertQueue(QUEUE);

  console.log("👂 Worker started and waiting for messages...");

  channel.consume(QUEUE, async (msg) => {
    if (msg) {
      const { path: imagePath } = JSON.parse(msg.content.toString());
      const filename = path.basename(imagePath);
      const outputPath = `processed/${filename}.jpg`;

      try {
        await sharp(imagePath).resize(800).toFile(outputPath);
        console.log(`✅ Processed: ${filename}`);
        channel.ack(msg);
      } catch (err) {
        console.error("❌ Error processing image:", err);
      }
    }
  });
}

startWorker();
