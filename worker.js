const amqp = require("amqplib");
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");

const QUEUE = "image_jobs";
const RETRY_QUEUE = "image_retry_jobs";
const WORKER_ID = process.env.WORKER_ID || Math.floor(Math.random() * 1000);

async function startWorker() {
  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE);
  await channel.assertQueue(RETRY_QUEUE);

  console.log(`👷 Worker ${WORKER_ID} listening for messages...`);

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;
    const job = JSON.parse(msg.content.toString());
const { path: imagePath, originalName, retries = 0 } = job;
const filename = originalName || path.basename(imagePath);
    
    const outputPath = `processed/${filename}`;

    const start = Date.now();

    try {
      // 💣 Bilerek hata fırlatalım (örnek: "fail" içeren dosyalar)
      if (filename.toLowerCase().includes("fail")) {
        throw new Error("Simulated failure");
      }

      // Görseli işleme (örnek: resize)
      await sharp(imagePath).resize(800).toFile(outputPath);

      const duration = ((Date.now() - start) / 1000).toFixed(2);

      console.log(`✅ Worker ${WORKER_ID} processed: ${filename} (${duration}s)`);

      const log = {
        worker: WORKER_ID,
        filename,
        status: "success",
        duration,
        retries,
        timestamp: new Date().toISOString(),
      };

      fs.appendFileSync("worker-logs.json", JSON.stringify(log) + "\n");
      channel.ack(msg);
    } catch (err) {
      console.log(`❌ Worker ${WORKER_ID} failed: ${filename}`);

      const log = {
        worker: WORKER_ID,
        filename,
        status: "failed",
        error: err.message,
        retries,
        timestamp: new Date().toISOString(),
      };
      fs.appendFileSync("worker-logs.json", JSON.stringify(log) + "\n");

      // Retry limit (örnek: 2 denemeden sonra bırak)
      if (retries < 2) {
        const retryJob = { path: imagePath, retries: retries + 1 };
        console.log(`🔁 Retrying ${filename} (attempt ${retries + 1}) in 5s...`);
        setTimeout(() => {
          channel.sendToQueue(RETRY_QUEUE, Buffer.from(JSON.stringify(retryJob)));
        }, 5000);
      }

      channel.ack(msg); // mevcut mesajı tamamla
    }
  });

  // Retry kuyruğunu dinleyen worker kısmı
  channel.consume(RETRY_QUEUE, async (msg) => {
    if (!msg) return;
    const job = JSON.parse(msg.content.toString());
    console.log(`♻️ Worker ${WORKER_ID} received retry: ${path.basename(job.path)}`);
    channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(job))); // ana kuyruğa geri gönder
    channel.ack(msg);
  });
}

startWorker();
