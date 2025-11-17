// worker.js
const amqp = require("amqplib");
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");

const QUEUE = "image_jobs";
const RETRY_QUEUE = "image_retry_jobs";
const DEAD_QUEUE = "dead_jobs";
const LOG_EXCHANGE = "logs";
const WORKER_ID = process.env.WORKER_ID || Math.floor(Math.random() * 1000);
const MAX_RETRIES = 3; // 0..2 => 3 attempts

async function startWorker() {
const connection = await amqp.connect("amqp://localhost", {
  heartbeat: 30   // 5 yerine 30 saniye
});
  const ch = await connection.createChannel();

  connection.on("error", (err) => {
      console.error("RabbitMQ connection error:", err);
    });

    connection.on("close", () => {
      console.error("RabbitMQ connection closed. Reconnecting...");
      setTimeout(startWorker, 2000);
    });

  await ch.assertQueue(QUEUE, { durable: true });
  await ch.assertQueue(RETRY_QUEUE, { durable: true });
  await ch.assertQueue(DEAD_QUEUE, { durable: true });
  await ch.assertExchange(LOG_EXCHANGE, "fanout", { durable: true });

  console.log(`Worker ${WORKER_ID} started and waiting for messages...`);

  // helper to publish log
  function publishLog(payload) {
    ch.publish(LOG_EXCHANGE, "", Buffer.from(JSON.stringify(payload)));
  }

  // consume main queue
  ch.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;
      const job = JSON.parse(msg.content.toString());
      const { jobId, path: imagePath,originalName, retries = 0 } = job;
      const filename = originalName || path.basename(imagePath);
      const outputPath = `processed/${filename}`;

      // publish processing log
      publishLog({
        jobId,
        filename,
        status: "processing",
        worker: WORKER_ID,
        retries,
        timestamp: new Date().toISOString(),
      });

      const start = Date.now();
      try {
        // simulate a failure for filenames containing "fail"
        if (filename.toLowerCase().includes("fail")) {
          throw new Error("Simulated failure (filename contains 'fail')");
        }

        // actual image processing
        await sharp(imagePath).resize(800).toFile(outputPath);

        const duration = ((Date.now() - start) / 1000).toFixed(2);

        publishLog({
          jobId,
          filename,
          status: "success",
          worker: WORKER_ID,
          duration,
          retries,
          timestamp: new Date().toISOString(),
        });

        console.log(`Worker ${WORKER_ID} success: ${filename}`);
        ch.ack(msg);
      } catch (err) {
        console.log(`Worker ${WORKER_ID} failed: ${filename} err=${err.message}`);

        const newRetries = (retries ?? 0) + 1;

        publishLog({
          jobId,
          filename,
          status: "failed",
          worker: WORKER_ID,
          error: err.message,
          retries,
          timestamp: new Date().toISOString(),
        });

        if (newRetries >= MAX_RETRIES) {
          // send to dead queue
          const deadJob = { jobId, path: imagePath, retries: newRetries };
          ch.sendToQueue(DEAD_QUEUE, Buffer.from(JSON.stringify(deadJob)), {
            persistent: true,
          });
          publishLog({
            jobId,
            filename,
            status: "dead",
            worker: WORKER_ID,
            retries: newRetries,
            timestamp: new Date().toISOString(),
          });
          console.log(`Worker ${WORKER_ID} moved ${filename} to DLQ`);
        } else {
          // schedule retry: push to retry queue (we can delay with setTimeout here)
          const retryJob = { jobId, path: imagePath, retries: newRetries };
          console.log(`🔁 Worker ${WORKER_ID} scheduling retry ${filename} attempt ${newRetries}`);
          // small delay before requeue to avoid busy-loop
          setTimeout(() => {
            ch.sendToQueue(RETRY_QUEUE, Buffer.from(JSON.stringify(retryJob)), {
              persistent: true,
            });
            publishLog({
              jobId,
              filename,
              status: "retried",
              worker: WORKER_ID,
              retries: newRetries,
              timestamp: new Date().toISOString(),
            });
          }, 5000);
        }
        ch.ack(msg);
      }
    },
    { noAck: false }
  );

  // retry queue consumer: simply puts job back to main queue
  ch.consume(
    RETRY_QUEUE,
    (msg) => {
      if (!msg) return;
      const job = JSON.parse(msg.content.toString());
      // publish to main queue
      ch.sendToQueue(QUEUE, Buffer.from(JSON.stringify(job)), { persistent: true });
      ch.ack(msg);
    },
    { noAck: false }
  );

  // optional: consumer for dead queue just logging (or you can inspect jobs via producer)
  ch.consume(
    DEAD_QUEUE,
    (msg) => {
      if (!msg) return;
      const job = JSON.parse(msg.content.toString());
      console.log(`DLQ entry: ${job.jobId} (${path.basename(job.path)})`);
      // ack and keep in dead queue? We'll ack so DLQ is a log channel; if you want DLQ persistent listing, you can store elsewhere.
      ch.ack(msg);
    },
    { noAck: false }
  );
}

startWorker().catch((err) => {
  console.error("Worker error:", err);
  process.exit(1);
});
