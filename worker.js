// worker.js
const amqp = require("amqplib");
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");

const CONFIG = {
  RABBIT_URL: process.env.RABBIT_URL || "amqp://localhost",
  HEARTBEAT: parseInt(process.env.RABBIT_HEARTBEAT, 10) || 30,
  QUEUE: "image_jobs",
  RETRY_QUEUE: "image_retry_jobs",
  DEAD_QUEUE: "dead_jobs",
  LOG_EXCHANGE: "logs",
  WORKER_ID: process.env.WORKER_ID || Math.floor(Math.random() * 1000),
  MAX_RETRIES: parseInt(process.env.MAX_RETRIES, 10) || 3,
  PROCESSED_DIR: path.join(__dirname, "processed"),
};

// ensure processed dir exists
if (!fs.existsSync(CONFIG.PROCESSED_DIR)) fs.mkdirSync(CONFIG.PROCESSED_DIR, { recursive: true });

async function startWorker() {
  const connection = await amqp.connect(CONFIG.RABBIT_URL, { heartbeat: CONFIG.HEARTBEAT });
  const ch = await connection.createChannel();

  connection.on("error", (err) => console.error("RabbitMQ connection error:", err));
  connection.on("close", () => {
    console.error("RabbitMQ closed, reconnecting in 2s...");
    setTimeout(startWorker, 2000);
  });

  await ch.assertQueue(CONFIG.QUEUE, { durable: true });
  await ch.assertQueue(CONFIG.RETRY_QUEUE, { durable: true });
  await ch.assertQueue(CONFIG.DEAD_QUEUE, { durable: true });
  await ch.assertExchange(CONFIG.LOG_EXCHANGE, "fanout", { durable: true });

  console.log(`Worker ${CONFIG.WORKER_ID} started`);

  function publishLog(payload) {
    // safe publish: try/catch so worker doesn't crash if publish fails
    try {
      ch.publish(CONFIG.LOG_EXCHANGE, "", Buffer.from(JSON.stringify(payload)));
    } catch (e) {
      console.error("Failed to publish log:", e);
    }
  }

  ch.consume(
    CONFIG.QUEUE,
    async (msg) => {
      if (!msg) return;
      let job;
      try {
        job = JSON.parse(msg.content.toString());
      } catch (e) {
        console.error("Invalid job message, acking and skipping:", e);
        ch.ack(msg);
        return;
      }

      const {
        jobId,
        filepath,        
        filename,        
        originalName,    
        retries = 0,
      } = job;

      // decide which name to check for 'fail' token (prefer originalName)
      const nameToCheck = (originalName || filename || "").toString();
      // processed output filename - ensure it has an image extension frontend will accept
      const processedFilename = (filename || path.basename(filepath || "")) + ".jpg";
      const outputPath = path.join(CONFIG.PROCESSED_DIR, processedFilename);

      // publish processing start
      publishLog({
        jobId,
        filename: processedFilename,
        originalName: originalName || null,
        status: "processing",
        worker: CONFIG.WORKER_ID,
        retries,
        timestamp: new Date().toISOString(),
      });

      const start = Date.now();

      try {
        // Simulate failure only on FIRST attempt if originalName (or nameToCheck) contains 'fail'
        const failFirstAttempt = nameToCheck.toLowerCase().includes("fail") && retries === 0;
        if (failFirstAttempt) {
          throw new Error("Simulated failure (first attempt for 'fail' filename)");
        }

        // Actual image processing with sharp — write as JPEG to ensure front-end sees extension
        await sharp(filepath)
          .resize({
            width: 800,
            withoutEnlargement: true
          })
          .jpeg({ quality: 90 })
          .toFile(outputPath);


        const duration = ((Date.now() - start) / 1000).toFixed(2);

        // publish success (use processedFilename so producer can map)
        publishLog({
          jobId,
          filename: processedFilename,
          originalName: originalName || null,
          status: "success",
          worker: CONFIG.WORKER_ID,
          duration,
          retries,
          timestamp: new Date().toISOString(),
        });

        // cleanup uploaded file (best-effort)
        fs.unlink(filepath, (err) => {
          if (err) console.warn("Failed to delete uploaded file:", filepath, err.message);
        });

        ch.ack(msg);
      } catch (err) {
        const newRetries = (retries || 0) + 1;
        console.log(`Worker ${CONFIG.WORKER_ID} failed: ${nameToCheck} (attempt ${newRetries}) -> ${err.message}`);

        publishLog({
          jobId,
          filename: processedFilename,
          originalName: originalName || null,
          status: "failed",
          worker: CONFIG.WORKER_ID,
          retries: newRetries - 1, // previous retries
          error: err.message,
          timestamp: new Date().toISOString(),
        });

        if (newRetries >= CONFIG.MAX_RETRIES) {
          // move to DLQ
          ch.sendToQueue(
            CONFIG.DEAD_QUEUE,
            Buffer.from(JSON.stringify({ jobId, filepath, processedFilename, retries: newRetries })),
            { persistent: true }
          );
          publishLog({
            jobId,
            filename: processedFilename,
            originalName: originalName || null,
            status: "dead",
            worker: CONFIG.WORKER_ID,
            retries: newRetries,
            timestamp: new Date().toISOString(),
          });
        } else {
          // schedule retry by sending into retry queue after a small delay
          setTimeout(() => {
            ch.sendToQueue(
              CONFIG.RETRY_QUEUE,
              Buffer.from(JSON.stringify({ ...job, retries: newRetries })),
              { persistent: true }
            );
            publishLog({
              jobId,
              filename: processedFilename,
              originalName: originalName || null,
              status: "retried",
              worker: CONFIG.WORKER_ID,
              retries: newRetries,
              timestamp: new Date().toISOString(),
            });
          }, 3000);
        }

        ch.ack(msg);
      }
    },
    { noAck: false }
  );

  // retry queue consumer: push back to main queue
  ch.consume(
    CONFIG.RETRY_QUEUE,
    (msg) => {
      if (!msg) return;
      try {
        const job = JSON.parse(msg.content.toString());
        ch.sendToQueue(CONFIG.QUEUE, Buffer.from(JSON.stringify(job)), { persistent: true });
      } catch (e) {
        console.error("Invalid retry job:", e);
      } finally {
        ch.ack(msg);
      }
    },
    { noAck: false }
  );

  // dead queue logging
  ch.consume(
    CONFIG.DEAD_QUEUE,
    (msg) => {
      if (!msg) return;
      try {
        const job = JSON.parse(msg.content.toString());
        console.log(`DLQ entry: ${job.jobId} (${job.processedFilename || path.basename(job.filepath || "")})`);
      } catch (e) {
        console.error("Invalid DLQ message:", e);
      } finally {
        ch.ack(msg);
      }
    },
    { noAck: false }
  );
}

startWorker().catch((err) => {
  console.error("Worker error:", err);
  process.exit(1);
});
