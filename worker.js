// worker.js
const amqp = require("amqplib");
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");

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
  JOB_STORE: path.join(__dirname, "jobs.json"),
};

// Ensure processed dir exists
if (!fs.existsSync(CONFIG.PROCESSED_DIR)) fs.mkdirSync(CONFIG.PROCESSED_DIR, { recursive: true });
if (!fs.existsSync(CONFIG.JOB_STORE)) fs.writeFileSync(CONFIG.JOB_STORE, JSON.stringify({}, null, 2));

// Helper functions
function sanitizeBaseName(name = "") {
  const base = String(name)
    .replace(/\.[^/.]+$/, "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-");
  return base || "file";
}

function shortId(id) {
  return String(id || "").replace(/-/g, "").slice(0, 8);
}

// Job store helpers
function readJobs() {
  try {
    const raw = fs.readFileSync(CONFIG.JOB_STORE, "utf-8");
    return raw ? JSON.parse(raw) : {};
  } catch (e) {
    console.error("Failed to read job store, returning empty:", e);
    return {};
  }
}

function writeJobs(jobs) {
  const tmp = CONFIG.JOB_STORE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(jobs, null, 2));
  fs.renameSync(tmp, CONFIG.JOB_STORE);
}

// Start worker
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
    try {
      ch.publish(CONFIG.LOG_EXCHANGE, "", Buffer.from(JSON.stringify(payload)));
    } catch (e) {
      console.error("Failed to publish log:", e);
    }
  }

  ch.consume(CONFIG.QUEUE, async (msg) => {
    if (!msg) return;

    let job;
    try {
      job = JSON.parse(msg.content.toString());
    } catch (e) {
      console.error("Invalid job message, acking and skipping:", e);
      ch.ack(msg);
      return;
    }

    const { jobId, filepath, filename, originalName, retries = 0 } = job;

    const orig = originalName || filename || path.basename(filepath || "");
    const base = sanitizeBaseName(orig);
    const procFilename = `${base}-${shortId(jobId)}.jpg`;
    const outputPath = path.join(CONFIG.PROCESSED_DIR, procFilename);

    publishLog({
      jobId,
      filename: procFilename,
      originalName: originalName || null,
      status: "processing",
      worker: CONFIG.WORKER_ID,
      retries,
      timestamp: new Date().toISOString(),
    });

    const start = Date.now();

    try {
      // Simulated failure for first attempt if filename contains 'fail'
      const failFirstAttempt = orig.toLowerCase().includes("fail") && retries === 0;
      if (failFirstAttempt) throw new Error("Simulated failure (first attempt for 'fail' filename)");

      await sharp(filepath)
        .resize({ width: 800, withoutEnlargement: true })
        .jpeg({ quality: 90 })
        .toFile(outputPath);

      const duration = ((Date.now() - start) / 1000).toFixed(2);

      // publish success
      publishLog({
        jobId,
        filename: procFilename,
        originalName: originalName || null,
        processedFilename: procFilename,
        status: "success",
        worker: CONFIG.WORKER_ID,
        duration,
        retries,
        timestamp: new Date().toISOString(),
      });

      // update job store
      const jobs = readJobs();
      jobs[jobId] = jobs[jobId] || {};
      jobs[jobId].processedFilename = procFilename;
      jobs[jobId].originalName = originalName || jobs[jobId].originalName || orig;
      jobs[jobId].status = "success";
      jobs[jobId].lastUpdated = new Date().toISOString();
      writeJobs(jobs);

      // cleanup uploaded file
      fs.unlink(filepath, (err) => {
        if (err) console.warn("Failed to delete uploaded file:", filepath, err.message);
      });

      ch.ack(msg);
    } catch (err) {
      const newRetries = retries + 1;
      console.error(`Worker ${CONFIG.WORKER_ID} failed job ${jobId}: ${err.message}`);

      // log failure
      publishLog({
        jobId,
        filename: procFilename,
        originalName: originalName || null,
        status: "failed",
        worker: CONFIG.WORKER_ID,
        retries: retries,
        error: err.message,
        timestamp: new Date().toISOString(),
      });

      if (newRetries >= CONFIG.MAX_RETRIES) {
        // move to DLQ
        ch.sendToQueue(
          CONFIG.DEAD_QUEUE,
          Buffer.from(JSON.stringify({ jobId, filepath, processedFilename: procFilename, retries: newRetries })),
          { persistent: true }
        );
        publishLog({
          jobId,
          filename: procFilename,
          originalName: originalName || null,
          status: "dead",
          worker: CONFIG.WORKER_ID,
          retries: newRetries,
          timestamp: new Date().toISOString(),
        });
      } else {
        // retry after 3s
        setTimeout(() => {
          ch.sendToQueue(
            CONFIG.RETRY_QUEUE,
            Buffer.from(JSON.stringify({ ...job, retries: newRetries })),
            { persistent: true }
          );
          publishLog({
            jobId,
            filename: procFilename,
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
  }, { noAck: false });

  // retry queue: push back to main queue
  ch.consume(CONFIG.RETRY_QUEUE, (msg) => {
    if (!msg) return;
    try {
      const job = JSON.parse(msg.content.toString());
      ch.sendToQueue(CONFIG.QUEUE, Buffer.from(JSON.stringify(job)), { persistent: true });
    } catch (e) {
      console.error("Invalid retry job:", e);
    } finally {
      ch.ack(msg);
    }
  }, { noAck: false });

  // DLQ logging
  ch.consume(CONFIG.DEAD_QUEUE, (msg) => {
    if (!msg) return;
    try {
      const job = JSON.parse(msg.content.toString());
      console.log(`DLQ entry: ${job.jobId} (${job.processedFilename || path.basename(job.filepath || "")})`);
    } catch (e) {
      console.error("Invalid DLQ message:", e);
    } finally {
      ch.ack(msg);
    }
  }, { noAck: false });
}

startWorker().catch((err) => {
  console.error("Worker error:", err);
  process.exit(1);
});
