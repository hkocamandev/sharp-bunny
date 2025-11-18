// producer.js
// Express server + RabbitMQ publisher/consumer for logs + socket.io + job store
// CommonJS style to match your project

const express = require("express");
const multer = require("multer");
const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const http = require("http");
const { Server } = require("socket.io");

///////////////////////
// CONFIG / CONSTANTS
///////////////////////
const CONFIG = {
  RABBIT_URL: process.env.RABBIT_URL || "amqp://localhost",
  HEARTBEAT: parseInt(process.env.RABBIT_HEARTBEAT, 10) || 30,
  QUEUE: "image_jobs",
  RETRY_QUEUE: "image_retry_jobs",
  DEAD_QUEUE: "dead_jobs",
  LOG_EXCHANGE: "logs", // fanout
  JOB_STORE: path.join(__dirname, "jobs.json"),
  UPLOAD_DIR: path.join(__dirname, "uploads"),
  PROCESSED_DIR: path.join(__dirname, "processed"),
  MAX_UPLOAD: parseInt(process.env.MAX_UPLOAD_COUNT, 10) || 20,
};

///////////////////////
// Ensure folders & job store exist
///////////////////////
if (!fs.existsSync(CONFIG.UPLOAD_DIR)) fs.mkdirSync(CONFIG.UPLOAD_DIR, { recursive: true });
if (!fs.existsSync(CONFIG.PROCESSED_DIR)) fs.mkdirSync(CONFIG.PROCESSED_DIR, { recursive: true });
if (!fs.existsSync(CONFIG.JOB_STORE)) fs.writeFileSync(CONFIG.JOB_STORE, JSON.stringify({}, null, 2));

///////////////////////
// Job store helpers (atomic write)
///////////////////////
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
  // atomic write: write to temp and rename
  const tmp = CONFIG.JOB_STORE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(jobs, null, 2));
  fs.renameSync(tmp, CONFIG.JOB_STORE);
}

///////////////////////
// Express + Socket.io + Multer
///////////////////////
const app = express();
const server = http.createServer(app);
const io = new Server(server);

const upload = multer({ dest: CONFIG.UPLOAD_DIR });

app.use(express.static(path.join(__dirname, "public")));
app.use("/processed", express.static(CONFIG.PROCESSED_DIR));

///////////////////////
// RabbitMQ connection & log consumer (subscribe to logs exchange)
///////////////////////
let amqpChannel = null;
let amqpConn = null;

async function connectRabbit() {
  try {
    amqpConn = await amqp.connect(CONFIG.RABBIT_URL, { heartbeat: CONFIG.HEARTBEAT });
    amqpConn.on("error", (err) => {
      console.error("RabbitMQ connection error (producer):", err);
    });
    amqpConn.on("close", () => {
      console.warn("RabbitMQ connection closed (producer). Reconnecting in 2s...");
      amqpConn = null;
      amqpChannel = null;
      setTimeout(connectRabbit, 2000);
    });

    const ch = await amqpConn.createChannel();
    await ch.assertQueue(CONFIG.QUEUE, { durable: true });
    await ch.assertQueue(CONFIG.RETRY_QUEUE, { durable: true });
    await ch.assertQueue(CONFIG.DEAD_QUEUE, { durable: true });
    await ch.assertExchange(CONFIG.LOG_EXCHANGE, "fanout", { durable: true });

    // create anonymous queue for logs, bind to exchange
    const qok = await ch.assertQueue("", { exclusive: true });
    await ch.bindQueue(qok.queue, CONFIG.LOG_EXCHANGE, "");

    // consume logs and broadcast via socket.io and update job store
    ch.consume(qok.queue, (msg) => {
      if (!msg) return;
      try {
        const log = JSON.parse(msg.content.toString());
        // update job store
        const jobs = readJobs();
        if (log.jobId) {
          jobs[log.jobId] = jobs[log.jobId] || {};
          jobs[log.jobId].id = log.jobId;
          // preserve originalName if exists
          if (!jobs[log.jobId].originalName && log.originalName) jobs[log.jobId].originalName = log.originalName;
          jobs[log.jobId].filename = log.filename || jobs[log.jobId].filename;
          if (log.status === "success") {
            jobs[log.jobId].processedFilename = log.filename;
          }

          jobs[log.jobId].status = log.status;
          jobs[log.jobId].retries = log.retries ?? jobs[log.jobId].retries ?? 0;
          if (log.duration) jobs[log.jobId].duration = log.duration;
          if (log.error) jobs[log.jobId].error = log.error;
          jobs[log.jobId].lastUpdated = log.timestamp;
          // if moved to dead, mark
          if (log.status === "dead") jobs[log.jobId].status = "dead";
          writeJobs(jobs);
        }
        // broadcast to websocket clients
        io.emit("job_log", log);
      } catch (err) {
        console.error("Error processing incoming log (producer):", err);
      } finally {
        ch.ack(msg);
      }
    });

    amqpChannel = ch;
    console.log("Connected to RabbitMQ (producer) and listening logs exchange");
  } catch (err) {
    console.error("Failed to connect to RabbitMQ (producer):", err);
    setTimeout(connectRabbit, 2000);
  }
}

connectRabbit();

///////////////////////
// HTTP endpoints
///////////////////////

// Upload endpoint

// Error handling middleware for multer
function multerErrorHandler(err, req, res, next) {
  if (err instanceof multer.MulterError) {
    if (err.code === "LIMIT_UNEXPECTED_FILE") {
      return res.status(400).json({ error: `Maximum ${CONFIG.MAX_UPLOAD} files allowed.` });
    }
    return res.status(400).json({ error: err.message });
  }
  next(err);
}

// Upload endpoint
app.post(
  "/upload",
  (req, res, next) => {
    // Early check before Multer processes files
    const files = req.files || [];
    const contentLength = parseInt(req.headers["content-length"] || "0", 10);

    // This only works if files come via multipart/form-data and we can check total number of parts
    // Otherwise, Multer limit will handle it
    next();
  },
  upload.array("images", CONFIG.MAX_UPLOAD),
  multerErrorHandler,
  async (req, res) => {
    const files = req.files || [];
    if (!files.length) return res.status(400).json({ error: "No files uploaded." });

    if (files.length > CONFIG.MAX_UPLOAD) {
      // Safety check if somehow Multer didn't catch
      // Cleanup uploaded temp files
      for (const f of files) fs.unlinkSync(f.path);
      return res.status(400).json({ error: `Maximum ${CONFIG.MAX_UPLOAD} files allowed.` });
    }

    const jobs = readJobs();

    for (const file of files) {
      const jobId = uuidv4();
      const job = {
        jobId,
        filepath: file.path,
        filename: path.basename(file.path),
        originalName: file.originalname,
        retries: 0,
        createdAt: new Date().toISOString(),
      };

      // store initial job record
      jobs[jobId] = {
        id: jobId,
        filename: job.filename,
        originalName: job.originalName,
        status: "queued",
        retries: 0,
        createdAt: job.createdAt,
        lastUpdated: job.createdAt,
      };

      // send message to queue
      if (amqpChannel) {
        amqpChannel.sendToQueue(CONFIG.QUEUE, Buffer.from(JSON.stringify(job)), { persistent: true });

        // publish 'queued' log
        const log = {
          jobId,
          filename: job.filename,
          originalName: job.originalName,
          status: "queued",
          retries: 0,
          worker: null,
          timestamp: new Date().toISOString(),
        };
        amqpChannel.publish(CONFIG.LOG_EXCHANGE, "", Buffer.from(JSON.stringify(log)));
      } else {
        console.warn("AMQP channel not ready, cannot queue job");
      }
    }

    writeJobs(jobs);
    res.json({ message: `${files.length} file(s) queued.` });
  }
);


// Clear processed (keep .gitkeep)
app.post("/clear-processed", (req, res) => {
  const dir = CONFIG.PROCESSED_DIR;
  try {
    const files = fs.readdirSync(dir);
    for (const f of files) {
      if (f === ".gitkeep") continue;
      const p = path.join(dir, f);
      if (fs.statSync(p).isFile()) fs.unlinkSync(p);
    }
    res.json({ message: "Gallery cleared (except .gitkeep)" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Clear jobs (reset job store)
app.post("/clear-jobs", (req, res) => {
  try {
    writeJobs({});
    res.json({ message: "Jobs cleared" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// get jobs
app.get("/jobs", (req, res) => {
  const jobs = readJobs();
  const arr = Object.values(jobs).sort((a, b) => new Date(b.lastUpdated) - new Date(a.lastUpdated));
  res.json(arr.slice(0, 200));
});

// processed list with originalName mapping
app.get("/processed", (req, res) => {
  const dir = CONFIG.PROCESSED_DIR;
  if (!fs.existsSync(dir)) return res.json([]);

  const files = fs.readdirSync(dir);
  const jobs = readJobs();

  const result = files
    .filter((f) => /\.(png|jpe?g|gif|webp|svg)$/i.test(f))
    .map((filename) => {
      const match = Object.values(jobs).find((j) => {
        return (
          j.filename === filename ||       
          j.originalName === filename ||   
          j.processedFilename === filename 
        );
      });

      return {
        url: `/processed/${filename}`,
        filename,
        originalName: match?.originalName ?? filename,
        status: match?.status ?? null,
        retries: match?.retries ?? 0,
      };
    });

  res.json(result);
});


// dead jobs
app.get("/dead", (req, res) => {
  const jobs = readJobs();
  const dead = Object.values(jobs).filter((j) => j.status === "dead");
  res.json(dead);
});

///////////////////////
// Socket.io events (logging only)
///////////////////////
io.on("connection", (socket) => {
  console.log("Client connected via socket.io");
  socket.on("disconnect", () => console.log("Client disconnected"));
});

///////////////////////
// start server
///////////////////////
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Producer server running at http://localhost:${PORT}`);
});
