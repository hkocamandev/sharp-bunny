// producer.js
const express = require("express");
const multer = require("multer");
const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const upload = multer({ dest: "uploads/" });

// Queues / Exchange
const QUEUE = "image_jobs";
const RETRY_QUEUE = "image_retry_jobs";
const DEAD_QUEUE = "dead_jobs";
const LOG_EXCHANGE = "logs"; // fanout

// Simple persistent job store (file)
const JOB_STORE = "jobs.json";
function readJobs() {
  if (!fs.existsSync(JOB_STORE)) return {};
  try {
    return JSON.parse(fs.readFileSync(JOB_STORE, "utf-8") || "{}");
  } catch {
    return {};
  }
}
function writeJobs(jobs) {
  fs.writeFileSync(JOB_STORE, JSON.stringify(jobs, null, 2));
}

// ensure job store exists
if (!fs.existsSync(JOB_STORE)) writeJobs({});

let amqpChannel;

async function connectRabbit() {
  const conn = await amqp.connect("amqp://localhost");
  const ch = await conn.createChannel();

  // queues/exchange
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.assertQueue(RETRY_QUEUE, { durable: true });
  await ch.assertQueue(DEAD_QUEUE, { durable: true });
  await ch.assertExchange(LOG_EXCHANGE, "fanout", { durable: true });

  // create an anonymous queue bound to logs exchange so we can consume logs
  const qok = await ch.assertQueue("", { exclusive: true });
  await ch.bindQueue(qok.queue, LOG_EXCHANGE, "");

  // consume logs and broadcast via socket.io and update job store
  ch.consume(qok.queue, (msg) => {
    if (!msg) return;
    try {
      const log = JSON.parse(msg.content.toString());
      // update job store
      const jobs = readJobs();
      if (log.jobId) {
        jobs[log.jobId] = jobs[log.jobId] || {};
        // merge some fields
        jobs[log.jobId].id = log.jobId;
        jobs[log.jobId].filename = log.filename;
        jobs[log.jobId].status = log.status;
        jobs[log.jobId].retries = log.retries ?? jobs[log.jobId].retries ?? 0;
        if (log.duration) jobs[log.jobId].duration = log.duration;
        if (log.error) jobs[log.jobId].error = log.error;
        jobs[log.jobId].lastUpdated = log.timestamp;
        writeJobs(jobs);
      }
      // broadcast to websocket clients
      io.emit("job_log", log);
    } catch (err) {
      console.error("Error processing incoming log:", err);
    } finally {
      ch.ack(msg);
    }
  });

  amqpChannel = ch;
  console.log("Connected to RabbitMQ and listening logs exchange");
}

connectRabbit().catch((err) => {
  console.error("RabbitMQ connection error:", err);
  process.exit(1);
});

// serve static frontend
app.use(express.static("public"));
app.use("/processed", express.static("processed"));

// Upload endpoint - create job and send to queue
app.post("/upload", upload.array("images", 20), async (req, res) => {
  const files = req.files || [];
  if (!files.length) return res.status(400).json({ error: "No files" });

  const jobs = readJobs();

  files.forEach((file) => {
    const jobId = uuidv4();
    const job = {
      jobId,
      path: file.path,
      originalName: file.originalname,
      filename: path.basename(file.path),
      originalName: file.originalname,
      retries: 0,
      createdAt: new Date().toISOString(),
    };

    // store initial job record
    jobs[jobId] = {
      id: jobId,
      filename: job.filename,
      status: "queued",
      retries: 0,
      createdAt: job.createdAt,
      lastUpdated: job.createdAt,
    };

    // send message to queue
    amqpChannel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(job)), {
      persistent: true,
    });

    // also publish a "queued" log to logs exchange (so UI sees it)
    const log = {
      jobId,
      filename: job.filename,
      status: "queued",
      retries: 0,
      worker: null,
      timestamp: new Date().toISOString(),
    };
    amqpChannel.publish(LOG_EXCHANGE, "", Buffer.from(JSON.stringify(log)));
  });

  writeJobs(jobs);
  res.json({ message: `${files.length} file(s) queued.` });
});

app.post("/clear-processed", (req, res) => {
  const dir = path.join(__dirname, "processed");

  fs.readdir(dir, (err, files) => {
    if (err) return res.json({ error: err });

    for (const f of files) {
      if (f === ".gitkeep") continue;
      fs.unlinkSync(path.join(dir, f));
    }

    res.json({ message: "Gallery cleared (except .gitkeep)" });
  });
});

app.post("/clear-jobs", (req, res) => {
  const jobsFile = path.join(__dirname, "jobs.json");
  fs.writeFileSync(jobsFile, JSON.stringify([], null, 2));

  res.json({ message: "Jobs cleared" });
});



// API: get recent logs via job store (polling fallback)
app.get("/jobs", (req, res) => {
  const jobs = readJobs();
  // return as array sorted by lastUpdated
  const arr = Object.values(jobs).sort((a, b) =>
    new Date(b.lastUpdated) - new Date(a.lastUpdated)
  );
  res.json(arr.slice(0, 200));
});

app.get("/processed", (req, res) => {
  const dir = "processed";
  if (!fs.existsSync(dir)) return res.json([]);

  const files = fs.readdirSync(dir); // all files in processed folder
  const jobs = readJobs();           // load job store

  const result = files.map(filename => {
    // find the job that is related to filename in job store
    const match = Object.values(jobs).find(j => j.filename === filename);

    return {
      url: `/processed/${filename}`,
      filename,
      originalName: match?.originalName || null,
      status: match?.status || null,
      retries: match?.retries || 0,
    };
  });

  res.json(result);
});


// API: dead jobs list (filter jobs with status dead)
app.get("/dead", (req, res) => {
  const jobs = readJobs();
  const dead = Object.values(jobs).filter((j) => j.status === "dead");
  res.json(dead);
});

// socket.io connection info
io.on("connection", (socket) => {
  console.log("Client connected via socket.io");
  socket.on("disconnect", () => console.log(" Client disconnected"));
});

const PORT = 3000;
server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
