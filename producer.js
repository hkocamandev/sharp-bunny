// producer.js
// Express server + RabbitMQ publisher/consumer for logs + socket.io + job store

const express = require("express");
const multer = require("multer");
const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const http = require("http");
const { Server } = require("socket.io");
const sharp = require("sharp");


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
  WATERMARK_QUEUE: "watermark_jobs",
  JOB_STORE: path.join(__dirname, "jobs.json"),
  UPLOAD_DIR: path.join(__dirname, "uploads"),
  PROCESSED_DIR: path.join(__dirname, "processed"),
  WATERMARKED_DIR: path.join(__dirname, "watermarked"),
  MAX_UPLOAD: parseInt(process.env.MAX_UPLOAD_COUNT, 10) || 20,

};

// utils
function sanitizeBaseName(name = "") {
  // remove extension, normalize, keep alphanumeric and dashes/underscores
  const base = String(name)
    .replace(/\.[^/.]+$/, "")         // drop extension
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")         // remove symbols
    .trim()
    .replace(/\s+/g, "-");            // spaces -> dashes
  return base || "file";
}

function shortId(id) {
  return String(id || "").replace(/-/g, "").slice(0, 8);
}

///////////////////////
// Ensure folders & job store exist
///////////////////////
if (!fs.existsSync(CONFIG.UPLOAD_DIR)) fs.mkdirSync(CONFIG.UPLOAD_DIR, { recursive: true });
if (!fs.existsSync(CONFIG.PROCESSED_DIR)) fs.mkdirSync(CONFIG.PROCESSED_DIR, { recursive: true });
if (!fs.existsSync(CONFIG.WATERMARKED_DIR)) fs.mkdirSync(CONFIG.WATERMARKED_DIR, { recursive: true });
if (!fs.existsSync(CONFIG.JOB_STORE)) fs.writeFileSync(CONFIG.JOB_STORE, JSON.stringify({}, null, 2));
CONFIG.WATERMARK_QUEUE = process.env.WATERMARK_QUEUE || "watermark_jobs";
CONFIG.WATERMARKED_DIR = path.join(__dirname, "watermarked");
if (!fs.existsSync(CONFIG.WATERMARKED_DIR)) fs.mkdirSync(CONFIG.WATERMARKED_DIR, { recursive: true });


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
// serve processed and watermarked directories
app.use("/processed", express.static(CONFIG.PROCESSED_DIR));
app.use("/watermarked", express.static(CONFIG.WATERMARKED_DIR));



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
    await ch.assertQueue(CONFIG.WATERMARK_QUEUE, { durable: true });

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
    const existingFiles = new Set(fs.readdirSync(CONFIG.PROCESSED_DIR).map(f => f.toLowerCase()));

    for (const file of files) {
      if (existingFiles.has(file.originalname.toLowerCase())) {
        console.log(`Skipping already uploaded file: ${file.originalname}`);
        fs.unlinkSync(file.path); // delete temp file
        continue; // next file
      }
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
  try {
    const dirs = [CONFIG.PROCESSED_DIR, CONFIG.WATERMARKED_DIR];
    for (const dir of dirs) {
      if (!fs.existsSync(dir)) continue;
      const files = fs.readdirSync(dir);
      for (const f of files) {
        if (f === ".gitkeep") continue;
        const p = path.join(dir, f);
        if (fs.statSync(p).isFile()) fs.unlinkSync(p);
      }
    }
    res.json({ message: "Gallery cleared (processed + watermarked, except .gitkeep)" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


///////////////////////
// add-watermark endpoint
// Scans processed/ and enqueues watermark jobs only for not-yet-watermarked images
///////////////////////
app.post("/add-watermark", async (req, res) => {
  try {
    if (!fs.existsSync(CONFIG.PROCESSED_DIR)) return res.status(400).json({ error: "No processed images available." });

    const processedFiles = fs.readdirSync(CONFIG.PROCESSED_DIR).filter(f => /\.(png|jpe?g|gif|webp)$/i.test(f));
    if (!processedFiles.length) return res.status(400).json({ error: "No processed images found." });

    const jobs = readJobs();
    let enqueued = 0;

    for (const fname of processedFiles) {
      const ext = path.extname(fname);
      const base = path.basename(fname, ext);
      const wmName = `${base}_wm${ext}`;
      const wmPath = path.join(CONFIG.WATERMARKED_DIR, wmName);

      // skip if already watermarked file exists
      if (fs.existsSync(wmPath)) continue;

      const jobId = uuidv4();
      const job = {
        jobId,
        processedPath: path.join(CONFIG.PROCESSED_DIR, fname),
        processedFilename: fname,
        watermarkedFilename: wmName,
        originalName: (() => {
          // try to find job record that created the processed file
          const match = Object.values(jobs).find(j => j.processedFilename === fname || j.filename === fname);
          return match?.originalName || null;
        })(),
        retries: 0,
        createdAt: new Date().toISOString(),
      };

      // ensure watermark queue declared (we did earlier in connectRabbit ideally)
      if (amqpChannel) {
        amqpChannel.sendToQueue(CONFIG.WATERMARK_QUEUE, Buffer.from(JSON.stringify(job)), { persistent: true });
        const log = {
          jobId,
          filename: wmName,
          originalName: job.originalName || fname,
          status: "queued",
          retries: 0,
          worker: null,
          timestamp: new Date().toISOString(),
        };
        amqpChannel.publish(CONFIG.LOG_EXCHANGE, "", Buffer.from(JSON.stringify(log)));
      }

      jobs[jobId] = {
        id: jobId,
        filename: wmName,
        originalName: job.originalName || fname,
        status: "queued",
        retries: 0,
        createdAt: job.createdAt,
        lastUpdated: job.createdAt,
        processedFilename: fname,
        watermarkedFilename: wmName,
      };

      enqueued++;
    }

    writeJobs(jobs);
    return res.json({ enqueued, message: `${enqueued} file(s) enqueued for watermark.` });
  } catch (err) {
    console.error("add-watermark error:", err);
    return res.status(500).json({ error: err.message });
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
  const jobs = readJobs();

  const processedFiles = fs.existsSync(CONFIG.PROCESSED_DIR)
    ? fs.readdirSync(CONFIG.PROCESSED_DIR).filter(f => /\.(png|jpe?g|gif|webp)$/i.test(f))
    : [];

  const watermarkedFiles = fs.existsSync(CONFIG.WATERMARKED_DIR)
    ? fs.readdirSync(CONFIG.WATERMARKED_DIR).filter(f => /\.(png|jpe?g|gif|webp)$/i.test(f))
    : [];

  const map = new Map();

  // helper to infer original name from processed filename when job entry missing
  function inferOriginalFromProcessed(fname) {
    // aim: "cat001-93c551a4.jpg" -> "cat001.jpg"
    const ext = path.extname(fname);
    const base = path.basename(fname, ext);
    // remove trailing -<shortid> if present
    const maybe = base.replace(/-[0-9a-f]{6,}$/i, "");
    return `${maybe}${ext}`;
  }

  // 1) add processed files
  for (const f of processedFiles) {
    const match = Object.values(jobs).find(j => j.processedFilename === f || j.filename === f);
    const original = match?.originalName || inferOriginalFromProcessed(f);
    map.set(original, {
      url: `/processed/${f}`,   // served from processed directory
      filename: f,
      originalName: original,
      status: match?.status || "processed",
    });
  }

  // 2) overlay watermarked files (take precedence)
  for (const wf of watermarkedFiles) {
    // assume watermarked name pattern: <processed-base>_wm.ext  OR maybe <processed-base>-<id>_wm.ext
    const ext = path.extname(wf);
    const base = path.basename(wf, ext).replace(/_wm$/, ""); // remove _wm if present
    // find job that produced processed file whose basename matches 'base'
    const match = Object.values(jobs).find(j => {
      const pf = (j.processedFilename || j.filename || "").replace(/\.[^/.]+$/, "");
      return pf === base || pf.startsWith(base);
    });

    const original = match?.originalName || `${base}${ext}`;
    // serve watermarked files via /watermarked route
    map.set(original, {
      url: `/watermarked/${wf}`,
      filename: wf,
      originalName: original,
      status: match?.status || "watermarked",
    });
  }

  res.json(Array.from(map.values()));
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
