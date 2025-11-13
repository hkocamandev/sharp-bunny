require("dotenv").config();

const express = require("express");
const multer = require("multer");
const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");

const app = express();
const upload = multer({ dest: "uploads/" });
const QUEUE = "image_jobs";

let channel;

// Static files (frontend)
app.use(express.static("public"));

// RabbitMQ connection
async function connectQueue() {
  const connection = await amqp.connect("amqp://localhost");
  channel = await connection.createChannel();
  await channel.assertQueue(QUEUE);
  console.log("Connected to RabbitMQ");
}
connectQueue();

// multiple files upload endpoint
const maxUploadCount = parseInt(process.env.MAX_UPLOAD_COUNT) || 10;



app.post("/upload", (req, res) => {
  upload.array("images", maxUploadCount)(req, res, (err) => {
    if (err) {
      if (err instanceof multer.MulterError && err.code === "LIMIT_UNEXPECTED_FILE") {
        return res.status(400).json({ message: `You can upload up to ${maxUploadCount} images at a time.` });
      }
      return res.status(500).json({ message: "Upload failed.", error: err.message });
    }

    const files = req.files;
    if (!files || files.length === 0)
      return res.status(400).json({ message: "No files uploaded." });

    files.forEach((file) => {
      const job = { path: file.path,
        originalName: file.originalname
      };
      channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(job)));
      console.log("Sent to queue:", file.path);
    });

    res.json({ message: `${files.length} image(s) queued for processing.` });
  });
});


// Listig processed images
app.get("/processed", (req, res) => {
  const files = fs.readdirSync("processed");
  const images = files.map((f) => "/processed/" + f);
  res.json(images);
});

// Reading log files endpoint
app.get("/logs", (req, res) => {
  if (!fs.existsSync("worker-logs.json")) return res.json([]);

  const lines = fs.readFileSync("worker-logs.json", "utf-8").trim().split("\n");
  const logs = lines.map((line) => JSON.parse(line));
  res.json(logs);
});

// making processed folder available
app.use("/processed", express.static("processed"));

app.listen(3000, () => console.log(" Server running on http://localhost:3000"));
