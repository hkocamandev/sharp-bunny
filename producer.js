const express = require("express");
const multer = require("multer");
const amqp = require("amqplib");

const app = express();
const upload = multer({ dest: "uploads/" });
const QUEUE = "image_jobs";

let channel;

// RabbitMQ bağlantısı
async function connectQueue() {
  const connection = await amqp.connect("amqp://localhost");
  channel = await connection.createChannel();
  await channel.assertQueue(QUEUE);
  console.log("✅ Connected to RabbitMQ");
}
connectQueue();

// Çoklu dosya yükleme endpoint’i
app.post("/upload", upload.array("images", 10), async (req, res) => {
  const files = req.files;

  if (!files || files.length === 0) {
    return res.status(400).json({ error: "No files uploaded." });
  }

  files.forEach((file) => {
    const job = { path: file.path };
    channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(job)));
    console.log("📤 Sent to queue:", file.path);
  });

  res.json({
    message: `${files.length} image(s) queued for processing.`,
  });
});

app.listen(3000, () => console.log("🚀 API running on http://localhost:3000"));
