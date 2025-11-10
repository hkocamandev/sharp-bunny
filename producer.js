const express = require("express");
const multer = require("multer");
const amqp = require("amqplib");

const app = express();
const upload = multer({ dest: "uploads/" });
const QUEUE = "image_jobs";

let channel;

// RabbitMQ cnnection
async function connectQueue() {
  const connection = await amqp.connect("amqp://localhost");
  channel = await connection.createChannel();
  await channel.assertQueue(QUEUE);
  console.log("RabbitMQ connected and queue created");
}

connectQueue();

// API: Image upload endpoint
app.post("/upload", upload.single("image"), async (req, res) => {
  const imagePath = req.file.path;

  // Send message to queue
  channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify({ path: imagePath })));
  console.log("Message sent to queue:", imagePath);

  res.json({ status: "Image uploaded, processing in background..." });
});

app.listen(3000, () => console.log("API running on http://localhost:3000"));
