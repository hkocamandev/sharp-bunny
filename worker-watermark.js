// worker-watermark.js
const sharp = require("sharp");
const fs = require("fs");
const path = require("path");
const amqp = require("amqplib");
const { v4: uuidv4 } = require("uuid");

const CONFIG = {
  RABBIT_URL: process.env.RABBIT_URL || "amqp://localhost",
  QUEUE: process.env.WATERMARK_QUEUE || "watermark_jobs",
  WATERMARKED_DIR: path.join(__dirname, "watermarked"),
  MAX_RETRIES: 3,
  WORKER_ID: process.env.WORKER_ID || `wm-${Math.floor(Math.random() * 1000)}`,
};

if (!fs.existsSync(CONFIG.WATERMARKED_DIR)) fs.mkdirSync(CONFIG.WATERMARKED_DIR, { recursive: true });

async function start() {
  const conn = await amqp.connect(CONFIG.RABBIT_URL);
  const ch = await conn.createChannel();
  await ch.assertQueue(CONFIG.QUEUE, { durable: true });

  console.log(`Watermark worker ${CONFIG.WORKER_ID} started`);

  function publishLog(log) {
    try {
      ch.publish("logs", "", Buffer.from(JSON.stringify(log)));
    } catch (err) {
      console.error("Log publish failed:", err);
    }
  }

  ch.consume(CONFIG.QUEUE, async (msg) => {
    if (!msg) return;
    let job;
    try {
      job = JSON.parse(msg.content.toString());
    } catch (e) {
      ch.ack(msg);
      return;
    }

    const { jobId, processedPath, processedFilename, watermarkedFilename, originalName, retries = 0 } = job;
    const outPath = path.join(CONFIG.WATERMARKED_DIR, watermarkedFilename);

    try {
      const img = sharp(processedPath);
      const meta = await img.metadata();
      const w = meta.width || 800;
      const h = meta.height || 600;
      const text = "@watermark";

      // SVG tiled watermark
      let svgParts = [`<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}">`];
      const stepX = 200;
      const stepY = 120;
      for (let y = 40; y < h; y += stepY) {
        for (let x = 0; x < w + stepX; x += stepX) {
          svgParts.push(`<text x="${x}" y="${y}" font-size="36" fill="white" opacity="0.3" font-family="Arial">${text}</text>`);
        }
      }
      svgParts.push("</svg>");
      const svgBuffer = Buffer.from(svgParts.join(""));

      // Preserve original format + quality
      const ext = path.extname(outPath).toLowerCase();
      let pipeline = img.composite([{ input: svgBuffer, gravity: "northwest" }]);

      if (ext === ".png") pipeline = pipeline.png({ compressionLevel: 9 });
      else if (ext === ".jpg" || ext === ".jpeg") pipeline = pipeline.jpeg({ quality: 90 });
      else if (ext === ".webp") pipeline = pipeline.webp({ quality: 90 });

      await pipeline.toFile(outPath);

      // Success log
      publishLog({
        jobId,
        filename: watermarkedFilename,
        originalName: originalName || processedFilename,
        status: "success",
        worker: CONFIG.WORKER_ID,
        timestamp: new Date().toISOString(),
      });

      // Watermarked log
      publishLog({
        jobId,
        filename: watermarkedFilename,
        originalName: originalName || processedFilename,
        status: "watermarked",
        worker: CONFIG.WORKER_ID,
        timestamp: new Date().toISOString(),
      });

      console.log(`Watermarked: ${watermarkedFilename}`);
      ch.ack(msg);
    } catch (err) {
      console.error("Watermark failed:", err);
      ch.ack(msg);
    }
  }, { noAck: false });
}

start().catch(console.error);
