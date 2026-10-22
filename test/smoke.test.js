// Smoke tests for the libraries the producer and workers depend on.
//
// They drive sharp and multer the same way producer.js, worker.js and
// worker-watermark.js do, without needing RabbitMQ, so a dependency upgrade
// that changes either API fails here instead of in a running worker.
const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const sharp = require("sharp");
const multer = require("multer");
const express = require("express");
const { v4: uuidv4 } = require("uuid");

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "sharp-bunny-"));
}

async function sourceImage(dir) {
  const file = path.join(dir, "src.png");
  await sharp({ create: { width: 1200, height: 700, channels: 3, background: "#336699" } })
    .png()
    .toFile(file);
  return file;
}

test("worker.js: resize to 800px wide JPEG", async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const out = path.join(dir, "processed.jpg");

  await sharp(await sourceImage(dir))
    .resize({ width: 800, withoutEnlargement: true })
    .jpeg({ quality: 90 })
    .toFile(out);

  const meta = await sharp(out).metadata();
  assert.equal(meta.width, 800);
  assert.equal(meta.format, "jpeg");
});

test("worker-watermark.js: SVG watermark composite keeps each output format", async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const img = sharp(await sourceImage(dir));
  const { width, height } = await img.metadata();
  const svg = Buffer.from(
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">` +
      `<text x="0" y="40" font-size="36" fill="white" opacity="0.3" font-family="Arial">@watermark</text></svg>`
  );

  for (const [ext, format] of [[".png", "png"], [".jpg", "jpeg"], [".webp", "webp"]]) {
    let pipeline = img.clone().composite([{ input: svg, gravity: "northwest" }]);
    if (ext === ".png") pipeline = pipeline.png({ compressionLevel: 9 });
    else if (ext === ".jpg") pipeline = pipeline.jpeg({ quality: 90 });
    else pipeline = pipeline.webp({ quality: 90 });
    const out = path.join(dir, "wm" + ext);
    await pipeline.toFile(out);
    assert.equal((await sharp(out).metadata()).format, format);
  }
});

test("producer.js: multer accepts up to the limit and rejects more with a MulterError", async (t) => {
  const dir = tempDir();
  const src = fs.readFileSync(await sourceImage(dir));
  const app = express();
  const upload = multer({ dest: path.join(dir, "uploads") });
  app.post(
    "/upload",
    upload.array("images", 2),
    (err, req, res, next) => {
      if (err instanceof multer.MulterError) return res.status(400).json({ code: err.code });
      next(err);
    },
    (req, res) => res.json({ count: req.files.length, id: uuidv4() })
  );
  const server = app.listen(0);
  t.after(() => {
    server.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });
  const url = `http://127.0.0.1:${server.address().port}/upload`;
  const form = (n) => {
    const f = new FormData();
    for (let i = 0; i < n; i++) f.append("images", new Blob([src]), `img${i}.png`);
    return f;
  };

  const ok = await fetch(url, { method: "POST", body: form(2) });
  assert.equal(ok.status, 200);
  assert.equal((await ok.json()).count, 2);

  const tooMany = await fetch(url, { method: "POST", body: form(3) });
  assert.equal(tooMany.status, 400);
  assert.equal((await tooMany.json()).code, "LIMIT_UNEXPECTED_FILE");
});
