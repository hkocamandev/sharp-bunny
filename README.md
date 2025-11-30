# Image Processing Pipeline with RabbitMQ  
A scalable, fault-tolerant image processing system built with **Node.js**, **RabbitMQ**, and **Sharp**.  
The project supports **asynchronous resizing**, **automatic retries**, **dead-letter handling**, and **watermark generation**.  
It includes a real-time dashboard for monitoring logs, job status, and the processed image gallery.

---

## 🚀 Features

### ✔️ 1. File Upload & Job Queueing
- Users upload one or more image files through the web dashboard.
- Each file is registered as a **job** and published to the RabbitMQ *processing queue*.
- Duplicate filename protection is handled at the frontend.

### ✔️ 2. Image Resizing Worker
- Listens to the main processing queue.
- Uses **Sharp** to resize images (max width 800px, JPEG 90%).
- Saves processed files into the `/processed` directory.
- Logs detailed events:
  - success  
  - retries  
  - simulated failures  
  - final dead-letter state  
- Updates `jobs.json` for persistent job history.

### ✔️ 3. Retry & Dead-Letter Queue (DLQ)
- Each failed job is retried automatically after 3 seconds.
- After the maximum retry limit is reached, the job is moved to a **dead-letter queue**.
- DLQ events are logged separately.

### ✔️ 4. Watermark Worker
- Listens to the `watermark_jobs` queue.
- Creates tiled SVG watermarks (`@watermark`) and composites them on the processed images.
- Preserves original output format (PNG/JPEG/WebP).
- Saves results to `/watermarked`.

### ✔️ 5. Real-Time Monitoring Dashboard  
Built using Tailwind CSS + Socket.IO:

#### 🖼️ Gallery  
Displays processed and watermarked images dynamically.

#### 📡 Live Logs  
Shows:
- status updates  
- worker ID  
- retry count  
- timestamps  
- errors  

#### ⏳ Job Status Auto–Tracking  
Indicates:
- queued  
- in-progress  
- completed  

#### 🔧 Controls  
- Upload images  
- Queue watermark jobs  
- Clear logs  
- Clear processed gallery  

---

## 🛠️ Architecture Overview
```
        ┌────────────┐
        │  Frontend  │
        │  Upload UI │
        └─────┬──────┘
              │ POST /upload
              ▼
    ┌────────────────────────┐
    │      Producer API      │
    │ Publishes resize jobs  │
    └───────┬────────────────┘
            │
            ▼
    ┌────────────────────────┐
    │ RabbitMQ Queues        │
    │ - resize_jobs          │
    │ - retry_jobs           │
    │ - dead_jobs            │
    │ - watermark_jobs       │
    └────────┬───────────────┘
             │
┌────────────┴─────────────┐
│                          │
▼                          ▼
Resize Worker     Watermark Worker
(processed/)     (watermarked/)


```

## 📂 Project Structure
```
.
├── producer/
├── worker/
├── worker-watermark/
├── processed/
├── watermarked/
├── jobs.json
├── worker-logs.json
├── public/
│ ├── index.html
│ └── main.js
├── docker-compose.yml
└── README.md

```


## 🐳 Running with Docker

### 1. Start RabbitMQ
```
docker-compose up -d
RabbitMQ Management UI:

http://localhost:15672
User: guest  
Pass: guest
```
▶️ Running the System
```
1. Start Producer / API
node producer.js
2. Start Resize Worker
WORKER_ID=1 node worker.js
WORKER_ID=2 node worker.j
3. Start Watermark Worker
node worker-watermark.js
```
🌐 Access Dashboard
```
Open:
http://localhost:3000
Dashboard includes:
Upload area
Watermarker button
clearing gallery and logs buttons
Gallery
Real-time logs
Retry/dead-letter status
```
📜 Logging
```
jobs.json
Stores the following fields per job:
filename

status

retries

timestamps

worker-logs.json
Stores:

worker activity

errors

final job states
```
🔄 Retry Logic Details
```
Condition	Behavior
Worker throws an error	Retry after 3 seconds
Retry count < MAX_RETRIES	Requeue to retry_jobs
Retry count ≥ MAX_RETRIES	Move to dead_jobs
Filename contains “fail”	Simulated first-attempt failure
```
🏷️ Watermark Logic
```
Generates tiled SVG watermark using text: @watermark

Applies watermark to the processed image

Maintains output format (JPEG, PNG, WebP)

Saves output to /watermarked

Logs watermarked status
```
📦 Environment Variables
```
Variable	Description	Default
RABBITMQ_USER	RabbitMQ username	guest
RABBITMQ_PASS	RabbitMQ password	guest
MAX_UPLOAD_COUNT	Max files per upload	50
```
✨ Future Improvements
```
Add GPU-based image processing

Add distributed multi-worker scaling

Add user authentication for the dashboard

Add queue metrics dashboard

Add EXIF metadata extraction
```
📄 License
MIT License © 2025



