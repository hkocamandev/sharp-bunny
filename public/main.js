const socket = io();

const logsDiv = document.getElementById("logs");
const gallery = document.getElementById("gallery");
const statusSpan = document.getElementById("status");
const fileInput = document.getElementById("images");
const fileText = document.getElementById("fileText");
const watermarkBtn = document.getElementById("watermarkBtn"); // ekle

// Job tracker
const jobsMap = {}; // jobId -> log

// Update status element
function updateStatus() {
    const allJobs = Object.values(jobsMap);
    const queuedCount = allJobs.filter(j => j.status === "queued").length;
    const finishedCount = allJobs.filter(j => ["success","watermarked"].includes(j.status)).length;

    const allJobsFinished = queuedCount === 0 && finishedCount > 0;

    if (allJobsFinished) {
        statusSpan.textContent = "Done";
        // opsiyonel: fade out
        setTimeout(() => statusSpan.textContent = "", 3000);
    } else {
        statusSpan.textContent = queuedCount > 0 ? `${queuedCount} file(s) queued` : "";
    }
}

// Socket Logs
socket.on("job_log", (log) => {
    jobsMap[log.jobId] = log;

    // log div update
    const el = document.createElement("div");
    el.className = `log-item ${log.status || ""}`;
    el.innerHTML = `
        <b>${log.filename}</b> — <span>${(log.status || "").toUpperCase()}</span>
        ${log.retries ? ` - retries: ${log.retries}` : ""}
        ${log.worker ? ` - worker: ${log.worker}` : ""}
        ${log.duration ? ` - ${log.duration}s` : ""}
        ${log.error ? ` - error: ${log.error}` : ""}
        <div class="text-gray-500 text-xs">${new Date(log.timestamp).toLocaleString()}</div>
    `;
    logsDiv.prepend(el);

    // galeriyi güncelle
    if (["success","watermarked"].includes(log.status)) loadGallery();

    updateStatus();
});

// Load Gallery
async function loadGallery() {
    const res = await fetch("/processed");
    const imgs = await res.json();

    const imageFiles = imgs.filter((i) =>
        /\.(png|jpg|jpeg|gif|webp)$/i.test(i.filename || i.originalName || "")
    );

    gallery.innerHTML = imageFiles
        .map(
            (item) => `
            <div class="text-center">
                <img src="${item.url}" class="w-full rounded-lg shadow-sm" />
                <div class="text-xs mt-1 text-gray-600">
                    ${item.originalName || item.filename}
                </div>
            </div>
        `
        )
        .join("");
}

// Load Initial Logs
async function loadJobsOnce() {
    const res = await fetch("/jobs");
    const jobs = await res.json();
    jobs.slice(0, 15).forEach((j) => {
        jobsMap[j.id] = j;
        const el = document.createElement("div");
        el.className = `log-item ${j.status || ""}`;
        el.innerHTML = `
            <b>${j.filename}</b> — ${(j.status || "").toUpperCase()}
            <div class="text-gray-500 text-xs">${new Date(j.lastUpdated).toLocaleString()}</div>
        `;
        logsDiv.prepend(el);
    });
    updateStatus();
}

// File select
document.querySelector("button[type='button']").addEventListener("click", () => fileInput.click());
fileInput.addEventListener("change", () => {
    fileText.textContent = fileInput.files.length
        ? `${fileInput.files.length} file(s) selected`
        : "No file selected";
});

// Upload
document.getElementById("uploadForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const files = fileInput.files;
    if (!files.length) return alert("Select files");

    const fd = new FormData();
    [...files].forEach((f) => {
        // Duplicate check
        const exists = Object.values(jobsMap).find(j => j.originalName === f.name);
        if (exists) {
            alert(`${f.name} already exists, skipping`);
        } else {
            fd.append("images", f);
        }
    });

    if (!fd.has("images")) return;

    statusSpan.textContent = `${fd.getAll("images").length} file(s) queued for resizing...`;

    try {
        const res = await fetch("/upload", { method: "POST", body: fd });
        const data = await res.json();
        if (!res.ok) statusSpan.textContent = data.error || "Resize failed";
    } catch (err) {
        statusSpan.textContent = "Resize failed: " + err.message;
    }

    setTimeout(() => (statusSpan.textContent = ""), 2500);
    loadGallery();
});

// Watermark
watermarkBtn.addEventListener("click", async () => {
    try {
        statusSpan.textContent = "Sending watermark request...";
        const res = await fetch("/add-watermark", { method: "POST" });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Error");
        statusSpan.textContent = `${data.enqueued} file(s) queued for watermark.`;
        setTimeout(loadGallery, 800);
    } catch (err) {
        statusSpan.textContent = "Error sending watermark request.";
        console.error(err);
    }
});

// Clear logs
document.getElementById("clearLogsBtn").addEventListener("click", async () => {
    logsDiv.innerHTML = "";
    await fetch("/clear-jobs", { method: "POST" });
    location.reload();
});

// Clear gallery
document.getElementById("clearGalleryBtn").addEventListener("click", async () => {
    gallery.innerHTML = "";
    await fetch("/clear-processed", { method: "POST" });
    statusSpan.textContent = "";
});

// Init
loadGallery();
loadJobsOnce();
