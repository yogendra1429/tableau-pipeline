const express = require('express');
const { spawn } = require('child_process');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const os = require('os');
require('dotenv').config();

const app = express();
app.use(express.json());

const jobStatus = {};
const queue = [];
let isProcessing = false;

async function processQueue() {
    if (isProcessing || queue.length === 0) return;
    isProcessing = true;

    const { jobId, csvUrl } = queue.shift();
    console.log(`[Queue] Starting Job: ${jobId}`);
    jobStatus[jobId].status = "RUNNING";

    const isWindows = os.platform() === 'win32';
    const pythonPath = process.env.PYTHON_PATH || (isWindows ? 'python' : 'python3');

    const py = spawn(
        isWindows ? `"${pythonPath}"` : pythonPath,
        ['pipeline.py', jobId, csvUrl],
        { shell: isWindows, env: process.env }
    );

    py.stdout.on('data', d => console.log(`[${jobId}] ${d}`));
    py.stderr.on('data', d => {
        console.error(`[${jobId}] ERROR: ${d}`);
        jobStatus[jobId].error = d.toString();
    });

    py.on('close', (code) => {
        jobStatus[jobId].status = code === 0 ? "SUCCESS" : "FAILED";
        jobStatus[jobId].end_time = new Date().toISOString();
        console.log(`[Queue] Finished Job: ${jobId} with code ${code}`);
        
        isProcessing = false;
        processQueue(); 
    });
}

app.post('/ingest', (req, res) => {
    const { csvUrl } = req.body;
    if (!csvUrl) return res.status(400).json({ error: "Missing csvUrl" });

    const urls = Array.isArray(csvUrl) ? csvUrl : [csvUrl];
    const newJobIds = [];

    urls.forEach(url => {
        const jobId = uuidv4().slice(0, 8);
        jobStatus[jobId] = {
            status: "QUEUED",
            csvUrl: url,
            start_time: new Date().toISOString()
        };
        queue.push({ jobId, csvUrl: url });
        newJobIds.push(jobId);
    });

    processQueue();
    res.status(202).json({ jobs_queued: newJobIds.length, job_ids: newJobIds });
});

app.get('/status/:jobId', (req, res) => {
    res.json(jobStatus[req.params.jobId] || { status: "NOT_FOUND" });
});

app.listen(5000, () => console.log("Production Pipeline running on port 5000"));
