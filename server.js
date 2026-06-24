/**
 * RISYNC Studio — Backend Server
 * Render.com deployment
 *
 * Handles:
 *  - WebRTC signalling (WebSocket)
 *  - Session / room management
 *  - Participant join/leave
 *  - Job queue for Colab worker
 *  - MongoDB session history
 */

require('dotenv').config();
const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const cors       = require('cors');
const multer     = require('multer');
const { v4: uuid } = require('uuid');
const { MongoClient } = require('mongodb');
const path       = require('path');
const fs         = require('fs');

// ── CONFIG ────────────────────────────────────────────────────
const PORT       = process.env.PORT || 3000;
const MONGO_URI  = process.env.MONGO_URI || '';
const FRONTEND   = process.env.FRONTEND_URL || '*';
const UPLOAD_DIR = process.env.UPLOAD_DIR || '/tmp/risync-uploads';

if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ── EXPRESS ───────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

app.use(cors({ origin: FRONTEND, credentials: true }));
app.use(express.json());
app.use('/uploads', express.static(UPLOAD_DIR));

const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 500 * 1024 * 1024 }, // 500MB
});

// ── MONGODB ───────────────────────────────────────────────────
let db = null;
async function connectDB() {
  if (!MONGO_URI) { console.log('No MONGO_URI — running without DB'); return; }
  try {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db('risync');
    console.log('MongoDB connected');
  } catch (e) {
    console.error('MongoDB error:', e.message);
  }
}

// ── IN-MEMORY STORES (fallback when no DB) ───────────────────
const rooms = new Map();   // roomId → Room
const jobs  = new Map();   // jobId  → Job

// ── ROOM STRUCTURE ────────────────────────────────────────────
function createRoom(hostId, hostName, sessionName, roomCode) {
  const room = {
    id:          roomCode || uuid().substring(0, 8).toUpperCase(),
    sessionName: sessionName || 'Untitled Session',
    hostId,
    hostName,
    createdAt:   new Date(),
    participants: new Map(), // peerId → Participant
    jobs:         [],
  };
  rooms.set(room.id, room);
  return room;
}

function getRoomSummary(room) {
  return {
    id:          room.id,
    sessionName: room.sessionName,
    hostName:    room.hostName,
    createdAt:   room.createdAt,
    participants: [...room.participants.values()].map(p => ({
      peerId: p.peerId,
      name:   p.name,
      role:   p.role,
      isHost: p.isHost,
      joinedAt: p.joinedAt,
    })),
  };
}

// ── WEBSOCKET SERVER ──────────────────────────────────────────
const wss = new WebSocket.Server({ server });

// Map: ws → { peerId, roomId, name, role }
const clients = new Map();

function broadcast(roomId, message, excludePeerId = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const payload = JSON.stringify(message);
  room.participants.forEach((participant, peerId) => {
    if (peerId === excludePeerId) return;
    const ws = participant.ws;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  });
}

function sendTo(roomId, targetPeerId, message) {
  const room = rooms.get(roomId);
  if (!room) return;
  const participant = room.participants.get(targetPeerId);
  if (participant?.ws?.readyState === WebSocket.OPEN) {
    participant.ws.send(JSON.stringify(message));
  }
}

wss.on('connection', (ws) => {
  const peerId = uuid();
  clients.set(ws, { peerId, roomId: null });

  ws.send(JSON.stringify({ type: 'connected', peerId }));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const client = clients.get(ws);

    switch (msg.type) {

      // ── Create a new session room ───────────────────────────
      case 'create-room': {
        // Accept roomCode from client or generate one
        const room = createRoom(client.peerId, msg.name || 'Host', msg.sessionName, msg.roomCode);
        client.roomId = room.id;
        room.participants.set(client.peerId, {
          peerId:   client.peerId,
          name:     msg.name || 'Host',
          role:     msg.role || 'host',
          isHost:   true,
          joinedAt: new Date(),
          ws,
        });
        ws.send(JSON.stringify({
          type:     'room-created',
          roomId:   room.id,
          roomCode: room.id,  // send both for compatibility
          peerId:   client.peerId,
          room:     getRoomSummary(room),
        }));
        console.log(`Room created: ${room.id} by ${msg.name}`);
        break;
      }

      // ── Join an existing room ───────────────────────────────
      case 'join-room': {
        // Accept both roomId and roomCode
        const roomKey = msg.roomId || msg.roomCode;
        const room = rooms.get(roomKey);
        if (!room) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room not found: ' + roomKey }));
          return;
        }
        client.roomId = msg.roomId;
        room.participants.set(client.peerId, {
          peerId:   client.peerId,
          name:     msg.name || 'Musician',
          role:     msg.role || 'musician',
          isHost:   false,
          joinedAt: new Date(),
          ws,
        });
        // Tell the joiner about the room and existing peers
        ws.send(JSON.stringify({
          type:   'room-joined',
          roomId: room.id,
          peerId: client.peerId,
          room:   getRoomSummary(room),
        }));
        // Tell all existing peers about the new joiner
        broadcast(room.id, {
          type:     'peer-joined',
          peerId:   client.peerId,
          name:     msg.name || 'Musician',
          role:     msg.role || 'musician',
        }, client.peerId);
        console.log(`${msg.name} joined room ${room.id}`);
        break;
      }

      // ── WebRTC signalling: offer ────────────────────────────
      case 'offer': {
        sendTo(client.roomId, msg.targetPeerId, {
          type:       'offer',
          fromPeerId: client.peerId,
          sdp:        msg.sdp,
        });
        break;
      }

      // ── WebRTC signalling: answer ───────────────────────────
      case 'answer': {
        sendTo(client.roomId, msg.targetPeerId, {
          type:       'answer',
          fromPeerId: client.peerId,
          sdp:        msg.sdp,
        });
        break;
      }

      // ── WebRTC signalling: ICE candidate ───────────────────
      case 'ice-candidate': {
        sendTo(client.roomId, msg.targetPeerId, {
          type:       'ice-candidate',
          fromPeerId: client.peerId,
          candidate:  msg.candidate,
        });
        break;
      }

      // ── Participant metadata update ─────────────────────────
      case 'update-meta': {
        const room = rooms.get(client.roomId);
        if (!room) return;
        const p = room.participants.get(client.peerId);
        if (p) {
          if (msg.name)  p.name  = msg.name;
          if (msg.role)  p.role  = msg.role;
          if (msg.label) p.label = msg.label;
        }
        broadcast(client.roomId, {
          type:     'peer-meta',
          peerId:   client.peerId,
          name:     msg.name,
          role:     msg.role,
          label:    msg.label,
        }, client.peerId);
        break;
      }

      // ── Chat / session message ──────────────────────────────
      case 'chat': {
        broadcast(client.roomId, {
          type:      'chat',
          fromPeerId: client.peerId,
          fromName:   clients.get(ws)?.name || 'Unknown',
          text:       msg.text,
          ts:         Date.now(),
        });
        break;
      }

      // ── Host control: mute a participant ───────────────────
      case 'host-mute': {
        sendTo(client.roomId, msg.targetPeerId, {
          type:   'muted-by-host',
          muted:  msg.muted,
        });
        break;
      }

      // ── Ping / keepalive ────────────────────────────────────
      case 'flip-camera': {
        sendTo(client.roomId, msg.targetPeerId, {
          type:       'flip-camera',
          fromPeerId: client.peerId,
        });
        break;
      }

      case 'ping': {
        ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
        break;
      }
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (client?.roomId) {
      const room = rooms.get(client.roomId);
      if (room) {
        room.participants.delete(client.peerId);
        broadcast(client.roomId, {
          type:   'peer-left',
          peerId: client.peerId,
        });
        // Clean up empty rooms after 10 mins
        if (room.participants.size === 0) {
          setTimeout(() => {
            if (rooms.get(client.roomId)?.participants.size === 0) {
              rooms.delete(client.roomId);
              console.log(`Room ${client.roomId} cleaned up`);
            }
          }, 10 * 60 * 1000);
        }
      }
    }
    clients.delete(ws);
  });
});

// ── REST API ──────────────────────────────────────────────────

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    rooms:  rooms.size,
    clients: clients.size,
    uptime: process.uptime(),
  });
});

// Get room info (for joining via link)
app.get('/api/rooms/:roomId', (req, res) => {
  const room = rooms.get(req.params.roomId.toUpperCase());
  if (!room) return res.status(404).json({ error: 'Room not found' });
  res.json(getRoomSummary(room));
});

// ── JOB QUEUE (Colab worker) ──────────────────────────────────

// Submit a new video generation job
app.post('/api/jobs', upload.fields([
  { name: 'audio', maxCount: 1 },
  { name: 'video', maxCount: 1 },
]), (req, res) => {
  const jobId = uuid();
  let payload = {};
  try { payload = JSON.parse(req.body.payload || '{}'); } catch {}

  const job = {
    jobId,
    status:      'queued',
    progress:    0,
    statusLabel: 'Queued — waiting for Colab worker',
    createdAt:   new Date(),
    payload,
    audioPath:   req.files?.audio?.[0]?.path || null,
    videoPath:   req.files?.video?.[0]?.path || null,
    videoUrl:    null,
    log:         [],
    error:       null,
  };
  jobs.set(jobId, job);

  if (db) {
    db.collection('jobs').insertOne({ ...job, audioPath: null, videoPath: null })
      .catch(e => console.error('DB insert error:', e.message));
  }

  console.log(`Job created: ${jobId}`);
  res.json({ jobId, status: 'queued' });
});

// Get job status (frontend polls this)
app.get('/api/jobs/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json({
    jobId:       job.jobId,
    status:      job.status,
    progress:    job.progress,
    statusLabel: job.statusLabel,
    videoUrl:    job.videoUrl,
    log:         job.log[job.log.length - 1] || null,
    error:       job.error,
  });
});

// Colab worker polls this for queued jobs
app.get('/api/jobs/next/pending', (req, res) => {
  const secret = req.headers['x-worker-secret'];
  if (process.env.WORKER_SECRET && secret !== process.env.WORKER_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }
  const job = [...jobs.values()].find(j => j.status === 'queued');
  if (!job) return res.json({ job: null });

  job.status      = 'processing';
  job.statusLabel = 'Colab worker picked up job';
  job.progress    = 5;

  res.json({
    job: {
      jobId:     job.jobId,
      payload:   job.payload,
      audioUrl:  job.audioPath ? `/uploads/${path.basename(job.audioPath)}` : null,
      videoUrl:  job.videoPath ? `/uploads/${path.basename(job.videoPath)}` : null,
    }
  });
});

// Colab worker updates job progress
app.patch('/api/jobs/:jobId', (req, res) => {
  const secret = req.headers['x-worker-secret'];
  if (process.env.WORKER_SECRET && secret !== process.env.WORKER_SECRET) {
    return res.status(401).json({ error: 'Unauthorised' });
  }
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found' });

  const { status, progress, statusLabel, videoUrl, log, error } = req.body;
  if (status)      job.status      = status;
  if (progress)    job.progress    = progress;
  if (statusLabel) job.statusLabel = statusLabel;
  if (videoUrl)    job.videoUrl    = videoUrl;
  if (log)         job.log.push(log);
  if (error)       job.error       = error;

  if (db) {
    db.collection('jobs').updateOne(
      { jobId: job.jobId },
      { $set: { status, progress, statusLabel, videoUrl, error } }
    ).catch(e => console.error('DB update error:', e.message));
  }

  res.json({ ok: true });
});

// ── START ─────────────────────────────────────────────────────
connectDB().then(() => {
  server.listen(PORT, () => {
    console.log(`RISYNC backend running on port ${PORT}`);
    console.log(`WebSocket signalling ready`);
    console.log(`REST API ready`);
  });
});
