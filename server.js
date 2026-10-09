const http = require("http");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is not configured");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
  connectionTimeoutMillis: 15000,
  idleTimeoutMillis: 30000
});

const users = new Map();
const rooms = new Map();

/* =========================
   HTTP HEALTH CHECK
========================= */

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8"
  });

  res.end("Kallimni public server is running");
});

/* =========================
   WEBSOCKET SERVER
========================= */

const wss = new WebSocketServer({
  server,
  maxPayload: 64 * 1024
});

/* =========================
   GENERAL HELPERS
========================= */

function send(socket, data) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(data));
  }
}

function validId(value) {
  return (
    typeof value === "string" &&
    value.length >= 1 &&
    value.length <= 128 &&
    /^[a-zA-Z0-9_-]+$/.test(value)
  );
}

function removeClient(socket) {
  const userId = socket.userId;

  if (userId && users.has(userId)) {
    const sockets = users.get(userId);

    sockets.delete(socket);

    if (sockets.size === 0) {
      users.delete(userId);
    }

    console.log("Client removed from user:", userId);
  }

  for (const [roomId, members] of rooms.entries()) {
    members.delete(socket);

    if (members.size === 0) {
      rooms.delete(roomId);
    }
  }
}

function sendToUser(userId, packet) {
  const sockets = users.get(userId);

  if (!sockets || sockets.size === 0) {
    return false;
  }

  let delivered = false;

  for (const socket of sockets) {
    if (socket.readyState === WebSocket.OPEN) {
      send(socket, packet);
      delivered = true;
    }
  }

  return delivered;
}

/* =========================
   DATABASE INITIALIZATION
========================= */

async function initializeDatabase() {
  console.log("Initializing PostgreSQL...");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pending_messages (
      message_id UUID PRIMARY KEY,
      sender_id TEXT NOT NULL,
      target_id TEXT NOT NULL,
      payload TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS
    idx_pending_messages_target_created
    ON pending_messages (target_id, created_at)
  `);

  console.log("PostgreSQL initialized");
}

/* =========================
   PENDING MESSAGE DELIVERY
========================= */

async function deliverPending(socket, userId) {
  try {
    const result = await pool.query(
      `
      SELECT message_id, sender_id, target_id, payload, created_at
      FROM pending_messages
      WHERE target_id = $1
      ORDER BY created_at ASC
      LIMIT 500
      `,
      [userId]
    );

    console.log(
      "Pending messages found:",
      result.rows.length,
      "for user:",
      userId
    );

    for (const row of result.rows) {
      if (socket.readyState !== WebSocket.OPEN) {
        console.log(
          "Socket closed during pending delivery:",
          userId
        );
        return;
      }

      send(socket, {
        type: "packet",
        messageId: row.message_id,
        senderId: row.sender_id,
        targetId: row.target_id,
        data: row.payload,
        createdAt: row.created_at
      });
    }
  } catch (error) {
    console.error(
      "Pending message delivery error:",
      error.message
    );
  }
}

/* =========================
   WEBSOCKET CONNECTION
========================= */

wss.on("connection", (socket, request) => {
  socket.userId = null;

  console.log("================================");
  console.log("WebSocket client connected");
  console.log(
    "Client address:",
    request.socket.remoteAddress || "unknown"
  );
  console.log("================================");

  send(socket, {
    type: "connected",
    message: "اتصلت بخادم كلمني"
  });

  /* =========================
     INCOMING MESSAGES
  ========================= */

  socket.on("message", async (raw) => {
    try {
      const text = raw.toString();

      console.log(
        "WebSocket message received:",
        text.length,
        "bytes"
      );

      let data;

      try {
        data = JSON.parse(text);
      } catch (error) {
        console.error(
          "Invalid JSON received:",
          error.message
        );

        send(socket, {
          type: "error",
          message: "Invalid JSON"
        });

        return;
      }

      if (!data || typeof data.type !== "string") {
        send(socket, {
          type: "error",
          message: "Invalid message type"
        });

        return;
      }

      /* =========================
         REGISTER USER
      ========================= */

      if (data.type === "register") {
        const userId = data.userId;

        console.log(
          "Register request received:",
          userId
        );

        if (!validId(userId)) {
          console.error(
            "Registration rejected: invalid userId"
          );

          send(socket, {
            type: "error",
            message: "Invalid userId"
          });

          return;
        }

        /* Remove previous association for this socket. */

        removeClient(socket);

        socket.userId = userId;

        if (!users.has(userId)) {
          users.set(userId, new Set());
        }

        users.get(userId).add(socket);

        send(socket, {
          type: "registered",
          userId: userId,
          message: "تم تسجيل المستخدم بنجاح"
        });

        console.log(
          "User registered successfully:",
          userId
        );

        console.log(
          "Active connections for user:",
          users.get(userId).size
        );

        await deliverPending(socket, userId);

        return;
      }

      /* =========================
         REQUIRE REGISTRATION
      ========================= */

      if (!socket.userId) {
        console.warn(
          "Message rejected: user not registered"
        );

        send(socket, {
          type: "error",
          message: "User not registered"
        });

        return;
      }

      /* =========================
         MESSAGE ACKNOWLEDGEMENT
      ========================= */

      if (data.type === "ack") {
        const messageId = data.messageId;

        if (
          typeof messageId !== "string" ||
          !/^[0-9a-fA-F-]{36}$/.test(messageId)
        ) {
          send(socket, {
            type: "error",
            message: "Invalid messageId"
          });

          return;
        }

        const result = await pool.query(
          `
          DELETE FROM pending_messages
          WHERE message_id = $1
            AND target_id = $2
          `,
          [messageId, socket.userId]
        );

        send(socket, {
          type: "acknowledged",
          messageId: messageId
        });

        console.log(
          "Message acknowledged:",
          messageId,
          "deleted:",
          result.rowCount
        );

        return;
      }

      /* =========================
         SEND PRIVATE MESSAGE
      ========================= */

      if (data.type === "send") {
        const targetId = data.targetId;
        const payload = data.data;

        console.log(
          "Send request:",
          socket.userId,
          "->",
          targetId
        );

        if (!validId(targetId)) {
          send(socket, {
            type: "error",
            message: "Invalid targetId"
          });

          return;
        }

        if (targetId === socket.userId) {
          send(socket, {
            type: "error",
            message: "Cannot send message to yourself"
          });

          return;
        }

        if (
          typeof payload !== "string" ||
          Buffer.byteLength(payload, "utf8") > 60000
        ) {
          send(socket, {
            type: "error",
            message: "Invalid or oversized payload"
          });

          return;
        }

        const messageId = crypto.randomUUID();

        await pool.query(
          `
          INSERT INTO pending_messages (
            message_id,
            sender_id,
            target_id,
            payload
          )
          VALUES ($1, $2, $3, $4)
          `,
          [
            messageId,
            socket.userId,
            targetId,
            payload
          ]
        );

        console.log(
          "Message saved:",
          messageId,
          "target:",
          targetId
        );

        const packet = {
          type: "packet",
          messageId: messageId,
          senderId: socket.userId,
          targetId: targetId,
          data: payload,
          createdAt: new Date().toISOString()
        };

        const delivered = sendToUser(
          targetId,
          packet
        );

        send(socket, {
          type: "sent",
          messageId: messageId,
          targetId: targetId,
          delivered: delivered
        });

        console.log(
          "Message processing complete:",
          messageId,
          "delivered live:",
          delivered
        );

        return;
      }

      /* =========================
         LEGACY ROOM JOIN
      ========================= */

      if (data.type === "join") {
        const roomId = data.roomId;

        if (!validId(roomId)) {
          send(socket, {
            type: "error",
            message: "Invalid roomId"
          });

          return;
        }

        if (!rooms.has(roomId)) {
          rooms.set(roomId, new Set());
        }

        rooms.get(roomId).add(socket);

        send(socket, {
          type: "joined",
          roomId: roomId
        });

        console.log(
          "User joined room:",
          socket.userId,
          roomId
        );

        return;
      }

      /* =========================
         LEGACY ROOM MESSAGE
      ========================= */

      if (data.type === "message") {
        const roomId = data.roomId;
        const payload = data.data;

        if (!validId(roomId)) {
          send(socket, {
            type: "error",
            message: "Invalid roomId"
          });

          return;
        }

        if (
          typeof payload !== "string" ||
          Buffer.byteLength(payload, "utf8") > 60000
        ) {
          send(socket, {
            type: "error",
            message: "Invalid or oversized payload"
          });

          return;
        }

        const members = rooms.get(roomId);

        if (!members) {
          send(socket, {
            type: "error",
            message: "Room not found"
          });

          return;
        }

        const packet = {
          type: "message",
          roomId: roomId,
          senderId: socket.userId,
          data: payload,
          createdAt: new Date().toISOString()
        };

        for (const member of members) {
          if (
            member !== socket &&
            member.readyState === WebSocket.OPEN
          ) {
            send(member, packet);
          }
        }

        console.log(
          "Room message sent:",
          roomId,
          "from:",
          socket.userId
        );

        return;
      }

      /* =========================
         UNKNOWN MESSAGE TYPE
      ========================= */

      console.warn(
        "Unknown message type:",
        data.type
      );

      send(socket, {
        type: "error",
        message: "Unknown message type"
      });

    } catch (error) {
      console.error(
        "Message handler error:",
        error.stack || error.message
      );

      send(socket, {
        type: "error",
        message: "Internal server error"
      });
    }
  });

  /* =========================
     SOCKET CLOSE
  ========================= */

  socket.on("close", (code, reason) => {
    console.log(
      "WebSocket client disconnected:",
      "code:",
      code,
      "reason:",
      reason ? reason.toString() : ""
    );

    removeClient(socket);
  });

  /* =========================
     SOCKET ERROR
  ========================= */

  socket.on("error", (error) => {
    console.error(
      "WebSocket client error:",
      error.message
    );

    removeClient(socket);
  });
});

/* =========================
   WEBSOCKET SERVER ERRORS
========================= */

wss.on("error", (error) => {
  console.error(
    "WebSocket server error:",
    error.stack || error.message
  );
});

/* =========================
   START SERVER
========================= */

async function start() {
  try {
    await initializeDatabase();

    server.listen(PORT, "0.0.0.0", () => {
      console.log(
        `Kallimni server running on port ${PORT}`
      );
    });

  } catch (error) {
    console.error(
      "Server startup failed:",
      error.stack || error.message
    );

    process.exit(1);
  }
}

/* =========================
   GRACEFUL SHUTDOWN
========================= */

async function shutdown(signal) {
  console.log(
    "Received shutdown signal:",
    signal
  );

  for (const socket of wss.clients) {
    socket.close(1001, "Server shutting down");
  }

  server.close(async () => {
    try {
      await pool.end();

      console.log(
        "PostgreSQL connection pool closed"
      );
    } catch (error) {
      console.error(
        "Database shutdown error:",
        error.message
      );
    }

    process.exit(0);
  });

  setTimeout(() => {
    console.error(
      "Shutdown timed out; forcing exit"
    );

    process.exit(1);
  }, 10000).unref();
}

process.on("SIGTERM", () => {
  shutdown("SIGTERM");
});

process.on("SIGINT", () => {
  shutdown("SIGINT");
});

/* =========================
   START
========================= */

start();