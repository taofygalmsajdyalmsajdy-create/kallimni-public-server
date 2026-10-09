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

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8"
  });

  res.end("Kallimni public server is running");
});

const wss = new WebSocketServer({
  server,
  maxPayload: 64 * 1024
});

function send(socket, data) {
  if (socket.readyState !== WebSocket.OPEN) {
    return false;
  }

  try {
    socket.send(JSON.stringify(data));
    return true;
  } catch (error) {
    console.error("WebSocket send error:", error.message);
    return false;
  }
}

function validId(value) {
  return typeof value === "string" &&
    value.length >= 1 &&
    value.length <= 128 &&
    /^[a-zA-Z0-9_-]+$/.test(value);
}

function removeClient(socket) {
  if (socket.userId && users.has(socket.userId)) {
    const set = users.get(socket.userId);
    set.delete(socket);

    if (set.size === 0) {
      users.delete(socket.userId);
    }
  }

  if (socket.room && rooms.has(socket.room)) {
    const set = rooms.get(socket.room);
    set.delete(socket);

    if (set.size === 0) {
      rooms.delete(socket.room);
    }
  }

  socket.userId = null;
  socket.room = null;
}

function sendToUser(userId, packet) {
  const recipients = users.get(userId);
  let sent = false;

  if (!recipients) {
    return false;
  }

  for (const recipient of recipients) {
    if (recipient.readyState === WebSocket.OPEN) {
      if (send(recipient, packet)) {
        sent = true;
      }
    }
  }

  return sent;
}

// إنشاء جدول الرسائل الدائمة.
async function initializeDatabase() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pending_messages (
      message_id UUID PRIMARY KEY,
      sender_id VARCHAR(128) NOT NULL,
      target_id VARCHAR(128) NOT NULL,
      packet_data TEXT NOT NULL,
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

// إرسال الرسائل المحفوظة إلى المستخدم عند اتصاله.
async function deliverPending(socket, userId) {
  const result = await pool.query(
    `SELECT message_id, sender_id, packet_data, created_at
     FROM pending_messages
     WHERE target_id = $1
     ORDER BY created_at ASC
     LIMIT 500`,
    [userId]
  );

  for (const row of result.rows) {
    if (socket.readyState !== WebSocket.OPEN) {
      break;
    }

    // تحقق أن الاتصال ما زال مسجلًا لهذا المستخدم.
    if (socket.userId !== userId) {
      break;
    }

    send(socket, {
      type: "packet",
      from: row.sender_id,
      data: row.packet_data,
      timestamp: new Date(row.created_at).getTime(),
      messageId: row.message_id,
      pending: true
    });
  }
}

wss.on("connection", (socket) => {
  socket.userId = null;
  socket.room = null;
  socket.username = "مستخدم";

  send(socket, {
    type: "connected",
    message: "اتصلت بخادم كلمني"
  });

  socket.on("message", async (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      // تسجيل المستخدم.
      if (data.type === "register") {
        const userId = String(data.userId || "").trim();

        if (!validId(userId)) {
          send(socket, {
            type: "error",
            message: "معرّف المستخدم غير صالح"
          });
          return;
        }

        removeClient(socket);
        socket.userId = userId;

        if (!users.has(userId)) {
          users.set(userId, new Set());
        }

        users.get(userId).add(socket);

        send(socket, {
          type: "registered",
          userId,
          message: "تم تسجيل الاتصال"
        });

        try {
          await deliverPending(socket, userId);
        } catch (error) {
          console.error("Pending delivery error:", error.message);

          send(socket, {
            type: "error",
            message: "تعذر جلب الرسائل المحفوظة"
          });
        }

        return;
      }

      // تأكيد استلام رسالة محفوظة.
      // لا تُحذف الرسالة إلا إذا كان المستلم هو صاحب الاتصال.
      if (data.type === "ack") {
        if (!socket.userId) {
          send(socket, {
            type: "error",
            message: "سجل المستخدم أولًا"
          });
          return;
        }

        const messageId = String(data.messageId || "");

        if (!/^[0-9a-fA-F-]{36}$/.test(messageId)) {
          send(socket, {
            type: "error",
            message: "معرّف الرسالة غير صالح"
          });
          return;
        }

        await pool.query(
          `DELETE FROM pending_messages
           WHERE message_id = $1 AND target_id = $2`,
          [messageId, socket.userId]
        );

        send(socket, {
          type: "acknowledged",
          messageId
        });

        return;
      }

      // إرسال رسالة إلى مستخدم.
      if (data.type === "send") {
        if (!socket.userId) {
          send(socket, {
            type: "error",
            message: "سجل المستخدم أولًا"
          });
          return;
        }

        const targetId = String(data.targetId || "").trim();
        const packetData = data.data;

        if (
          !validId(targetId) ||
          typeof packetData !== "string" ||
          packetData.length === 0 ||
          Buffer.byteLength(packetData, "utf8") > 60000
        ) {
          send(socket, {
            type: "error",
            message: "بيانات الحزمة غير صالحة"
          });
          return;
        }

        if (targetId === socket.userId) {
          send(socket, {
            type: "error",
            message: "لا يمكن إرسال الرسالة إلى نفسك"
          });
          return;
        }

        const messageId = crypto.randomUUID();
        const timestamp = Date.now();

        // حفظ الرسالة أولًا؛ إذا فشل الحفظ لا نعلن نجاح الإرسال.
        await pool.query(
          `INSERT INTO pending_messages
           (message_id, sender_id, target_id, packet_data)
           VALUES ($1, $2, $3, $4)`,
          [messageId, socket.userId, targetId, packetData]
        );

        const delivered = sendToUser(targetId, {
          type: "packet",
          from: socket.userId,
          data: packetData,
          timestamp,
          messageId,
          pending: true
        });

        send(socket, {
          type: "sent",
          targetId,
          messageId,
          delivered,
          timestamp
        });

        return;
      }

      // دعم نظام الغرف القديم.
      if (data.type === "join") {
        const room = String(data.room || "").trim();
        const username = String(data.username || "مستخدم").trim();

        if (!/^[a-zA-Z0-9_-]{1,64}$/.test(room)) {
          send(socket, {
            type: "error",
            message: "اسم الغرفة غير صالح"
          });
          return;
        }

        removeClient(socket);
        socket.room = room;
        socket.username = username.slice(0, 40) || "مستخدم";

        if (!rooms.has(room)) {
          rooms.set(room, new Set());
        }

        rooms.get(room).add(socket);

        send(socket, {
          type: "joined",
          room,
          username: socket.username,
          message: "أنت الآن متصل بغرفة كلمني"
        });

        return;
      }

      if (data.type === "message") {
        if (!socket.room) {
          send(socket, {
            type: "error",
            message: "انضم إلى غرفة أولًا"
          });
          return;
        }

        const messageText = String(data.text || "");

        if (!messageText.trim() || messageText.length > 4000) {
          send(socket, {
            type: "error",
            message: "الرسالة فارغة أو طويلة جدًا"
          });
          return;
        }

        const packet = {
          type: "message",
          room: socket.room,
          username: socket.username,
          text: messageText,
          timestamp: Date.now()
        };

        const roomClients = rooms.get(socket.room);

        if (roomClients) {
          for (const client of roomClients) {
            if (client !== socket) {
              send(client, packet);
            }
          }
        }

        send(socket, {
          type: "sent",
          timestamp: packet.timestamp
        });

        return;
      }

      send(socket, {
        type: "error",
        message: "نوع الطلب غير معروف"
      });
    } catch (error) {
      console.error("Message handler error:", error.message);

      send(socket, {
        type: "error",
        message: "تعذر معالجة الطلب"
      });
    }
  });

  socket.on("close", () => removeClient(socket));
  socket.on("error", () => removeClient(socket));
});

async function start() {
  try {
    await initializeDatabase();

    server.listen(PORT, "0.0.0.0", () => {
      console.log(`Kallimni server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Server startup failed:", error.message);
    process.exit(1);
  }
}

async function shutdown() {
  console.log("Shutting down Kallimni server");

  wss.close();
  server.close(async () => {
    try {
      await pool.end();
    } finally {
      process.exit(0);
    }
  });

  setTimeout(() => process.exit(1), 10000).unref();
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

start();