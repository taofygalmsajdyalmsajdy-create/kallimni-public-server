
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 3000;

// اتصالات المستخدمين المسجلين
const users = new Map();

// دعم الغرف القديمة مؤقتًا
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
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(data));
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

wss.on("connection", (socket) => {
  socket.userId = null;
  socket.room = null;
  socket.username = "مستخدم";

  send(socket, {
    type: "connected",
    message: "اتصلت بخادم كلمني"
  });

  socket.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(socket, {
        type: "error",
        message: "صيغة الرسالة غير صحيحة"
      });
      return;
    }

    // تسجيل اتصال التطبيق بمعرّف المستخدم
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

      return;
    }

    // توجيه حزمة إلى مستخدم محدد
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

      if (!validId(targetId) ||
          typeof packetData !== "string" ||
          packetData.length === 0 ||
          Buffer.byteLength(packetData, "utf8") > 60000) {
        send(socket, {
          type: "error",
          message: "بيانات الحزمة غير صالحة"
        });
        return;
      }

      const recipients = users.get(targetId);
      let delivered = false;

      if (recipients) {
        for (const recipient of recipients) {
          if (recipient.readyState === WebSocket.OPEN) {
            send(recipient, {
              type: "packet",
              from: socket.userId,
              data: packetData,
              timestamp: Date.now()
            });

            delivered = true;
          }
        }
      }

      send(socket, {
        type: "sent",
        targetId,
        delivered,
        timestamp: Date.now()
      });

      return;
    }

    // دعم نظام الغرف القديم
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
  });

  socket.on("close", () => removeClient(socket));
  socket.on("error", () => removeClient(socket));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Kallimni server running on port ${PORT}`);
});
