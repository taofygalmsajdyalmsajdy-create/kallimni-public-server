
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 3000;
const clients = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
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

function removeClient(socket) {
  const room = socket.room;

  if (room && clients.has(room)) {
    clients.get(room).delete(socket);

    if (clients.get(room).size === 0) {
      clients.delete(room);
    }
  }

  socket.room = null;
}

wss.on("connection", (socket) => {
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

      if (!clients.has(room)) {
        clients.set(room, new Set());
      }

      clients.get(room).add(socket);

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

      const roomClients = clients.get(socket.room);

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
