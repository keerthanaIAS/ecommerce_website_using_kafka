const WebSocket = require("ws");

const wss = new WebSocket.Server({ port: 4000 });

const clients = new Map();

wss.on("connection", (ws) => {
  ws.on("message", (msg) => {
    const data = JSON.parse(msg);
    if (data.type === "subscribe") {
      clients.set(data.orderId, ws);
    }
  });
});

function pushEvent(orderId, event) {
  const client = clients.get(orderId);
  if (client) {
    client.send(JSON.stringify(event));
  }
}

module.exports = { pushEvent };