"use client";

import { useEffect, useState } from "react";
import axios from "axios";

type Order = {
  orderId: string;
  product: string;
  status: string;
};

export default function Home() {
  const [product, setProduct] = useState("");
  const [order, setOrder] = useState<Order | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [orders, setOrders] = useState<Order[]>([]);

  const loadOrders = async () => {
    try {
      const res = await axios.get("http://localhost:3001/orders");
      setOrders(res.data);
    } catch (err) {
      console.error("Failed to load orders");
    }
  };

  const createOrder = async () => {
    if (!product.trim()) {
      setError("Enter product name");
      return;
    }

    try {
      setLoading(true);
      setError("");

      const res = await axios.post(
        "http://localhost:3001/order",
        { product }
      );

      setOrder(res.data);
      setProduct("");
      loadOrders();
    } catch (err) {
      setError("Failed to create order");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadOrders();
  }, []);

  return (
    <main
      style={{
        minHeight: "100vh",
        background: "#000",
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        fontFamily: "Arial, sans-serif",
      }}
    >
      <div
        style={{
          width: 500,
          background: "#fff",
          padding: 32,
          borderRadius: 18,
          boxShadow: "0 10px 30px rgba(255,255,255,0.1)",
          color: "#000",
        }}
      >
        <h1
          style={{
            marginBottom: 24,
            textAlign: "center",
          }}
        >
          Kafka Commerce
        </h1>

        <input
          value={product}
          onChange={(e) => setProduct(e.target.value)}
          placeholder="Enter product name"
          style={{
            width: "100%",
            padding: 14,
            borderRadius: 10,
            border: "2px solid #2563eb",
            background: "#f3f4f6",
            color: "#000",
            outline: "none",
            marginBottom: 16,
            fontSize: 16,
          }}
        />

        <button
          onClick={createOrder}
          disabled={loading}
          style={{
            width: "100%",
            padding: 14,
            borderRadius: 10,
            border: "none",
            background: "#2563eb",
            color: "#fff",
            cursor: "pointer",
            fontSize: 16,
            fontWeight: 600,
          }}
        >
          {loading ? "Creating..." : "Create Order"}
        </button>

        {error && (
          <p
            style={{
              color: "red",
              marginTop: 14,
              textAlign: "center",
            }}
          >
            {error}
          </p>
        )}

        {order && (
          <div
            style={{
              marginTop: 24,
              padding: 20,
              borderRadius: 12,
              background: "#f9fafb",
              border: "1px solid #ddd",
            }}
          >
            <h3>Order Created</h3>

            <p>
              <strong>ID:</strong> {order.orderId}
            </p>

            <p>
              <strong>Product:</strong> {order.product}
            </p>

            <p>
              <strong>Status:</strong> {order.status}
            </p>
          </div>
        )}

        <div
          style={{
            marginTop: 24,
            padding: 20,
            borderRadius: 12,
            background: "#f9fafb",
            border: "1px solid #ddd",
            height: 260,
            overflowY: "auto",
          }}
        >
          <h1><b>Order History</b></h1>
          {orders.map((o) => (
            <div key={o.orderId} >
              <p><b>ID:</b> {o.orderId}</p>
              <p><b>Product:</b> {o.product}</p>
              <p><b>Status:</b> {o.status}</p>
            </div>
          ))}
        </div>
      </div>
    </main>
  );
}


// "use client";

// import { useEffect, useRef, useState } from "react";
// import axios from "axios";

// type Order = {
//   orderId: string;
//   product: string;
//   status: string;
// };

// type Event = {
//   type: string;
//   payload: any;
//   timestamp: number;
// };

// export default function Home() {
//   const [product, setProduct] = useState("");
//   const [order, setOrder] = useState<Order | null>(null);
//   const [events, setEvents] = useState<Event[]>([]);
//   const wsRef = useRef<WebSocket | null>(null);

//   // -------------------------
//   // CONNECT TO WEBSOCKET
//   // -------------------------
//   const connectWS = (orderId: string) => {
//     if (wsRef.current) wsRef.current.close();

//     const ws = new WebSocket("ws://localhost:4000");
//     wsRef.current = ws;

//     ws.onopen = () => {
//       ws.send(
//         JSON.stringify({
//           type: "SUBSCRIBE",
//           orderId,
//         })
//       );
//     };

//     ws.onmessage = (msg) => {
//       const event: Event = JSON.parse(msg.data);

//       setEvents((prev) => [event, ...prev]);

//       // update order state if payload contains status
//       if (event.payload?.status) {
//         setOrder(event.payload);
//       }
//     };
//   };

//   // -------------------------
//   // CREATE ORDER
//   // -------------------------
//   const createOrder = async () => {
//     if (!product.trim()) return;

//     const res = await axios.post("http://localhost:3001/order", {
//       product,
//     });

//     setOrder(res.data);

//     setEvents((prev) => [
//       {
//         type: "ORDER_CREATED",
//         payload: res.data,
//         timestamp: Date.now(),
//       },
//       ...prev,
//     ]);

//     connectWS(res.data.orderId);
//   };

//   return (
//     <div style={styles.page}>
//       <h1 style={styles.title}>Kafka Event Dashboard</h1>

//       {/* INPUT */}
//       <div style={styles.card}>
//         <input
//           value={product}
//           onChange={(e) => setProduct(e.target.value)}
//           placeholder="Enter product"
//           style={styles.input}
//         />
//         <button onClick={createOrder} style={styles.button}>
//           Create Order
//         </button>
//       </div>

//       {/* ORDER STATE */}
//       {order && (
//         <div style={styles.card}>
//           <h3>Order State</h3>
//           <p>ID: {order.orderId}</p>
//           <p>Product: {order.product}</p>
//           <p>Status: {order.status}</p>
//         </div>
//       )}

//       {/* EVENT STREAM */}
//       <div style={styles.stream}>
//         <h2>Live Event Stream (Kafka → WS)</h2>

//         {events.map((e, i) => (
//           <div key={i} style={styles.event}>
//             <div style={styles.header}>
//               <strong>{e.type}</strong>
//               <span>{new Date(e.timestamp).toLocaleTimeString()}</span>
//             </div>

//             <pre style={styles.pre}>
//               {JSON.stringify(e.payload, null, 2)}
//             </pre>
//           </div>
//         ))}
//       </div>
//     </div>
//   );
// }

// // -------------------------
// // STYLES
// // -------------------------
// const styles: any = {
//   page: {
//     minHeight: "100vh",
//     background: "#0f172a",
//     color: "white",
//     padding: 30,
//     fontFamily: "Arial",
//   },
//   title: { fontSize: 24, marginBottom: 20 },

//   card: {
//     background: "#1e293b",
//     padding: 16,
//     borderRadius: 10,
//     marginBottom: 15,
//   },

//   input: {
//     padding: 10,
//     width: "60%",
//     marginRight: 10,
//   },

//   button: {
//     padding: 10,
//     background: "#22c55e",
//     border: "none",
//     cursor: "pointer",
//   },

//   stream: { marginTop: 20 },

//   event: {
//     background: "#334155",
//     padding: 12,
//     marginBottom: 10,
//     borderRadius: 8,
//   },

//   header: {
//     display: "flex",
//     justifyContent: "space-between",
//     marginBottom: 8,
//   },

//   pre: {
//     fontSize: 12,
//     color: "#cbd5e1",
//   },
// };