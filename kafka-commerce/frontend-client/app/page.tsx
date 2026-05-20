"use client";

import { useState } from "react";
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

  const createOrder = async () => {
    if (!product.trim()) {
      setError("Enter product name");
      return;
    }

    try {
      setLoading(true);
      setError("");

      const res = await axios.post(
        "http://localhost:3000/order",
        { product }
      );

      setOrder(res.data);
      setProduct("");
    } catch (err) {
      setError("Failed to create order");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

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
      </div>
    </main>
  );
}