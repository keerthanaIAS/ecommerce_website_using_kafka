const { exec } = require("child_process");

function run(cmd) {
  return new Promise((resolve) => {
    const p = exec(cmd);
    p.stdout.on("data", console.log);
    p.stderr.on("data", console.error);
    p.on("close", resolve);
  });
}

async function start() {
  console.log("Starting Kafka services...");

  await run("docker compose up -d");

  console.log("Waiting Kafka to stabilize...");
  await new Promise(r => setTimeout(r, 8000));

  console.log("Starting consumer...");
  require("./consumer");

  console.log("Starting producer...");
  require("./producer");
}

start();