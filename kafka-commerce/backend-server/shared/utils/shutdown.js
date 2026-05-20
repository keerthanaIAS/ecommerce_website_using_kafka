module.exports = (shutdownFn) => {
    process.on("SIGTERM", shutdownFn);
    process.on("SIGINT", shutdownFn);
};