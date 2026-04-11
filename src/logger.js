const LEVELS = { DEBUG: "DEBUG", INFO: "INFO", WARN: "WARN", ERROR: "ERROR" };

function formatLog(level, component, message, meta) {
  const timestamp = new Date().toISOString();
  const metaStr = meta
    ? " " +
      Object.entries(meta)
        .map(([k, v]) => `${k}=${v}`)
        .join(" ")
    : "";

  return `${timestamp} ${level.padEnd(5)} [${component}] ${message}${metaStr}`;
}

function createLogger(component) {
  return {
    debug: (message, meta) =>
      console.log(formatLog(LEVELS.DEBUG, component, message, meta)),
    info: (message, meta) =>
      console.log(formatLog(LEVELS.INFO, component, message, meta)),
    warn: (message, meta) =>
      console.warn(formatLog(LEVELS.WARN, component, message, meta)),
    error: (message, meta) =>
      console.error(formatLog(LEVELS.ERROR, component, message, meta)),
  };
}

module.exports = { createLogger };
