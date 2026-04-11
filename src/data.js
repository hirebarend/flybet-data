const crypto = require("crypto");
const { createLogger } = require("./logger");

const log = createLogger("data");

const API_BASE_URL = "https://aerodatabox.p.rapidapi.com";
const SAST_OFFSET_MS = 2 * 60 * 60 * 1000;

function toDate(value) {
  if (typeof value?.toDate === "function") {
    return value.toDate();
  }

  return new Date(typeof value === "string" ? value.replace(" ", "T") : value);
}

function toUtcIso(value) {
  if (!value) {
    return null;
  }

  return toDate(value).toISOString();
}

function formatLocalDateTime(date) {
  return new Date(date.getTime() + SAST_OFFSET_MS).toISOString().slice(0, 16);
}

function createFlightId(airlineIata, flightNumber, scheduledDeparture) {
  const raw = `${airlineIata}${flightNumber}::${scheduledDeparture.slice(0, 16)}`;
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 16);
}

async function requestJson(apiPath) {
  log.info(`GET ${API_BASE_URL}${apiPath}`);
  const start = Date.now();

  const response = await fetch(`${API_BASE_URL}${apiPath}`, {
    headers: {
      Accept: "application/json",
      "x-rapidapi-host": "aerodatabox.p.rapidapi.com",
      "x-rapidapi-key": process.env.AERODATABOX_API_KEY,
    },
  });

  const duration = Date.now() - start;

  if (response.status === 204) {
    log.info(`GET ${apiPath} completed`, { status: 204, duration: `${duration}ms` });
    return null;
  }

  if (!response.ok) {
    const body = await response.text();
    log.error(`GET ${apiPath} failed`, { status: response.status, duration: `${duration}ms` });
    throw new Error(`HTTP ${response.status}: ${body}`);
  }

  log.info(`GET ${apiPath} completed`, { status: response.status, duration: `${duration}ms` });
  return response.json();
}

function toFlight(entry, departureAirportCode) {
  const match = String(entry.number).match(/^([A-Z]{2})\s*(\d+)$/i) || [];
  const airlineIata = (match[1] || entry.airline?.iata || "FA").toUpperCase();
  const flightNumber = match[2] || String(entry.number).trim();

  const scheduledDeparture = toUtcIso(entry.departure.scheduledTime?.utc);

  const scheduledArrival = toUtcIso(entry.arrival.scheduledTime?.utc);

  return {
    id: createFlightId(airlineIata, flightNumber, scheduledDeparture),
    flight: flightNumber,
    airline: {
      iata: airlineIata,
    },
    departure: {
      airport: {
        code: departureAirportCode,
      },
      scheduled: scheduledDeparture,
      actual: null,
    },
    arrival: {
      airport: {
        code: entry.arrival.airport.iata,
      },
      scheduled: scheduledArrival,
      actual: null,
    },
    aircraft: {
      model: entry.aircraft?.model || null,
    },
    status: entry.status || null,
    cancelled: false,
  };
}

async function findLastFlight(db) {
  const snapshot = await db
    .collection("flights")
    .orderBy("departure.scheduled", "desc")
    .limit(1)
    .get();

  if (snapshot.empty) {
    return null;
  }

  return snapshot.docs[0].data();
}

async function findFutureDepartingFlights(airportCode, from, to) {
  const data = await requestJson(
    `/flights/airports/iata/${airportCode}/${formatLocalDateTime(from)}/${formatLocalDateTime(to)}` +
      "?withLeg=true&direction=Departure&withCodeshared=false&withCargo=false&withPrivate=false",
  );

  return (data?.departures || []).map((entry) => toFlight(entry, airportCode));
}

module.exports = {
  findLastFlight,
  findFutureDepartingFlights,
};
