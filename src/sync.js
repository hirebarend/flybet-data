const { initializeApp, cert } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");
const { findLastFlight, findFutureDepartingFlights } = require("./data");
const { createLogger } = require("./logger");

const log = createLogger("sync");

const AIRPORT_CODES = ["JNB", "CPT"];
const FUTURE_WINDOW_HOURS = 12;
const ONE_HOUR_MS = 60 * 60 * 1000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toDate(value) {
  if (typeof value?.toDate === "function") {
    return value.toDate();
  }

  return new Date(typeof value === "string" ? value.replace(" ", "T") : value);
}

async function main() {
  log.info("Process starting");

  initializeApp({
    credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
  });

  const db = getFirestore();
  log.info("Firebase initialized");
  const now = new Date();
  const lastFlight = await findLastFlight(db);

  if (lastFlight) {
    log.info(`Last flight found`, { scheduled: toDate(lastFlight.departure.scheduled).toISOString() });
  } else {
    log.info("No existing flights found in database");
  }

  const from =
    lastFlight && toDate(lastFlight.departure.scheduled) > now
      ? toDate(lastFlight.departure.scheduled)
      : now;
  const to = new Date(now.getTime() + FUTURE_WINDOW_HOURS * ONE_HOUR_MS);

  log.info(`Sync window calculated`, { from: from.toISOString(), to: to.toISOString() });

  if (from >= to) {
    log.info("No sync needed, window is empty");
    return;
  }

  log.debug(`Waiting before first API request`, { delay: "5000ms" });
  await sleep(5000);

  for (const airportCode of AIRPORT_CODES) {
    log.info(`Fetching departures`, { airport: airportCode });
    const allFlights = await findFutureDepartingFlights(airportCode, from, to);
    const flights = allFlights.filter(
      (flight) =>
        flight.airline.iata === "FA" &&
        AIRPORT_CODES.includes(flight.arrival.airport.code),
    );

    log.info(`Flights filtered`, { airport: airportCode, total: allFlights.length, matched: flights.length });

    for (const flight of flights) {
      log.debug(`Saving flight`, { flightId: flight.id, flight: `${flight.airline.iata}${flight.flight}`, route: `${flight.departure.airport.code}-${flight.arrival.airport.code}` });
      await db.collection("flights").doc(flight.id).set(flight, {
        merge: true,
      });
    }

    log.info(`Flights saved`, { airport: airportCode, count: flights.length });

    log.debug(`Waiting before next airport`, { delay: "5000ms" });
    await sleep(5000);
  }

  log.info("Process complete");
}

main().catch((error) => {
  log.error(`Fatal: ${error.message}`);
  process.exit(1);
});
