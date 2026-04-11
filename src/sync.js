const { initializeApp, cert } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");
const { findLastFlight, findFutureDepartingFlights } = require("./data");

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
  initializeApp({
    credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
  });

  const db = getFirestore();
  const now = new Date();
  const lastFlight = await findLastFlight(db);
  const from =
    lastFlight && toDate(lastFlight.departure.scheduled) > now
      ? toDate(lastFlight.departure.scheduled)
      : now;
  const to = new Date(now.getTime() + FUTURE_WINDOW_HOURS * ONE_HOUR_MS);

  if (from >= to) {
    return;
  }

  await sleep(5000);

  for (const airportCode of AIRPORT_CODES) {
    const flights = (
      await findFutureDepartingFlights(airportCode, from, to)
    ).filter(
      (flight) =>
        flight.airline.iata === "FA" &&
        AIRPORT_CODES.includes(flight.arrival.airport.code),
    );

    for (const flight of flights) {
      await db.collection("flights").doc(flight.id).set(flight, {
        merge: true,
      });
    }

    await sleep(5000);
  }
}

main().catch((error) => {
  console.error("Fatal:", error);
  process.exit(1);
});
