const { initializeApp, cert } = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");
const { createLogger } = require("./logger");

const log = createLogger("settle");

const API_BASE_URL = "https://aerodatabox.p.rapidapi.com";
const API_DELAY_MS = 5000;
const ONE_HOUR_MS = 60 * 60 * 1000;
const FIFTEEN_MINUTES_MS = 15 * 60 * 1000;
const MINIMUM_PROFIT = 25;
const SAST_OFFSET_MS = 2 * 60 * 60 * 1000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

function formatLocalDate(date) {
  return new Date(date.getTime() + SAST_OFFSET_MS).toISOString().slice(0, 10);
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

function calculatePayouts(bets, winningOutcome, marketOutcomes) {
  const marketBets = bets.filter((bet) => marketOutcomes.includes(bet.outcome));
  const winners = marketBets.filter((bet) => bet.outcome === winningOutcome);
  const losers = marketBets.filter((bet) => bet.outcome !== winningOutcome);
  const totalWinnerStake = winners.reduce((sum, bet) => sum + bet.amount, 0);
  const totalLoserStake = losers.reduce((sum, bet) => sum + bet.amount, 0);
  const payouts = new Map();

  for (const bet of winners) {
    const proportionalShare =
      totalLoserStake === 0
        ? 0
        : Math.round((bet.amount / totalWinnerStake) * totalLoserStake);

    payouts.set(
      bet.id,
      bet.amount + Math.max(proportionalShare, MINIMUM_PROFIT),
    );
  }

  for (const bet of losers) {
    payouts.set(bet.id, 0);
  }

  return payouts;
}

async function settleFlightBets(db, flightId) {
  log.debug(`Loading flight document`, { flightId });
  const flightSnapshot = await db.collection("flights").doc(flightId).get();

  if (!flightSnapshot.exists) {
    log.warn(`Flight document not found, skipping`, { flightId });
    return;
  }

  const flight = flightSnapshot.data();

  if (!flight.arrival.actual && flight.cancelled !== true) {
    log.debug(`Flight not yet arrived or cancelled, skipping settlement`, { flightId });
    return;
  }

  const bets = (
    await db.collection("bets").where("flight_id", "==", flightId).get()
  ).docs
    .map((doc) => ({ id: doc.id, ...doc.data() }))
    .filter((bet) => bet.settled !== true);

  if (bets.length === 0) {
    log.debug(`No unsettled bets found`, { flightId });
    return;
  }

  log.info(`Settling bets`, { flightId, count: bets.length });

  const departureDelayMs =
    toDate(flight.departure.actual).getTime() -
    toDate(flight.departure.scheduled).getTime();

  const arrivalDelayMs = flight.arrival.actual
    ? toDate(flight.arrival.actual).getTime() -
      toDate(flight.arrival.scheduled).getTime()
    : Number.POSITIVE_INFINITY;

  const markets = [
    {
      outcomes: ["onTimeDeparture", "delayedDeparture"],
      winner:
        !flight.departure.actual || departureDelayMs > FIFTEEN_MINUTES_MS
          ? "delayedDeparture"
          : "onTimeDeparture",
    },
    {
      outcomes: ["onTimeArrival", "delayedArrival"],
      winner:
        !flight.arrival.actual || arrivalDelayMs > FIFTEEN_MINUTES_MS
          ? "delayedArrival"
          : "onTimeArrival",
    },
    {
      outcomes: ["cancelled", "notCancelled"],
      winner: flight.cancelled === true ? "cancelled" : "notCancelled",
    },
  ];

  const totalPayouts = new Map();

  for (const market of markets) {
    const marketPayouts = calculatePayouts(
      bets,
      market.winner,
      market.outcomes,
    );

    for (const [betId, payout] of marketPayouts) {
      totalPayouts.set(betId, (totalPayouts.get(betId) || 0) + payout);
    }
  }

  for (const bet of bets) {
    const payout = totalPayouts.get(bet.id) || 0;

    if (payout > 0) {
      log.debug(`Crediting user balance`, { betId: bet.id, userId: bet.userId, payout });
      await db
        .collection("users")
        .doc(bet.userId)
        .update({
          balance: FieldValue.increment(payout),
        });
    }

    await db.collection("bets").doc(bet.id).update({
      settled: true,
      payout,
    });

    log.debug(`Bet settled`, { betId: bet.id, payout });
  }

  log.info(`Settlement complete`, { flightId, betsSettled: bets.length });
}

async function main() {
  log.info("Process starting");

  initializeApp({
    credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
  });

  const db = getFirestore();
  log.info("Firebase initialized");
  const now = new Date();
  const flights = (
    await db
      .collection("flights")
      .where(
        "arrival.scheduled",
        "<=",
        new Date(now.getTime() - ONE_HOUR_MS).toISOString(),
      )
      .orderBy("arrival.scheduled")
      .get()
  ).docs.filter((doc) => {
    return (
      doc.data().cancelled !== true &&
      (!doc.data().departure?.actual || !doc.data().arrival?.actual)
    );
  });

  log.info(`Found flights to check`, { count: flights.length });

  for (const doc of flights) {
    const flight = doc.data();
    const departureDate = toDate(flight.departure.scheduled);

    log.info(`Checking movement`, { flightId: doc.id, flight: `${flight.airline.iata || "FA"}${flight.flight}`, scheduled: flight.departure.scheduled });

    const response = await requestJson(
      `/flights/number/${encodeURIComponent(`${flight.airline.iata || "FA"}${flight.flight}`)}/${formatLocalDate(departureDate)}?dateLocalRole=Departure`,
    );

    let movement = response[0] || null;

    if (!movement) {
      log.warn(`No movement data returned`, { flightId: doc.id });
    }

    const departureActual = toUtcIso(movement?.departure?.revisedTime?.utc);
    const arrivalActual = toUtcIso(movement?.arrival?.revisedTime?.utc);
    const updates = {};
    let shouldSettle = false;

    if (movement?.status) {
      updates.status = movement.status;
    }

    if (movement?.aircraft?.model) {
      updates["aircraft.model"] = movement.aircraft.model;
    }

    if (!flight.departure.actual && departureActual) {
      updates["departure.actual"] = departureActual;
      log.debug(`Departure actual found`, { flightId: doc.id, departureActual });
    }

    if (!flight.arrival.actual && arrivalActual) {
      updates["arrival.actual"] = arrivalActual;
      updates.cancelled = false;
      shouldSettle = true;
      log.debug(`Arrival actual found`, { flightId: doc.id, arrivalActual });
    } else if (!flight.arrival.actual) {
      updates.cancelled = true;
      shouldSettle = true;
      log.debug(`No arrival data, marking as cancelled`, { flightId: doc.id });
    }

    if (Object.keys(updates).length > 0) {
      log.info(`Updating flight document`, { flightId: doc.id, fields: Object.keys(updates).join(",") });
      await db.collection("flights").doc(doc.id).update(updates);
    } else {
      log.debug(`No updates needed`, { flightId: doc.id });
    }

    if (shouldSettle) {
      await settleFlightBets(db, doc.id);
    }

    log.debug(`Waiting before next request`, { delay: `${API_DELAY_MS}ms` });
    await sleep(API_DELAY_MS);
  }

  log.info("Process complete");
}

main().catch((error) => {
  log.error(`Fatal: ${error.message}`);
  process.exit(1);
});
