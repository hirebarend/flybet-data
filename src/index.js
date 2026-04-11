const { initializeApp, cert } = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");

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
  console.log(`  GET ${apiPath}`);

  const response = await fetch(`${API_BASE_URL}${apiPath}`, {
    headers: {
      Accept: "application/json",
      "x-rapidapi-host": "aerodatabox.p.rapidapi.com",
      "x-rapidapi-key": process.env.AERODATABOX_API_KEY,
    },
  });

  if (response.status === 204) {
    return null;
  }

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${await response.text()}`);
  }

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
  const flightSnapshot = await db.collection("flights").doc(flightId).get();

  if (!flightSnapshot.exists) {
    return;
  }

  const flight = flightSnapshot.data();

  if (!flight.arrival.actual && flight.cancelled !== true) {
    return;
  }

  const bets = (
    await db.collection("bets").where("flight_id", "==", flightId).get()
  ).docs
    .map((doc) => ({ id: doc.id, ...doc.data() }))
    .filter((bet) => bet.settled !== true);

  if (bets.length === 0) {
    return;
  }

  console.log(`Settling ${bets.length} bets for ${flightId}`);

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
  }

  console.log(`Settled ${bets.length} bets for ${flightId}`);
}

async function main() {
  initializeApp({
    credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)),
  });

  const db = getFirestore();
  console.log("Starting settlement run");
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

  console.log(`Found ${flights.length} flights to check`);

  for (const doc of flights) {
    const flight = doc.data();
    const departureDate = toDate(flight.departure.scheduled);

    console.log(`Checking movement for ${doc.id}`);

    const response = await requestJson(
      `/flights/number/${encodeURIComponent(`${flight.airline.iata || "FA"}${flight.flight}`)}/${formatLocalDate(departureDate)}?dateLocalRole=Departure`,
    );

    let movement = response[0] || null;

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
    }

    if (!flight.arrival.actual && arrivalActual) {
      updates["arrival.actual"] = arrivalActual;
      updates.cancelled = false;
      shouldSettle = true;
    } else if (!flight.arrival.actual) {
      updates.cancelled = true;
      shouldSettle = true;
    }

    if (Object.keys(updates).length > 0) {
      await db.collection("flights").doc(doc.id).update(updates);
    }

    if (shouldSettle) {
      await settleFlightBets(db, doc.id);
    }

    await sleep(API_DELAY_MS);
  }

  console.log("Run complete");
}

main().catch((error) => {
  console.error("Fatal:", error);
  process.exit(1);
});
