const crypto = require("crypto");
const { initializeApp, cert, getApps } = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");

const AIRPORTS = ["JNB", "CPT"];
const API_BASE_URL = "https://aerodatabox.p.rapidapi.com";
const API_DELAY_MS = 5000;
const FUTURE_WINDOW_HOURS = 12;
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

function formatLocalDateTime(date) {
  return new Date(date.getTime() + SAST_OFFSET_MS).toISOString().slice(0, 16);
}

function formatLocalDate(date) {
  return new Date(date.getTime() + SAST_OFFSET_MS).toISOString().slice(0, 10);
}

function createFlightId(airlineIata, flightNumber, scheduledDeparture) {
  const raw = `${airlineIata}${flightNumber}::${scheduledDeparture.slice(0, 16)}`;
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 16);
}

async function requestJson(apiPath, apiKey) {
  console.log(`  GET ${apiPath}`);

  const response = await fetch(`${API_BASE_URL}${apiPath}`, {
    headers: {
      Accept: "application/json",
      "x-rapidapi-host": "aerodatabox.p.rapidapi.com",
      "x-rapidapi-key": apiKey,
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
  const payouts = [];

  for (const bet of winners) {
    const proportionalShare =
      totalLoserStake === 0
        ? 0
        : Math.round((bet.amount / totalWinnerStake) * totalLoserStake);

    payouts.push({
      betId: bet.id,
      userId: bet.userId,
      payout: bet.amount + Math.max(proportionalShare, MINIMUM_PROFIT),
    });
  }

  for (const bet of losers) {
    payouts.push({
      betId: bet.id,
      userId: bet.userId,
      payout: 0,
    });
  }

  return payouts;
}

async function settleFlightBets(db, flightId) {
  const flightRef = db.collection("flights").doc(flightId);
  let unsettledBetCount = 0;
  let winningPayoutCount = 0;
  let settled = false;

  await db.runTransaction(async (transaction) => {
    const flightSnapshot = await transaction.get(flightRef);

    if (!flightSnapshot.exists) {
      return;
    }

    const flight = flightSnapshot.data();

    if (!flight.arrival.actual && flight.cancelled !== true) {
      return;
    }

    const betsQuery = db.collection("bets").where("flight_id", "==", flightId);
    const betsSnapshot = await transaction.get(betsQuery);
    const unsettledBets = betsSnapshot.docs
      .map((doc) => ({ id: doc.id, ...doc.data() }))
      .filter((bet) => bet.settled !== true);

    if (unsettledBets.length === 0) {
      return;
    }

    unsettledBetCount = unsettledBets.length;

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

    const userCredits = new Map();
    const allPayouts = [];

    for (const market of markets) {
      const payouts = calculatePayouts(
        unsettledBets,
        market.winner,
        market.outcomes,
      );

      for (const payout of payouts) {
        allPayouts.push(payout);

        if (payout.payout > 0) {
          userCredits.set(
            payout.userId,
            (userCredits.get(payout.userId) || 0) + payout.payout,
          );
        }
      }
    }

    winningPayoutCount = allPayouts.filter(
      (payout) => payout.payout > 0,
    ).length;

    for (const [userId, credit] of userCredits) {
      transaction.update(db.collection("users").doc(userId), {
        balance: FieldValue.increment(credit),
      });
    }

    for (const payout of allPayouts) {
      transaction.update(db.collection("bets").doc(payout.betId), {
        settled: true,
        payout: payout.payout,
      });
    }

    settled = true;
  });

  if (settled) {
    console.log(
      `  Settled ${unsettledBetCount} bets for ${flightId} (${winningPayoutCount} winning payouts)`,
    );
    return;
  }

  console.log(`  No unsettled bets to process for ${flightId}`);
}

async function main() {
  const apiKey = process.env.AERODATABOX_API_KEY;
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;

  initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });

  const db = getFirestore();
  const now = new Date();

  const latestStoredRouteFlight = (
    await db
      .collection("flights")
      .orderBy("departure.scheduled", "desc")
      .limit(100)
      .get()
  ).docs
    .map((doc) => doc.data())
    .find(() => true);

  const latestStoredDeparture = latestStoredRouteFlight
    ? toDate(latestStoredRouteFlight.departure.scheduled)
    : null;
  const futureStart =
    latestStoredDeparture && latestStoredDeparture > now
      ? latestStoredDeparture
      : now;
  const futureEnd = new Date(now.getTime() + FUTURE_WINDOW_HOURS * ONE_HOUR_MS);

  console.log(
    `Future flight window: ${futureStart.toISOString()} -> ${futureEnd.toISOString()}`,
  );

  if (futureStart < futureEnd) {
    const futureFlights = [];

    for (const departureAirportCode of AIRPORTS) {
      const apiPath =
        `/flights/airports/iata/${departureAirportCode}/${formatLocalDateTime(futureStart)}/${formatLocalDateTime(futureEnd)}` +
        "?withLeg=true&direction=Departure&withCodeshared=false&withCargo=false&withPrivate=false";

      const data = await requestJson(apiPath, apiKey);
      const departures = data?.departures || [];
      let airportFlightCount = 0;

      for (const entry of departures) {
        const isFlySafair =
          entry.airline?.iata === "FA" || String(entry.number).startsWith("FA");
        const arrivalAirportCode = entry.arrival?.airport?.iata;

        if (!isFlySafair || !AIRPORTS.includes(arrivalAirportCode)) {
          continue;
        }

        const match = String(entry.number).match(/^([A-Z]{2})\s*(\d+)$/i) || [];
        const airlineIata = (
          match[1] ||
          entry.airline?.iata ||
          "FA"
        ).toUpperCase();
        const flightNumber = match[2] || String(entry.number).trim();
        const scheduledDeparture = toUtcIso(
          entry.departure.scheduledTime.utc ||
            entry.departure.scheduledTime.local ||
            entry.departure.scheduledTime,
        );
        const scheduledArrival = toUtcIso(
          entry.arrival.scheduledTime.utc ||
            entry.arrival.scheduledTime.local ||
            entry.arrival.scheduledTime,
        );

        futureFlights.push({
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
              code: arrivalAirportCode,
            },
            scheduled: scheduledArrival,
            actual: null,
          },
          aircraft: {
            model: entry.aircraft?.model || null,
          },
          status: entry.status || null,
          cancelled: false,
        });

        airportFlightCount += 1;
      }

      console.log(
        `  ${departureAirportCode}: fetched ${airportFlightCount} JNB/CPT flights`,
      );
      await sleep(API_DELAY_MS);
    }

    let storedFlightCount = 0;

    for (let index = 0; index < futureFlights.length; index += 400) {
      const flightChunk = futureFlights.slice(index, index + 400);
      const batch = db.batch();

      for (const flight of flightChunk) {
        batch.set(db.collection("flights").doc(flight.id), flight, {
          merge: true,
        });
      }

      await batch.commit();
      storedFlightCount += flightChunk.length;
    }

    console.log(`Stored ${storedFlightCount} future flights`);
  }

  const movementCutoff = new Date(now.getTime() - ONE_HOUR_MS).toISOString();
  const movementSnapshot = await db
    .collection("flights")
    .where("arrival.scheduled", "<=", movementCutoff)
    .orderBy("arrival.scheduled")
    .get();

  const flightsNeedingMovement = movementSnapshot.docs.filter((doc) => {
    const flight = doc.data();
    return (
      flight.cancelled !== true &&
      (!flight.departure?.actual || !flight.arrival?.actual)
    );
  });

  console.log(
    `Flights needing movement refresh: ${flightsNeedingMovement.length}`,
  );

  for (const doc of flightsNeedingMovement) {
    const flight = doc.data();
    const departureDate = toDate(flight.departure.scheduled);
    const flightCode = `${flight.airline.iata || "FA"}${flight.flight}`;

    console.log(`Checking movement for ${doc.id}`);

    const apiPath = `/flights/number/${encodeURIComponent(flightCode)}/${formatLocalDate(departureDate)}?dateLocalRole=Departure`;
    const data = await requestJson(apiPath, apiKey);
    const movementEntries = Array.isArray(data)
      ? data
      : data?.departure || data?.arrival
        ? [data]
        : [
            ...(data?.flights || []),
            ...(data?.departures || []),
            ...(data?.arrivals || []),
          ];

    let movement = movementEntries[0] || null;
    let closestDifference = Number.POSITIVE_INFINITY;

    for (const entry of movementEntries) {
      if (
        entry.departure?.airport?.iata !== flight.departure.airport.code ||
        entry.arrival?.airport?.iata !== flight.arrival.airport.code
      ) {
        continue;
      }

      const movementDeparture = toDate(
        entry.departure.scheduledTime.utc ||
          entry.departure.scheduledTime.local ||
          entry.departure.scheduledTime,
      );
      const difference = Math.abs(
        movementDeparture.getTime() - departureDate.getTime(),
      );

      if (difference < closestDifference) {
        movement = entry;
        closestDifference = difference;
      }
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
