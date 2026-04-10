const crypto = require('crypto');
const { initializeApp, cert, getApps } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');

const SOUTH_AFRICAN_AIRPORTS = ['JNB', 'CPT', 'DUR'];
const API_BASE_URL = 'https://aerodatabox.p.rapidapi.com';
const API_DELAY_MS = 5000;
const FUTURE_WINDOW_HOURS = 12;
const ONE_HOUR_MS = 60 * 60 * 1000;
const FIFTEEN_MINUTES_MS = 15 * 60 * 1000;
const MINIMUM_PROFIT = 25;
const SAST_OFFSET_MS = 2 * 60 * 60 * 1000;

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function toDate(value) {
  if (value == null) { return null; }
  if (typeof value.toDate === 'function') { return value.toDate(); }
  const normalized = typeof value === 'string' ? value.replace(' ', 'T') : value;
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? null : date;
}

function toUtcIso(value) {
  const date = toDate(value);
  return date ? date.toISOString() : null;
}

function toLocalDateTimeParam(date) {
  return new Date(date.getTime() + SAST_OFFSET_MS).toISOString().slice(0, 16);
}

function toLocalDateParam(date) {
  return new Date(date.getTime() + SAST_OFFSET_MS).toISOString().slice(0, 10);
}

function parseFlightNumber(raw) {
  const match = (raw || '').match(/^([A-Z]{2})\s*(\d+)$/i);
  if (!match) {
    return { airlineIata: 'FA', flightNumber: String(raw || '').trim() };
  }

  return {
    airlineIata: match[1].toUpperCase(),
    flightNumber: match[2],
  };
}

function formatFlightCode(airlineIata, flightNumber) {
  return `${airlineIata || 'FA'}${flightNumber || ''}`;
}

function generateFlightId(airlineIata, flightNumber, scheduledDeparture) {
  const raw = `${formatFlightCode(airlineIata, flightNumber)}::${(scheduledDeparture || '').slice(0, 16)}`;
  return crypto.createHash('sha256').update(raw).digest('hex').slice(0, 16);
}

function isFlySafairFlight(entry) {
  return entry.airline?.iata === 'FA' || String(entry.number || '').startsWith('FA');
}

function getActualTime(node) {
  return toUtcIso(node?.revisedTime?.utc || null);
}

function getScheduledTime(node) {
  return toUtcIso(
    node?.scheduledTime?.utc
    || node?.scheduledTime?.local
    || node?.scheduledTime
    || null
  );
}

function mapAirportDeparture(entry, airportCode) {
  const { airlineIata, flightNumber } = parseFlightNumber(entry.number);
  const scheduledDeparture = getScheduledTime(entry.departure);

  return {
    id: generateFlightId(airlineIata, flightNumber, scheduledDeparture),
    flight: flightNumber,
    airline: {
      iata: airlineIata || entry.airline?.iata || 'FA',
    },
    departure: {
      airport: {
        code: entry.departure?.airport?.iata || airportCode,
      },
      scheduled: scheduledDeparture,
      actual: null,
    },
    arrival: {
      airport: {
        code: entry.arrival?.airport?.iata || null,
      },
      scheduled: getScheduledTime(entry.arrival),
      actual: null,
    },
    aircraft: {
      model: entry.aircraft?.model || null,
    },
    status: entry.status || null,
    cancelled: false,
  };
}

async function fetchFromApi(apiPath, apiKey) {
  const url = `${API_BASE_URL}${apiPath}`;
  console.log(`  GET ${apiPath}`);

  const response = await fetch(url, {
    headers: {
      Accept: 'application/json',
      'x-rapidapi-host': 'aerodatabox.p.rapidapi.com',
      'x-rapidapi-key': apiKey,
    },
  });

  if (response.status === 204) { return null; }

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`HTTP ${response.status}: ${body}`);
  }

  return response.json();
}

async function fetchFutureDepartures(airportCode, from, to, apiKey) {
  const apiPath = `/flights/airports/iata/${airportCode}/${toLocalDateTimeParam(from)}/${toLocalDateTimeParam(to)}`
    + '?withLeg=true&direction=Departure&withCodeshared=false&withCargo=false&withPrivate=false';

  const data = await fetchFromApi(apiPath, apiKey);
  return (data?.departures || [])
    .filter(isFlySafairFlight)
    .map(entry => mapAirportDeparture(entry, airportCode));
}

function extractFlightEntries(data) {
  if (Array.isArray(data)) { return data; }
  if (data?.departure || data?.arrival) { return [data]; }

  const entries = [];
  for (const key of ['flights', 'departures', 'arrivals']) {
    if (Array.isArray(data?.[key])) {
      entries.push(...data[key]);
    }
  }

  return entries;
}

async function fetchFlightMovement(flight, apiKey) {
  const departureDate = toDate(flight.departure?.scheduled);
  if (!departureDate) { return null; }

  const flightCode = formatFlightCode(flight.airline?.iata, flight.flight);
  const apiPath = `/flights/number/${encodeURIComponent(flightCode)}/${toLocalDateParam(departureDate)}?dateLocalRole=Departure`;
  const data = await fetchFromApi(apiPath, apiKey);
  const entries = extractFlightEntries(data);

  if (entries.length === 0) { return null; }

  const targetTime = departureDate.getTime();
  let fallbackMatch = null;
  let fallbackDiff = Number.POSITIVE_INFINITY;
  let bestMatch = null;
  let bestDiff = Number.POSITIVE_INFINITY;

  for (const entry of entries) {
    const scheduledDeparture = getScheduledTime(entry.departure);
    const scheduledDate = toDate(scheduledDeparture);
    if (!scheduledDate) { continue; }

    const diff = Math.abs(scheduledDate.getTime() - targetTime);
    if (diff < fallbackDiff) {
      fallbackMatch = entry;
      fallbackDiff = diff;
    }

    const sameDepartureAirport = !flight.departure?.airport?.code
      || entry.departure?.airport?.iata === flight.departure.airport.code;
    const sameArrivalAirport = !flight.arrival?.airport?.code
      || entry.arrival?.airport?.iata === flight.arrival.airport.code;

    if (!sameDepartureAirport || !sameArrivalAirport) { continue; }

    if (diff < bestDiff) {
      bestMatch = entry;
      bestDiff = diff;
    }
  }

  return bestMatch || fallbackMatch;
}

function initializeFirestore() {
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!serviceAccountJson) {
    throw new Error('FIREBASE_SERVICE_ACCOUNT environment variable is required');
  }

  if (getApps().length === 0) {
    initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  }

  return getFirestore();
}

async function getFutureFetchStart(db, now) {
  const snapshot = await db.collection('flights')
    .orderBy('departure.scheduled', 'desc')
    .limit(1)
    .get();

  const latestScheduled = snapshot.empty
    ? null
    : toDate(snapshot.docs[0].data().departure?.scheduled);

  if (!latestScheduled || latestScheduled < now) {
    return now;
  }

  return latestScheduled;
}

async function upsertFlights(db, flights) {
  if (flights.length === 0) { return; }

  const batch = db.batch();
  for (const flight of flights) {
    batch.set(db.collection('flights').doc(flight.id), flight, { merge: true });
  }

  await batch.commit();
}

async function syncFutureFlights(db, apiKey, now) {
  const futureStart = await getFutureFetchStart(db, now);
  const futureEnd = new Date(now.getTime() + FUTURE_WINDOW_HOURS * ONE_HOUR_MS);

  console.log(`Future flight window: ${futureStart.toISOString()} -> ${futureEnd.toISOString()}`);

  if (futureStart >= futureEnd) {
    console.log('Future flight window already covered by Firestore');
    return [];
  }

  const allFlights = [];

  for (const airportCode of SOUTH_AFRICAN_AIRPORTS) {
    try {
      const flights = await fetchFutureDepartures(airportCode, futureStart, futureEnd, apiKey);
      allFlights.push(...flights);
      console.log(`  ${airportCode}: fetched ${flights.length} flights`);
    } catch (error) {
      console.error(`  Future departures error ${airportCode}: ${error.message}`);
    }

    await delay(API_DELAY_MS);
  }

  let storedCount = 0;
  for (let i = 0; i < allFlights.length; i += 400) {
    const chunk = allFlights.slice(i, i + 400);
    await upsertFlights(db, chunk);
    storedCount += chunk.length;
  }

  console.log(`Stored ${storedCount} future flights`);
  return allFlights;
}

async function getFlightsNeedingMovementRefresh(db, now) {
  const cutoffIso = new Date(now.getTime() - ONE_HOUR_MS).toISOString();
  const snapshot = await db.collection('flights')
    .where('arrival.scheduled', '<=', cutoffIso)
    .orderBy('arrival.scheduled')
    .get();

  return snapshot.docs
    .map(doc => ({ id: doc.id, flight: doc.data() }))
    .filter(({ flight }) => {
      if (flight.cancelled === true || !flight.arrival?.scheduled) {
        return false;
      }

      return !flight.departure?.actual || !flight.arrival?.actual;
    });
}

function calculatePayouts(bets, winningOutcome, allOutcomes) {
  const marketBets = bets.filter(bet => allOutcomes.includes(bet.outcome));
  if (marketBets.length === 0) { return []; }

  const winners = marketBets.filter(bet => bet.outcome === winningOutcome);
  const losers = marketBets.filter(bet => bet.outcome !== winningOutcome);
  const totalWinnerStake = winners.reduce((sum, bet) => sum + bet.amount, 0);
  const totalLoserStake = losers.reduce((sum, bet) => sum + bet.amount, 0);
  const payouts = [];

  for (const bet of winners) {
    const proportionalShare = totalLoserStake === 0
      ? 0
      : Math.round((bet.amount / totalWinnerStake) * totalLoserStake);
    const profit = Math.max(proportionalShare, MINIMUM_PROFIT);
    payouts.push({ betId: bet.id, userId: bet.userId, payout: bet.amount + profit });
  }

  for (const bet of losers) {
    payouts.push({ betId: bet.id, userId: bet.userId, payout: 0 });
  }

  return payouts;
}

async function settleFlightBets(db, flightId) {
  const flightRef = db.collection('flights').doc(flightId);
  let settled = false;
  let unsettledBetCount = 0;
  let winnerCount = 0;

  await db.runTransaction(async (transaction) => {
    const flightDoc = await transaction.get(flightRef);
    const flight = flightDoc.data();

    if (!flight) { return; }
    if (!flight.arrival?.actual && flight.cancelled !== true) { return; }

    const betsQuery = db.collection('bets').where('flight_id', '==', flightId);
    const betsSnapshot = await transaction.get(betsQuery);
    const unsettledBets = betsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(bet => !bet.settled);

    if (unsettledBets.length === 0) { return; }

    unsettledBetCount = unsettledBets.length;

    const departureScheduled = toDate(flight.departure?.scheduled);
    const departureActual = toDate(flight.departure?.actual);
    const arrivalScheduled = toDate(flight.arrival?.scheduled);
    const arrivalActual = toDate(flight.arrival?.actual);
    const departureIsDelayed = !departureActual
      || !departureScheduled
      || (departureActual.getTime() - departureScheduled.getTime()) > FIFTEEN_MINUTES_MS;
    const arrivalIsDelayed = !arrivalActual
      || !arrivalScheduled
      || (arrivalActual.getTime() - arrivalScheduled.getTime()) > FIFTEEN_MINUTES_MS;
    const isCancelled = flight.cancelled === true;

    const markets = [
      {
        outcomes: ['onTimeDeparture', 'delayedDeparture'],
        winner: departureIsDelayed ? 'delayedDeparture' : 'onTimeDeparture',
      },
      {
        outcomes: ['onTimeArrival', 'delayedArrival'],
        winner: arrivalIsDelayed ? 'delayedArrival' : 'onTimeArrival',
      },
      {
        outcomes: ['cancelled', 'notCancelled'],
        winner: isCancelled ? 'cancelled' : 'notCancelled',
      },
    ];

    const allPayouts = [];
    const userCredits = new Map();

    for (const { outcomes, winner } of markets) {
      const payouts = calculatePayouts(unsettledBets, winner, outcomes);
      for (const { betId, userId, payout } of payouts) {
        allPayouts.push({ betId, payout });
        if (payout > 0) {
          userCredits.set(userId, (userCredits.get(userId) || 0) + payout);
        }
      }
    }

    winnerCount = allPayouts.filter(payout => payout.payout > 0).length;

    for (const [userId, credit] of userCredits) {
      transaction.update(db.collection('users').doc(userId), {
        balance: FieldValue.increment(credit),
      });
    }

    for (const { betId, payout } of allPayouts) {
      transaction.update(db.collection('bets').doc(betId), {
        settled: true,
        payout,
      });
    }

    settled = true;
  });

  if (settled) {
    console.log(`  Settled ${unsettledBetCount} bets for ${flightId} (${winnerCount} winning payouts)`);
  } else {
    console.log(`  No unsettled bets to process for ${flightId}`);
  }
}

async function refreshFlightMovements(db, apiKey) {
  const candidates = await getFlightsNeedingMovementRefresh(db, new Date());
  console.log(`Flights needing movement refresh: ${candidates.length}`);

  for (const candidate of candidates) {
    const { id, flight } = candidate;
    console.log(`Checking movement for ${id}`);

    try {
      const movement = await fetchFlightMovement(flight, apiKey);
      const departureActual = getActualTime(movement?.departure);
      const arrivalActual = getActualTime(movement?.arrival);
      const updates = {};
      let shouldSettle = false;
      const needsDepartureActual = !flight.departure?.actual;
      const needsArrivalActual = !flight.arrival?.actual;

      if (movement?.status) {
        updates.status = movement.status;
      }

      if (movement?.aircraft?.model) {
        updates.aircraft = { model: movement.aircraft.model };
      }

      if (needsDepartureActual && departureActual) {
        updates['departure.actual'] = departureActual;
        updates.cancelled = false;
      }

      if (needsArrivalActual && arrivalActual) {
        updates['arrival.actual'] = arrivalActual;
        updates.cancelled = false;
        shouldSettle = true;
      } else if (needsArrivalActual) {
        updates.cancelled = true;
        shouldSettle = true;
      }

      if (Object.keys(updates).length > 0) {
        await db.collection('flights').doc(id).update(updates);
      }

      if (shouldSettle) {
        await settleFlightBets(db, id);
      }
    } catch (error) {
      console.error(`  Movement refresh error ${id}: ${error.message}`);
    }

    await delay(API_DELAY_MS);
  }
}

async function main() {
  const apiKey = process.env.AERODATABOX_API_KEY;
  if (!apiKey) {
    throw new Error('AERODATABOX_API_KEY environment variable is required');
  }

  const db = initializeFirestore();
  const now = new Date();

  console.log('Starting FlySafair sync');
  await syncFutureFlights(db, apiKey, now);
  await refreshFlightMovements(db, apiKey);
  console.log('Run complete');
}

main().catch(error => {
  console.error('Fatal:', error);
  process.exit(1);
});
