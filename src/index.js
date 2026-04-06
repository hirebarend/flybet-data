const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

const SOUTH_AFRICAN_AIRPORTS = ['JNB', 'CPT', 'DUR'];
const FLIGHTS_DATA_FILE = path.join(__dirname, '..', 'data', 'flights.jsonl');
const API_BASE_URL = 'https://aerodatabox.p.rapidapi.com';
const API_DELAY_MS = 1000;
const WINDOW_HOURS = 12;
const SAST_OFFSET_MS = 2 * 60 * 60 * 1000;
const SETTLEMENT_ONLY = process.env.SETTLEMENT_ONLY === 'true';

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function toLocalTimeParam(date) {
  return new Date(date.getTime() + SAST_OFFSET_MS).toISOString().slice(0, 16);
}

function toUtcIso(value) {
  if (!value) { return null; }
  return value.replace(' ', 'T');
}

function parseFlightNumber(raw) {
  const match = (raw || '').match(/^([A-Z]{2})\s*(\d+)$/);
  return match
    ? { iataCode: match[1], number: match[2] }
    : { iataCode: '', number: raw || '' };
}

function formatFlightNumber(flight) {
  return `${flight.iataCode}${flight.number}`;
}

function generateFlightId(flight, scheduledDeparture) {
  const raw = `${formatFlightNumber(flight)}::${(scheduledDeparture || '').slice(0, 16)}`;
  return crypto.createHash('sha256').update(raw).digest('hex').slice(0, 16);
}

function createLookupKey(record) {
  return `${formatFlightNumber(record.flight)}::${(record.departure.scheduled || '').slice(0, 16)}`;
}

function isFlySafairFlight(entry) {
  return entry.airline?.iata === 'FA' || entry.number.startsWith('FA');
}

function mapDeparture(entry, airportCode) {
  const flight = parseFlightNumber(entry.number);
  const scheduledDeparture = toUtcIso(entry.departure?.scheduledTime?.utc);

  return {
    id: generateFlightId(flight, scheduledDeparture),
    flight,
    airline: { name: entry.airline?.name || 'FlySafair' },
    departure: {
      airport: {
        code: entry.departure?.airport?.iata || airportCode,
        name: entry.departure?.airport?.name || null,
      },
      scheduled: scheduledDeparture,
      actual: toUtcIso(entry.departure?.revisedTime?.utc) || null,
      terminal: entry.departure?.terminal || null,
      gate: entry.departure?.gate || null,
    },
    arrival: {
      airport: {
        code: entry.arrival?.airport?.iata || null,
        name: entry.arrival?.airport?.name || null,
      },
      scheduled: toUtcIso(entry.arrival?.scheduledTime?.utc) || null,
      actual: toUtcIso(entry.arrival?.revisedTime?.utc) || null,
      terminal: entry.arrival?.terminal || null,
    },
    status: entry.status || null,
    aircraft: {
      model: entry.aircraft?.model || null,
      registration: entry.aircraft?.reg || null,
    },
  };
}

async function fetchFromApi(apiPath, apiKey) {
  const url = `${API_BASE_URL}${apiPath}`;
  console.log(`  GET ${apiPath}`);

  const response = await fetch(url, {
    headers: {
      'Accept': 'application/json',
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

async function fetchDepartures(airportCode, from, to, apiKey) {
  const apiPath = `/flights/airports/iata/${airportCode}/${toLocalTimeParam(from)}/${toLocalTimeParam(to)}`
    + '?withLeg=true&direction=Departure&withCodeshared=false&withCargo=false&withPrivate=false';

  const data = await fetchFromApi(apiPath, apiKey);
  return (data?.departures || [])
    .filter(isFlySafairFlight)
    .map(entry => mapDeparture(entry, airportCode));
}

function mergeFlightData(existing, incoming) {
  if (incoming.departure.actual) { existing.departure.actual = incoming.departure.actual; }
  if (incoming.arrival.actual) { existing.arrival.actual = incoming.arrival.actual; }
  if (incoming.status && incoming.status !== 'Unknown') { existing.status = incoming.status; }
  if (incoming.departure.airport.name && !existing.departure.airport.name) {
    existing.departure.airport.name = incoming.departure.airport.name;
  }
  if (incoming.arrival.airport.name && !existing.arrival.airport.name) {
    existing.arrival.airport.name = incoming.arrival.airport.name;
  }
  if (incoming.aircraft?.model) { existing.aircraft.model = incoming.aircraft.model; }
  if (incoming.aircraft?.registration) { existing.aircraft.registration = incoming.aircraft.registration; }
}

function initializeFirestore() {
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!serviceAccountJson) { return null; }

  initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  return getFirestore();
}

async function syncToFirestore(db, flights) {
  console.log(`Syncing ${flights.length} flights to Firestore...`);
  const BATCH_SIZE = 500;

  for (let i = 0; i < flights.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const chunk = flights.slice(i, i + BATCH_SIZE);

    for (const flight of chunk) {
      batch.set(db.collection('flights').doc(flight.id), flight, { merge: true });
    }

    await batch.commit();
    console.log(`  Committed batch ${Math.floor(i / BATCH_SIZE) + 1} (${chunk.length} docs)`);
  }

  console.log('Firestore sync complete');
}

async function main() {
  const apiKey = process.env.AERODATABOX_API_KEY;
  if (!apiKey) {
    console.error('AERODATABOX_API_KEY environment variable is required');
    process.exit(1);
  }

  const db = initializeFirestore();

  const flightsByKey = new Map();
  if (fs.existsSync(FLIGHTS_DATA_FILE)) {
    const content = fs.readFileSync(FLIGHTS_DATA_FILE, 'utf8').trim();
    if (content) {
      for (const line of content.split('\n')) {
        const flight = JSON.parse(line);
        flightsByKey.set(createLookupKey(flight), flight);
      }
    }
  }
  console.log(`Loaded ${flightsByKey.size} existing flights`);

  const now = new Date();
  const pastStart = new Date(now.getTime() - WINDOW_HOURS * 60 * 60 * 1000);
  const futureEnd = new Date(now.getTime() + WINDOW_HOURS * 60 * 60 * 1000);

  console.log(`Run mode: ${SETTLEMENT_ONLY ? 'settlement-only (past flights)' : 'full (past + future flights)'}`);

  let addedCount = 0;
  let updatedCount = 0;

  for (const airportCode of SOUTH_AFRICAN_AIRPORTS) {
    try {
      const pastFlights = await fetchDepartures(airportCode, pastStart, now, apiKey);
      for (const flight of pastFlights) {
        const key = createLookupKey(flight);
        if (flightsByKey.has(key)) {
          mergeFlightData(flightsByKey.get(key), flight);
          updatedCount++;
        } else {
          flightsByKey.set(key, flight);
          addedCount++;
        }
      }
    } catch (error) {
      console.error(`  Past departures error ${airportCode}: ${error.message}`);
    }
    await delay(API_DELAY_MS);

    if (!SETTLEMENT_ONLY) {
      try {
        const futureFlights = await fetchDepartures(airportCode, now, futureEnd, apiKey);
        for (const flight of futureFlights) {
          const key = createLookupKey(flight);
          if (flightsByKey.has(key)) {
            mergeFlightData(flightsByKey.get(key), flight);
            updatedCount++;
          } else {
            flightsByKey.set(key, flight);
            addedCount++;
          }
        }
      } catch (error) {
        console.error(`  Future departures error ${airportCode}: ${error.message}`);
      }
      await delay(API_DELAY_MS);
    }
  }

  console.log(`Added ${addedCount} new flights, updated ${updatedCount} existing flights`);

  const allFlights = Array.from(flightsByKey.values());
  allFlights.sort((a, b) => (a.departure.scheduled || '').localeCompare(b.departure.scheduled || ''));

  const dataDir = path.dirname(FLIGHTS_DATA_FILE);
  if (!fs.existsSync(dataDir)) { fs.mkdirSync(dataDir, { recursive: true }); }
  fs.writeFileSync(FLIGHTS_DATA_FILE, allFlights.map(f => JSON.stringify(f)).join('\n') + '\n');
  console.log(`Saved ${allFlights.length} total flights to ${FLIGHTS_DATA_FILE}`);

  if (db) {
    await syncToFirestore(db, allFlights);
  } else {
    console.log('FIREBASE_SERVICE_ACCOUNT not set, skipping Firestore sync');
  }
}

main().catch(error => {
  console.error('Fatal:', error);
  process.exit(1);
});
