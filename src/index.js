const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

const FLYSAFAIR_IATA_CODE = 'FA';
const FLYSAFAIR_AIRLINE_NAME = 'FlySafair';
const SOUTH_AFRICAN_AIRPORTS = ['JNB', 'CPT', 'DUR', 'PLZ', 'BFN', 'GRJ', 'ELS'];
const FLIGHTS_DATA_FILE = path.join(__dirname, '..', 'data', 'flights.jsonl');
const API_BASE_URL = 'https://aerodatabox.p.rapidapi.com';
const API_REQUEST_DELAY_MS = 1000;
const MAX_DEPARTURES_WINDOW_HOURS = 12; // AeroDataBox airport departures endpoint limit
const UPCOMING_FLIGHTS_WINDOW_HOURS = 24; // How far ahead to fetch flights
const PAST_STATUS_UPDATE_WINDOW_HOURS = 48; // How far back to update flight statuses

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Fetch JSON from the AeroDataBox API. Returns null for 204 (no content).
 */
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

  if (response.status === 204) return null;

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`HTTP ${response.status}: ${body}`);
  }

  return response.json();
}

/**
 * Convert a UTC Date to South African Standard Time (UTC+2) string for API parameters.
 * Returns format YYYY-MM-DDTHH:mm (required by AeroDataBox local-time endpoints).
 */
function toSouthAfricanLocalTimeString(date) {
  const southAfricanTime = new Date(date.getTime() + 2 * 60 * 60 * 1000);
  return southAfricanTime.toISOString().slice(0, 16);
}

/**
 * Normalize a date-time string to ISO 8601 format.
 * The API may return "2026-04-05 11:30+02:00"; this converts to "2026-04-05T11:30+02:00".
 */
function normalizeToIsoDateTime(dateTimeString) {
  if (!dateTimeString) return null;
  return dateTimeString.replace(' ', 'T');
}

/**
 * Parse a flight number string like "FA 212" into its airline code and number components.
 */
function parseFlightNumber(flightNumberString) {
  const match = (flightNumberString || '').match(/^([A-Z]{2})\s*(\d+)$/);
  if (match) {
    return { iataCode: match[1], number: match[2] };
  }
  return { iataCode: '', number: flightNumberString || '' };
}

/**
 * Format a parsed flight number object back to a full string, e.g. "FA212".
 */
function formatFullFlightNumber(flightNumber) {
  return `${flightNumber.iataCode}${flightNumber.number}`;
}

/**
 * Generate a deterministic unique ID for a flight based on its number and scheduled departure.
 */
function generateFlightId(flightNumber, scheduledDepartureTime) {
  const raw = `${formatFullFlightNumber(flightNumber)}::${(scheduledDepartureTime || '').slice(0, 16)}`;
  return crypto.createHash('sha256').update(raw).digest('hex').slice(0, 16);
}

/**
 * Create a lookup key for deduplication: "FA212::2026-04-05T11:30".
 */
function createFlightLookupKey(flight) {
  return `${formatFullFlightNumber(flight.flight)}::${(flight.departure.scheduled || '').slice(0, 16)}`;
}

/**
 * Check if a departure entry belongs to FlySafair.
 */
function isFlySafairFlight(departureEntry) {
  return departureEntry.airline?.iata === FLYSAFAIR_IATA_CODE
    || departureEntry.number.startsWith(FLYSAFAIR_IATA_CODE);
}

/**
 * Convert an API departure entry into the structured flight record format.
 */
function mapDepartureToFlightRecord(departureEntry, queriedAirportCode) {
  const flightNumber = parseFlightNumber(departureEntry.number);
  const scheduledDeparture = normalizeToIsoDateTime(departureEntry.departure?.scheduledTime?.local);

  return {
    id: generateFlightId(flightNumber, scheduledDeparture),
    flight: flightNumber,
    airline: {
      name: departureEntry.airline?.name || FLYSAFAIR_AIRLINE_NAME,
    },
    departure: {
      airport: {
        code: departureEntry.departure?.airport?.iata || queriedAirportCode,
        name: departureEntry.departure?.airport?.name || null,
      },
      scheduled: scheduledDeparture,
      actual: normalizeToIsoDateTime(departureEntry.departure?.revisedTime?.local) || null,
      terminal: departureEntry.departure?.terminal || null,
      gate: departureEntry.departure?.gate || null,
    },
    arrival: {
      airport: {
        code: departureEntry.arrival?.airport?.iata || null,
        name: departureEntry.arrival?.airport?.name || null,
      },
      scheduled: normalizeToIsoDateTime(departureEntry.arrival?.scheduledTime?.local) || null,
      actual: normalizeToIsoDateTime(departureEntry.arrival?.revisedTime?.local) || null,
      terminal: departureEntry.arrival?.terminal || null,
    },
    status: departureEntry.status || null,
    aircraft: {
      model: departureEntry.aircraft?.model || null,
      registration: departureEntry.aircraft?.reg || null,
    },
  };
}

/**
 * Migrate an old-format flight record to the new structured format.
 */
function migrateOldFlightRecord(record) {
  if (record.flight && record.id) return record; // Already in new format

  const flightNumber = parseFlightNumber(record.flightNumber);
  const scheduledDeparture = normalizeToIsoDateTime(record.scheduledDeparture);

  return {
    id: generateFlightId(flightNumber, scheduledDeparture),
    flight: flightNumber,
    airline: { name: FLYSAFAIR_AIRLINE_NAME },
    departure: {
      airport: {
        code: record.departureAirport || null,
        name: null,
      },
      scheduled: scheduledDeparture,
      actual: normalizeToIsoDateTime(record.actualDeparture) || null,
      terminal: null,
      gate: null,
    },
    arrival: {
      airport: {
        code: record.arrivalAirport || null,
        name: null,
      },
      scheduled: normalizeToIsoDateTime(record.scheduledArrival) || null,
      actual: normalizeToIsoDateTime(record.actualArrival) || null,
      terminal: null,
    },
    status: record.status || null,
    aircraft: { model: null, registration: null },
  };
}

/**
 * Initialize Firestore using a service account JSON from an environment variable.
 * Returns null if the environment variable is not set.
 */
function initializeFirestore() {
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!serviceAccountJson) return null;

  let serviceAccount;
  try {
    serviceAccount = JSON.parse(serviceAccountJson);
  } catch (error) {
    throw new Error(`Failed to parse FIREBASE_SERVICE_ACCOUNT: ${error.message}`);
  }

  initializeApp({ credential: cert(serviceAccount) });
  return getFirestore();
}

/**
 * Push all flight records to the Firestore "flights" collection.
 * Uses the flight id as the document ID for upsert behavior.
 */
async function syncToFirestore(db, flights) {
  console.log(`Syncing ${flights.length} flights to Firestore...`);
  const BATCH_SIZE = 500; // Firestore batch write limit

  for (let i = 0; i < flights.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const chunk = flights.slice(i, i + BATCH_SIZE);

    for (const flight of chunk) {
      const docRef = db.collection('flights').doc(flight.id);
      batch.set(docRef, flight);
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

  // Load existing flights from JSONL, migrating old records to the new schema
  const existingFlights = [];
  if (fs.existsSync(FLIGHTS_DATA_FILE)) {
    const content = fs.readFileSync(FLIGHTS_DATA_FILE, 'utf8').trim();
    if (content) {
      for (const line of content.split('\n')) {
        existingFlights.push(migrateOldFlightRecord(JSON.parse(line)));
      }
    }
  }
  console.log(`Loaded ${existingFlights.length} existing flights`);

  const flightsByKey = new Map();
  for (const flight of existingFlights) {
    flightsByKey.set(createFlightLookupKey(flight), flight);
  }

  // Fetch upcoming flights (next 24 hours) via airport departures endpoint.
  // The endpoint has a 12-hour max window, so we split into two time chunks.
  console.log('Fetching upcoming flights...');
  const now = new Date();
  const midPoint = new Date(now.getTime() + MAX_DEPARTURES_WINDOW_HOURS * 60 * 60 * 1000);
  const endPoint = new Date(now.getTime() + UPCOMING_FLIGHTS_WINDOW_HOURS * 60 * 60 * 1000);
  const timeWindows = [
    [toSouthAfricanLocalTimeString(now), toSouthAfricanLocalTimeString(midPoint)],
    [toSouthAfricanLocalTimeString(midPoint), toSouthAfricanLocalTimeString(endPoint)],
  ];

  const upcomingFlights = [];
  for (const airportCode of SOUTH_AFRICAN_AIRPORTS) {
    for (const [windowStart, windowEnd] of timeWindows) {
      try {
        const apiPath = `/flights/airports/iata/${airportCode}/${windowStart}/${windowEnd}`
          + '?withLeg=true&direction=Departure&withCodeshared=false&withCargo=false&withPrivate=false';
        const data = await fetchFromApi(apiPath, apiKey);
        const departures = data?.departures || [];
        for (const departure of departures) {
          if (!isFlySafairFlight(departure)) continue;
          upcomingFlights.push(mapDepartureToFlightRecord(departure, airportCode));
        }
      } catch (error) {
        console.error(`  Departures error ${airportCode} [${windowStart}..${windowEnd}]: ${error.message}`);
      }
      await delay(API_REQUEST_DELAY_MS);
    }
  }
  console.log(`Found ${upcomingFlights.length} FlySafair departures`);

  // Merge: add new flights, update existing ones with fresh data
  let addedCount = 0;
  for (const flight of upcomingFlights) {
    const key = createFlightLookupKey(flight);
    if (!flightsByKey.has(key)) {
      flightsByKey.set(key, flight);
      addedCount++;
    } else {
      const existingFlight = flightsByKey.get(key);
      if (flight.departure.actual && !existingFlight.departure.actual) {
        existingFlight.departure.actual = flight.departure.actual;
      }
      if (flight.arrival.actual && !existingFlight.arrival.actual) {
        existingFlight.arrival.actual = flight.arrival.actual;
      }
      if (flight.status && flight.status !== 'Unknown') {
        existingFlight.status = flight.status;
      }
      // Update airport names if newly available
      if (flight.departure.airport.name && !existingFlight.departure.airport.name) {
        existingFlight.departure.airport.name = flight.departure.airport.name;
      }
      if (flight.arrival.airport.name && !existingFlight.arrival.airport.name) {
        existingFlight.arrival.airport.name = flight.arrival.airport.name;
      }
    }
  }
  console.log(`Added ${addedCount} new flights`);

  // Update past flights with actual departure/arrival times via flight status endpoint
  console.log('Updating past flight statuses...');
  const allFlights = Array.from(flightsByKey.values());
  const pastStatusCutoff = new Date(now.getTime() - PAST_STATUS_UPDATE_WINDOW_HOURS * 60 * 60 * 1000);
  let updatedStatusCount = 0;

  for (const flight of allFlights) {
    if (!flight.departure.scheduled) continue;

    const scheduledDepartureTime = new Date(flight.departure.scheduled);
    if (scheduledDepartureTime > now) continue;
    if (scheduledDepartureTime < pastStatusCutoff) continue;
    if (flight.status === 'Canceled') continue;
    if (flight.departure.actual && flight.arrival.actual) continue;

    try {
      const flightDateLocal = flight.departure.scheduled.slice(0, 10);
      const fullFlightNumber = formatFullFlightNumber(flight.flight);
      const statusApiPath = `/flights/number/${encodeURIComponent(fullFlightNumber)}/${flightDateLocal}`;
      const statusResults = await fetchFromApi(statusApiPath, apiKey) || [];

      for (const result of statusResults) {
        const resultScheduledDeparture = normalizeToIsoDateTime(result.departure?.scheduledTime?.local || '');
        if ((resultScheduledDeparture || '').slice(0, 16) === flight.departure.scheduled.slice(0, 16)) {
          flight.departure.actual = normalizeToIsoDateTime(result.departure?.revisedTime?.local)
            || normalizeToIsoDateTime(result.departure?.runwayTime?.local)
            || flight.departure.actual;
          flight.arrival.actual = normalizeToIsoDateTime(result.arrival?.revisedTime?.local)
            || normalizeToIsoDateTime(result.arrival?.runwayTime?.local)
            || flight.arrival.actual;
          if (result.status !== 'Unknown') flight.status = result.status;
          // Update aircraft info if available from status endpoint
          if (result.aircraft?.model) flight.aircraft.model = result.aircraft.model;
          if (result.aircraft?.reg) flight.aircraft.registration = result.aircraft.reg;
          updatedStatusCount++;
          break;
        }
      }
    } catch (error) {
      console.error(`  Status error ${formatFullFlightNumber(flight.flight)}: ${error.message}`);
    }
    await delay(API_REQUEST_DELAY_MS);
  }
  console.log(`Updated ${updatedStatusCount} flights with actual times`);

  // Save all flights sorted by scheduled departure time
  const dataDirectory = path.dirname(FLIGHTS_DATA_FILE);
  if (!fs.existsSync(dataDirectory)) fs.mkdirSync(dataDirectory, { recursive: true });
  allFlights.sort((a, b) => (a.departure.scheduled || '').localeCompare(b.departure.scheduled || ''));
  fs.writeFileSync(FLIGHTS_DATA_FILE, allFlights.map(f => JSON.stringify(f)).join('\n') + '\n');
  console.log(`Saved ${allFlights.length} total flights to ${FLIGHTS_DATA_FILE}`);

  // Sync to Firestore if configured
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
