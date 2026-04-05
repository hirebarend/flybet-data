const fs = require('fs');
const path = require('path');
const { getAirportDepartures, getFlightStatus, sleep } = require('./api');

// FlySafair (IATA: FA) operates domestic routes across these South African airports
const AIRPORTS = ['JNB', 'CPT', 'DUR', 'PLZ', 'BFN', 'GRJ', 'ELS'];
const DATA_FILE = path.join(__dirname, '..', 'data', 'flights.jsonl');
const API_DELAY_MS = 1000;
const FIDS_MAX_WINDOW_HOURS = 12; // AeroDataBox FIDS endpoint limit
const UPCOMING_WINDOW_HOURS = 24; // How far ahead to fetch flights
const PAST_UPDATE_WINDOW_HOURS = 48; // How far back to update flight statuses

// --- Helpers ---

/**
 * Convert a UTC Date to SAST (UTC+2) formatted as YYYY-MM-DDTHH:mm.
 * Used for AeroDataBox API local time parameters.
 */
function toSASTString(date) {
  const sast = new Date(date.getTime() + 2 * 60 * 60 * 1000);
  return sast.toISOString().slice(0, 16);
}

/**
 * Unique key for a flight: number + scheduled departure time (to minute precision).
 */
function flightKey(f) {
  return `${f.flightNumber}::${(f.scheduledDeparture || '').slice(0, 16)}`;
}

/**
 * Check if a FIDS flight entry belongs to FlySafair.
 */
function isFlySafair(dep) {
  return dep.airline?.iata === 'FA' || dep.number.startsWith('FA');
}

// --- Data I/O ---

function loadFlights() {
  if (!fs.existsSync(DATA_FILE)) return [];
  const content = fs.readFileSync(DATA_FILE, 'utf8').trim();
  if (!content) return [];
  return content.split('\n').map(line => JSON.parse(line));
}

function saveFlights(flights) {
  const dir = path.dirname(DATA_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  flights.sort((a, b) => (a.scheduledDeparture || '').localeCompare(b.scheduledDeparture || ''));
  fs.writeFileSync(DATA_FILE, flights.map(f => JSON.stringify(f)).join('\n') + '\n');
}

// --- Fetch upcoming flights via FIDS ---

function parseFIDSFlight(dep, queriedAirport) {
  return {
    flightNumber: dep.number,
    departureAirport: dep.departure?.airport?.iata || queriedAirport,
    arrivalAirport: dep.arrival?.airport?.iata || null,
    scheduledDeparture: dep.departure?.scheduledTime?.local || null,
    scheduledArrival: dep.arrival?.scheduledTime?.local || null,
    actualDeparture: dep.departure?.revisedTime?.local || null,
    actualArrival: dep.arrival?.revisedTime?.local || null,
    status: dep.status,
  };
}

async function fetchUpcomingFlights(apiKey) {
  const now = new Date();
  const mid = new Date(now.getTime() + FIDS_MAX_WINDOW_HOURS * 60 * 60 * 1000);
  const end = new Date(now.getTime() + UPCOMING_WINDOW_HOURS * 60 * 60 * 1000);

  // FIDS endpoint has a 12-hour max window, so split into two chunks
  const windows = [
    [toSASTString(now), toSASTString(mid)],
    [toSASTString(mid), toSASTString(end)],
  ];

  const flights = [];

  for (const airport of AIRPORTS) {
    for (const [from, to] of windows) {
      try {
        const departures = await getAirportDepartures(airport, from, to, apiKey);
        for (const dep of departures) {
          if (!isFlySafair(dep)) continue;
          flights.push(parseFIDSFlight(dep, airport));
        }
      } catch (err) {
        console.error(`  FIDS error ${airport} [${from}..${to}]: ${err.message}`);
      }
      await sleep(API_DELAY_MS);
    }
  }

  return flights;
}

// --- Update past flights with actual times ---

async function updatePastFlights(flights, apiKey) {
  const now = new Date();
  const cutoff = new Date(now.getTime() - PAST_UPDATE_WINDOW_HOURS * 60 * 60 * 1000);
  let updated = 0;

  for (const flight of flights) {
    if (!flight.scheduledDeparture) continue;

    const depTime = new Date(flight.scheduledDeparture);
    if (depTime > now) continue;
    if (depTime < cutoff) continue;
    if (flight.status === 'Canceled') continue;
    if (flight.actualDeparture && flight.actualArrival) continue;

    try {
      const dateLocal = flight.scheduledDeparture.slice(0, 10);
      const results = await getFlightStatus(flight.flightNumber, dateLocal, apiKey);

      for (const r of results) {
        const rDep = (r.departure?.scheduledTime?.local || '').slice(0, 16);
        if (rDep === flight.scheduledDeparture.slice(0, 16)) {
          flight.actualDeparture = r.departure?.revisedTime?.local
            || r.departure?.runwayTime?.local
            || flight.actualDeparture;
          flight.actualArrival = r.arrival?.revisedTime?.local
            || r.arrival?.runwayTime?.local
            || flight.actualArrival;
          if (r.status !== 'Unknown') flight.status = r.status;
          updated++;
          break;
        }
      }
    } catch (err) {
      console.error(`  Status error ${flight.flightNumber}: ${err.message}`);
    }
    await sleep(API_DELAY_MS);
  }

  return updated;
}

// --- Main ---

async function main() {
  const apiKey = process.env.AERODATABOX_API_KEY;
  if (!apiKey) {
    console.error('AERODATABOX_API_KEY environment variable is required');
    process.exit(1);
  }

  // Load existing data
  const existing = loadFlights();
  console.log(`Loaded ${existing.length} existing flights`);

  const flightMap = new Map();
  for (const f of existing) flightMap.set(flightKey(f), f);

  // Fetch upcoming flights (next 24 hours)
  console.log('Fetching upcoming flights...');
  const upcoming = await fetchUpcomingFlights(apiKey);
  console.log(`Found ${upcoming.length} FlySafair departures from FIDS`);

  // Merge: add new flights, update existing ones with fresh data
  let added = 0;
  for (const f of upcoming) {
    const key = flightKey(f);
    if (!flightMap.has(key)) {
      flightMap.set(key, f);
      added++;
    } else {
      const prev = flightMap.get(key);
      if (f.actualDeparture && !prev.actualDeparture) prev.actualDeparture = f.actualDeparture;
      if (f.actualArrival && !prev.actualArrival) prev.actualArrival = f.actualArrival;
      if (f.status && f.status !== 'Unknown') prev.status = f.status;
    }
  }
  console.log(`Added ${added} new flights`);

  // Update past flights with actual departure/arrival times
  console.log('Updating past flight statuses...');
  const allFlights = Array.from(flightMap.values());
  const updatedCount = await updatePastFlights(allFlights, apiKey);
  console.log(`Updated ${updatedCount} flights with actual times`);

  // Save
  saveFlights(allFlights);
  console.log(`Saved ${allFlights.length} total flights to ${DATA_FILE}`);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
