const BASE_URL = 'https://prod.api.market/api/v1/aedbx/aerodatabox';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function apiRequest(path, apiKey) {
  const url = `${BASE_URL}${path}`;
  console.log(`  GET ${path}`);

  const response = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'x-magicapi-key': apiKey,
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
 * Fetch departures from an airport within a local time window (max 12 hours).
 * Uses withLeg=true to include both departure and arrival info for each flight.
 */
async function getAirportDepartures(airportIata, fromLocal, toLocal, apiKey) {
  const path = `/flights/airports/iata/${airportIata}/${fromLocal}/${toLocal}`
    + '?withLeg=true&direction=Departure&withCodeshared=false&withCargo=false&withPrivate=false';
  const data = await apiRequest(path, apiKey);
  return data?.departures || [];
}

/**
 * Fetch flight status for a specific flight number on a given local date (YYYY-MM-DD).
 * Returns an array of flight instances for that day.
 */
async function getFlightStatus(flightNumber, dateLocal, apiKey) {
  const path = `/flights/number/${encodeURIComponent(flightNumber)}/${dateLocal}`;
  return await apiRequest(path, apiKey) || [];
}

module.exports = { getAirportDepartures, getFlightStatus, sleep };
