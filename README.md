# flybet-data

FlySafair flight data collector powered by the [AeroDataBox API](https://aerodatabox.com/).

## What it does

A GitHub Actions workflow runs every 3 hours to:

1. **Fetch upcoming flights** — Queries the FIDS (airport departures) endpoint for all FlySafair (FA) airports in South Africa for the next 24 hours
2. **Update past flights** — Queries the Flight Status endpoint for flights that should have departed, updating them with actual departure/arrival times
3. **Persist data** — Saves all flight records to `data/flights.jsonl` (JSON Lines format) and commits to the repository

## Data format

Each line in `data/flights.jsonl` is a JSON object:

```json
{"flightNumber":"FA101","departureAirport":"JNB","arrivalAirport":"CPT","scheduledDeparture":"2026-04-05T06:00:00+02:00","scheduledArrival":"2026-04-05T08:10:00+02:00","actualDeparture":null,"actualArrival":null,"status":"Expected"}
```

Fields:
- `flightNumber` — FlySafair flight number (e.g. FA101)
- `departureAirport` / `arrivalAirport` — IATA airport codes
- `scheduledDeparture` / `scheduledArrival` — Scheduled local times
- `actualDeparture` / `actualArrival` — Actual times (null until flight completes)
- `status` — Flight status (Expected, Departed, Arrived, Canceled, etc.)

## Airports covered

JNB, CPT, DUR, PLZ, BFN, GRJ, ELS

## Setup

1. Sign up for an [AeroDataBox API key](https://apimarket.aerodatabox.com/)
2. Add the key as a repository secret named `AERODATABOX_API_KEY`
3. The workflow runs automatically every 3 hours, or trigger manually via Actions → Update FlySafair Flights → Run workflow

## Local development

```bash
AERODATABOX_API_KEY=your_key node src/index.js
```

Requires Node.js 20+.