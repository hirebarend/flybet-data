# flybet-data

FlySafair flight data collector and bet settlement system powered by the [AeroDataBox API](https://aerodatabox.com/) and [Firebase Firestore](https://firebase.google.com/docs/firestore).

## What it does

A GitHub Actions workflow runs every 3 hours to:

1. **Fetch past departures** — Queries the FIDS (airport departures) endpoint for all FlySafair (FA) airports in South Africa for the previous 12 hours, updating existing flights with revised/actual times
2. **Fetch upcoming departures** — Queries the same endpoint for the next 12 hours to discover new flights
3. **Persist data** — Saves all flight records to `data/flights.jsonl` (JSON Lines format), commits to the repository, and syncs to Firestore
4. **Settle bets** — Evaluates unsettled flights that have passed the 3-hour settlement window and pays out winners using pari-mutuel logic with a R25 minimum profit per winning bet

## Data format

Each line in `data/flights.jsonl` is a JSON object. All timestamps are in UTC ISO 8601 format:

```json
{"id":"a1b2c3d4e5f6g7h8","flight":{"iataCode":"FA","number":"101"},"airline":{"name":"FlySafair"},"departure":{"airport":{"code":"JNB","name":"O.R. Tambo International"},"scheduled":"2026-04-05T04:00","actual":null,"terminal":"B","gate":"B2"},"arrival":{"airport":{"code":"CPT","name":"Cape Town International"},"scheduled":"2026-04-05T06:10","actual":null,"terminal":null},"status":"Expected","aircraft":{"model":"Boeing 737-800","registration":"ZS-ABC"}}
```

Key fields:
- `id` — Deterministic SHA-256 hash based on flight number + scheduled departure
- `flight` — Parsed flight number (`iataCode` + `number`)
- `departure.scheduled` / `arrival.scheduled` — Scheduled times (UTC)
- `departure.actual` / `arrival.actual` — Revised/actual times (UTC, null until available)
- `status` — Flight status (Expected, Departed, Arrived, Canceled, etc.)

## Bet settlement

Settlement runs after flight data collection. A flight is eligible for settlement 3 hours after both its departure and arrival reference times. Three markets are evaluated per flight:

| Market | On-time outcome | Delayed outcome |
|---|---|---|
| Departure | `onTimeDeparture` | `delayedDeparture` |
| Arrival | `onTimeArrival` | `delayedArrival` |
| Cancellation | `notCancelled` | `cancelled` |

A departure or arrival is considered **delayed** if the actual time is more than 15 minutes after the scheduled time, or if no actual time exists. A flight is **cancelled** if neither actual departure nor actual arrival exists.

Winners receive their stake back plus a proportional share of the losing pool (pari-mutuel). Each winning bet is guaranteed a minimum profit of **R25** — the house covers any shortfall.

## Airports covered

JNB, CPT, DUR, PLZ, BFN, GRJ, ELS

## AeroDataBox API cost analysis

The project uses the [AeroDataBox API on RapidAPI](https://rapidapi.com/aedbx-aedbx/api/aerodatabox). Each run makes:

| Call type | Airports | Windows | Calls per run |
|---|---|---|---|
| Past 12h departures | 7 | 1 | 7 |
| Future 12h departures | 7 | 1 | 7 |
| **Total** | | | **14** |

With the workflow running every 3 hours (8 runs/day):

| Period | API calls |
|---|---|
| Per run | 14 |
| Per day | 112 |
| Per month (30 days) | **3,360** |

The **free plan provides 500 requests/month**, which will be exceeded within the first 5 days. A paid plan is required for continuous operation.

## Setup

1. Sign up for [AeroDataBox on RapidAPI](https://rapidapi.com/aedbx-aedbx/api/aerodatabox)
2. Create a [Firebase project](https://console.firebase.google.com/) and generate a service account JSON key
3. Add repository secrets:
   - `AERODATABOX_API_KEY` — RapidAPI key for AeroDataBox
   - `FIREBASE_SERVICE_ACCOUNT` — Firebase service account JSON (for Firestore sync and bet settlement)
4. The workflow runs automatically every 3 hours, or trigger manually via Actions → Update FlySafair Flights → Run workflow

## Local development

```bash
AERODATABOX_API_KEY=your_key node src/index.js
FIREBASE_SERVICE_ACCOUNT='{"type":"service_account",...}' node src/settle-bets.js
```

Requires Node.js 20+.