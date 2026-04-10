# flybet-data

FlySafair flight sync and bet settlement worker powered by the [AeroDataBox API](https://aerodatabox.com/) and [Firebase Firestore](https://firebase.google.com/docs/firestore).

## What it does

An hourly GitHub Actions workflow runs `src/index.js` and performs the full job in one pass:

1. Fetches future FlySafair departures for the next 24 hours.
2. Uses the latest stored `departure.scheduled` in Firestore as the start of that window, so it does not repeatedly request the full horizon.
3. Checks flights that are more than 1 hour past their scheduled departure or arrival and still miss the relevant actual timestamp.
4. Requests movement data for those flights and updates `departure.actual`, `arrival.actual`, `status`, and `cancelled`.
5. Settles bets immediately when a flight becomes final by getting an actual arrival time or being marked cancelled.

Firestore is the only source of truth. The repository does not store synced flight data files.

## Flight schema

New flight documents use this shape:

```json
{
  "id": "0dc9924f760b100f",
  "flight": "100",
  "airline": {
    "iata": "FA"
  },
  "departure": {
    "airport": {
      "code": "CPT"
    },
    "scheduled": "2026-04-10T16:13:12.973Z",
    "actual": "2026-04-10T16:13:12.973Z"
  },
  "arrival": {
    "airport": {
      "code": "JNB"
    },
    "scheduled": "2026-04-10T16:13:12.973Z",
    "actual": "2026-04-10T16:13:12.973Z"
  },
  "aircraft": {
    "model": "Boeing 737-800"
  },
  "status": "Arrived",
  "cancelled": false
}
```

All stored timestamps are UTC ISO 8601 strings.

## Bet settlement rules

The worker keeps the existing settlement rules:

- Departure market: `onTimeDeparture` vs `delayedDeparture`
- Arrival market: `onTimeArrival` vs `delayedArrival`
- Cancellation market: `cancelled` vs `notCancelled`

A departure or arrival is delayed when the actual timestamp is missing or more than 15 minutes after the scheduled timestamp.

Winners receive their stake back plus a proportional share of the losing pool. Each winning bet is guaranteed a minimum profit of `25`.

Bets are read by `flight_id`, and only bets with `settled !== true` are processed.

## Cancelled flights

If a flight is more than 1 hour past its scheduled arrival and the movement lookup still does not provide an actual arrival, the worker marks the flight with:

```json
{
  "cancelled": true
}
```

That cancelled state is then used for settlement. There is no separate `settled` flag on the flight document.

## Setup

1. Sign up for [AeroDataBox on RapidAPI](https://rapidapi.com/aedbx-aedbx/api/aerodatabox).
2. Create a Firebase project and generate a service account JSON key.
3. Add repository secrets:
   - `AERODATABOX_API_KEY`
   - `FIREBASE_SERVICE_ACCOUNT`

## Local development

```bash
AERODATABOX_API_KEY=your_key FIREBASE_SERVICE_ACCOUNT='{"type":"service_account",...}' node src/index.js
```

Requires Node.js 20 or later.
