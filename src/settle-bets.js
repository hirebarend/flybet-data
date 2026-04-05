const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');

const THREE_HOURS_MS = 3 * 60 * 60 * 1000;
const FIFTEEN_MINUTES_MS = 15 * 60 * 1000;

/**
 * Convert a Firestore value to a Date. Handles Timestamps, strings, and Date objects.
 * Returns null for null/undefined or unparseable values.
 */
function toDate(value) {
  if (value == null) return null;
  if (typeof value.toDate === 'function') return value.toDate();
  const date = new Date(value);
  return isNaN(date.getTime()) ? null : date;
}

async function main() {
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!serviceAccountJson) {
    throw new Error('FIREBASE_SERVICE_ACCOUNT environment variable is required');
  }

  initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  const db = getFirestore();
  const now = Date.now();

  console.log('Starting bet settlement...');

  const flightsSnapshot = await db.collection('flights').get();
  console.log(`Checking ${flightsSnapshot.size} flights`);

  let settledCount = 0;

  for (const flightDoc of flightsSnapshot.docs) {
    const flight = flightDoc.data();
    const flightId = flightDoc.id;

    // Skip already-settled flights
    if (flight.settled === true) continue;

    // Settlement requires 3 hours after the reference time for both departure and arrival.
    // Reference time = actual if available, otherwise scheduled.
    const departureRef = toDate(flight.departure?.actual) || toDate(flight.departure?.scheduled);
    const arrivalRef = toDate(flight.arrival?.actual) || toDate(flight.arrival?.scheduled);

    if (!departureRef || now < departureRef.getTime() + THREE_HOURS_MS) continue;
    if (!arrivalRef || now < arrivalRef.getTime() + THREE_HOURS_MS) continue;

    const flightLabel = flight.flight
      ? `${flight.flight.iataCode}${flight.flight.number}`
      : flightId;
    console.log(`\nSettling ${flightLabel} (${flightId})`);

    // Load all unsettled bets for this flight
    const betsSnapshot = await db.collection('bets')
      .where('flightId', '==', flightId)
      .get();

    const bets = betsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(bet => !bet.settled);

    if (bets.length === 0) {
      await db.collection('flights').doc(flightId).update({ settled: true });
      console.log('  No bets to settle, marked flight as settled');
      settledCount++;
      continue;
    }

    // --- Determine winning outcomes ---

    // Departure: delayed if no actual time, or actual is more than 15 min after scheduled
    const departureDelayed = !flight.departure?.actual
      || (toDate(flight.departure.actual) - toDate(flight.departure.scheduled)) > FIFTEEN_MINUTES_MS;

    // Arrival: delayed if no actual time, or actual is more than 15 min after scheduled
    const arrivalDelayed = !flight.arrival?.actual
      || (toDate(flight.arrival.actual) - toDate(flight.arrival.scheduled)) > FIFTEEN_MINUTES_MS;

    // Cancellation: cancelled if neither actual departure nor actual arrival exists
    const isCancelled = !flight.departure?.actual && !flight.arrival?.actual;

    // --- Calculate pari-mutuel payouts for all three markets ---

    const betPayouts = new Map();   // betId -> payout amount
    const userCredits = new Map();  // userId -> total amount to credit

    const markets = [
      {
        outcomes: ['onTimeDeparture', 'delayedDeparture'],
        winner: departureDelayed ? 'delayedDeparture' : 'onTimeDeparture',
      },
      {
        outcomes: ['onTimeArrival', 'delayedArrival'],
        winner: arrivalDelayed ? 'delayedArrival' : 'onTimeArrival',
      },
      {
        outcomes: ['cancelled', 'notCancelled'],
        winner: isCancelled ? 'cancelled' : 'notCancelled',
      },
    ];

    for (const { outcomes, winner } of markets) {
      const marketBets = bets.filter(b => outcomes.includes(b.outcome));
      if (marketBets.length === 0) continue;

      const winners = marketBets.filter(b => b.outcome === winner);
      const losers = marketBets.filter(b => b.outcome !== winner);

      const totalWinStake = winners.reduce((sum, b) => sum + b.amount, 0);
      const totalLoseStake = losers.reduce((sum, b) => sum + b.amount, 0);

      // Winners get their stake back plus a proportional share of the losing pool
      for (const bet of winners) {
        const payout = totalLoseStake === 0
          ? bet.amount
          : Math.round(bet.amount + (bet.amount / totalWinStake) * totalLoseStake);
        betPayouts.set(bet.id, payout);
        userCredits.set(bet.userId, (userCredits.get(bet.userId) || 0) + payout);
      }

      // Losers get nothing (stake was already deducted at placement)
      for (const bet of losers) {
        betPayouts.set(bet.id, 0);
      }
    }

    const winnerCount = [...betPayouts.values()].filter(p => p > 0).length;
    console.log(`  ${bets.length} bets, ${winnerCount} winners`);

    // --- Execute settlement in a single Firestore transaction ---

    await db.runTransaction(async (transaction) => {
      // Guard against double-settlement (race condition)
      const freshDoc = await transaction.get(db.collection('flights').doc(flightId));
      if (freshDoc.data()?.settled === true) {
        console.log('  Already settled (race condition), skipping');
        return;
      }

      // Credit each winning user's balance
      for (const [userId, credit] of userCredits) {
        transaction.update(db.collection('users').doc(userId), {
          balance: FieldValue.increment(credit),
        });
      }

      // Mark each bet as settled with its payout
      for (const [betId, payout] of betPayouts) {
        transaction.update(db.collection('bets').doc(betId), {
          settled: true,
          payout,
        });
      }

      // Mark flight as settled
      transaction.update(db.collection('flights').doc(flightId), {
        settled: true,
      });
    });

    console.log('  Settled successfully');
    settledCount++;
  }

  console.log(`\nSettlement complete. Settled ${settledCount} flights.`);
}

main().catch(error => {
  console.error('Fatal:', error);
  process.exit(1);
});
