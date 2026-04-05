const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');

const THREE_HOURS_MS = 3 * 60 * 60 * 1000;
const FIFTEEN_MINUTES_MS = 15 * 60 * 1000;

const DEPARTURE_OUTCOMES = ['onTimeDeparture', 'delayedDeparture'];
const ARRIVAL_OUTCOMES = ['onTimeArrival', 'delayedArrival'];
const CANCELLATION_OUTCOMES = ['cancelled', 'notCancelled'];

/**
 * Initialize Firestore using a service account JSON from an environment variable.
 */
function initializeFirestore() {
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!serviceAccountJson) {
    throw new Error('FIREBASE_SERVICE_ACCOUNT environment variable is required');
  }

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
 * Convert a Firestore field value to a JavaScript Date.
 * Handles Firestore Timestamps, Date objects, and ISO 8601 strings.
 */
function toDate(value) {
  if (value == null) return null;
  if (typeof value.toDate === 'function') return value.toDate();
  if (value instanceof Date) return value;
  const d = new Date(value);
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Check if enough time has passed for settlement (>= 3 hours after reference time).
 */
function canSettle(referenceDate, now) {
  if (!referenceDate) return false;
  return now.getTime() >= referenceDate.getTime() + THREE_HOURS_MS;
}

/**
 * Get the reference timestamp for the departure market.
 * Uses actual departure if available, otherwise scheduled departure.
 */
function getDepartureReference(flight) {
  return toDate(flight.departure?.actual) || toDate(flight.departure?.scheduled);
}

/**
 * Get the reference timestamp for the arrival and cancellation markets.
 * Uses actual arrival if available, otherwise scheduled arrival.
 */
function getArrivalReference(flight) {
  return toDate(flight.arrival?.actual) || toDate(flight.arrival?.scheduled);
}

/**
 * Determine the winning outcome for the departure market.
 * Delayed if actual is more than 15 minutes after scheduled, or if actual is null.
 */
function getDepartureWinner(flight) {
  const actual = toDate(flight.departure?.actual);
  const scheduled = toDate(flight.departure?.scheduled);

  if (!actual) return 'delayedDeparture';

  const diffMs = actual.getTime() - scheduled.getTime();
  return diffMs > FIFTEEN_MINUTES_MS ? 'delayedDeparture' : 'onTimeDeparture';
}

/**
 * Determine the winning outcome for the arrival market.
 * Delayed if actual is more than 15 minutes after scheduled, or if actual is null.
 */
function getArrivalWinner(flight) {
  const actual = toDate(flight.arrival?.actual);
  const scheduled = toDate(flight.arrival?.scheduled);

  if (!actual) return 'delayedArrival';

  const diffMs = actual.getTime() - scheduled.getTime();
  return diffMs > FIFTEEN_MINUTES_MS ? 'delayedArrival' : 'onTimeArrival';
}

/**
 * Determine the winning outcome for the cancellation market.
 * Cancelled if neither departure.actual nor arrival.actual is available.
 */
function getCancellationWinner(flight) {
  const hasActualDeparture = toDate(flight.departure?.actual) !== null;
  const hasActualArrival = toDate(flight.arrival?.actual) !== null;

  return (!hasActualDeparture && !hasActualArrival) ? 'cancelled' : 'notCancelled';
}

/**
 * Calculate pari-mutuel payouts for a single market.
 * Returns an array of { betId, userId, payout } for winning bets.
 */
function calculatePayouts(bets, winningOutcome) {
  const winners = bets.filter(b => b.outcome === winningOutcome);
  const losers = bets.filter(b => b.outcome !== winningOutcome);

  if (winners.length === 0) {
    return [];
  }

  const totalWinningStakes = winners.reduce((sum, b) => sum + b.amount, 0);
  const totalLosingStakes = losers.reduce((sum, b) => sum + b.amount, 0);

  return winners.map(bet => ({
    betId: bet.id,
    userId: bet.userId,
    payout: totalLosingStakes === 0
      ? bet.amount
      : Math.round(bet.amount + (bet.amount / totalWinningStakes) * totalLosingStakes),
  }));
}

/**
 * Settle a single market for a flight using a Firestore transaction.
 * Credits winning users, marks bets as settled, and marks the market as settled.
 */
async function settleMarket(db, flightId, marketName, bets, winningOutcome) {
  if (bets.length === 0) {
    console.log(`  No bets for ${marketName} market, marking as settled`);
    await db.collection('flights').doc(flightId).set(
      { settled: { [marketName]: true } },
      { merge: true },
    );
    return;
  }

  const payouts = calculatePayouts(bets, winningOutcome);
  console.log(`  ${marketName}: winner=${winningOutcome}, bets=${bets.length}, winners=${payouts.length}`);

  // Aggregate payouts per user (a user may have multiple winning bets)
  const payoutsByUser = new Map();
  for (const { userId, payout } of payouts) {
    payoutsByUser.set(userId, (payoutsByUser.get(userId) || 0) + payout);
  }

  await db.runTransaction(async (transaction) => {
    const flightRef = db.collection('flights').doc(flightId);
    const flightDoc = await transaction.get(flightRef);

    // Double-check market is not already settled (race condition protection)
    if (flightDoc.exists && flightDoc.data()?.settled?.[marketName]) {
      console.log(`  ${marketName} already settled (race condition), skipping`);
      return;
    }

    // Credit each winning user's balance (aggregated per user)
    for (const [userId, totalPayout] of payoutsByUser) {
      const userRef = db.collection('users').doc(userId);
      transaction.update(userRef, {
        balance: FieldValue.increment(totalPayout),
      });
    }

    // Mark each winning bet as settled with its payout amount
    for (const { betId, payout } of payouts) {
      const betRef = db.collection('bets').doc(betId);
      transaction.update(betRef, {
        settled: true,
        payout: payout,
      });
    }

    // Mark each losing bet as settled with 0 payout
    const winningBetIds = new Set(payouts.map(p => p.betId));
    for (const bet of bets) {
      if (!winningBetIds.has(bet.id)) {
        const betRef = db.collection('bets').doc(bet.id);
        transaction.update(betRef, {
          settled: true,
          payout: 0,
        });
      }
    }

    // Mark market as settled on the flight document
    transaction.set(flightRef, {
      settled: { [marketName]: true },
    }, { merge: true });
  });

  console.log(`  ${marketName} market settled successfully`);
}

async function main() {
  console.log('Starting bet settlement...');
  const now = new Date();
  console.log(`Current time: ${now.toISOString()}`);

  const db = initializeFirestore();

  const flightsSnapshot = await db.collection('flights').get();
  console.log(`Found ${flightsSnapshot.size} flights`);

  let settledCount = 0;

  for (const flightDoc of flightsSnapshot.docs) {
    const flight = flightDoc.data();
    const flightId = flightDoc.id;
    const settled = flight.settled || {};

    // Determine which markets are eligible for settlement
    const marketsToSettle = [];

    if (!settled.departure) {
      const depRef = getDepartureReference(flight);
      if (depRef && canSettle(depRef, now)) {
        marketsToSettle.push('departure');
      }
    }

    if (!settled.arrival) {
      const arrRef = getArrivalReference(flight);
      if (arrRef && canSettle(arrRef, now)) {
        marketsToSettle.push('arrival');
      }
    }

    if (!settled.cancellation) {
      const cancRef = getArrivalReference(flight);
      if (cancRef && canSettle(cancRef, now)) {
        marketsToSettle.push('cancellation');
      }
    }

    if (marketsToSettle.length === 0) continue;

    const flightLabel = flight.flight
      ? `${flight.flight.iataCode}${flight.flight.number}`
      : flightId;
    console.log(`\nSettling flight ${flightLabel} (${flightId}): markets [${marketsToSettle.join(', ')}]`);

    // Query all bets for this flight
    const betsSnapshot = await db.collection('bets')
      .where('flightId', '==', flightId)
      .get();

    // Filter out already-settled bets
    const bets = betsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(bet => bet.settled !== true);

    // Group bets by market
    const departureBets = bets.filter(b => DEPARTURE_OUTCOMES.includes(b.outcome));
    const arrivalBets = bets.filter(b => ARRIVAL_OUTCOMES.includes(b.outcome));
    const cancellationBets = bets.filter(b => CANCELLATION_OUTCOMES.includes(b.outcome));

    for (const market of marketsToSettle) {
      try {
        let marketBets, winningOutcome;

        switch (market) {
          case 'departure':
            marketBets = departureBets;
            winningOutcome = getDepartureWinner(flight);
            break;
          case 'arrival':
            marketBets = arrivalBets;
            winningOutcome = getArrivalWinner(flight);
            break;
          case 'cancellation':
            marketBets = cancellationBets;
            winningOutcome = getCancellationWinner(flight);
            break;
        }

        await settleMarket(db, flightId, market, marketBets, winningOutcome);
        settledCount++;
      } catch (error) {
        console.error(`  Error settling ${market} for flight ${flightId}: ${error.message}`);
      }
    }
  }

  console.log(`\nBet settlement complete. Settled ${settledCount} markets.`);
}

main().catch(error => {
  console.error('Fatal:', error);
  process.exit(1);
});
