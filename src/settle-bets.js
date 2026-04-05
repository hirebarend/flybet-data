const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');

const THREE_HOURS_MS = 3 * 60 * 60 * 1000;
const FIFTEEN_MINUTES_MS = 15 * 60 * 1000;
const MINIMUM_PROFIT = 25;

function toDate(value) {
  if (value == null) { return null; }
  if (typeof value.toDate === 'function') { return value.toDate(); }
  const date = new Date(value);
  return isNaN(date.getTime()) ? null : date;
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

async function main() {
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!serviceAccountJson) {
    throw new Error('FIREBASE_SERVICE_ACCOUNT environment variable is required');
  }

  initializeApp({ credential: cert(JSON.parse(serviceAccountJson)) });
  const db = getFirestore();
  const now = Date.now();

  console.log('Starting bet settlement...');

  const flightsSnapshot = await db.collection('flights')
    .where('settled', '!=', true)
    .get();
  console.log(`Checking ${flightsSnapshot.size} unsettled flights`);

  let settledCount = 0;

  for (const flightDoc of flightsSnapshot.docs) {
    const flight = flightDoc.data();
    const flightId = flightDoc.id;

    const departureTime = toDate(flight.departure?.actual) || toDate(flight.departure?.scheduled);
    const arrivalTime = toDate(flight.arrival?.actual) || toDate(flight.arrival?.scheduled);

    if (!departureTime || now < departureTime.getTime() + THREE_HOURS_MS) { continue; }
    if (!arrivalTime || now < arrivalTime.getTime() + THREE_HOURS_MS) { continue; }

    console.log(`\nSettling flight ${flightId}`);

    const betsSnapshot = await db.collection('bets')
      .where('flightId', '==', flightId)
      .get();

    const unsettledBets = betsSnapshot.docs
      .map(doc => ({ id: doc.id, ...doc.data() }))
      .filter(bet => !bet.settled);

    if (unsettledBets.length === 0) {
      await db.collection('flights').doc(flightId).update({ settled: true });
      console.log('  No bets to settle, marked flight as settled');
      settledCount++;
      continue;
    }

    const departureIsDelayed = !flight.departure?.actual
      || (toDate(flight.departure.actual) - toDate(flight.departure.scheduled)) > FIFTEEN_MINUTES_MS;

    const arrivalIsDelayed = !flight.arrival?.actual
      || (toDate(flight.arrival.actual) - toDate(flight.arrival.scheduled)) > FIFTEEN_MINUTES_MS;

    const isCancelled = !flight.departure?.actual && !flight.arrival?.actual;

    const markets = [
      { outcomes: ['onTimeDeparture', 'delayedDeparture'], winner: departureIsDelayed ? 'delayedDeparture' : 'onTimeDeparture' },
      { outcomes: ['onTimeArrival', 'delayedArrival'], winner: arrivalIsDelayed ? 'delayedArrival' : 'onTimeArrival' },
      { outcomes: ['cancelled', 'notCancelled'], winner: isCancelled ? 'cancelled' : 'notCancelled' },
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

    const winnerCount = allPayouts.filter(p => p.payout > 0).length;
    console.log(`  ${unsettledBets.length} bets, ${winnerCount} winners`);

    await db.runTransaction(async (transaction) => {
      const freshDoc = await transaction.get(db.collection('flights').doc(flightId));
      if (freshDoc.data()?.settled === true) {
        console.log('  Already settled (race condition), skipping');
        return;
      }

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
