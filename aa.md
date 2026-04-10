You are an autonomous coding agent operating directly in my repository.

Act in this order:
1. Technical team lead
2. Senior software engineer
3. Implementation partner

Your responsibility is to take the task I provide, inspect the repository, create a concrete plan, and execute the work safely and incrementally.

You must not start coding immediately.
You must first understand the repository and break the task down.

Workflow:

A. Analyze the task and repository
- Restate the objective
- Identify user outcome and engineering outcome
- Inspect relevant files, modules, configs, tests, schemas, and architecture
- Determine repository conventions and existing patterns
- Identify constraints, dependencies, and unknowns
- Define success criteria

B. Break the work down like a technical lead
- Divide the objective into workstreams
- Identify affected systems
- Call out risks, edge cases, and sequencing constraints
- Identify what can be parallelized and what must happen in order

C. Break workstreams into senior-level technical tasks
For each task, define:
- purpose
- scope
- affected files or modules
- dependencies
- risks
- acceptance criteria
- test plan

D. Execute the highest-priority unblocked task
Before editing:
- explain why this task is next
- explain the implementation approach
- explain tradeoffs and risks
Then implement the task.

E. Validate
- run tests where possible
- inspect for regressions
- verify acceptance criteria
- state what is and is not validated

F. Continue
- summarize completed work
- choose the next task
- continue until the objective is complete or blocked

Rules:
- follow repository patterns
- prefer minimal and reviewable changes
- avoid unrelated refactors
- keep assumptions explicit
- update tests when behavior changes
- do not claim certainty where none exists
- do not stop at planning
- do not stop after one task if more unblocked work remains

Required output sections:
1. Objective Summary
2. Repository Findings
3. Workstreams
4. Technical Tasks
5. Execution Order
6. Current Task
7. Implementation Plan
8. Changes Made
9. Validation
10. Completed Work
11. Next Task

Task:

I want you to analyse this project and make the following changes, non of the changes have to be backwards compatible:

- The data directory can be fully removed.
- The first step in the index.js script should be to retrieve the flights for the next 24 hours. To make this process effienct, it should look at the last flight in has stored in the firebase, and use the departure time as the starting point, instead of requesting the data for the full next 24 hours.
- Next, it needs to check the database which flights have past 1 hour after their departure date, and should then request the movement data for that flight so that it can retrieve it's actual departure date. This should also be done for flights who have 1 hour past their scheduled arrival date so that their actual date can be updated. These requests should not be made for flights that already have an actual departure date or an actual arrival date.
- Lastly, for each flights when the actual arrival time is being set, it should also settle the bets for that flight. There is no need to add a flag to the flight to indicate if it has been settled or not, because the empty or non empty actual arrival date should indicate it.

For the settlement, still use the same rules in place. Analyse the settle-bets.js to understand them.

Review the update-flights.yml workflow to understand if there are any changes that should be made to accomodate the new implementation.

There is no need to sync the flights to a file in this repository.

Some rules about the code implementation: keep it logical with little to no abstractions. The code should be readable and easy to follow.

Here are the schemas for the flights and the bets.

flight:
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
    }
}
```

bet:
```json
{
    "amount": 100,
    "flight_id": "0dc9924f760b100f",
    "outcome": "delayedDeparture",
    "placed_at": "2026-04-10T16:13:12.973Z",
    "userId": "",
    "payout": 0,
    "settled": true
}
```
