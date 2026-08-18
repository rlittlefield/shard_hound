# Rebalancing without resharding: MOVE KEYS and ADD SHARD

This branch wires ShardHound up to the PgDog topology commands from the
[rlittlefield/pgdog](https://github.com/rlittlefield/pgdog) PR stack:

1. [PR #1](https://github.com/rlittlefield/pgdog/pull/1) — fleet visibility
   (`SHOW INSTANCES`, the `pgdog.instances` registry) and a per-database
   omni-write barrier (`OMNI_WRITES ON|OFF`).
2. [PR #2](https://github.com/rlittlefield/pgdog/pull/2) — `ADD SHARD`: grow the
   cluster by one shard end to end (schema sync, omni data copy, WAL catch-up,
   coordinated cutover) while traffic flows.
3. [PR #3](https://github.com/rlittlefield/pgdog/pull/3) — `MOVE KEYS`: move
   individual organizations between shards semi-live. Only writes naming the
   moving keys pause (about a second) at the flip.

Together they close the loop that the lookup-routing demo opens: placement is
stored per organization (`organizations.shard_id`), `MOVE KEYS` rebalances
tenants one at a time, and `ADD SHARD` grows the fleet when it fills up.

**The intent is that a full reshard never happens.** Because placement is
stored rather than hashed, no operation ever moves every row: `MOVE KEYS`
relocates exactly the tenants named, and `ADD SHARD` only copies the small
omnisharded tables. Both commands enforce this — they refuse hash-routed
tables outright. `MOVE KEYS` needs no new shard: it works between shards 0
and 1 today.

## What this branch already changes

- **compose.yaml**: all shards run `wal_level=logical` (both commands sync data
  over logical replication); a `shard_2` container (host port `5435`) waits
  empty; the control DB and PgDog move to host ports `5436`/`6433`/`9091`
  because the local k3s cluster owns `5432`/`6432`/`9090`.
- **docker/pgdog/pgdog.toml**: a `provisioning = true` entry declares shard 2
  in its final shape without serving it, and the sharded-tables rule gains
  `move_query = "UPDATE organizations SET shard_id = $2 WHERE id = $1"`, the
  UPDATE `MOVE KEYS` runs on every shard at the cutover.
- **docker/pgdog/users.toml**: the app user gets `schema_admin = true`, which
  the schema sync, the instance registry and both topology tasks require.
- **Migration `20260811220000`**: every tenant table gets a unique
  `(organization_id, id)` index and uses it as its **replica identity**.
  `MOVE KEYS` refuses tables whose replica identity hides the sharding column,
  because DELETEs and identity-only UPDATEs in the WAL stream carry only
  identity columns.
- **`mix shard_hound.sequence_ranges --shard N`**: moves each shard's tenant id
  sequences into a disjoint range (shard `n` starts at `n × 10¹²`). Moved rows
  keep their ids, so ids must be unique across the fleet. Data generated before
  this task ran can hold colliding ids across shards; regenerate
  (`docker compose down -v`, re-migrate, re-run the task, generate) before
  running move experiments.

## Runbook

Admin console:

```bash
docker compose exec postgres \
  psql "host=pgdog port=6432 dbname=admin user=admin password=pgdog"
```

### Move a tenant

This is the primary experiment, and it needs no new shard — it moves keys
between shards 0 and 1 as they exist today.

```sql
-- Pick an organization id from the dashboard (one placed on shard 0), then:
MOVE KEYS shard_hound_dev 1 900001;
SHOW TASKS;
CUTOVER SHARD shard_hound_dev 1;   -- or MOVE KEYS ... AUTO
```

The task copies only that organization's rows (parents before children, so live
foreign keys hold), catches up over a WAL filter, pauses writes for just that
key at the flip, runs `move_query` on every shard, invalidates lookup caches
fleet-wide, re-routes the parked writes to the new shard, and deletes the moved
rows from the source children-first.

Verify placement moved:

```bash
docker compose exec shard_1 psql -U postgres -d shard_hound_dev -c \
  "SELECT count(*) FROM devices WHERE organization_id = 900001;"
```

### Grow the cluster (when the fleet fills up)

The generator UI's **Shards** panel does all of this with one button:
`ADD SHARD <n> AUTO`, then — once the cutover lands — it adopts the migration
ledger, applies the shard's sequence range, and registers the shard in the
placement policy table. Shards 2, 3 and 4 are pre-declared in compose and
pgdog.toml, so the button works three times. The manual equivalent:

```sql
SHOW INSTANCES;          -- every live pgdog, heartbeating on shard 0
ADD SHARD shard_hound_dev 2;
SHOW TASKS;              -- validating → syncing schema → syncing data →
                         -- replicating → awaiting cutover
CUTOVER SHARD shard_hound_dev 2;   -- or ADD SHARD ... AUTO in one step
```

The task snapshots the omnisharded tables (`organizations`,
`shard_hound_packages`) from shard 0, streams WAL until caught up, then the
cutover pauses only omni writes, drains to zero, activates shard 2 and resumes.
Sharded traffic never pauses. `STOP_TASK` aborts cleanly any time before the
swap. Tenant data never moves here: shard 2 starts empty and fills through
`MOVE KEYS` (and new organizations placed on it).

Afterwards, give the new shard its sequence range before assigning tenants:

```bash
DATABASE_PORT=5435 mix shard_hound.sequence_ranges --shard 2
```

The config file is mounted read-only, so the `provisioning` flag stays in
`pgdog.toml` after activation; PgDog reconciles against the `pgdog.config`
marker it wrote on shard 2 at startup and after every `RELOAD`, so restarts
converge on three shards on their own.

## Proposed application integration

The infrastructure above is enough to drive everything from `psql`. To make
resharding a first-class part of the demo app, in order of value:

### 1. `ShardHound.PgDog.Admin` — a client for the admin database ✅ built

Implemented in `lib/shard_hound/pgdog/admin.ex`, but not on Postgrex as
originally sketched: the admin database rejects everything outside its command
set, including the `pg_type` bootstrap query every Postgrex connection runs
(and `Postgrex.SimpleConnection` cannot decode rows without those types).
Admin commands are text in, text rows out — the wire protocol's simple query
flow — so the module speaks it directly over `:gen_tcp`, one short-lived
connection per command. It exposes `move_keys/3`, `tasks/0` and a generic
`command/1`; `add_shard`, `cutover` and `stop_task` are trivial additions on
the same base.

### 2. Dynamic shard count ✅ built

`ShardHound.Topology` reads the serving topology from `SHOW POOLS` (falling
back to the `:shard_count` config without PgDog), and placement policy lives
in the omnisharded `shards` table (`ShardHound.Shards`): new organizations
round-robin over the rows with `enabled_for_new_orgs`, toggled per shard from
the UI's Shards panel. `ADD SHARD` registers the new shard's row on
activation, and — because the table is omnisharded — the new shard receives
the policy table itself through the omni data sync.

> **Note:** the fork needs commit `704c5f7` (predicate arrays cast to the
> sharding column's type). Without it every MOVE KEYS against a non-varchar
> key fails validation with `operator does not exist: bigint = text`.

### Findings from driving ADD SHARD end to end

- **`resharding_copy_format = "text"` is required** with `integer` columns in
  omnisharded tables (`organizations.shard_id`): the binary COPY decode
  assumes 8-byte ints and the omni data sync fails with `binary format
  mismatch (likely int -> bigint)`. Set in `pgdog.toml`.
- **The schema sync upgrades integer primary keys to bigint** on the new
  shard. For sharded tables that's the point; for an *omnisharded* table it
  silently diverges column types across shards, which breaks prepared
  statements in creative ways (stale parameter OIDs, partial broadcast
  writes). Give omnisharded tables bigint keys (the `shards` table learned
  this the hard way).
- **Restarting pgdog leaves a ghost row in `pgdog.instances`**: compose stops
  the container with SIGINT, the graceful path waits on open app connections
  past Docker's 10s timeout, and the SIGKILL skips deregistration. A cutover
  started within the ~15s liveness window then fails with `didn't pause omni
  writes in time`. Wait out the window (or retry) after a pgdog restart.
- **`schema_migrations` rows don't ride the schema sync** (pg_dump DDL only),
  so a fresh shard reports every migration pending — Phoenix's dev
  repo-status check 503s when a read round-robins there. The UI adopts the
  ledger automatically after activation (`Topology.adopt_schema_migrations/1`,
  an idempotent broadcast insert).
- **pgdog's prepared-statement cache survives DDL**: after `ALTER TABLE ...
  ALTER COLUMN ... TYPE`, cached statements keep the old parameter types and
  fail until pgdog restarts.
- **Startup convergence broke after growing by more than one shard** (fixed
  by fork commit `646830b`): every cutover refreshes every shard's
  `pgdog.config` marker to the new total, so after growing 2→4 shard 2's
  marker reads `(2, 4)` — and a restart with the stale manifest refused to
  converge because it expected `(2, serving + 1)`. The cluster came back
  serving only shards 0–1 with tenants stranded on 2 and 3. The fix accepts
  any marker width that includes the shard. Independent of the fix: **remove
  the `provisioning` flag from the manifest after each activation** — the
  documented step this branch had skipped.
- **`REPLICA IDENTITY USING INDEX` doesn't survive the schema sync** either:
  the new shard gets the indexes but reverts to the default identity, so the
  first MOVE KEYS *out of* an added shard is refused. The UI re-asserts the
  identities after activation (`DemoData.ensure_replica_identities/0` — DDL
  broadcasts through PgDog, so one statement covers every shard).

### 3. A resharding LiveView

A `/resharding` page that turns the whole story into a demo:

- one card per shard: organization/device counts (the existing
  `pgdog_shard` directive pattern), plus the empty provisioning shard;
- an **Add shard** button and per-organization **Move to shard N** actions
  driving the admin client;
- live task progress by polling `Admin.tasks/0` once a second while a task is
  running (validating → syncing → replicating → awaiting cutover), with a
  **Cut over** confirmation button when the task parks, and **Abort**
  (`STOP_TASK`) before the point of no return;
- `SHOW INSTANCES` in a footer, which becomes interesting the moment a second
  pgdog container joins the compose file to demo fleet-coordinated cutovers.

### 4. Guardrails worth demoing

- The fail-closed behaviors are the best part of the demo: `MOVE KEYS` with a
  key already on the target, a `CUTOVER` with a dead peer instance, and
  `STOP_TASK` mid-copy all refuse or roll back cleanly and are easy to show.
- A live writer during a move: keep the data generator running against the
  moving organization and verify zero lost rows after the flip (this mirrors
  the fork's integration suite).

### Longer-term option: composite primary keys

The sequence-range task treats the symptom; the schema-level cure is composite
primary keys `(organization_id, id)` on tenant tables (the shape the fork's
integration suite uses). Ids then only need to be unique per organization, the
PK doubles as the replica identity, and `commands.device_id` would become a
composite foreign key. That is a bigger, Ecto-visible change and is not needed
for these experiments.

## Caveats

- **Omnisharded sequences**: `shard_hound_packages` and `organizations` are
  broadcast, and their rows must stay identical on every shard, so their ids
  must come from the application (the generator's `stable_id/1` hash does
  this), never from per-shard sequences.
- **Migrations vs. ADD SHARD**: a shard added by `ADD SHARD` gets its schema
  from shard 0 via pg_dump, including the `schema_migrations` rows — after
  activation it takes future `mix ecto.migrate` runs like any other shard
  (port `5435`).
- **Parked tasks retain WAL** on the source shard (`SHOW REPLICATION_SLOTS`);
  don't leave a task at "awaiting cutover" overnight.
