# PgDog lookup-routing demo

This setup uses a **local build of pgdog-enterprise**: `compose.yaml` pins
`pgdog:34c6c7a7`, built from
[pgdogdev/pgdog-enterprise](https://github.com/pgdogdev/pgdog-enterprise) at
commit `34c6c7a7` (PgDog Enterprise v0.1.59). It carries the `ADD SHARD` and
`MOVE KEYS` topology commands, **hybrid tables** (`kind = "hybrid"`) and a
marker-driven topology: which declared shards serve is decided by the
`pgdog.config` marker on every shard, stamped by each cutover, so
`pgdog.toml` needs no `provisioning` flags and no edits per activation.

There is no build script for it yet: check out the repository at that
commit and `docker build` it as `pgdog:34c6c7a7` (or build another commit
and change the `image:` tag in `compose.yaml` to match).

`scripts/build-pgdog-image.sh` still builds the previous image,
`pgdog:move-keys-broadcast-null-v3`, from the `move-keys-broadcast-null-v3`
branch of [rlittlefield/pgdog](https://github.com/rlittlefield/pgdog) (the
PR stack [#1](https://github.com/rlittlefield/pgdog/pull/1),
[#2](https://github.com/rlittlefield/pgdog/pull/2),
[#3](https://github.com/rlittlefield/pgdog/pull/3) rebased onto upstream
v0.1.57). To run that build instead, point `compose.yaml` back at its tag.

See [`docs/resharding.md`](resharding.md) for the resharding experiments.

It starts the database services:

- `postgres`: an unsharded control database used by Oban, exposed on port `5436`
- `shard_0`: PostgreSQL 18, exposed directly on port `5433`
- `shard_1`: PostgreSQL 18, exposed directly on port `5434`
- `shard_2`–`shard_19`: PostgreSQL 18 on ports `5435` and `5437`–`5453`
  (shard `n` maps to port `5433 + n`, shifted up one past `5436`); the
  non-serving ones idle empty as future shards, activated one at a time by
  `ADD SHARD`
- `pgdog`: the application endpoint on port `6433`, with metrics on `9091`

Host ports avoid `5432`, `6432` and `9090` because the local k3s cluster forwards those to its
own stack.

The Phoenix Repo uses PgDog by default in development. Oban uses the control database directly
because queue state must not be broadcast or independently duplicated across shards.

The dashboard's global totals issue one direct count per shard and sum the results in Elixir. This
also provides a small example of `pgdog_shard` directives for queries that intentionally span the
whole fleet rather than routing from an organization key.

## Start the cluster

```bash
docker compose up -d --wait
```

Apply the application migrations directly to each physical shard. Do not run migrations through
PgDog while learning the routing behavior.

```bash
DATABASE_PORT=5433 mix ecto.migrate -r ShardHound.Repo
DATABASE_PORT=5434 mix ecto.migrate -r ShardHound.Repo
mix ecto.migrate -r ShardHound.ObanRepo
```

Give each shard's tenant id sequences a disjoint range, so rows can move between shards without
primary key collisions (see [`docs/resharding.md`](resharding.md)):

```bash
DATABASE_PORT=5433 mix shard_hound.sequence_ranges --shard 0
DATABASE_PORT=5434 mix shard_hound.sequence_ranges --shard 1
```

Do not migrate the future shards: `ADD SHARD` provisions each from
shard 0, and the UI adopts its migration ledger, sequence range and placement
row on activation.

Start Phoenix. Its domain Repo connects to PgDog on port `6433`; its Oban Repo connects to the
control database on port `5436`.

```bash
mix phx.server
```

## How placement works

`organizations` is omnisharded, so PgDog broadcasts organization writes to both shards. Each row
contains a stable, zero-based `shard_id`.

All tenant tables are matched by their `organization_id` column. On a cache miss PgDog executes:

```sql
SELECT shard_id FROM organizations WHERE id = $1
```

With `lookup_result = "shard"`, the result is used directly as the shard number. It is not hashed.
The organization row must be committed before inserting tenant rows.

Organization generation jobs then run `SET LOCAL pgdog.sharding_key = '<organization id>'` at the
start of their data transaction. PgDog resolves the value with the same lookup query and pins the
bulk inserts to that shard. This avoids treating a multi-row prepared insert as cross-shard traffic.

`custom_packages` is a **hybrid** (`kind = "hybrid"`) table: rows keyed by an `organization_id`
route to their tenant's shard like any tenant table, but rows with a NULL `organization_id` are
shared defaults that PgDog broadcasts to every shard (and `ADD SHARD` copies onto a new shard).
Like the omnisharded tables, broadcast rows must stay identical everywhere, so the generator gives
default packages app-supplied `stable_id/1` ids rather than per-shard sequence values. The
table runs with `REPLICA IDENTITY FULL` because no identity index can include its nullable
sharding column. The generator UI's **Hybrid table check** panel verifies both halves on every
shard; see [`docs/resharding.md`](resharding.md#hybrid-tables-kind--hybrid).

## Exercise fixed placement

Open a client through PgDog using the PostgreSQL client already available in the control container:

```bash
docker compose exec postgres \
  psql -h pgdog -p 6432 -U postgres -d shard_hound_dev
```

Create a mapping assigned to shard 1. The organization insert is broadcast to both shards.

```sql
INSERT INTO organizations
  (id, name, slug, shard_id, inserted_at, updated_at)
VALUES
  (900001, 'Lookup Demo', 'lookup-demo', 1, now(), now());
```

Insert a tenant row in a separate statement. PgDog looks up organization `900001` and sends this
row only to shard 1.

```sql
INSERT INTO devices
  (organization_id, serial_number, hostname, platform, architecture, os_version,
   metadata, inserted_at, updated_at)
VALUES
  (900001, 'LOOKUP-001', 'lookup-001', 'macos', 'arm64', '15.6', '{}', now(), now());
```

Leave the PgDog client with `\q`, then inspect each physical shard:

```bash
docker compose exec shard_0 \
  psql -U postgres -d shard_hound_dev -c \
  "SELECT id, shard_id FROM organizations WHERE id = 900001; SELECT serial_number FROM devices WHERE organization_id = 900001;"

docker compose exec shard_1 \
  psql -U postgres -d shard_hound_dev -c \
  "SELECT id, shard_id FROM organizations WHERE id = 900001; SELECT serial_number FROM devices WHERE organization_id = 900001;"
```

The organization exists on both shards, while the device exists only on shard 1.

Try an unmapped organization ID to see the fail-closed behavior:

```sql
INSERT INTO devices
  (organization_id, serial_number, hostname, platform, architecture, os_version,
   metadata, inserted_at, updated_at)
VALUES
  (999999, 'MISSING-001', 'missing-001', 'macos', 'arm64', '15.6', '{}', now(), now());
```

PgDog rejects it rather than hashing it or choosing an arbitrary shard.

## Observe lookups

Lookup cache and query metrics are exposed by PgDog:

```bash
curl -s http://localhost:9091/metrics | rg sharding_lookup
```

Useful logs are available with:

```bash
docker compose logs -f pgdog
```

## Reset the local demo

This deletes only the new Docker-backed local databases. It does not run PgDog resharding.

```bash
docker compose down -v
```
