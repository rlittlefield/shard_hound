defmodule ShardHound.DemoData do
  import Ecto.Query

  alias ShardHound.DemoData.CatalogParams
  alias ShardHound.DemoData.GenerationParams
  alias ShardHound.DemoData.GenerateDatasetWorker
  alias ShardHound.DeviceManagement.CustomPackage
  alias ShardHound.DeviceManagement.Deployment
  alias ShardHound.DeviceManagement.Device
  alias ShardHound.DeviceManagement.DeviceSoftware
  alias ShardHound.DeviceManagement.Organization
  alias ShardHound.DeviceManagement.ShardHoundPackage
  alias ShardHound.Repo
  alias ShardHound.Topology

  @terminal_states ~w(completed discarded cancelled)
  @active_states Oban.Job.states() |> Enum.map(&to_string/1) |> Kernel.--(@terminal_states)

  @omnisharded_tables ~w(organizations shard_hound_packages shards)
  # Hybrid (`kind = "hybrid"`) tables: keyed rows are tenant-local, NULL
  # organization_id rows are shared defaults broadcast to every shard.
  @hybrid_tables ~w(custom_packages)
  @count_tables @omnisharded_tables ++
                  ~w(devices device_software groups group_devices custom_packages deployments commands)

  # Each shard's tenant id sequences live in a disjoint range so rows
  # keep their ids when MOVE KEYS relocates them.
  @sequence_range_size 1_000_000_000_000

  def sequence_range_start(shard), do: shard * @sequence_range_size

  @doc """
  Moves a shard's tenant id sequences into their disjoint range,
  routed through PgDog with `pgdog_shard` directives. Run against a
  shard that `ADD SHARD` just activated, before tenants land on it;
  `mix shard_hound.sequence_ranges` is the direct-connection
  equivalent for shards PgDog isn't serving yet.
  """
  def apply_sequence_ranges(shard) do
    base = sequence_range_start(shard)

    for table <- @count_tables -- @omnisharded_tables do
      sequence = "#{table}_id_seq"

      Repo.query!(
        "/* pgdog_shard: #{shard} */ SELECT setval('#{sequence}', GREATEST((SELECT last_value FROM #{sequence}), #{base}))"
      )
    end

    :ok
  end

  def change_generation(params \\ %GenerationParams{}, attrs \\ %{}) do
    GenerationParams.changeset(params, attrs)
  end

  def enqueue_generation(attrs) do
    changeset =
      %GenerationParams{}
      |> change_generation(attrs)
      |> validate_against_catalog()

    if changeset.valid? do
      generation_id = Ecto.UUID.generate()

      args =
        changeset
        |> Ecto.Changeset.apply_changes()
        |> Map.from_struct()
        |> Map.put(:generation_id, generation_id)

      case args |> GenerateDatasetWorker.new() |> Oban.insert() do
        {:ok, job} -> {:ok, generation_id, job}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, changeset}
    end
  end

  # Device software inventory is drawn from the managed catalog, so a
  # generation can't ask for more apps per device than the catalog
  # holds. Checked at enqueue time (one query), not on every keystroke.
  defp validate_against_catalog(changeset) do
    software = Ecto.Changeset.get_field(changeset, :software_per_device)
    managed = catalog_stats().managed_packages

    if software && software > managed do
      Ecto.Changeset.add_error(
        changeset,
        :software_per_device,
        "exceeds the shared catalog — seed at least #{software} managed packages first (#{managed} seeded)"
      )
    else
      changeset
    end
  end

  @real_packages [
    %{
      name: "Google Chrome",
      slug: "google-chrome",
      publisher: "Google",
      version: "140.0.7339.41"
    },
    %{name: "Mozilla Firefox", slug: "mozilla-firefox", publisher: "Mozilla", version: "142.0"},
    %{
      name: "Visual Studio Code",
      slug: "visual-studio-code",
      publisher: "Microsoft",
      version: "1.103.0"
    },
    %{name: "Slack", slug: "slack", publisher: "Salesforce", version: "4.45.64"},
    %{name: "Zoom Workplace", slug: "zoom", publisher: "Zoom", version: "6.5.7"},
    %{name: "1Password", slug: "1password", publisher: "AgileBits", version: "8.11.6"},
    %{name: "Docker Desktop", slug: "docker-desktop", publisher: "Docker", version: "4.44.3"},
    %{
      name: "Microsoft Teams",
      slug: "microsoft-teams",
      publisher: "Microsoft",
      version: "25206.1207"
    },
    %{name: "Notion", slug: "notion", publisher: "Notion Labs", version: "4.13.0"},
    %{name: "Figma", slug: "figma", publisher: "Figma", version: "125.6.5"}
  ]

  @doc """
  The managed catalog's package definitions: a handful of recognizable
  real packages, padded with synthetic ones up to `count`. String keys,
  matching what workers used to receive through Oban's JSON args.
  """
  def package_definitions(count) do
    real = Enum.take(@real_packages, count)

    synthetic =
      if count > length(@real_packages) do
        Enum.map((length(@real_packages) + 1)..count, fn index ->
          number = index |> Integer.to_string() |> String.pad_leading(4, "0")

          %{
            name: "Managed App #{number}",
            slug: "managed-app-#{number}",
            publisher: "Publisher #{rem(index, 40) + 1}",
            version: "#{rem(index, 9) + 1}.#{rem(index, 20)}.#{rem(index, 7)}"
          }
        end)
      else
        []
      end

    Enum.map(real ++ synthetic, fn package ->
      %{
        "name" => package.name,
        "slug" => package.slug,
        "publisher" => package.publisher,
        "version" => package.version
      }
    end)
  end

  def change_catalog(params \\ %CatalogParams{}, attrs \\ %{}) do
    CatalogParams.changeset(params, attrs)
  end

  @doc """
  Seeds the shared, not-per-organization data on every shard:

  - the managed catalog (`shard_hound_packages`, omnisharded — the
    whole table broadcasts);
  - the default packages (NULL-key rows in the hybrid
    `custom_packages` table, broadcast by the hybrid-kind routing).

  Both are upserts on app-supplied stable ids, so re-seeding with a
  higher count only adds rows and broadcast copies stay identical
  across shards. Must run outside any `pgdog.sharding_key` pin — a
  keyed transaction would land the writes on a single shard.
  """
  def seed_shared_catalog(attrs) do
    changeset = change_catalog(%CatalogParams{}, attrs)

    if changeset.valid? do
      params = Ecto.Changeset.apply_changes(changeset)

      params.managed_packages
      |> package_definitions()
      |> seed_managed_packages()

      seed_default_packages(params.default_packages)

      {:ok, params}
    else
      {:error, changeset}
    end
  end

  @doc """
  Current size of the shared catalog: managed packages (omni) and
  default packages (hybrid NULL-key rows). Broadcast rows are identical
  everywhere, so one shard answers for the fleet.
  """
  def catalog_stats do
    prefix =
      if Application.fetch_env!(:shard_hound, :pgdog_enabled),
        do: "/* pgdog_shard: 0 */ ",
        else: ""

    %{rows: [[managed, defaults]]} =
      Repo.query!(
        prefix <>
          "SELECT (SELECT count(*) FROM shard_hound_packages), " <>
          "(SELECT count(*) FROM custom_packages WHERE organization_id IS NULL)"
      )

    %{managed_packages: managed, default_packages: defaults}
  end

  defp seed_managed_packages(definitions) do
    now = DateTime.utc_now(:second)

    rows =
      Enum.map(definitions, fn package ->
        %{
          id: stable_id("managed-package:#{package["slug"]}:universal:universal"),
          name: package["name"],
          slug: package["slug"],
          platform: "universal",
          architecture: "universal",
          installer_type: "managed",
          latest_version: package["version"],
          metadata: %{publisher: package["publisher"]},
          inserted_at: now,
          updated_at: now
        }
      end)

    rows
    |> Enum.chunk_every(2_000)
    |> Enum.each(
      &Repo.insert_all(ShardHoundPackage, &1,
        conflict_target: [:slug, :platform, :architecture],
        on_conflict: {:replace, [:name, :latest_version, :metadata, :updated_at]}
      )
    )
  end

  # Default packages live in the hybrid custom_packages table with a
  # NULL organization_id; PgDog broadcasts each insert to every shard,
  # so ids are app-supplied to keep broadcast copies identical.
  defp seed_default_packages(count) do
    now = DateTime.utc_now(:second)

    rows =
      Range.new(1, count, 1)
      |> Enum.map(fn package_index ->
        %{
          id: stable_id("default-package:#{package_index}"),
          organization_id: nil,
          name: "Default Tool #{package_index}",
          slug: "default-tool-#{package_index}",
          platform: if(rem(package_index, 2) == 0, do: "windows", else: "macos"),
          architecture: "universal",
          installer_type: "default",
          latest_version: "#{rem(package_index, 4) + 1}.#{package_index}.0",
          metadata: %{catalog: "default"},
          inserted_at: now,
          updated_at: now
        }
      end)

    rows
    |> Enum.chunk_every(200)
    |> Enum.each(
      &Repo.insert_all(CustomPackage, &1,
        conflict_target: [:id],
        on_conflict: {:replace, [:name, :latest_version, :metadata, :updated_at]}
      )
    )
  end

  def generation_status(nil), do: empty_status()

  def generation_status(generation_id) do
    states =
      Oban.Job
      |> where(
        [job],
        fragment("? @> ?", job.args, type(^%{"generation_id" => generation_id}, :map))
      )
      |> group_by([job], job.state)
      |> select([job], {job.state, count(job.id)})
      |> oban_repo().all()
      |> Map.new()

    %{
      total: Enum.sum(Map.values(states)),
      active: sum_states(states, @active_states),
      completed: Map.get(states, "completed", 0),
      failed: sum_states(states, ~w(discarded cancelled)),
      states: states
    }
  end

  @doc """
  Re-asserts every tenant table's replica identity. The ADD SHARD
  schema sync carries the `(organization_id, id)` indexes but loses
  the `REPLICA IDENTITY USING INDEX` setting, which MOVE KEYS refuses
  on. DDL through PgDog broadcasts, so this reaches the new shard and
  no-ops on the rest. Hybrid tables have a nullable sharding column no
  identity index can include, so they use REPLICA IDENTITY FULL.
  """
  def ensure_replica_identities do
    for table <- @count_tables -- @omnisharded_tables do
      if table in @hybrid_tables do
        Repo.query!("ALTER TABLE #{table} REPLICA IDENTITY FULL")
      else
        Repo.query!(
          "ALTER TABLE #{table} REPLICA IDENTITY USING INDEX #{table}_organization_id_id_index"
        )
      end
    end

    :ok
  end

  @doc """
  Lists every organization with its current placement. Organizations
  are omnisharded, so any shard answers with the full, identical set.
  """
  def organizations_with_shards do
    Organization
    |> order_by([o], asc: o.name, asc: o.id)
    |> select([o], %{id: o.id, name: o.name, shard_id: o.shard_id})
    |> Repo.all()
  end

  @doc """
  Audits tenant placement: every tenant row must live on the shard its
  organization's `shard_id` names. Each shard is asked, per tenant
  table, for rows whose local `organizations` copy (broadcast, so the
  placement column is the same everywhere) points at a different
  shard.

  Returns the total number of organizations, how many are clean, and
  one entry per organization with stray rows: which tables, on which
  shard, and how many rows. Rows copied by an in-flight MOVE KEYS task
  show up here until its cutover flips the placement.
  """
  def audit_placement do
    shards =
      if Application.fetch_env!(:shard_hound, :pgdog_enabled) do
        Topology.shard_ids()
      else
        []
      end

    problems =
      for shard <- shards,
          table <- @count_tables -- @omnisharded_tables,
          [id, name, expected_shard, count] <- wrong_shard_rows(table, shard),
          reduce: %{} do
        acc ->
          problem = %{table: table, shard: shard, count: count}

          Map.update(
            acc,
            id,
            %{id: id, name: name, expected_shard: expected_shard, rows: [problem]},
            &%{&1 | rows: [problem | &1.rows]}
          )
      end

    problems =
      problems
      |> Map.values()
      |> Enum.map(&%{&1 | rows: Enum.sort_by(&1.rows, fn row -> {row.table, row.shard} end)})
      |> Enum.sort_by(& &1.id)

    total = organization_count(shards)

    %{
      audited_at: DateTime.utc_now(:second),
      total: total,
      clean: total - length(problems),
      problems: problems
    }
  end

  defp wrong_shard_rows(table, shard) do
    %{rows: rows} =
      Repo.query!(
        "/* pgdog_shard: #{shard} */ " <>
          "SELECT o.id, o.name, o.shard_id, count(*) FROM #{table} t " <>
          "JOIN organizations o ON o.id = t.organization_id " <>
          "WHERE o.shard_id <> #{shard} GROUP BY o.id, o.name, o.shard_id"
      )

    rows
  end

  defp organization_count([]), do: Repo.aggregate(Organization, :count)
  defp organization_count([shard | _]), do: direct_count(Organization, shard)

  @hybrid_table "custom_packages"

  @doc """
  Verifies the hybrid `custom_packages` table on every serving shard,
  answering two questions after a topology change:

  - Did `ADD SHARD` copy the NULL-key default rows? Every shard must
    hold the same default rows: same count, same fingerprint (an md5
    over the ordered ids, slugs and versions), matched against the
    lowest shard as the reference.
  - Did `MOVE KEYS` move the keyed rows, and only those? No keyed row
    may sit on a shard other than its organization's, and every
    custom-package deployment on a shard must resolve to a package
    that shard holds (the tenant's own row, or a default).

  Per shard it also reports the replica identity: MOVE KEYS refuses a
  hybrid table unless it is FULL, because no identity index can
  include a nullable sharding column. Rows copied by an in-flight
  MOVE KEYS task show up as strays until its cutover flips placement.

  Without PgDog there is a single unsharded database, reported as one
  `nil` shard with the stray check skipped.
  """
  def hybrid_report do
    shards =
      if Application.fetch_env!(:shard_hound, :pgdog_enabled) do
        Topology.shard_ids()
      else
        [nil]
      end

    checks = Enum.map(shards, &hybrid_shard_check/1)
    reference = List.first(checks)

    checks =
      Enum.map(checks, fn check ->
        defaults_match? = check.default_fingerprint == reference.default_fingerprint

        %{check | defaults_match: defaults_match?}
        |> Map.put(:ok, hybrid_shard_ok?(check, defaults_match?))
      end)

    %{
      table: @hybrid_table,
      checked_at: DateTime.utc_now(:second),
      expected_defaults: reference.default_rows,
      shards: checks,
      ok: Enum.all?(checks, & &1.ok)
    }
  end

  defp hybrid_shard_ok?(check, defaults_match?) do
    defaults_match? and check.stray_rows == 0 and check.dangling_deployments == 0 and
      check.replica_identity == "full" and check.nullable_key
  end

  defp hybrid_shard_check(shard) do
    prefix = if shard, do: "/* pgdog_shard: #{shard} */ ", else: ""

    %{rows: [[default_rows, fingerprint, keyed_rows, replident, nullable]]} =
      Repo.query!(
        prefix <>
          "SELECT (SELECT count(*) FROM #{@hybrid_table} WHERE organization_id IS NULL), " <>
          "(SELECT md5(coalesce(string_agg(id::text || ':' || slug || ':' || latest_version, ',' ORDER BY id), '')) " <>
          "FROM #{@hybrid_table} WHERE organization_id IS NULL), " <>
          "(SELECT count(*) FROM #{@hybrid_table} WHERE organization_id IS NOT NULL), " <>
          "(SELECT relreplident::text FROM pg_class WHERE oid = '#{@hybrid_table}'::regclass), " <>
          "(SELECT NOT attnotnull FROM pg_attribute " <>
          "WHERE attrelid = '#{@hybrid_table}'::regclass AND attname = 'organization_id')"
      )

    # Placement predicates only mean something with a sharded fleet:
    # a single database holds every tenant by definition.
    home = if shard, do: "o.shard_id = #{shard}", else: "true"
    away = if shard, do: "o.shard_id <> #{shard}", else: "false"

    %{rows: [[stray_rows]]} =
      Repo.query!(
        prefix <>
          "SELECT count(*) FROM #{@hybrid_table} p " <>
          "JOIN organizations o ON o.id = p.organization_id WHERE #{away}"
      )

    %{rows: [[dangling]]} =
      Repo.query!(
        prefix <>
          "SELECT count(*) FROM deployments d " <>
          "JOIN organizations o ON o.id = d.organization_id " <>
          "WHERE d.package_type = 'custom' AND #{home} AND NOT EXISTS (" <>
          "SELECT 1 FROM #{@hybrid_table} p WHERE p.id = d.package_id " <>
          "AND (p.organization_id IS NULL OR p.organization_id = d.organization_id))"
      )

    %{
      shard: shard,
      default_rows: default_rows,
      default_fingerprint: fingerprint,
      defaults_match: true,
      keyed_rows: keyed_rows,
      stray_rows: stray_rows,
      dangling_deployments: dangling,
      replica_identity: replica_identity_name(replident),
      nullable_key: nullable
    }
  end

  defp replica_identity_name("f"), do: "full"
  defp replica_identity_name("d"), do: "default"
  defp replica_identity_name("i"), do: "index"
  defp replica_identity_name("n"), do: "nothing"
  defp replica_identity_name(other), do: other

  @doc """
  Deletes all generated demo data and clears the generation queue.

  `organizations` is omnisharded, so PgDog broadcasts the TRUNCATE to
  every shard, and CASCADE follows the foreign keys through every
  tenant table. Sequences are left alone: each shard keeps its
  disjoint id range. Running generation jobs are cancelled before the
  truncate so a mid-flight transaction can't repopulate tables.
  """
  def reset_demo_data do
    Oban.cancel_all_jobs(Oban.Job)
    Repo.query!("TRUNCATE organizations CASCADE")
    Repo.query!("TRUNCATE shard_hound_packages")
    oban_repo().delete_all(Oban.Job)
    :ok
  end

  def database_stats do
    if Application.fetch_env!(:shard_hound, :pgdog_enabled) do
      %{
        organizations: direct_count(Organization, 0),
        devices: sharded_count(Device),
        software: sharded_count(DeviceSoftware),
        deployments: sharded_count(Deployment)
      }
    else
      %{
        organizations: Repo.aggregate(Organization, :count),
        devices: Repo.aggregate(Device, :count),
        software: Repo.aggregate(DeviceSoftware, :count),
        deployments: Repo.aggregate(Deployment, :count)
      }
    end
  end

  @doc """
  Counts every table's rows on every shard, one direct count per
  shard via `pgdog_shard` directives. Omnisharded tables report the
  same figure on each shard because their rows are broadcast.

  Without PgDog there is a single unsharded database, reported as one
  `nil` shard.
  """
  def shard_table_counts do
    shards =
      if Application.fetch_env!(:shard_hound, :pgdog_enabled) do
        Topology.shard_ids()
      else
        [nil]
      end

    rows =
      for table <- @count_tables do
        %{
          table: table,
          omni: table in @omnisharded_tables,
          hybrid: table in @hybrid_tables,
          counts: Enum.map(shards, &table_count(table, &1))
        }
      end

    %{shards: shards, rows: rows}
  end

  defp sharded_count(schema) do
    Enum.reduce(Topology.shard_ids(), 0, fn shard, total ->
      total + direct_count(schema, shard)
    end)
  end

  defp direct_count(schema, shard) do
    table_count(schema.__schema__(:source), shard)
  end

  defp table_count(table, nil) do
    %{rows: [[count]]} = Repo.query!("SELECT count(*) FROM #{table}")
    count
  end

  defp table_count(table, shard) do
    %{rows: [[count]]} =
      Repo.query!("/* pgdog_shard: #{shard} */ SELECT count(*) FROM #{table}")

    count
  end

  def stable_id(value) do
    <<integer::unsigned-integer-size(64), _rest::binary>> = :crypto.hash(:sha256, value)
    rem(integer, 9_223_372_036_854_775_806) + 1
  end

  defp empty_status do
    %{total: 0, active: 0, completed: 0, failed: 0, states: %{}}
  end

  defp sum_states(states, names) do
    Enum.reduce(names, 0, &(&2 + Map.get(states, &1, 0)))
  end

  defp oban_repo do
    :shard_hound
    |> Application.fetch_env!(Oban)
    |> Keyword.fetch!(:repo)
  end
end
