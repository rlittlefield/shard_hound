defmodule ShardHoundWeb.DataGeneratorLive do
  use ShardHoundWeb, :live_view

  alias ShardHound.DemoData
  alias ShardHound.DemoData.CatalogParams
  alias ShardHound.DemoData.GenerationParams
  alias ShardHound.PgDog
  alias ShardHound.Shards
  alias ShardHound.Topology

  @impl true
  def mount(_params, _session, socket) do
    params = %GenerationParams{}

    {:ok,
     socket
     |> assign(:page_title, "Dataset Generator")
     |> assign(:generation_id, nil)
     |> assign(:refresh_ref, nil)
     |> assign(:counts_ref, nil)
     |> assign(:generation_status, DemoData.generation_status(nil))
     |> assign(:database_stats, DemoData.database_stats())
     |> assign(:estimate, estimate(params))
     |> assign(:form, to_form(DemoData.change_generation(params)))
     |> assign(:catalog_form, to_form(DemoData.change_catalog()))
     |> assign(:catalog_stats, DemoData.catalog_stats())
     |> assign(:audit, nil)
     |> assign(:hybrid_report, nil)
     |> assign(:hybrid_trigger, nil)
     |> assign(:pgdog_enabled, Application.fetch_env!(:shard_hound, :pgdog_enabled))
     |> assign(:shard_ids, Topology.shard_ids())
     |> assign(:organizations, DemoData.organizations_with_shards())
     |> refresh_shards_panel()
     |> assign(:selected_org_ids, MapSet.new())
     |> assign(:move_source, 0)
     |> assign(:move_target, "1")
     |> assign(:admin_task, nil)
     |> refresh_counts()}
  end

  @impl true
  def handle_event("validate", %{"generation_params" => params}, socket) do
    changeset =
      %GenerationParams{}
      |> DemoData.change_generation(params)
      |> Map.put(:action, :validate)

    {:noreply,
     socket
     |> assign(:estimate, estimate(Ecto.Changeset.apply_changes(changeset)))
     |> assign(:form, to_form(changeset))}
  end

  def handle_event("generate", %{"generation_params" => params}, socket) do
    case DemoData.enqueue_generation(params) do
      {:ok, generation_id, _job} ->
        {:noreply,
         socket
         |> put_flash(:info, "Generation queued. Oban is distributing organization jobs.")
         |> cancel_refresh()
         |> assign(:generation_id, generation_id)
         |> assign(:generation_status, DemoData.generation_status(generation_id))
         |> schedule_refresh()}

      {:error, %Ecto.Changeset{data: %GenerationParams{}} = changeset} ->
        {:noreply, assign(socket, :form, to_form(Map.put(changeset, :action, :validate)))}

      {:error, _reason} ->
        {:noreply, put_flash(socket, :error, "The generation job could not be queued.")}
    end
  end

  def handle_event("validate_catalog", %{"catalog_params" => params}, socket) do
    changeset =
      %CatalogParams{}
      |> DemoData.change_catalog(params)
      |> Map.put(:action, :validate)

    {:noreply, assign(socket, :catalog_form, to_form(changeset))}
  end

  def handle_event("seed_catalog", %{"catalog_params" => params}, socket) do
    case DemoData.seed_shared_catalog(params) do
      {:ok, seeded} ->
        {:noreply,
         socket
         |> put_flash(
           :info,
           "Shared catalog seeded on every shard: #{seeded.managed_packages} managed, " <>
             "#{seeded.default_packages} default packages."
         )
         |> assign(:catalog_stats, DemoData.catalog_stats())
         |> refresh_counts()}

      {:error, changeset} ->
        {:noreply, assign(socket, :catalog_form, to_form(Map.put(changeset, :action, :validate)))}
    end
  end

  def handle_event("reset", _params, socket) do
    :ok = DemoData.reset_demo_data()

    {:noreply,
     socket
     |> put_flash(:info, "Demo data cleared on every shard.")
     |> cancel_refresh()
     |> assign(:generation_id, nil)
     |> assign(:generation_status, DemoData.generation_status(nil))
     |> assign(:database_stats, DemoData.database_stats())
     |> assign(:organizations, DemoData.organizations_with_shards())
     |> assign(:catalog_stats, DemoData.catalog_stats())
     |> refresh_shards_panel()
     |> refresh_counts()}
  end

  def handle_event("refresh_counts", _params, socket) do
    {:noreply, refresh_counts(socket)}
  end

  def handle_event("audit", _params, socket) do
    {:noreply, assign(socket, :audit, DemoData.audit_placement())}
  end

  def handle_event("hybrid_check", _params, socket) do
    {:noreply, run_hybrid_check(socket, "manual check")}
  end

  # Switching origin tabs clears the selection: MOVE KEYS moves keys
  # from exactly one source shard per call.
  def handle_event("move_source", %{"shard" => shard}, socket) do
    source = String.to_integer(shard)

    target =
      if socket.assigns.move_target == to_string(source) do
        socket.assigns.shard_ids
        |> Enum.find(&(&1 != source))
        |> to_string()
      else
        socket.assigns.move_target
      end

    {:noreply,
     socket
     |> assign(:move_source, source)
     |> assign(:move_target, target)
     |> assign(:selected_org_ids, MapSet.new())}
  end

  # Toggles the whole current origin tab: select every organization on
  # it, or clear the selection if they're all already selected.
  def handle_event("move_select_all", _params, socket) do
    ids =
      socket.assigns.organizations
      |> source_orgs(socket.assigns.move_source)
      |> MapSet.new(&to_string(&1.id))

    selected =
      if MapSet.subset?(ids, socket.assigns.selected_org_ids) do
        MapSet.new()
      else
        ids
      end

    {:noreply, assign(socket, :selected_org_ids, selected)}
  end

  def handle_event("move_selection", params, socket) do
    {:noreply,
     socket
     |> assign(:selected_org_ids, MapSet.new(Map.get(params, "org_ids", [])))
     |> assign(:move_target, params["target_shard"] || socket.assigns.move_target)}
  end

  def handle_event("move_shard", params, socket) do
    keys = Map.get(params, "org_ids", [])
    target = String.to_integer(params["target_shard"])

    case PgDog.Admin.move_keys(target, keys) do
      {:ok, task_id} ->
        {:noreply,
         socket
         |> put_flash(
           :info,
           "MOVE KEYS task #{task_id} started: #{length(keys)} organization(s) to shard #{target} (AUTO)."
         )
         |> assign(:selected_org_ids, MapSet.new())
         |> track_admin_task(:move_keys, task_id)}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, "MOVE KEYS refused: #{message}")}
    end
  end

  def handle_event("add_shard", _params, socket) do
    shard = Topology.next_shard()

    case PgDog.Admin.add_shard(shard) do
      {:ok, task_id} ->
        {:noreply,
         socket
         |> put_flash(
           :info,
           "ADD SHARD task #{task_id} started: provisioning shard #{shard} (AUTO)."
         )
         |> track_admin_task({:add_shard, shard}, task_id)}

      {:error, message} ->
        {:noreply, put_flash(socket, :error, "ADD SHARD refused: #{message}")}
    end
  end

  def handle_event("toggle_shard", %{"shard" => shard}, socket) do
    shard = String.to_integer(shard)
    row = Enum.find(socket.assigns.policy_shards, &(&1.shard_id == shard))
    :ok = Shards.set_enabled(shard, !row.enabled_for_new_orgs)

    {:noreply, refresh_shards_panel(socket)}
  end

  @impl true
  def handle_info(
        {:refresh_generation, generation_id},
        %{assigns: %{generation_id: generation_id}} = socket
      ) do
    status = DemoData.generation_status(generation_id)
    socket = assign(socket, :refresh_ref, nil)

    socket =
      if status.active > 0 do
        schedule_refresh(socket)
      else
        socket
        |> assign(:database_stats, DemoData.database_stats())
        |> assign(:organizations, DemoData.organizations_with_shards())
        |> refresh_shards_panel()
      end

    {:noreply, assign(socket, :generation_status, status)}
  end

  def handle_info({:refresh_generation, _stale_generation_id}, socket), do: {:noreply, socket}

  def handle_info(:refresh_counts, socket), do: {:noreply, refresh_counts(socket)}

  def handle_info(:poll_admin_task, %{assigns: %{admin_task: nil}} = socket),
    do: {:noreply, socket}

  def handle_info(:poll_admin_task, socket) do
    task = socket.assigns.admin_task
    root = task_row(task.id)

    cond do
      root != nil and root["status"] == "running" and task.polls < 300 ->
        Process.send_after(self(), :poll_admin_task, 1_000)

        {:noreply,
         assign(socket, :admin_task, %{
           task
           | status: root["status"],
             inner_status: root["inner_status"],
             polls: task.polls + 1
         })}

      true ->
        outcome = if root, do: root["status"], else: "finished"

        {:noreply,
         socket
         |> assign(:admin_task, nil)
         |> finish_admin_task(task, outcome)}
    end
  end

  defp track_admin_task(socket, kind, task_id) do
    Process.send_after(self(), :poll_admin_task, 1_000)

    assign(socket, :admin_task, %{
      id: task_id,
      kind: kind,
      status: "running",
      inner_status: nil,
      polls: 0
    })
  end

  # A finished ADD SHARD leaves a live shard with a synced schema but
  # no migration ledger, blank sequences and no placement row: adopt
  # the ledger, give its sequences their disjoint range and register
  # it for new organizations before anything lands on it.
  defp finish_admin_task(socket, %{kind: {:add_shard, shard}}, "finished") do
    :ok = Topology.adopt_schema_migrations(shard)
    :ok = DemoData.ensure_replica_identities()
    :ok = DemoData.apply_sequence_ranges(shard)
    :ok = Shards.register(shard)

    socket
    |> put_flash(
      :info,
      "ADD SHARD finished: shard #{shard} is live, sequenced, and enabled for new organizations."
    )
    |> refresh_topology()
    |> run_hybrid_check("ADD SHARD #{shard}")
  end

  defp finish_admin_task(socket, task, outcome) do
    flash_kind = if outcome == "finished", do: :info, else: :error

    socket
    |> put_flash(flash_kind, "#{task_label(task.kind)} task #{task.id} #{outcome}.")
    |> refresh_topology()
    |> run_hybrid_check("#{task_label(task.kind)} task #{task.id} (#{outcome})")
  end

  # The hybrid table is the one place both topology commands touch
  # differently (ADD SHARD copies its NULL-key rows, MOVE KEYS moves
  # only its keyed rows), so every finished task re-runs the check and
  # the panel shows what triggered the latest result.
  defp run_hybrid_check(socket, trigger) do
    socket
    |> assign(:hybrid_report, DemoData.hybrid_report())
    |> assign(:hybrid_trigger, trigger)
  end

  defp refresh_topology(socket) do
    socket
    |> assign(:shard_ids, Topology.shard_ids())
    |> refresh_shards_panel()
    |> assign(:organizations, DemoData.organizations_with_shards())
    |> refresh_counts()
  end

  defp refresh_shards_panel(socket) do
    socket
    |> assign(:policy_shards, Shards.list())
    |> assign(:org_counts, Shards.organization_counts())
  end

  defp task_label(:move_keys), do: "MOVE KEYS"
  defp task_label({:add_shard, _shard}), do: "ADD SHARD"

  # Root tasks carry their own id and no parent; subtasks (the
  # per-table copy phases) hang off `parent_id`.
  defp task_row(task_id) do
    case PgDog.Admin.tasks() do
      {:ok, rows} ->
        Enum.find(rows, fn row ->
          row["id"] == to_string(task_id) and row["parent_id"] in [nil, ""]
        end)

      {:error, _message} ->
        nil
    end
  end

  # Recounts every table on every shard and restarts the once-a-minute
  # timer, so a manual refresh also pushes the next automatic one out.
  defp refresh_counts(socket) do
    if socket.assigns.counts_ref, do: Process.cancel_timer(socket.assigns.counts_ref)

    counts_ref =
      if connected?(socket) do
        Process.send_after(self(), :refresh_counts, 60_000)
      end

    socket
    |> assign(:shard_counts, DemoData.shard_table_counts())
    |> assign(:counts_updated_at, DateTime.utc_now(:second))
    |> assign(:counts_ref, counts_ref)
  end

  defp schedule_refresh(socket) do
    refresh_ref =
      Process.send_after(self(), {:refresh_generation, socket.assigns.generation_id}, 1_000)

    assign(socket, :refresh_ref, refresh_ref)
  end

  defp cancel_refresh(%{assigns: %{refresh_ref: nil}} = socket), do: socket

  defp cancel_refresh(socket) do
    Process.cancel_timer(socket.assigns.refresh_ref)
    assign(socket, :refresh_ref, nil)
  end

  defp estimate(params) do
    organizations = params.organizations || 0
    devices = organizations * (params.devices_per_organization || 0)
    software = devices * (params.software_per_device || 0)
    groups = organizations * (params.groups_per_organization || 0)
    custom_packages = organizations * (params.custom_packages_per_organization || 0)
    deployments = organizations * (params.deployments_per_organization || 0)

    %{
      organizations: organizations,
      devices: devices,
      software: software,
      groups: groups,
      custom_packages: custom_packages,
      deployments: deployments,
      total: organizations + devices + software + groups + custom_packages + deployments
    }
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <section id="data-generator" class="space-y-9">
        <div class="grid gap-8 lg:grid-cols-[minmax(0,1fr)_22rem] lg:items-end">
          <div class="max-w-3xl">
            <div class="mb-5 flex items-center gap-3 text-xs font-semibold uppercase tracking-[0.22em] text-cyan-300">
              <span class="h-px w-8 bg-cyan-300/60"></span> Synthetic fleet foundry
            </div>
            <h1 class="text-balance text-4xl font-semibold tracking-[-0.04em] text-white sm:text-6xl">
              Build a fleet worth <span class="text-cyan-300">sharding.</span>
            </h1>
            <p class="mt-5 max-w-2xl text-pretty text-base leading-7 text-slate-400 sm:text-lg">
              Queue logically connected organizations, devices, software inventory, dynamic groups,
              packages, and deployments. Every tenant row carries the organization shard key.
            </p>
          </div>

          <div class="space-y-2">
            <div class="grid grid-cols-2 gap-px overflow-hidden rounded-2xl border border-white/8 bg-white/8 shadow-2xl shadow-black/20">
              <.stat value={@database_stats.organizations} label="Organizations" />
              <.stat value={@database_stats.devices} label="Devices" />
              <.stat value={@database_stats.software} label="Software rows" />
              <.stat value={@database_stats.deployments} label="Deployments" />
            </div>
            <button
              id="reset-data-button"
              type="button"
              phx-click="reset"
              data-confirm="Delete all demo data on every shard? Queued generation jobs are cancelled too."
              phx-disable-with="Resetting..."
              class="inline-flex w-full min-h-9 items-center justify-center gap-2 rounded-xl border border-rose-400/25 bg-rose-400/5 px-4 text-xs font-semibold text-rose-200 transition hover:border-rose-400/50 hover:bg-rose-400/10 disabled:cursor-wait disabled:opacity-70"
            >
              <.icon name="hero-arrow-path" class="size-3.5" /> Reset all demo data
            </button>
          </div>
        </div>

        <div class="grid gap-6 xl:grid-cols-[minmax(0,1fr)_21rem]">
          <div class="overflow-hidden rounded-3xl border border-white/8 bg-[#0c1220]/95 shadow-[0_24px_80px_rgba(0,0,0,0.28)]">
            <div class="flex items-center justify-between border-b border-white/8 px-6 py-5 sm:px-8">
              <div>
                <h2 class="text-lg font-semibold text-white">Generation profile</h2>
                <p class="mt-1 text-sm text-slate-500">
                  One coordinator job, then one job per organization.
                </p>
              </div>
              <span class="hidden rounded-full border border-violet-300/20 bg-violet-300/10 px-3 py-1 text-xs font-semibold text-violet-200 sm:block">
                shard key: organization_id
              </span>
            </div>

            <.form
              for={@form}
              id="data-generator-form"
              phx-change="validate"
              phx-submit="generate"
              class="p-6 sm:p-8"
            >
              <div class="grid gap-x-5 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
                <%= for field <- GenerationParams.count_fields() do %>
                  <.input
                    field={@form[field.name]}
                    type="number"
                    label={field.label}
                    min={field.min}
                    max={field.max}
                    class={[
                      "w-full rounded-xl border border-white/10 bg-[#070b14] px-4 py-3 text-sm font-medium text-white outline-none transition placeholder:text-slate-700 focus:border-cyan-300/60 focus:ring-4 focus:ring-cyan-300/5"
                    ]}
                    error_class={[
                      "w-full rounded-xl border border-rose-400/60 bg-rose-400/5 px-4 py-3 text-sm font-medium text-white outline-none ring-4 ring-rose-400/5"
                    ]}
                  />
                <% end %>
              </div>

              <div class="mt-7 flex flex-col gap-5 border-t border-white/8 pt-6 sm:flex-row sm:items-center sm:justify-between">
                <div class="flex items-center gap-3 text-sm text-slate-400">
                  <span class="grid size-9 place-items-center rounded-xl bg-cyan-300/10 text-cyan-300">
                    <.icon name="hero-circle-stack" class="size-5" />
                  </span>
                  <span>
                    Estimated
                    <strong class="font-semibold text-white">{format_number(@estimate.total)}</strong>
                    primary rows
                  </span>
                </div>
                <.button
                  id="generate-data-button"
                  type="submit"
                  phx-disable-with="Queueing jobs..."
                  class="group inline-flex min-h-12 items-center justify-center gap-2 rounded-xl bg-cyan-300 px-6 text-sm font-bold text-slate-950 shadow-[0_12px_36px_rgba(34,211,238,0.16)] transition hover:-translate-y-0.5 hover:bg-cyan-200 hover:shadow-[0_16px_44px_rgba(34,211,238,0.25)] active:translate-y-0 disabled:cursor-wait disabled:opacity-70"
                >
                  Queue generation
                  <.icon
                    name="hero-arrow-right"
                    class="size-4 transition group-hover:translate-x-0.5"
                  />
                </.button>
              </div>
            </.form>
          </div>

          <aside class="space-y-6">
            <div class="rounded-3xl border border-white/8 bg-[#0c1220]/95 p-6">
              <div class="flex items-center justify-between">
                <h2 class="text-sm font-semibold uppercase tracking-[0.16em] text-slate-400">
                  Shared catalog
                </h2>
                <span class="rounded-full border border-violet-300/20 bg-violet-300/10 px-2 py-0.5 text-[10px] font-semibold text-violet-200">
                  every shard
                </span>
              </div>
              <p class="mt-3 text-xs leading-5 text-slate-500">
                Not per-organization: managed packages fill the omnisharded
                <span class="font-mono">shard_hound_packages</span>
                catalog (device software draws from it), default packages are the hybrid
                <span class="font-mono">custom_packages</span>
                table's NULL-key broadcast rows.
              </p>
              <.form
                for={@catalog_form}
                id="catalog-form"
                phx-change="validate_catalog"
                phx-submit="seed_catalog"
                class="mt-4 space-y-3"
              >
                <%= for field <- CatalogParams.count_fields() do %>
                  <.input
                    field={@catalog_form[field.name]}
                    type="number"
                    label={field.label}
                    min={field.min}
                    max={field.max}
                    class={[
                      "w-full rounded-xl border border-white/10 bg-[#070b14] px-4 py-3 text-sm font-medium text-white outline-none transition placeholder:text-slate-700 focus:border-cyan-300/60 focus:ring-4 focus:ring-cyan-300/5"
                    ]}
                    error_class={[
                      "w-full rounded-xl border border-rose-400/60 bg-rose-400/5 px-4 py-3 text-sm font-medium text-white outline-none ring-4 ring-rose-400/5"
                    ]}
                  />
                <% end %>
                <button
                  id="seed-catalog-button"
                  type="submit"
                  phx-disable-with="Seeding every shard..."
                  class="inline-flex w-full min-h-10 items-center justify-center gap-2 rounded-xl border border-violet-300/25 bg-violet-300/10 px-4 text-xs font-semibold text-violet-100 transition hover:border-violet-300/50 hover:bg-violet-300/15 disabled:cursor-wait disabled:opacity-70"
                >
                  <.icon name="hero-globe-alt" class="size-4" /> Seed shared catalog
                </button>
              </.form>
              <p id="catalog-stats" class="mt-3 text-xs text-slate-500">
                Currently seeded:
                <span class="tabular-nums text-slate-300">
                  {format_number(@catalog_stats.managed_packages)}
                </span>
                managed,
                <span class="tabular-nums text-slate-300">
                  {format_number(@catalog_stats.default_packages)}
                </span>
                default packages.
              </p>
            </div>

            <div class="rounded-3xl border border-white/8 bg-[#0c1220]/95 p-6">
              <div class="flex items-center justify-between">
                <h2 class="text-sm font-semibold uppercase tracking-[0.16em] text-slate-400">
                  Row forecast
                </h2>
                <span class="size-2 rounded-full bg-cyan-300 shadow-[0_0_12px_rgba(34,211,238,0.7)]"></span>
              </div>
              <dl class="mt-5 space-y-3 text-sm">
                <.forecast label="Devices" value={@estimate.devices} />
                <.forecast label="Software" value={@estimate.software} />
                <.forecast label="Dynamic groups" value={@estimate.groups} />
                <.forecast label="Custom packages" value={@estimate.custom_packages} />
                <.forecast label="Deployments" value={@estimate.deployments} border />
              </dl>
            </div>

            <div
              :if={@generation_id}
              id="generation-status"
              class="rounded-3xl border border-emerald-300/15 bg-emerald-300/[0.04] p-6"
            >
              <div class="flex items-center gap-3">
                <span class="relative flex size-3">
                  <span
                    :if={@generation_status.active > 0}
                    class="absolute inline-flex size-full animate-ping rounded-full bg-emerald-400 opacity-60"
                  ></span>
                  <span class="relative inline-flex size-3 rounded-full bg-emerald-400"></span>
                </span>
                <h2 class="text-sm font-semibold text-emerald-100">
                  {if(@generation_status.active > 0,
                    do: "Generation active",
                    else: "Generation settled"
                  )}
                </h2>
              </div>
              <div class="mt-5 grid grid-cols-3 gap-2 text-center">
                <.job_count label="Active" value={@generation_status.active} />
                <.job_count label="Done" value={@generation_status.completed} />
                <.job_count label="Failed" value={@generation_status.failed} />
              </div>
              <p class="mt-4 truncate font-mono text-[10px] text-slate-600" title={@generation_id}>
                {@generation_id}
              </p>
            </div>
          </aside>
        </div>

        <div
          id="shard-counts"
          class="overflow-hidden rounded-3xl border border-white/8 bg-[#0c1220]/95 shadow-[0_24px_80px_rgba(0,0,0,0.28)]"
        >
          <div class="flex flex-wrap items-center justify-between gap-4 border-b border-white/8 px-6 py-5 sm:px-8">
            <div>
              <h2 class="text-lg font-semibold text-white">Rows per shard</h2>
              <p class="mt-1 text-sm text-slate-500">
                One direct count per shard and table. Updated
                <span class="tabular-nums text-slate-300">
                  {Calendar.strftime(@counts_updated_at, "%H:%M:%S")} UTC
                </span>
                — refreshes every minute.
              </p>
            </div>
            <button
              id="refresh-counts-button"
              type="button"
              phx-click="refresh_counts"
              phx-disable-with="Counting..."
              class="inline-flex min-h-10 items-center justify-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 text-xs font-semibold text-slate-200 transition hover:border-cyan-300/40 hover:text-cyan-200 disabled:cursor-wait disabled:opacity-70"
            >
              <.icon name="hero-arrow-path" class="size-4" /> Refresh now
            </button>
          </div>
          <div class="overflow-x-auto px-6 py-4 sm:px-8">
            <table class="w-full min-w-[28rem] text-sm">
              <thead>
                <tr class="text-xs uppercase tracking-wider text-slate-500">
                  <th class="py-2 pr-4 text-left font-semibold">Table</th>
                  <th
                    :for={shard <- @shard_counts.shards}
                    class="py-2 pl-4 text-right font-semibold"
                  >
                    {shard_label(shard)}
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr :for={row <- @shard_counts.rows} class="border-t border-white/5">
                  <td class="py-2.5 pr-4 font-mono text-xs text-slate-300">
                    {row.table}
                    <span
                      :if={row.omni}
                      class="ml-2 rounded-full border border-violet-300/20 bg-violet-300/10 px-2 py-0.5 text-[10px] font-semibold text-violet-200"
                      title="Broadcast to every shard; counts match by design"
                    >
                      omni
                    </span>
                    <span
                      :if={row.hybrid}
                      class="ml-2 rounded-full border border-amber-300/20 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold text-amber-200"
                      title="Hybrid: NULL-key default rows broadcast to every shard, keyed rows stay on their tenant's shard"
                    >
                      hybrid
                    </span>
                  </td>
                  <td
                    :for={count <- row.counts}
                    class="py-2.5 pl-4 text-right tabular-nums text-slate-200"
                  >
                    {format_number(count)}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <div
          :if={@pgdog_enabled}
          id="shards-panel"
          class="overflow-hidden rounded-3xl border border-white/8 bg-[#0c1220]/95 shadow-[0_24px_80px_rgba(0,0,0,0.28)]"
        >
          <div class="flex flex-wrap items-center justify-between gap-4 border-b border-white/8 px-6 py-5 sm:px-8">
            <div>
              <h2 class="text-lg font-semibold text-white">Shards</h2>
              <p class="mt-1 text-sm text-slate-500">
                The serving topology, and which shards accept new organizations.
                <span class="font-mono text-xs text-slate-400">ADD SHARD &hellip; AUTO</span>
                activates the next declared shard: schema, omni data, WAL catch-up, cutover.
              </p>
            </div>
            <div class="flex items-center gap-3">
              <.task_chip id="shards-task-status" task={@admin_task} />
              <button
                id="add-shard-button"
                type="button"
                phx-click="add_shard"
                data-confirm={"Activate shard #{length(@shard_ids)}? Activation is permanent: once tenants land on it, the only way off is MOVE KEYS."}
                disabled={@admin_task != nil}
                phx-disable-with="Provisioning..."
                class="inline-flex min-h-10 items-center justify-center gap-2 rounded-xl bg-cyan-300 px-5 text-xs font-bold text-slate-950 transition hover:-translate-y-0.5 hover:bg-cyan-200 active:translate-y-0 disabled:cursor-not-allowed disabled:opacity-40"
              >
                ADD SHARD {length(@shard_ids)}
              </button>
            </div>
          </div>
          <div class="px-6 py-4 sm:px-8">
            <table class="w-full min-w-[24rem] text-sm">
              <thead>
                <tr class="text-xs uppercase tracking-wider text-slate-500">
                  <th class="py-2 pr-4 text-left font-semibold">Shard</th>
                  <th class="py-2 pr-4 text-left font-semibold">Status</th>
                  <th class="py-2 pr-4 text-right font-semibold">Organizations</th>
                  <th class="py-2 pl-4 text-right font-semibold">New organizations</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={row <- @policy_shards} class="border-t border-white/5">
                  <td class="py-2.5 pr-4 font-mono text-xs text-slate-200">
                    shard {row.shard_id}
                  </td>
                  <td class="py-2.5 pr-4">
                    <span class={[
                      "rounded-full border px-2.5 py-1 font-mono text-[11px]",
                      (row.shard_id in @shard_ids &&
                         "border-emerald-300/20 bg-emerald-300/10 text-emerald-200") ||
                        "border-amber-300/20 bg-amber-300/10 text-amber-200"
                    ]}>
                      {if row.shard_id in @shard_ids, do: "serving", else: "not serving"}
                    </span>
                  </td>
                  <td class="py-2.5 pr-4 text-right tabular-nums text-slate-200">
                    {format_number(Map.get(@org_counts, row.shard_id, 0))}
                  </td>
                  <td class="py-2.5 pl-4 text-right">
                    <label class="inline-flex cursor-pointer items-center gap-2 text-xs text-slate-400">
                      {if row.enabled_for_new_orgs, do: "enabled", else: "disabled"}
                      <input
                        id={"shard-toggle-#{row.shard_id}"}
                        type="checkbox"
                        checked={row.enabled_for_new_orgs}
                        phx-click="toggle_shard"
                        phx-value-shard={row.shard_id}
                        class="toggle toggle-sm border-white/20 bg-[#070b14] checked:border-cyan-300/60 checked:bg-cyan-300/30"
                      />
                    </label>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div
          :if={@pgdog_enabled}
          id="move-keys"
          class="overflow-hidden rounded-3xl border border-white/8 bg-[#0c1220]/95 shadow-[0_24px_80px_rgba(0,0,0,0.28)]"
        >
          <div class="flex flex-wrap items-center justify-between gap-4 border-b border-white/8 px-6 py-5 sm:px-8">
            <div>
              <h2 class="text-lg font-semibold text-white">Move organizations</h2>
              <p class="mt-1 text-sm text-slate-500">
                One <span class="font-mono text-xs text-slate-400">MOVE KEYS &hellip; AUTO</span>
                call for every selected organization: copy, catch up, cut over. Pick the
                origin shard, select tenants, choose a target.
              </p>
            </div>
            <.task_chip id="move-task-status" task={@admin_task} />
          </div>

          <div
            id="move-source-tabs"
            class="flex flex-wrap gap-1 border-b border-white/8 px-6 pt-3 sm:px-8"
          >
            <button
              :for={shard <- @shard_ids}
              id={"move-source-tab-#{shard}"}
              type="button"
              phx-click="move_source"
              phx-value-shard={shard}
              class={[
                "rounded-t-xl border-b-2 px-4 py-2 text-xs font-semibold transition",
                (shard == @move_source &&
                   "border-cyan-300 bg-cyan-300/5 text-cyan-200") ||
                  "border-transparent text-slate-500 hover:text-slate-300"
              ]}
            >
              shard {shard}
              <span class="ml-1 font-mono text-[10px] opacity-70">
                {format_number(Map.get(@org_counts, shard, 0))}
              </span>
            </button>
          </div>

          <form id="move-keys-form" phx-change="move_selection" phx-submit="move_shard">
            <div class="max-h-80 overflow-y-auto px-6 py-4 sm:px-8">
              <p
                :if={source_orgs(@organizations, @move_source) == []}
                class="py-4 text-sm text-slate-500"
              >
                No organizations on shard {@move_source}.
              </p>
              <table
                :if={source_orgs(@organizations, @move_source) != []}
                class="w-full min-w-[24rem] text-sm"
              >
                <thead>
                  <tr class="text-xs uppercase tracking-wider text-slate-500">
                    <th class="w-10 py-2">
                      <input
                        id="move-select-all"
                        type="checkbox"
                        name="select_all"
                        checked={all_selected?(@organizations, @move_source, @selected_org_ids)}
                        phx-change="move_select_all"
                        title="Select every organization on this shard"
                        class="size-4 rounded border-white/20 bg-[#070b14] text-cyan-300 focus:ring-cyan-300/40"
                      />
                    </th>
                    <th class="py-2 pr-4 text-left font-semibold">Organization</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    :for={org <- source_orgs(@organizations, @move_source)}
                    class="border-t border-white/5"
                  >
                    <td class="py-2.5">
                      <input
                        id={"move-org-#{org.id}"}
                        type="checkbox"
                        name="org_ids[]"
                        value={org.id}
                        checked={MapSet.member?(@selected_org_ids, to_string(org.id))}
                        class="size-4 rounded border-white/20 bg-[#070b14] text-cyan-300 focus:ring-cyan-300/40"
                      />
                    </td>
                    <td class="py-2.5 pr-4">
                      <label for={"move-org-#{org.id}"} class="cursor-pointer">
                        <span class="text-slate-200">{org.name}</span>
                        <span class="ml-2 font-mono text-[11px] text-slate-600">{org.id}</span>
                      </label>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div class="flex flex-wrap items-center justify-between gap-4 border-t border-white/8 px-6 py-5 sm:px-8">
              <label class="flex items-center gap-3 text-sm text-slate-400">
                Target
                <select
                  name="target_shard"
                  class="rounded-xl border border-white/10 bg-[#070b14] px-3 py-2 text-sm font-medium text-white outline-none focus:border-cyan-300/60"
                >
                  <option
                    :for={shard <- @shard_ids}
                    :if={shard != @move_source}
                    value={shard}
                    selected={to_string(shard) == @move_target}
                  >
                    shard {shard}
                  </option>
                </select>
              </label>
              <.button
                id="move-shard-button"
                type="submit"
                disabled={MapSet.size(@selected_org_ids) == 0 or @admin_task != nil}
                phx-disable-with="Moving..."
                class="inline-flex min-h-11 items-center justify-center gap-2 rounded-xl bg-violet-300 px-6 text-sm font-bold text-slate-950 transition hover:-translate-y-0.5 hover:bg-violet-200 active:translate-y-0 disabled:cursor-not-allowed disabled:opacity-40"
              >
                MOVE SHARD
                <span :if={MapSet.size(@selected_org_ids) > 0}>
                  ({MapSet.size(@selected_org_ids)})
                </span>
              </.button>
            </div>
          </form>
        </div>

        <div
          id="placement-audit"
          class="overflow-hidden rounded-3xl border border-white/8 bg-[#0c1220]/95 shadow-[0_24px_80px_rgba(0,0,0,0.28)]"
        >
          <div class="flex flex-wrap items-center justify-between gap-4 border-b border-white/8 px-6 py-5 sm:px-8">
            <div>
              <h2 class="text-lg font-semibold text-white">Placement audit</h2>
              <p class="mt-1 text-sm text-slate-500">
                Confirms every tenant row lives on the shard its organization is placed on.
                Runs only on demand. Rows copied by an in-flight MOVE KEYS task count as
                strays until its cutover.
              </p>
            </div>
            <button
              id="run-audit-button"
              type="button"
              phx-click="audit"
              phx-disable-with="Auditing..."
              class="inline-flex min-h-10 items-center justify-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 text-xs font-semibold text-slate-200 transition hover:border-cyan-300/40 hover:text-cyan-200 disabled:cursor-wait disabled:opacity-70"
            >
              <.icon name="hero-shield-check" class="size-4" /> Run audit
            </button>
          </div>

          <div :if={@audit} id="audit-results" class="px-6 py-5 sm:px-8">
            <div class="flex flex-wrap items-center gap-3">
              <span class={[
                "grid size-9 place-items-center rounded-xl",
                @audit.problems == [] && "bg-emerald-300/10 text-emerald-300",
                @audit.problems != [] && "bg-rose-400/10 text-rose-300"
              ]}>
                <.icon
                  name={
                    if @audit.problems == [],
                      do: "hero-check-circle",
                      else: "hero-exclamation-triangle"
                  }
                  class="size-5"
                />
              </span>
              <div>
                <div class="text-sm font-semibold text-white">
                  {format_number(@audit.clean)} of {format_number(@audit.total)} organizations with no problems detected
                </div>
                <div class="text-xs text-slate-500">
                  Audited at {Calendar.strftime(@audit.audited_at, "%H:%M:%S")} UTC
                </div>
              </div>
            </div>

            <ul :if={@audit.problems != []} class="mt-5 space-y-3">
              <li
                :for={problem <- @audit.problems}
                class="rounded-2xl border border-rose-400/15 bg-rose-400/[0.04] px-4 py-3"
              >
                <div class="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                  <span class="text-sm font-semibold text-rose-100">{problem.name}</span>
                  <span class="font-mono text-[11px] text-slate-500">id {problem.id}</span>
                  <span class="text-xs text-slate-400">
                    placed on shard {problem.expected_shard}
                  </span>
                </div>
                <div class="mt-2 flex flex-wrap gap-2">
                  <span
                    :for={row <- problem.rows}
                    class="rounded-full border border-rose-400/20 bg-rose-400/10 px-2.5 py-1 font-mono text-[11px] text-rose-200"
                  >
                    {format_number(row.count)} × {row.table} on shard {row.shard}
                  </span>
                </div>
              </li>
            </ul>
          </div>
        </div>

        <div
          id="hybrid-check"
          class="overflow-hidden rounded-3xl border border-white/8 bg-[#0c1220]/95 shadow-[0_24px_80px_rgba(0,0,0,0.28)]"
        >
          <div class="flex flex-wrap items-center justify-between gap-4 border-b border-white/8 px-6 py-5 sm:px-8">
            <div>
              <h2 class="text-lg font-semibold text-white">
                Hybrid table check
                <span class="ml-2 rounded-full border border-amber-300/20 bg-amber-300/10 px-2 py-0.5 align-middle text-[10px] font-semibold text-amber-200">
                  custom_packages
                </span>
              </h2>
              <p class="mt-1 text-sm text-slate-500">
                Confirms <span class="font-mono text-xs text-slate-400">ADD SHARD</span>
                copied the NULL-key default rows to every shard (same rows, same fingerprint) and
                <span class="font-mono text-xs text-slate-400">MOVE KEYS</span>
                moved only the keyed rows: no strays, and every custom-package deployment resolves
                on its shard. Runs on demand and after every finished task.
              </p>
            </div>
            <button
              id="hybrid-check-button"
              type="button"
              phx-click="hybrid_check"
              phx-disable-with="Checking..."
              class="inline-flex min-h-10 items-center justify-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 text-xs font-semibold text-slate-200 transition hover:border-amber-300/40 hover:text-amber-200 disabled:cursor-wait disabled:opacity-70"
            >
              <.icon name="hero-squares-2x2" class="size-4" /> Check hybrid table
            </button>
          </div>

          <div :if={@hybrid_report} id="hybrid-check-results" class="px-6 py-5 sm:px-8">
            <div class="flex flex-wrap items-center gap-3">
              <span class={[
                "grid size-9 place-items-center rounded-xl",
                @hybrid_report.ok && "bg-emerald-300/10 text-emerald-300",
                !@hybrid_report.ok && "bg-rose-400/10 text-rose-300"
              ]}>
                <.icon
                  name={
                    if @hybrid_report.ok,
                      do: "hero-check-circle",
                      else: "hero-exclamation-triangle"
                  }
                  class="size-5"
                />
              </span>
              <div>
                <div class="text-sm font-semibold text-white">
                  {if @hybrid_report.ok,
                    do: "Hybrid table check passed",
                    else: "Hybrid table check failed"} on {length(@hybrid_report.shards)} shard(s): {format_number(
                    @hybrid_report.expected_defaults
                  )} default packages expected everywhere
                </div>
                <div class="text-xs text-slate-500">
                  Checked at {Calendar.strftime(@hybrid_report.checked_at, "%H:%M:%S")} UTC
                  after {@hybrid_trigger}
                </div>
              </div>
            </div>

            <div class="mt-5 overflow-x-auto">
              <table class="w-full min-w-[36rem] text-sm">
                <thead>
                  <tr class="text-xs uppercase tracking-wider text-slate-500">
                    <th class="py-2 pr-4 text-left font-semibold">Shard</th>
                    <th
                      class="py-2 px-4 text-right font-semibold"
                      title="Rows with a NULL organization_id"
                    >
                      Default rows
                    </th>
                    <th
                      class="py-2 px-4 text-left font-semibold"
                      title="Same default rows as the reference shard"
                    >
                      Defaults
                    </th>
                    <th
                      class="py-2 px-4 text-right font-semibold"
                      title="Rows with an organization_id"
                    >
                      Keyed rows
                    </th>
                    <th
                      class="py-2 px-4 text-right font-semibold"
                      title="Keyed rows whose organization is placed on another shard"
                    >
                      Strays
                    </th>
                    <th
                      class="py-2 px-4 text-right font-semibold"
                      title="Custom-package deployments on this shard whose package is missing here"
                    >
                      Dangling deployments
                    </th>
                    <th
                      class="py-2 pl-4 text-left font-semibold"
                      title="MOVE KEYS needs FULL on a hybrid table"
                    >
                      Replica identity
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    :for={check <- @hybrid_report.shards}
                    id={"hybrid-check-shard-#{check.shard || "db"}"}
                    class="border-t border-white/5"
                  >
                    <td class="py-2.5 pr-4 font-mono text-xs text-slate-200">
                      {shard_label(check.shard)}
                      <span
                        :if={!check.ok}
                        class="ml-2 rounded-full border border-rose-400/20 bg-rose-400/10 px-2 py-0.5 text-[10px] font-semibold text-rose-200"
                      >
                        problem
                      </span>
                    </td>
                    <td class="py-2.5 px-4 text-right tabular-nums text-slate-200">
                      {format_number(check.default_rows)}
                    </td>
                    <td class="py-2.5 px-4">
                      <.check_badge ok={check.defaults_match} yes="identical" no="differ" />
                    </td>
                    <td class="py-2.5 px-4 text-right tabular-nums text-slate-200">
                      {format_number(check.keyed_rows)}
                    </td>
                    <td class="py-2.5 px-4 text-right tabular-nums">
                      <.count_badge value={check.stray_rows} />
                    </td>
                    <td class="py-2.5 px-4 text-right tabular-nums">
                      <.count_badge value={check.dangling_deployments} />
                    </td>
                    <td class="py-2.5 pl-4">
                      <.check_badge
                        ok={check.replica_identity == "full" and check.nullable_key}
                        yes={check.replica_identity}
                        no={
                          if check.nullable_key,
                            do: check.replica_identity,
                            else: check.replica_identity <> ", key NOT NULL"
                        }
                      />
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </section>
    </Layouts.app>
    """
  end

  defp shard_label(nil), do: "Database"
  defp shard_label(shard), do: "Shard #{shard}"

  defp source_orgs(organizations, source) do
    Enum.filter(organizations, &(&1.shard_id == source))
  end

  defp all_selected?(organizations, source, selected) do
    orgs = source_orgs(organizations, source)
    orgs != [] and Enum.all?(orgs, &MapSet.member?(selected, to_string(&1.id)))
  end

  attr :id, :string, required: true
  attr :task, :map, required: true

  defp task_chip(assigns) do
    ~H"""
    <div
      :if={@task}
      id={@id}
      class="flex items-center gap-2 rounded-xl border border-cyan-300/20 bg-cyan-300/5 px-4 py-2 text-xs text-cyan-100"
    >
      <span class="relative flex size-2.5">
        <span class="absolute inline-flex size-full animate-ping rounded-full bg-cyan-300 opacity-60"></span>
        <span class="relative inline-flex size-2.5 rounded-full bg-cyan-300"></span>
      </span>
      task {@task.id}: {@task.inner_status || @task.status}
    </div>
    """
  end

  attr :ok, :boolean, required: true
  attr :yes, :string, required: true
  attr :no, :string, required: true

  defp check_badge(assigns) do
    ~H"""
    <span class={[
      "rounded-full border px-2.5 py-1 font-mono text-[11px]",
      (@ok && "border-emerald-300/20 bg-emerald-300/10 text-emerald-200") ||
        "border-rose-400/20 bg-rose-400/10 text-rose-200"
    ]}>
      {if @ok, do: @yes, else: @no}
    </span>
    """
  end

  attr :value, :integer, required: true

  defp count_badge(assigns) do
    ~H"""
    <span class={[
      "font-mono text-xs",
      (@value == 0 && "text-emerald-200") || "font-semibold text-rose-200"
    ]}>
      {format_number(@value)}
    </span>
    """
  end

  attr :label, :string, required: true
  attr :value, :integer, required: true

  defp stat(assigns) do
    ~H"""
    <div class="bg-[#0c1220] p-4">
      <div class="text-2xl font-semibold tabular-nums text-white">{format_number(@value)}</div>
      <div class="mt-1 text-xs uppercase tracking-wider text-slate-500">{@label}</div>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :value, :integer, required: true
  attr :border, :boolean, default: false

  defp forecast(assigns) do
    ~H"""
    <div class={["flex justify-between", @border && "border-t border-white/8 pt-3"]}>
      <dt class="text-slate-500">{@label}</dt>
      <dd class="font-medium tabular-nums text-slate-200">{format_number(@value)}</dd>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :value, :integer, required: true

  defp job_count(assigns) do
    ~H"""
    <div class="rounded-xl bg-black/15 px-2 py-3">
      <div class="text-lg font-semibold tabular-nums text-white">{@value}</div>
      <div class="text-[10px] uppercase tracking-wider text-slate-500">{@label}</div>
    </div>
    """
  end

  defp format_number(number) do
    number
    |> Integer.to_string()
    |> String.reverse()
    |> String.graphemes()
    |> Enum.chunk_every(3)
    |> Enum.map_join(",", &Enum.join/1)
    |> String.reverse()
  end
end
