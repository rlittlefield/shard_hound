defmodule ShardHound.DemoData.GenerateOrganizationWorker do
  use Oban.Worker,
    queue: :data_generation,
    max_attempts: 3,
    unique: [
      period: :infinity,
      fields: [:worker, :args],
      keys: [:generation_id, :organization_index]
    ]

  import Ecto.Query

  alias ShardHound.DemoData
  alias ShardHound.DeviceManagement.CustomPackage
  alias ShardHound.DeviceManagement.Deployment
  alias ShardHound.DeviceManagement.Device
  alias ShardHound.DeviceManagement.DeviceSoftware
  alias ShardHound.DeviceManagement.Group
  alias ShardHound.DeviceManagement.GroupDevice
  alias ShardHound.DeviceManagement.Organization
  alias ShardHound.DeviceManagement.ShardHoundPackage
  alias ShardHound.Repo

  # insert_all binds one parameter per value and Postgres caps a
  # statement at 65_535 binds; 4_000 rows of ~13 columns stays under.
  @insert_chunk 4_000

  @impl Oban.Worker
  def perform(%Oban.Job{args: args}) do
    organization = insert_organization(args)

    if organization_data_exists?(organization.id) do
      :ok
    else
      managed_packages = list_managed_packages()
      definitions = Enum.map(managed_packages, &definition/1)

      if length(definitions) < args["software_per_device"] do
        {:error, :managed_catalog_smaller_than_software_per_device}
      else
        case Repo.transaction(
               fn ->
                 generate_organization_data(organization, managed_packages, definitions, args)
               end,
               timeout: :infinity
             ) do
          {:ok, _organization} -> :ok
          {:error, reason} -> {:error, reason}
        end
      end
    end
  end

  defp insert_organization(args) do
    generation_id = args["generation_id"]
    display_key = String.slice(generation_id, 0, 8)
    organization_index = args["organization_index"]

    # Placement policy: round-robin over the shards enabled for new
    # organizations in the omnisharded shards table.
    enabled_shards = ShardHound.Shards.enabled_shard_ids()

    organization = %Organization{
      id: DemoData.stable_id("organization:#{generation_id}:#{organization_index}"),
      name: "Demo Organization #{display_key}-#{organization_index}",
      slug: "demo-#{generation_id}-#{organization_index}",
      shard_id: Enum.at(enabled_shards, rem(organization_index - 1, length(enabled_shards)))
    }

    Repo.insert!(organization,
      conflict_target: :slug,
      on_conflict: {:replace, [:name, :updated_at]},
      returning: true
    )
  end

  defp organization_data_exists?(organization_id) do
    Repo.exists?(from device in Device, where: device.organization_id == ^organization_id)
  end

  defp generate_organization_data(organization, managed_packages, definitions, args) do
    now = DateTime.utc_now(:second)

    Repo.query!("SET LOCAL pgdog.sharding_key = '#{organization.id}'")

    # Group membership is only tracked for the slugs groups filter on;
    # tracking every slug at 20k devices x 2k apps would hold tens of
    # millions of device ids in memory for nothing.
    group_slugs = group_slugs(definitions, args)

    devices = insert_devices(organization.id, args, now)
    memberships = insert_software(organization.id, devices, definitions, group_slugs, args, now)
    groups = insert_groups(organization.id, definitions, memberships, args, now)
    custom_packages = insert_custom_packages(organization.id, args, now)

    # Default packages (NULL organization_id) are broadcast to every
    # shard, so this keyed transaction finds them locally. Deployments
    # then reference a mix of the org's own packages and the shared
    # defaults.
    deployable_packages = custom_packages ++ list_default_packages()

    insert_deployments(organization.id, groups, managed_packages, deployable_packages, args, now)

    organization
  end

  defp insert_devices(organization_id, args, now) do
    generation_key = String.slice(args["generation_id"], 0, 8)
    organization_index = args["organization_index"]

    Enum.map(1..args["devices_per_organization"], fn device_index ->
      platform = if rem(device_index, 3) == 0, do: "windows", else: "macos"

      %{
        organization_id: organization_id,
        serial_number: "#{generation_key}-#{organization_index}-#{device_index}",
        hostname: "device-#{organization_index}-#{device_index}",
        platform: platform,
        architecture: if(platform == "macos", do: "arm64", else: "x86_64"),
        os_version: if(platform == "macos", do: "15.6", else: "11-24H2"),
        last_seen_at: DateTime.add(now, -rem(device_index, 168), :hour),
        metadata: %{office: "site-#{rem(device_index, 12) + 1}"},
        inserted_at: now,
        updated_at: now
      }
    end)
    |> Enum.chunk_every(@insert_chunk)
    |> Enum.flat_map(fn chunk ->
      {_count, devices} = Repo.insert_all(Device, chunk, returning: [:id, :serial_number])
      devices
    end)
  end

  defp insert_software(organization_id, devices, definitions, group_slugs, args, now) do
    # Batch size adapts so each pass builds ~@insert_chunk rows even
    # when every device reports thousands of apps.
    device_batch_size = max(1, div(@insert_chunk, args["software_per_device"]))

    devices
    |> Enum.with_index(1)
    |> Enum.chunk_every(device_batch_size)
    |> Enum.reduce(%{}, fn device_batch, memberships ->
      {software_rows, memberships} =
        Enum.reduce(device_batch, {[], memberships}, fn {device, device_index},
                                                        {rows, memberships} ->
          definitions
          |> rotate(device_index - 1)
          |> Enum.take(args["software_per_device"])
          |> Enum.with_index()
          |> Enum.reduce({rows, memberships}, fn {software, software_index},
                                                 {rows, memberships} ->
            latest? = rem(device_index + software_index, 4) != 0

            version =
              if latest?, do: software["version"], else: previous_version(software["version"])

            row = %{
              organization_id: organization_id,
              device_id: device.id,
              name: software["name"],
              publisher: software["publisher"],
              version: version,
              bundle_identifier: "com.demo.#{software["slug"]}",
              installed_at:
                DateTime.add(now, -rem(device_index * (software_index + 1), 720), :hour),
              metadata: %{managed: rem(software_index, 2) == 0},
              inserted_at: now,
              updated_at: now
            }

            memberships =
              if latest? and MapSet.member?(group_slugs, software["slug"]) do
                Map.update(memberships, software["slug"], [device.id], &[device.id | &1])
              else
                memberships
              end

            {[row | rows], memberships}
          end)
        end)

      software_rows
      |> Enum.chunk_every(@insert_chunk)
      |> Enum.each(&Repo.insert_all(DeviceSoftware, &1))

      memberships
    end)
  end

  defp group_slugs(definitions, args) do
    Range.new(1, args["groups_per_organization"], 1)
    |> Enum.map(&cycle_at(definitions, &1)["slug"])
    |> MapSet.new()
  end

  defp insert_groups(organization_id, definitions, memberships, args, now) do
    group_specs =
      Range.new(1, args["groups_per_organization"], 1)
      |> Enum.map(fn group_index ->
        software = cycle_at(definitions, group_index)
        name = "Latest #{software["name"]} #{group_index}"

        row = %{
          organization_id: organization_id,
          name: name,
          description: "Devices reporting the current managed version of #{software["name"]}",
          filter: %{
            "field" => "device_software.version",
            "software_slug" => software["slug"],
            "operator" => "equals_latest"
          },
          refreshed_at: now,
          inserted_at: now,
          updated_at: now
        }

        %{name: name, software_slug: software["slug"], row: row}
      end)

    groups =
      group_specs
      |> Enum.map(& &1.row)
      |> Enum.chunk_every(@insert_chunk)
      |> Enum.flat_map(fn chunk ->
        {_count, groups} = Repo.insert_all(Group, chunk, returning: [:id, :name])
        groups
      end)

    groups_by_name = Map.new(groups, &{&1.name, &1})

    group_specs
    |> Enum.flat_map(fn spec ->
      group = Map.fetch!(groups_by_name, spec.name)

      Enum.map(Map.get(memberships, spec.software_slug, []), fn device_id ->
        %{
          organization_id: organization_id,
          group_id: group.id,
          device_id: device_id,
          inserted_at: now,
          updated_at: now
        }
      end)
    end)
    |> Enum.chunk_every(@insert_chunk)
    |> Enum.each(&Repo.insert_all(GroupDevice, &1))

    groups
  end

  defp insert_custom_packages(organization_id, args, now) do
    rows =
      Range.new(1, args["custom_packages_per_organization"], 1)
      |> Enum.map(fn package_index ->
        %{
          organization_id: organization_id,
          name: "Internal Tool #{package_index}",
          slug: "internal-tool-#{package_index}",
          platform: if(rem(package_index, 2) == 0, do: "windows", else: "macos"),
          architecture: "universal",
          installer_type: "custom",
          latest_version: "#{rem(package_index, 4) + 1}.#{package_index}.0",
          metadata: %{owner: "IT Engineering"},
          inserted_at: now,
          updated_at: now
        }
      end)

    {_count, packages} =
      Repo.insert_all(CustomPackage, rows, returning: [:id, :name, :latest_version])

    packages
  end

  defp insert_deployments(
         organization_id,
         groups,
         managed_packages,
         custom_packages,
         args,
         now
       ) do
    Range.new(1, args["deployments_per_organization"], 1)
    |> Enum.map(fn deployment_index ->
      {package_type, package} =
        deployment_package(deployment_index, managed_packages, custom_packages)

      group = cycle_at(groups, deployment_index)

      %{
        organization_id: organization_id,
        group_id: group.id,
        package_id: package.id,
        package_type: package_type,
        name: "Deploy #{package.name} to #{group.name}",
        target_version: package.latest_version,
        status: Enum.at(~w(pending scheduled running completed), rem(deployment_index, 4)),
        scheduled_at: DateTime.add(now, deployment_index, :hour),
        metadata: %{created_by: "demo generator"},
        inserted_at: now,
        updated_at: now
      }
    end)
    |> Enum.chunk_every(@insert_chunk)
    |> Enum.each(&Repo.insert_all(Deployment, &1))
  end

  # The shared managed catalog is omnisharded, so any shard answers
  # with the full set. Ordered so every organization sees the same
  # rotation.
  defp list_managed_packages do
    Repo.all(from package in ShardHoundPackage, order_by: package.slug)
  end

  defp definition(%ShardHoundPackage{} = package) do
    %{
      "name" => package.name,
      "slug" => package.slug,
      "publisher" => package.metadata["publisher"],
      "version" => package.latest_version
    }
  end

  defp list_default_packages do
    Repo.all(
      from package in CustomPackage,
        where: is_nil(package.organization_id),
        order_by: package.slug
    )
  end

  defp deployment_package(index, managed_packages, custom_packages)

  defp deployment_package(index, managed_packages, []) do
    {"shard_hound", cycle_at(managed_packages, index)}
  end

  defp deployment_package(index, _managed_packages, custom_packages) when rem(index, 2) == 0 do
    {"custom", cycle_at(custom_packages, index)}
  end

  defp deployment_package(index, managed_packages, _custom_packages) do
    {"shard_hound", cycle_at(managed_packages, index)}
  end

  defp rotate(items, offset) do
    {left, right} = Enum.split(items, rem(offset, length(items)))
    right ++ left
  end

  defp previous_version(version), do: version <> "-old"

  defp cycle_at(items, one_based_index) do
    Enum.at(items, rem(one_based_index - 1, length(items)))
  end
end
