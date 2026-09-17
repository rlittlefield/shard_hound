defmodule ShardHound.DemoDataTest do
  use ShardHound.DataCase, async: false
  use Oban.Testing, repo: ShardHound.ObanRepo

  import Ecto.Query

  alias ShardHound.DemoData
  alias ShardHound.DemoData.GenerateDatasetWorker
  alias ShardHound.DemoData.GenerateOrganizationWorker
  alias ShardHound.DeviceManagement.CustomPackage
  alias ShardHound.DeviceManagement.Deployment
  alias ShardHound.DeviceManagement.Device
  alias ShardHound.DeviceManagement.DeviceSoftware
  alias ShardHound.DeviceManagement.Group
  alias ShardHound.DeviceManagement.GroupDevice
  alias ShardHound.DeviceManagement.Organization
  alias ShardHound.DeviceManagement.ShardHoundPackage

  test "enqueue refuses more software per device than the seeded catalog" do
    {:ok, _params} =
      DemoData.seed_shared_catalog(%{"managed_packages" => "2", "default_packages" => "0"})

    assert {:error, changeset} =
             DemoData.enqueue_generation(%{
               "organizations" => "1",
               "devices_per_organization" => "1",
               "software_per_device" => "3",
               "groups_per_organization" => "1",
               "custom_packages_per_organization" => "0",
               "deployments_per_organization" => "0"
             })

    assert [message] = errors_on(changeset).software_per_device
    assert message =~ "exceeds the shared catalog"
  end

  test "seeding the shared catalog is an idempotent upsert" do
    {:ok, _params} =
      DemoData.seed_shared_catalog(%{"managed_packages" => "12", "default_packages" => "5"})

    {:ok, _params} =
      DemoData.seed_shared_catalog(%{"managed_packages" => "12", "default_packages" => "5"})

    assert DemoData.catalog_stats() == %{managed_packages: 12, default_packages: 5}

    assert ShardHound.Repo.aggregate(
             from(package in CustomPackage, where: is_nil(package.organization_id)),
             :count
           ) == 5
  end

  test "workers create a connected organization dataset" do
    generation_id = Ecto.UUID.generate()

    {:ok, _params} =
      DemoData.seed_shared_catalog(%{"managed_packages" => "3", "default_packages" => "2"})

    args = %{
      generation_id: generation_id,
      organizations: 1,
      devices_per_organization: 4,
      software_per_device: 2,
      groups_per_organization: 2,
      custom_packages_per_organization: 2,
      deployments_per_organization: 3
    }

    assert :ok = perform_job(GenerateDatasetWorker, args)
    assert_enqueued(worker: GenerateOrganizationWorker, args: %{generation_id: generation_id})
    assert ShardHound.DemoData.generation_status(generation_id).active == 1

    organization_job =
      Oban.Job
      |> where([job], job.worker == ^inspect(GenerateOrganizationWorker))
      |> ShardHound.ObanRepo.one!()

    assert :ok = perform_job(GenerateOrganizationWorker, organization_job.args)
    assert :ok = perform_job(GenerateOrganizationWorker, organization_job.args)

    organization = ShardHound.Repo.one!(Organization)

    assert ShardHound.Repo.aggregate(Device, :count) == 4
    assert ShardHound.Repo.aggregate(DeviceSoftware, :count) == 8
    assert ShardHound.Repo.aggregate(Group, :count) == 2
    assert ShardHound.Repo.aggregate(GroupDevice, :count) > 0

    # 2 tenant-local custom packages, plus the 2 shared defaults with a
    # NULL organization_id seeded into the hybrid table.
    assert ShardHound.Repo.aggregate(
             from(package in CustomPackage, where: not is_nil(package.organization_id)),
             :count
           ) == 2

    assert ShardHound.Repo.aggregate(CustomPackage, :count) == 4
    assert ShardHound.Repo.aggregate(ShardHoundPackage, :count) == 3
    assert ShardHound.Repo.aggregate(Deployment, :count) == 3

    assert ShardHound.Repo.exists?(
             from software in DeviceSoftware,
               where: software.organization_id == ^organization.id and software.version != ""
           )
  end

  test "hybrid report verifies default rows, keyed placement and replica identity" do
    {:ok, _params} =
      DemoData.seed_shared_catalog(%{"managed_packages" => "1", "default_packages" => "3"})

    organization =
      ShardHound.Repo.insert!(%Organization{name: "Hybrid Org", slug: "hybrid-org", shard_id: 0})

    package =
      ShardHound.Repo.insert!(%CustomPackage{
        organization_id: organization.id,
        name: "Internal Tool",
        slug: "internal-tool",
        platform: "macos",
        architecture: "universal",
        installer_type: "custom",
        latest_version: "1.0.0"
      })

    group =
      ShardHound.Repo.insert!(%Group{
        organization_id: organization.id,
        name: "All devices",
        description: "everything",
        filter: %{}
      })

    deployment = fn package_id ->
      ShardHound.Repo.insert!(%Deployment{
        organization_id: organization.id,
        group_id: group.id,
        package_id: package_id,
        package_type: "custom",
        name: "Deploy",
        target_version: "1.0.0",
        status: "pending"
      })
    end

    # One deployment on the tenant's own package, one on a shared
    # default: both resolve on this database.
    deployment.(package.id)
    deployment.(DemoData.stable_id("default-package:1"))

    report = DemoData.hybrid_report()

    assert report.table == "custom_packages"
    assert report.ok
    assert report.expected_defaults == 3

    assert [
             %{
               shard: nil,
               default_rows: 3,
               defaults_match: true,
               keyed_rows: 1,
               stray_rows: 0,
               dangling_deployments: 0,
               replica_identity: "full",
               nullable_key: true,
               ok: true
             }
           ] = report.shards

    # A deployment whose package never arrived (what a MOVE KEYS that
    # dropped keyed rows, or an ADD SHARD that skipped the defaults,
    # would leave behind) fails the check.
    deployment.(DemoData.stable_id("default-package:missing"))

    report = DemoData.hybrid_report()
    refute report.ok
    assert [%{dangling_deployments: 1, ok: false}] = report.shards
  end

  test "tenant foreign keys reject cross-organization device data" do
    organization = ShardHound.Repo.insert!(%Organization{name: "One", slug: "one"})
    other_organization = ShardHound.Repo.insert!(%Organization{name: "Two", slug: "two"})

    device =
      ShardHound.Repo.insert!(%Device{
        organization_id: organization.id,
        serial_number: "ONE-1",
        hostname: "one-1",
        platform: "macos",
        architecture: "arm64",
        os_version: "15.6"
      })

    assert_raise Ecto.ConstraintError, fn ->
      ShardHound.Repo.insert!(%DeviceSoftware{
        organization_id: other_organization.id,
        device_id: device.id,
        name: "Google Chrome",
        version: "140.0",
        bundle_identifier: "com.google.Chrome"
      })
    end
  end
end
