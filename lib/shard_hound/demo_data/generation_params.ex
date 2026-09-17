defmodule ShardHound.DemoData.GenerationParams do
  use Ecto.Schema
  import Ecto.Changeset

  # Per-organization generation only. The shared catalog (managed
  # packages, default packages) is seeded separately with
  # `DemoData.seed_shared_catalog/1` — those rows are broadcast to
  # every shard and aren't multiplied by the organization count.
  @count_fields [
    %{name: :organizations, label: "Organizations", min: 1, max: 10_000},
    %{name: :devices_per_organization, label: "Devices / organization", min: 1, max: 20_000},
    %{name: :software_per_device, label: "Software / device", min: 1, max: 2_000},
    %{name: :groups_per_organization, label: "Groups / organization", min: 1, max: 1_000},
    %{
      name: :custom_packages_per_organization,
      label: "Custom packages / organization",
      min: 0,
      max: 200
    },
    %{
      name: :deployments_per_organization,
      label: "Deployments / organization",
      min: 0,
      max: 10_000
    }
  ]

  @primary_key false
  embedded_schema do
    field :organizations, :integer, default: 5
    field :devices_per_organization, :integer, default: 100
    field :software_per_device, :integer, default: 6
    field :groups_per_organization, :integer, default: 4
    field :custom_packages_per_organization, :integer, default: 3
    field :deployments_per_organization, :integer, default: 5
  end

  def count_fields, do: @count_fields

  def changeset(params, attrs \\ %{}) do
    fields = Enum.map(@count_fields, & &1.name)

    Enum.reduce(
      @count_fields,
      params |> cast(attrs, fields) |> validate_required(fields),
      fn field, changeset ->
        validate_number(changeset, field.name,
          greater_than_or_equal_to: field.min,
          less_than_or_equal_to: field.max
        )
      end
    )
  end
end
