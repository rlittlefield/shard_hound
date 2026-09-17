defmodule ShardHound.DemoData.CatalogParams do
  use Ecto.Schema
  import Ecto.Changeset

  # The shared catalog is everything that is NOT per-organization:
  #
  # - `managed_packages`: the vendor catalog (`shard_hound_packages`,
  #   an omnisharded table — the whole table broadcasts). Device
  #   software inventory and `package_type = "shard_hound"`
  #   deployments draw from it, so it caps `software_per_device`.
  # - `default_packages`: NULL-key rows in the hybrid `custom_packages`
  #   table — shared defaults living alongside each tenant's own rows,
  #   broadcast to every shard by PgDog's hybrid-table (`kind = "hybrid"`) routing.
  @count_fields [
    %{name: :managed_packages, label: "Shared managed packages (omni)", min: 1, max: 2_000},
    %{name: :default_packages, label: "Default packages (hybrid)", min: 0, max: 200}
  ]

  @primary_key false
  embedded_schema do
    field :managed_packages, :integer, default: 50
    field :default_packages, :integer, default: 25
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
