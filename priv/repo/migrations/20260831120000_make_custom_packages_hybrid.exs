defmodule ShardHound.Repo.Migrations.MakeCustomPackagesHybrid do
  use Ecto.Migration

  # custom_packages becomes a hybrid ("broadcast_null") table: rows
  # keyed by organization_id stay on their tenant's shard, rows with a
  # NULL organization_id are shared defaults that PgDog broadcasts to
  # every shard and that ADD SHARD copies onto a new shard.
  #
  # A replica identity index can't include a nullable column, so the
  # table drops the (organization_id, id) identity for REPLICA
  # IDENTITY FULL — which MOVE KEYS accepts, because the full old row
  # in the WAL still carries the sharding column. The identity must
  # change before the column can go nullable: Postgres refuses DROP
  # NOT NULL on a column inside the replica identity index.
  def change do
    execute(
      "ALTER TABLE custom_packages REPLICA IDENTITY FULL",
      "ALTER TABLE custom_packages REPLICA IDENTITY USING INDEX custom_packages_organization_id_id_index"
    )

    execute(
      "ALTER TABLE custom_packages ALTER COLUMN organization_id DROP NOT NULL",
      "ALTER TABLE custom_packages ALTER COLUMN organization_id SET NOT NULL"
    )
  end
end
