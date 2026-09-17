defmodule ShardHound.DemoData.GenerateDatasetWorker do
  use Oban.Worker,
    queue: :data_generation_coordinator,
    max_attempts: 3,
    unique: [period: :infinity, fields: [:worker, :args], keys: [:generation_id]]

  alias ShardHound.DemoData.GenerateOrganizationWorker

  # Fans out one job per organization. The shared catalog (managed
  # packages, default packages) is seeded separately with
  # `DemoData.seed_shared_catalog/1`; organization jobs read it from
  # the database, so definitions no longer ride along in job args.
  @impl Oban.Worker
  def perform(%Oban.Job{args: args}) do
    Enum.each(1..args["organizations"], fn organization_index ->
      args
      |> Map.put("organization_index", organization_index)
      |> GenerateOrganizationWorker.new()
      |> Oban.insert!()
    end)

    :ok
  end
end
