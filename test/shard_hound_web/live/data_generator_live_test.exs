defmodule ShardHoundWeb.DataGeneratorLiveTest do
  use ShardHoundWeb.ConnCase, async: false
  use Oban.Testing, repo: ShardHound.ObanRepo

  import Phoenix.LiveViewTest

  alias ShardHound.DemoData.GenerateDatasetWorker

  test "renders the generation form and updates its values", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    assert has_element?(view, "#data-generator")
    assert has_element?(view, "#data-generator-form")
    assert has_element?(view, "#generate-data-button")

    view
    |> form("#data-generator-form", %{
      "generation_params" => %{
        "organizations" => "2",
        "devices_per_organization" => "10"
      }
    })
    |> render_change()

    assert has_element?(view, "#generation_params_organizations[value='2']")
  end

  test "seeds the shared catalog from its own panel", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    assert has_element?(view, "#catalog-form")

    html =
      view
      |> form("#catalog-form", %{
        "catalog_params" => %{"managed_packages" => "4", "default_packages" => "3"}
      })
      |> render_submit()

    assert html =~ "Shared catalog seeded on every shard"
    assert ShardHound.DemoData.catalog_stats() == %{managed_packages: 4, default_packages: 3}
  end

  test "queues a generation coordinator", %{conn: conn} do
    {:ok, _params} =
      ShardHound.DemoData.seed_shared_catalog(%{
        "managed_packages" => "2",
        "default_packages" => "0"
      })

    {:ok, view, _html} = live(conn, ~p"/")

    view
    |> form("#data-generator-form", %{
      "generation_params" => %{
        "organizations" => "1",
        "devices_per_organization" => "2",
        "software_per_device" => "2",
        "groups_per_organization" => "1",
        "custom_packages_per_organization" => "1",
        "deployments_per_organization" => "1"
      }
    })
    |> render_submit()

    assert has_element?(view, "#generation-status")
    assert_enqueued(worker: GenerateDatasetWorker, args: %{organizations: 1})
  end

  test "runs the placement audit only on demand", %{conn: conn} do
    ShardHound.Repo.insert!(%ShardHound.DeviceManagement.Organization{
      name: "Audit Org",
      slug: "audit-org",
      shard_id: 0
    })

    {:ok, view, _html} = live(conn, ~p"/")

    refute has_element?(view, "#audit-results")

    html = view |> element("#run-audit-button") |> render_click()

    assert html =~ "1 of 1"
    assert html =~ "organizations with no problems detected"
  end

  test "runs the hybrid table check only on demand", %{conn: conn} do
    {:ok, _params} =
      ShardHound.DemoData.seed_shared_catalog(%{
        "managed_packages" => "1",
        "default_packages" => "2"
      })

    {:ok, view, _html} = live(conn, ~p"/")

    refute has_element?(view, "#hybrid-check-results")

    html = view |> element("#hybrid-check-button") |> render_click()

    assert html =~ "Hybrid table check passed"
    assert html =~ "2 default packages expected everywhere"
    assert html =~ "after manual check"
    assert has_element?(view, "#hybrid-check-shard-db")
  end

  test "hides the move panel when pgdog is disabled", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    refute has_element?(view, "#move-keys")
  end
end
