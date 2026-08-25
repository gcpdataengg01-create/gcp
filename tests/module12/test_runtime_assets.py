from pathlib import Path


def test_runtime_and_demo_assets_exist():
    required = [
        "scripts/module12/deploy_runtime.ps1",
        "scripts/module12/build_fx_image.ps1",
        "scripts/module12/trigger_backfill.ps1",
        "scripts/module12/capture_idempotency.ps1",
        "sql/module12/idempotency_fingerprint.sql",
        "sql/module12/reconciliation.sql",
        "sql/module12/join_preservation.sql",
        "jobs/fx_extract/main.py",
        "jobs/fx_extract/Dockerfile",
        "docs/module12-composer-monitoring-testing.md",
    ]
    for path in required:
        assert Path(path).exists(), path


def test_fx_runtime_is_not_placeholder():
    assert Path("jobs/fx_extract/main.py").stat().st_size > 500
    assert Path("jobs/fx_extract/Dockerfile").stat().st_size > 50
