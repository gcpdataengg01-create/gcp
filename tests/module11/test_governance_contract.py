from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_policy_tag_is_attached_to_customer_id_templates():
    customer_schema = (
        ROOT / "infra/modules/warehouse/schemas/dim_customer.json.tftpl"
    ).read_text(encoding="utf-8")
    fact_schema = (
        ROOT / "infra/modules/warehouse/schemas/fct_sales_line.json.tftpl"
    ).read_text(encoding="utf-8")

    assert '"name":"customer_id"' in customer_schema
    assert '"policyTags"' in customer_schema
    assert '${customer_policy_tag_name}' in customer_schema

    assert '"name":"customer_id"' in fact_schema
    assert '"policyTags"' in fact_schema
    assert '${customer_policy_tag_name}' in fact_schema


def test_dataplex_scan_contains_all_c9_rules():
    governance = (ROOT / "infra/modules/governance/main.tf").read_text(
        encoding="utf-8"
    )

    for rule_id in ["c9-001", "c9-002", "c9-003", "c9-004", "c9-005"]:
        assert rule_id in governance

    # Dataplex SQL assertions must use its source-table placeholder for rules
    # recomputed directly against the published fact.
    assert "$${data()}" in governance


def test_semantic_view_does_not_expose_customer_identifier():
    governance = (ROOT / "infra/modules/governance/main.tf").read_text(
        encoding="utf-8"
    )

    view_start = governance.index('resource "google_bigquery_table" "sales_view"')
    access_start = governance.index(
        'resource "google_bigquery_dataset_access" "authorized_sales_view"'
    )
    view_block = governance[view_start:access_start]

    assert "customer_id" not in view_block
    assert "line_amount_gbp" in view_block
    assert "line_amount_eur" in view_block


def test_discovery_is_disabled_for_explicit_schema_governance():
    governance = (ROOT / "infra/modules/governance/main.tf").read_text(
        encoding="utf-8"
    )

    assert "enabled = true" not in governance
    assert governance.count("enabled = false") >= 10
