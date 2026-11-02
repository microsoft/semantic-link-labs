import pytest

from sempy_labs.semantic_model._generate import generate_direct_lake_semantic_model


def test_generate_direct_lake_semantic_model_unique_list_skips_duplicate_error():
    """Unique list input should not trigger duplicate table name validation."""
    with pytest.raises(ValueError, match="The 'source' parameter must be provided."):
        generate_direct_lake_semantic_model(
            dataset="test_dataset",
            tables=["gold.product_sales_summary", "silver.dim_date"],
            source=None,
        )


def test_generate_direct_lake_semantic_model_unique_dict_skips_duplicate_error():
    """Unique dict input should not trigger duplicate table name validation."""
    with pytest.raises(ValueError, match="The 'source' parameter must be provided."):
        generate_direct_lake_semantic_model(
            dataset="test_dataset",
            tables={
                "product_sales_summary": "gold.product_sales_summary",
                "dim_date": "silver.dim_date",
            },
            source=None,
        )


def test_generate_direct_lake_semantic_model_duplicate_names_in_list_raises():
    """Schema-qualified names that collapse to same table name should fail."""
    with pytest.raises(ValueError, match="Duplicate table names are not allowed"):
        generate_direct_lake_semantic_model(
            dataset="test_dataset",
            tables=["gold.sales", "silver.sales"],
            source="dummy_source",
        )
