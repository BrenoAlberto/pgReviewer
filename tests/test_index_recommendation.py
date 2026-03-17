from pgreviewer.core.models import IndexRecommendation


def test_index_recommendation_serialization():
    rec = IndexRecommendation(
        table="users",
        columns=["email"],
        index_type="btree",
        partial_predicate=None,
        create_statement="CREATE INDEX CONCURRENTLY idx_users_email ON users (email)",
        cost_before=100.0,
        cost_after=10.0,
        improvement_pct=90.0,
        estimated_size_bytes=1024,
        validated=True,
        rationale="High cost sequential scan detected on email filter",
        confidence=0.88,
    )

    # Test to_dict
    data = rec.to_dict()
    assert data["table"] == "users"
    assert data["columns"] == ["email"]
    assert data["improvement_pct"] == 90.0
    assert data["validated"] is True
    assert data["confidence"] == 0.88

    # Test from_dict
    rec2 = IndexRecommendation.from_dict(data)
    assert rec2 == rec
    assert rec2.table == "users"
    assert rec2.improvement_pct == 90.0
    assert rec2.validated is True
    assert rec2.confidence == 0.88


def test_index_recommendation_partial_serialization():
    rec = IndexRecommendation(
        table="orders",
        columns=["status"],
        index_type="btree",
        partial_predicate="status = 'pending'",
        create_statement=(
            "CREATE INDEX CONCURRENTLY idx_orders_pending "
            "ON orders (status) WHERE status = 'pending'"
        ),
        cost_before=500.0,
        cost_after=50.0,
        improvement_pct=90.0,
        estimated_size_bytes=None,
        validated=False,
        rationale="Optimization for pending orders",
    )

    data = rec.to_dict()
    assert data["partial_predicate"] == "status = 'pending'"
    assert data["estimated_size_bytes"] is None

    rec2 = IndexRecommendation.from_dict(data)
    assert rec2 == rec
