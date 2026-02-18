import pytest


def test_sanity():
    """Basic sanity check to ensure pytest runs."""
    assert True


@pytest.mark.parametrize(
    "a, b, expected",
    [
        (1, 1, 2),
        (2, 3, 5),
        (0, 0, 0),
    ],
)
def test_addition(a, b, expected):
    """Dummy parametrized test."""
    assert a + b == expected


def test_exception():
    """Ensure exceptions are properly raised."""
    with pytest.raises(ZeroDivisionError):
        _ = 1 / 0


def test_package_import():
    """
    Optional: verify that trading_runtime can be imported.
    Remove if not needed.
    """
    try:
        import trading_runtime  # noqa: F401
    except ImportError as exc:
        pytest.fail(f"Failed to import trading_runtime: {exc}")
