# tests/test_placeholder.py
# Placeholder — confirms CI pipeline is wired correctly
# Real producer tests will be added in Step 2.3

def test_ci_is_working():
    """CI smoke test — if this passes, GitHub Actions is wired correctly."""
    assert 1 + 1 == 2, "Basic sanity check failed"

def test_python_version():
    """Confirm we are running on Python 3.10.x"""
    import sys
    assert sys.version_info.major == 3
    assert sys.version_info.minor == 10