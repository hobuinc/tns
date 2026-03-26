"""Opt-in placeholder for live AWS integration coverage."""

from __future__ import annotations

import os

import pytest


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TNS_RUN_AWS_TESTS") != "1",
        reason="Set TNS_RUN_AWS_TESTS=1 to run live AWS integration tests.",
    ),
]


def test_live_aws_pipeline_placeholder():
    """Document that live AWS testing is intentionally out of the unit suite."""
    pytest.skip(
        "Live AWS integration coverage should be run against deployed infrastructure."
    )
