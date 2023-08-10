# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=missing-function-docstring
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest


@pytest.fixture
def test_files():  # pylint: disable=redefined-outer-name
    return Path(__file__).parent / "test_files"


@pytest.fixture(autouse=True)
def mock_mermaid_request(test_files):  # pylint: disable=redefined-outer-name
    """
    Prevents real requests to https://mermaid.ink/
    """
    with patch("canals.pipeline.draw.mermaid.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        with open(test_files / "mermaid_mock" / "test_response.png", "rb") as file:
            mock_response.content = file.read()
        mock_get.return_value = mock_response
        yield
