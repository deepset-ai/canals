# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0

from sample_components import Double
from canals.serialization import component_to_dict, component_from_dict


def test_double_default():
    component = Double()
    results = component.run(value=10)
    assert results == {"value": 20}
