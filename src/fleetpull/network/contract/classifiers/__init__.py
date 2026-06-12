"""Per-provider response classifiers."""

from fleetpull.network.contract.classifiers.geotab import GeotabResponseClassifier
from fleetpull.network.contract.classifiers.motive import MotiveResponseClassifier
from fleetpull.network.contract.classifiers.samsara import SamsaraResponseClassifier

__all__: list[str] = [
    'GeotabResponseClassifier',
    'MotiveResponseClassifier',
    'SamsaraResponseClassifier',
]
