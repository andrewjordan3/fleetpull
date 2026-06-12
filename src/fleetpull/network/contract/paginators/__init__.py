"""Per-provider pagination strategies."""

from fleetpull.network.contract.paginators.geotab import GeotabFeedPagination
from fleetpull.network.contract.paginators.motive import MotivePagination
from fleetpull.network.contract.paginators.samsara import SamsaraPagination
from fleetpull.network.contract.paginators.single_page import SinglePageStrategy

__all__: list[str] = [
    'GeotabFeedPagination',
    'MotivePagination',
    'SamsaraPagination',
    'SinglePageStrategy',
]
