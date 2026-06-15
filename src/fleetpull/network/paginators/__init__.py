"""Per-provider pagination strategies."""

from fleetpull.network.paginators.geotab import GeotabFeedPagination
from fleetpull.network.paginators.motive import MotivePagination
from fleetpull.network.paginators.samsara import SamsaraPagination
from fleetpull.network.paginators.single_page import SinglePageStrategy

__all__: list[str] = [
    'GeotabFeedPagination',
    'MotivePagination',
    'SamsaraPagination',
    'SinglePageStrategy',
]
