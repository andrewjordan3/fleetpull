"""Two-way parity between the public catalog and the discovery registry.

The catalog is a static committed module, so its drift protection is this
test: every catalog identity must resolve in ``build_endpoint_registry``'s
output with the mode its type claims, and every registered endpoint must
appear in the catalog under the mode-matching identity type. The check
itself is a pure function over plain data so the permanent negative-shape
tests below can feed it planted breakage (a missing entry, an
unresolvable identity, a wrong-typed identity) and prove each direction
actually fires.
"""

from collections.abc import Iterable, Mapping

from fleetpull.api import SnapshotEndpoint, WindowedEndpoint, available_endpoints
from fleetpull.api.identity import EndpointIdentity
from fleetpull.config import GeotabConfig, MotiveConfig, SamsaraConfig
from fleetpull.endpoints import build_endpoint_registry
from fleetpull.endpoints.shared import FeedMode, SnapshotMode, SyncMode, WatermarkMode
from fleetpull.vocabulary import Provider

type _RegistryKey = tuple[Provider, str]


def _identity_type_for_mode(
    sync_mode: SyncMode,
) -> type[SnapshotEndpoint] | type[WindowedEndpoint] | None:
    """The identity type a mode must be cataloged under; None = undecided.

    FeedMode deliberately maps to nothing: no feed endpoint exists, and
    whether its identity is honestly 'windowed' is a naming decision to
    make when the first one lands -- an unmapped mode fails parity
    loudly rather than being bucketed silently.
    """
    match sync_mode:
        case SnapshotMode():
            return SnapshotEndpoint
        case WatermarkMode():
            return WindowedEndpoint
        case FeedMode():
            return None


def _parity_violations(
    catalog: Iterable[EndpointIdentity],
    registry_modes: Mapping[_RegistryKey, SyncMode],
) -> list[str]:
    """Every way the catalog and the registry disagree, one line each."""
    violations: list[str] = []
    catalog_by_key: dict[_RegistryKey, EndpointIdentity] = {
        (identity.provider, identity.name): identity for identity in catalog
    }
    for key, identity in catalog_by_key.items():
        sync_mode = registry_modes.get(key)
        if sync_mode is None:
            violations.append(f'catalog identity {key} resolves to no endpoint')
            continue
        expected_type = _identity_type_for_mode(sync_mode)
        if expected_type is None:
            violations.append(
                f'{key}: no identity type maps {type(sync_mode).__name__}'
            )
        elif type(identity) is not expected_type:
            violations.append(
                f'{key}: cataloged as {type(identity).__name__}, mode '
                f'{type(sync_mode).__name__} requires {expected_type.__name__}'
            )
    for missing_key in registry_modes.keys() - catalog_by_key.keys():
        violations.append(f'registry endpoint {missing_key} missing from catalog')
    return violations


def _registry_modes() -> dict[_RegistryKey, SyncMode]:
    """The real registry's key → sync-mode map, via discovery.

    The registry exposes only ``get``; parity needs the full key set, so
    this reaches the private map -- sanctioned for tests, and better than
    growing the production surface an enumeration method nobody else uses.
    """
    registry = build_endpoint_registry(
        [MotiveConfig(), GeotabConfig(), SamsaraConfig()]
    )
    return {key: definition.sync_mode for key, definition in registry._by_key.items()}


def test_catalog_and_registry_agree_both_ways() -> None:
    assert _parity_violations(available_endpoints(), _registry_modes()) == []


# --------------------------------------------------------------------------- #
# Permanent negative shapes: each planted breakage must be reported.
# --------------------------------------------------------------------------- #
def test_registry_endpoint_missing_from_catalog_is_reported() -> None:
    modes: dict[_RegistryKey, SyncMode] = {(Provider.MOTIVE, 'planted'): SnapshotMode()}
    violations = _parity_violations([], modes)
    assert violations == [
        "registry endpoint (<Provider.MOTIVE: 'motive'>, 'planted') missing from catalog"
    ]


def test_unresolvable_catalog_identity_is_reported() -> None:
    phantom = SnapshotEndpoint(Provider.MOTIVE, 'phantom')
    violations = _parity_violations([phantom], {})
    assert len(violations) == 1
    assert 'resolves to no endpoint' in violations[0]
    assert 'phantom' in violations[0]


def test_wrong_typed_catalog_identity_is_reported() -> None:
    wrong_typed = WindowedEndpoint(Provider.MOTIVE, 'vehicles')
    modes: dict[_RegistryKey, SyncMode] = {
        (Provider.MOTIVE, 'vehicles'): SnapshotMode()
    }
    violations = _parity_violations([wrong_typed], modes)
    assert len(violations) == 1
    assert 'WindowedEndpoint' in violations[0]
    assert 'SnapshotEndpoint' in violations[0]


def test_unmapped_mode_is_reported_not_bucketed() -> None:
    feed_identity = WindowedEndpoint(Provider.MOTIVE, 'planted_feed')
    modes: dict[_RegistryKey, SyncMode] = {
        (Provider.MOTIVE, 'planted_feed'): FeedMode()
    }
    violations = _parity_violations([feed_identity], modes)
    assert len(violations) == 1
    assert 'no identity type maps FeedMode' in violations[0]
