# src/fleetpull/network/decoders/_window_stamp.py
"""The shared window stamp for window-grain report decoders.

Window-grain rollup surfaces return rows carrying NO event time of any
kind -- each row is the provider's aggregate over exactly the requested
window, so the row's time identity is the window the SENT spec asked
for. The decoders for those surfaces stamp every record with the
synthesized ``windowStartDate``/``windowEndDate`` keys, copied VERBATIM
from the sent spec's own window params.

The stamp keys are OUR vocabulary, deliberately PROVIDER-UNIFORM: they
are synthesized, not mirrored, so uniformity costs no wire fidelity and
buys one recognizable window-identity shape across providers (the
Samsara fuel-energy pair and the Motive utilization pair both land
``window_start``/``window_end`` on their models). That is why this
helper is shared across provider decoder modules -- a deliberate,
narrow exception to the decoders' blast-radius-over-DRY rule, which
covers provider ENVELOPE logic: only the param NAMES vary per provider,
and each caller passes its own.
"""

from typing import Final

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import RequestSpec
from fleetpull.vocabulary import JsonObject

__all__: list[str] = [
    'WINDOW_END_KEY',
    'WINDOW_START_KEY',
    'window_stamp_from_sent_spec',
]

# The synthesized window-identity keys merged onto each report record.
# Chosen collision-free against every walked report census (no
# time-shaped key on any window-grain surface's wire rows), and named
# after what they carry: the window that was asked of the provider.
WINDOW_START_KEY: Final[str] = 'windowStartDate'
WINDOW_END_KEY: Final[str] = 'windowEndDate'


def window_stamp_from_sent_spec(
    sent: RequestSpec, *, start_param: str, end_param: str
) -> JsonObject:
    """The window-identity keys the sent spec contributes to its records.

    Copied VERBATIM from the sent spec's own window params --
    wire-truthful: the stamp is exactly what was asked of the provider,
    whose answer each rollup row is. A sent spec lacking either param is
    a wiring bug (every window-grain builder renders both), and silently
    unstamped rows would strip the rows' time identity -- so it fails
    loudly instead.

    Args:
        sent: The spec that produced the page being decoded.
        start_param: The provider's window-start param name (e.g.
            Samsara's ``startDate``, Motive's ``start_date``).
        end_param: The provider's window-end param name.

    Returns:
        ``{'windowStartDate': ..., 'windowEndDate': ...}``, values
        verbatim from the sent params.

    Raises:
        ProviderResponseError: The sent spec lacks either window param.
    """
    params = sent.params or {}
    if start_param not in params or end_param not in params:
        raise ProviderResponseError(
            detail=(
                f'sent spec lacks the {start_param!r}/{end_param!r} window '
                'params to stamp report rows with'
            )
        )
    return {
        WINDOW_START_KEY: params[start_param],
        WINDOW_END_KEY: params[end_param],
    }
