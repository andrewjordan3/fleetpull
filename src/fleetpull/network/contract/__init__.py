"""Provider-agnostic request contract: specs, auth, classification, pagination.

Deliberately still an empty face: callers currently import the
submodules directly (``from fleetpull.network.contract.request import
RequestSpec``). Populating this ``__init__`` to its consumed surface
(house convention) is the next prompt's tree-wide topology correction,
done alongside every other face in one pass. It is safe to populate now
that ``ResponseCategory`` lives in ``fleetpull.vocabulary`` — the
foundational ``exceptions`` module no longer reaches into this package,
so a populated face can no longer re-form the former
``exceptions`` -> ``contract.__init__`` -> ``envelopes`` -> ``exceptions``
cycle.
"""

__all__: list[str] = []
