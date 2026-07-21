# src/fleetpull/models/motive/group.py
"""The Motive group response model (``GET /v1/groups``, captured 2026-07-21).

One record per organizational group. Written from a whole-population walk
(152 records, four pages at ``per_page`` 50): every modeled key was present
on all 152 records, so everything here is required -- nullability is the
only optionality, and it follows the census exactly. ``parent_id`` is null
on root groups and an existing group id otherwise (the groups form a
tree).

``user`` is the group's owner/creator reference -- the compact
users-endpoint account shape, carried by the shared ``UserSummary`` (the
``shared.py`` promotion rule: the identical wire shape already rides the
driving-period and idle-event driver references). On THIS surface the
census observed ``username`` and ``driver_company_id`` null on all 152
records and ``role``/``status``/names populated on all 152;
``UserSummary``'s union-lax optionality absorbs both facts, and the
sibling surfaces are where the null-here keys carry values.
"""

from pydantic import Field

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import UserSummary

__all__: list[str] = ['Group']


class Group(ResponseModel):
    """One Motive organizational group.

    A pure mirror of the whole-population census: five keys, all present
    on every record.

    Attributes:
        group_id: Motive's internal group identifier (wire key ``id``).
        company_id: Parent company identifier.
        name: The group's display name.
        parent_id: The parent group's id; null on root groups (the
            groups form a tree).
        user: The group's owner/creator reference (the shared compact
            users-endpoint account shape).
    """

    group_id: int = Field(alias='id')
    company_id: int
    name: str
    parent_id: int | None
    user: UserSummary
