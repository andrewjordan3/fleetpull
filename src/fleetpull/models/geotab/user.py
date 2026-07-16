# src/fleetpull/models/geotab/user.py
"""GeoTab User response model (JSON-RPC ``Get`` on ``typeName: User``).

Written from captured live responses (2026-07-16 probe session: a
full-population sweep of all 157 accounts plus seven captured record
variants), never from docs. Drivers ARE users in GeoTab -- ``isDriver``
splits the population (129/157 captured) and the driver-only key block
(``licenseNumber``, ``licenseProvince``, ``viewDriversOwnDataOnly``)
is ABSENT, not null, on non-driver accounts; the sweep observed no null
value and no type variance on any key, so optionality here is
absence-shaped. Fields the sweep proved on every record are required;
the partial-presence fields are optional (presence counts in the
attribute docs).

Excluded fields (``extra='ignore'`` makes exclusion exactly "don't
model it"):

- ``activeDashboardReports``, ``activeDefaultDashboards``,
  ``availableDashboardReports``, ``bookmarks``, ``cannedResponseOptions``,
  ``companyGroups``, ``driverGroups``, ``jobPriorities``, ``keys``,
  ``mapViews``, ``mediaFiles``, ``privateUserGroups``, ``reportGroups``,
  ``securityGroups`` -- lists; the records layer's schema derivation
  (DESIGN section 9) supports scalars, enums, ``list[scalar]``, and
  nested models only, and these are UI/grouping plumbing whose struct
  shapes have no honest column until the ``list[nested model]``
  derivation vertical lands (the Device exclusion precedent).
- ``iAMMetadata`` -- identity-provider plumbing (an IAM GUID, a
  connection name, two provisioning flags), present on only 42/157
  swept records.

Empty strings lift to null on the contact/bookkeeping string fields
captured empty (``""`` is these fields' no-value shape, the
coercion-boundary rule); every other sentinel mirrors verbatim:
``activeTo`` of ``2050-01-01`` is GeoTab's still-active sentinel, and
``hosRuleSet`` carries the literal string ``"None"`` on non-driving
accounts -- the provider's vocabulary, never interpreted.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator. The five acronym keys the generator cannot produce
(``acceptedEULA``, ``isEULAAccepted``, ``isExemptHOSEnabled``,
``maxPCDistancePerDay``, ``wifiEULA``) carry explicit alias overrides,
each pinned against a captured value in the model tests (the Device
acronym-trap precedent).
"""

from datetime import datetime

from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import EmptyStrIsNone, ResponseModel

__all__: list[str] = ['User', 'UserAccessGroupFilterRef']


class UserAccessGroupFilterRef(ResponseModel):
    """The ``accessGroupFilter`` reference: a data-scope filter by id.

    Observed on exactly one of the 157 swept records (2026-07-16) -- an
    account whose visibility is restricted to an access group. The id is
    the ``a``-prefixed GUID-like form (the ExceptionEvent id shape), a
    pure reference mirrored as-is.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class User(ResponseModel):
    """One GeoTab User entity: a console account, driver or otherwise.

    A pure mirror of the captured scalar fields. Field semantics are
    GeoTab's; no value is derived or interpreted here. Groups below
    follow the captured record layout: identity and lifecycle, person
    and contact, the driver-only block, company/authority identity,
    locale and display preferences, HOS and driving flags, and the
    notification/UI flags.

    Attributes:
        id: GeoTab's user id -- the seek-paging sort key (hex-suffixed
            string, ascending; the id space trip ``driver`` refs point
            into).
        name: The login identifier -- an email address on most captured
            accounts, a bare username on one.
        active_from: Start of the account's active window (UTC).
        active_to: End of the active window; ``2050-01-01`` is GeoTab's
            still-active sentinel, stored as-is.
        last_access_date: Last console/app access (UTC); absent on one
            never-accessed swept account (156/157).
        is_driver: Whether the account is a driver -- the key whose
            value predicts the driver-only block's presence.
        license_number: Driver's license number; driver-only (129/157).
        license_province: Driver's license state/province; driver-only
            (129/157).
        view_drivers_own_data_only: Driver data-visibility restriction;
            driver-only (129/157).
        access_group_filter: Data-scope filter reference; observed on
            one swept record (1/157).
        max_pc_distance_per_day: Personal-conveyance distance cap;
            ``0`` captured everywhere it appears (126/157, not aligned
            with the driver split).
        hos_rule_set: HOS ruleset name; the literal string ``"None"``
            on non-driving accounts, mirrored verbatim.
        carrier_number: Motor-carrier number; captured on driver
            accounts, empty (lifted to null) elsewhere.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity and lifecycle.
    id: str
    name: str
    active_from: datetime
    active_to: datetime
    last_access_date: datetime | None = None
    is_auto_added: bool
    user_authentication_type: str
    change_password: bool
    access_group_filter: UserAccessGroupFilterRef | None = None

    # Person and contact.
    first_name: str
    last_name: str
    designation: EmptyStrIsNone
    employee_no: EmptyStrIsNone
    phone_number: EmptyStrIsNone
    phone_number_extension: EmptyStrIsNone
    comment: EmptyStrIsNone

    # Driver-only block (absent, not null, on non-driver accounts).
    is_driver: bool
    license_number: str | None = None
    license_province: str | None = None
    view_drivers_own_data_only: bool | None = None

    # Company / authority identity.
    company_name: str
    company_address: str
    authority_name: str
    authority_address: str
    carrier_number: EmptyStrIsNone

    # Locale and display preferences.
    language: str
    country_code: str
    time_zone_id: str
    date_format: str
    first_day_of_week: str
    display_currency: str
    is_metric: bool
    fuel_economy_unit: str
    electric_energy_economy_unit: str
    default_page: str
    default_map_engine: EmptyStrIsNone
    default_google_map_style: str
    default_here_map_style: str
    default_open_street_map_style: str
    zone_display_mode: str
    feature_preview: EmptyStrIsNone

    # HOS and driving flags.
    hos_rule_set: str
    is_exempt_hos_enabled: bool = Field(alias='isExemptHOSEnabled')
    is_yard_move_enabled: bool
    is_personal_conveyance_enabled: bool
    is_adverse_driving_enabled: bool
    max_pc_distance_per_day: int | None = Field(
        default=None, alias='maxPCDistancePerDay'
    )

    # EULA state.
    is_eula_accepted: bool = Field(alias='isEULAAccepted')
    accepted_eula: int = Field(alias='acceptedEULA')
    wifi_eula: int = Field(alias='wifiEULA')

    # Notification and UI flags.
    is_email_report_enabled: bool
    is_maintenance_notification_enabled: bool
    is_service_disruption_notifications_enabled: bool
    sms_notifications_opt_in: bool
    whats_app_notifications_opt_in: bool
    is_news_enabled: bool
    is_labs_enabled: bool
    is_ace_disclaimer_disabled: bool
    show_click_once_warning: bool
    show_rate_this_app: bool
