# src/fleetpull/models/motive/user.py
"""The Motive user response model (``GET /v1/users``, captured 2026-07-21).

One record per account — driver or otherwise. Written from a
whole-population walk (2,665 records, 27 pages at ``per_page`` 100) whose
shape is perfectly role-partitioned: ``role='driver'`` records (2,359)
carry a driver-only key block on top of the shared keys, while ``admin``
(32) and ``fleet_user`` (274) records carry exactly the shared keys —
zero partial-presence keys within any role. This is the Samsara drivers
decision-1 posture with the split inverted: one population and ONE
dataset, but a role-dependent shape rather than a provider filter quirk —
the ``role`` column carries the split, and the driver-only block is
ABSENT, not null, on non-driver records.

Requiredness mirrors the census: the shared keys, present on every one of
the 2,665 records, are required (nullable exactly where the census
observed null); the driver-only keys are optional on the model — absent
for non-drivers — with nullability per the census inside the driver role.
``role`` (``driver``/``admin``/``fleet_user`` observed) and ``status``
(``active``: 1,020 / ``deactivated``: 1,645) are census-closed
vocabularies only, not API-enforced on output, so they mirror as plain
strings; likewise ``duty_status`` (observed ``on_duty``/``off_duty``/
``driving``), ``eld_mode`` (``logs``/``none``/``exempt``),
``violation_alerts`` (``never``/``1_hour``/``45_minutes``/
``30_minutes``/``15_minutes``), and the HOS cycle pair (``cycle2``
observed values like ``70_8_2020``/``60_7_o_2020`` on 37 drivers).
``joined_at`` is a DATE-ONLY wire value (``YYYY-MM-DD``, 34 of 2,359
drivers populated -- the whole-population value census), recovered as a
``date``; null on the rest.

The always-present key partition, exactly: 22 keys ride every record
(20 modeled; ``external_ids`` and ``phone_ext`` never populated
anywhere); ``admin``/``fleet_user`` records add 3 keys of their own
(``expires_at``, ``phone2``, ``phone_country_code2`` -- never populated
among them); driver records add 39 (38 modeled;
``associated_dispatcher_id`` never populated).

Excluded fields, per capture discipline: the six never-populated keys
named above were present but always null/empty across all 2,665
records -- the value types are unobservable (the value-unobservable
exclusion rule); they join the model when a capture types them
(``extra='ignore'`` makes exclusion exactly "don't model it"). Contrast
``joined_at``: value-OBSERVED (34 dates), hence modeled.
"""

from datetime import date, datetime

from pydantic import Field

from fleetpull.model_contract import ResponseModel

__all__: list[str] = ['User']


class User(ResponseModel):
    """One Motive user account: a driver, admin, or fleet_user.

    A pure mirror of the role-partitioned census. Groups below follow
    the partition: the shared block first (required on every record),
    then the driver-only block (absent, not null, on non-driver
    records — every field there defaults to ``None``). Field semantics
    are Motive's; no value is derived or interpreted here.

    Attributes:
        user_id: Motive's internal user identifier (wire key ``id``).
        first_name: User's first name.
        last_name: User's last name.
        email: User's email address; null when unset.
        phone: Contact phone number; null when unset.
        phone_country_code: Phone country code; null when unset.
        company_reference_id: Company-assigned reference identifier;
            null when unset.
        role: Free-form user-role string; ``driver`` / ``admin`` /
            ``fleet_user`` observed — the column that carries the
            shape partition.
        status: Free-form account-status string; ``active`` /
            ``deactivated`` observed.
        group_ids: Identifiers of the groups the user belongs to;
            often empty.
        metric_units: Whether the account displays metric units.
        time_zone: Rails-style time-zone display name; null when unset.
        created_at: When the account was created.
        updated_at: Last modification timestamp.
        mobile_current_sign_in_at: Current mobile-session sign-in;
            null when none.
        mobile_last_active_at: Last mobile activity; null when none.
        mobile_last_sign_in_at: Previous mobile sign-in; null when none.
        web_current_sign_in_at: Current web-session sign-in; null when
            none.
        web_last_active_at: Last web activity; null when none.
        web_last_sign_in_at: Previous web sign-in; null when none.
        username: Login username; driver-only; null when unset.
        driver_company_id: Company-assigned driver identifier;
            driver-only; null when unset.
        drivers_license_number: Driver's license number; driver-only;
            null when unset.
        drivers_license_state: Driver's license state/province;
            driver-only; null when unset.
        joined_at: Driver-only; the driver's join date -- a DATE-ONLY
            wire value (``YYYY-MM-DD``; 34 of 2,359 populated), null on
            the rest.
            conservative nullable-str posture (see module docstring).
        duty_status: Free-form HOS duty-status string; driver-only.
        eld_mode: Free-form ELD-mode string; driver-only.
        cycle: Free-form HOS cycle string; driver-only; null when
            unset.
        cycle2: Second-jurisdiction HOS cycle; driver-only; null when
            unset.
        violation_alerts: Free-form violation-alert cadence string;
            driver-only.
        carrier_name: Carrier name; driver-only.
        carrier_street: Carrier street address; driver-only.
        carrier_city: Carrier city; driver-only.
        carrier_state: Carrier state; driver-only.
        carrier_zip: Carrier postal code; driver-only.
        terminal_street: Home-terminal street; driver-only; null when
            unset.
        terminal_city: Home-terminal city; driver-only; null when
            unset.
        terminal_state: Home-terminal state; driver-only; null when
            unset.
        terminal_zip: Home-terminal postal code; driver-only; null
            when unset.
        exception_24_hour_restart: HOS exception flag; driver-only.
        exception_8_hour_break: HOS exception flag; driver-only.
        exception_adverse_driving: HOS exception flag; driver-only.
        exception_ca_farm_school_bus: HOS exception flag; driver-only.
        exception_short_haul: HOS exception flag; driver-only.
        exception_wait_time: HOS exception flag; driver-only.
        exception_24_hour_restart2: Second-jurisdiction HOS exception
            flag; driver-only (likewise the other ``*2`` flags).
        exception_8_hour_break2: See above; driver-only.
        exception_adverse_driving2: See above; driver-only.
        exception_ca_farm_school_bus2: See above; driver-only.
        exception_short_haul2: See above; driver-only.
        exception_wait_time2: See above; driver-only.
        export_combined: Log-export setting; driver-only.
        export_odometers: Log-export setting; driver-only.
        export_recap: Log-export setting; driver-only.
        manual_driving_enabled: Driving-mode setting; driver-only.
        minute_logs: Minute-grain logging setting; driver-only.
        personal_conveyance_enabled: Driving-mode setting; driver-only.
        yard_moves_enabled: Driving-mode setting; driver-only.
    """

    # The shared block: present on every record of every role.
    user_id: int = Field(alias='id')
    first_name: str
    last_name: str
    email: str | None
    phone: str | None
    phone_country_code: str | None
    company_reference_id: str | None
    role: str
    status: str
    group_ids: list[int]
    metric_units: bool
    time_zone: str | None
    created_at: datetime
    updated_at: datetime
    mobile_current_sign_in_at: datetime | None
    mobile_last_active_at: datetime | None
    mobile_last_sign_in_at: datetime | None
    web_current_sign_in_at: datetime | None
    web_last_active_at: datetime | None
    web_last_sign_in_at: datetime | None

    # The driver-only block: absent, not null, on non-driver records.
    # Identity and licensing.
    username: str | None = None
    driver_company_id: str | None = None
    drivers_license_number: str | None = None
    drivers_license_state: str | None = None
    joined_at: date | None = None

    # HOS configuration.
    duty_status: str | None = None
    eld_mode: str | None = None
    cycle: str | None = None
    cycle2: str | None = None
    violation_alerts: str | None = None

    # Carrier and home-terminal identity.
    carrier_name: str | None = None
    carrier_street: str | None = None
    carrier_city: str | None = None
    carrier_state: str | None = None
    carrier_zip: str | None = None
    terminal_street: str | None = None
    terminal_city: str | None = None
    terminal_state: str | None = None
    terminal_zip: str | None = None

    # HOS exception flags (the *2 variants are the second jurisdiction).
    exception_24_hour_restart: bool | None = None
    exception_8_hour_break: bool | None = None
    exception_adverse_driving: bool | None = None
    exception_ca_farm_school_bus: bool | None = None
    exception_short_haul: bool | None = None
    exception_wait_time: bool | None = None
    exception_24_hour_restart2: bool | None = None
    exception_8_hour_break2: bool | None = None
    exception_adverse_driving2: bool | None = None
    exception_ca_farm_school_bus2: bool | None = None
    exception_short_haul2: bool | None = None
    exception_wait_time2: bool | None = None

    # Log-export settings.
    export_combined: bool | None = None
    export_odometers: bool | None = None
    export_recap: bool | None = None

    # Driving-mode settings.
    manual_driving_enabled: bool | None = None
    minute_logs: bool | None = None
    personal_conveyance_enabled: bool | None = None
    yard_moves_enabled: bool | None = None
