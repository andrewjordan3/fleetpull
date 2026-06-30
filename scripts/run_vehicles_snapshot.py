# scripts/run_vehicles_snapshot.py
"""Throwaway driver: fetch the Motive vehicles snapshot end to end and persist it.

Hardcoded config stands in for the not-yet-built YAML loader. Run from the repo
root once MOTIVE_API_KEY and the environment toggles are set, e.g.:

    uv run python scripts/run_vehicles_snapshot.py

Errors propagate with a traceback by design -- this is a debugging driver.
"""

import os
import random

from pydantic import SecretStr

from fleetpull.config import HttpConfig, MotiveConfig, RetryConfig
from fleetpull.endpoints.motive.vehicles import build_endpoint
from fleetpull.models.motive import Vehicle
from fleetpull.network.auth import StaticHeaderAuth
from fleetpull.network.classifiers import MotiveResponseClassifier
from fleetpull.network.client import ClientRuntime, ProviderProfile, TransportClient
from fleetpull.network.limits import RateLimitConfig, RateLimiterRegistry
from fleetpull.records import models_to_dataframe, validate_records
from fleetpull.storage import select_writer
from fleetpull.timing import SystemSleeper

# --- hardcoded config (stands in for the YAML loader) -----------------------

# Paste your Motive API key here for a quick run, or (preferred) set
# MOTIVE_API_KEY in the environment so it never touches a tracked file.
MOTIVE_API_KEY: str = os.environ.get('MOTIVE_API_KEY', '')

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True

# Where the parquet lands. Set this for your environment before running
# (e.g. a Windows path on the laptop, a POSIX path on Colab / a GCP VM).
DATASET_ROOT: str = ''

MOTIVE_BASE_URL: str = 'https://api.gomotive.com'
RECORDS_PER_PAGE: int = 100

# Placeholder Motive rate limits (real values TBD per DESIGN). A vehicles
# snapshot is a handful of requests, so these barely bind here.
MOTIVE_RATE_LIMIT: RateLimitConfig = RateLimitConfig(
    requests_per_period=60, period_seconds=60.0, burst=10, max_concurrency=2
)


def main() -> None:
    """Run the vehicles snapshot once and report what was persisted."""
    if not MOTIVE_API_KEY:
        raise SystemExit(
            'Set MOTIVE_API_KEY (environment or in this file) before running.'
        )
    if not DATASET_ROOT:
        raise SystemExit('Set DATASET_ROOT to a destination path before running.')

    motive_config: MotiveConfig = MotiveConfig(
        base_url=MOTIVE_BASE_URL, records_per_page=RECORDS_PER_PAGE
    )
    definition = build_endpoint(motive_config)
    scope: str = definition.quota_scope.value

    profile: ProviderProfile = ProviderProfile(
        auth=StaticHeaderAuth('X-API-Key', SecretStr(MOTIVE_API_KEY)),
        classifier=MotiveResponseClassifier(),
    )
    runtime: ClientRuntime = ClientRuntime(
        http_config=HttpConfig(use_truststore=USE_TRUSTSTORE),
        retry_config=RetryConfig(),
        limiter_registry=RateLimiterRegistry({scope: MOTIVE_RATE_LIMIT}),
        random_source=random.Random(),
        sleeper=SystemSleeper(),
    )

    spec = definition.spec_builder.build_spec(resume=None, path_values={})

    with TransportClient(profile, runtime) as client:
        records = [
            record
            for page in client.fetch_pages(spec, definition.page_decoder, scope)
            for record in page.records
        ]
    print(f'Fetched {len(records)} vehicle records from Motive.')

    models = validate_records(records, Vehicle)
    frame = models_to_dataframe(models, Vehicle)
    print(f'Built a {frame.height} x {frame.width} DataFrame:')
    print(frame.head())

    writer = select_writer(definition, DATASET_ROOT)
    writer.write(frame)
    result = writer.finalize()
    print(
        f'Persisted {result.rows_written} rows to {DATASET_ROOT} '
        f'({result.duplicates_dropped} exact duplicates dropped, '
        f'{result.files_written} file written).'
    )


if __name__ == '__main__':
    main()
