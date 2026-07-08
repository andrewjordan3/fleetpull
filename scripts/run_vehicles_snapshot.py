# scripts/run_vehicles_snapshot.py
"""Hand-run driver: fetch the Motive vehicles snapshot through the public API.

The after-picture of the roadmap item-5 build, and the closed half of the
audit's consumer-cost evidence (AUDIT.md): the hand composition this
script used to carry -- provider config, endpoint definition, provider
profile, client runtime, transport client, request spec, page loop,
validation, frame construction -- is now one public ``fetch`` call. The
in-memory verb persists nothing; consumers who want parquet on disk are
sync users (DESIGN §10). Run from the repo root once MOTIVE_API_KEY is
set, e.g.:

    uv run python scripts/run_vehicles_snapshot.py

Errors propagate with a traceback by design -- this is a debugging driver.
"""

from fleetpull import Endpoints, fetch

# Paste your Motive API key here for a quick run, or (preferred) set
# MOTIVE_API_KEY in the environment so it never touches a tracked file.
MOTIVE_API_KEY: str = ''

# True behind the Zscaler-intercepting laptop; False on Colab / a GCP VM.
USE_TRUSTSTORE: bool = True


def main() -> None:
    """Fetch the vehicles snapshot once and report the returned frame."""
    if not MOTIVE_API_KEY:
        raise SystemExit(
            'Set MOTIVE_API_KEY (environment or in this file) before running.'
        )

    frame = fetch(
        Endpoints.Motive.vehicles,
        auth=MOTIVE_API_KEY,
        use_truststore=USE_TRUSTSTORE,
    )
    print(f'Fetched a {frame.height} x {frame.width} vehicles DataFrame from Motive:')
    print(frame.head())


if __name__ == '__main__':
    main()
