# Local verification

Verified on 2026-10-09 with Python 3.14.4 on macOS arm64.
CI declares Python 3.11, 3.12 and 3.13; those remote matrix jobs were not run in this local check.

| Check | Local result |
| --- | --- |
| `check .` | PASS |
| `format --check .` | PASS |
| `-m pytest -q` | PASS: 23 passed in 0.06s |
| `-m apiqueue --help` | PASS |
| `examples/offline_demo.py` | PASS |
| `-m build --no-isolation` | PASS |

## Actual offline demo output

```text
{"id": "ok", "status": 200, "attempts": 1, "body": "{\"path\":\"/ok\",\"source\":\"offline fixture\"}", "error": null, "duration_ms": 0.6797080859541893}
{"id": "busy", "status": 200, "attempts": 2, "body": "{\"path\":\"/busy\",\"source\":\"offline fixture\"}", "error": null, "duration_ms": 19.502167124301195}
```

The demo uses real local tools or a mock HTTP transport. It does not verify any live service or model.

## Dependency advisory check

`pip-audit --local --progress-spinner off --format json` returned exit 0
on the shared verification environment: 59 package records and no known
vulnerabilities in the audited packages. The initial bootstrap pip 26.1 had
known advisories; upgrading it to 26.2.1 resolved them without ignores.
Unpublished local AgentKernel 0.1.0 was skipped by the advisory service.
This check covers installed dependency versions; it is not an application security audit.
