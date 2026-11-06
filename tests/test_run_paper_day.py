"""scripts/run_paper_day.sh argument logic under /bin/bash (macOS bash 3.2 with `set -u`), with accounts.yaml absent,
disabled and enabled. Nothing real is launched: UV is a stub that records its arguments, `date` is shadowed so the
ET clock reads 10:30, and the script runs in a temp REPO."""

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_paper_day.sh"
BASE = "run --frozen python -m trade_jev.live --exit-at-close --no-open --port 8767"


@pytest.fixture
def repo(tmp_path):
    (tmp_path / "bin").mkdir()
    (tmp_path / "repo").mkdir()
    (tmp_path / "uv").write_text('#!/bin/bash\necho "$@" > "$ARGS_FILE"\n')
    (tmp_path / "uv").chmod(0o755)
    (tmp_path / "bin" / "date").write_text(textwrap.dedent("""\
        #!/bin/bash
        if [ "$1" = "+%H%M" ]; then echo 1030; else exec /bin/date "$@"; fi
        """))
    (tmp_path / "bin" / "date").chmod(0o755)
    return tmp_path


def run_script(repo, accounts=None, extra_env=None):
    r = repo / "repo"
    if accounts is not None:
        (r / "accounts.yaml").write_text(accounts)
    env = {"PATH": f"{repo / 'bin'}:/usr/bin:/bin", "HOME": str(repo), "TRADE_JEV_REPO": str(r),
           "UV": str(repo / "uv"), "ARGS_FILE": str(repo / "args.txt"), **(extra_env or {})}
    p = subprocess.run(["/bin/bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, p.stdout + p.stderr
    log = next((r / "logs").glob("paper-*.log")).read_text()
    assert "unbound variable" not in log
    return (repo / "args.txt").read_text().strip()


def test_no_accounts_file_leaves_the_command_unchanged(repo):
    assert run_script(repo) == BASE


def test_disabled_accounts_leave_the_command_unchanged(repo):
    y = "accounts:\n  - name: a\n    enabled: false\n  # enabled: true\n    mode: dry_run\n"
    assert run_script(repo, y) == BASE


def test_an_enabled_account_adds_the_exec_url(repo):
    y = "port: 8768\naccounts:\n  - name: a\n    enabled: false\n  - name: b\n    enabled: true\n"
    assert run_script(repo, y) == BASE + " --exec-url http://127.0.0.1:8768"


def test_the_copy_port_follows_the_yaml_only_and_enabled_is_case_insensitive(repo):
    y = "port: 9100\naccounts:\n  - {name: b}\n  - name: b\n    enabled: True\n"
    assert run_script(repo, y).endswith("--exec-url http://127.0.0.1:9100")
    # the env var is no longer a second source (the daemon and the extension never read it)
    assert run_script(repo, y, {"TRADE_JEV_COPY_PORT": "9200"}).endswith("--exec-url http://127.0.0.1:9100")
    assert run_script(repo, "accounts:\n  - enabled: TRUE\n").endswith("--exec-url http://127.0.0.1:8768")
