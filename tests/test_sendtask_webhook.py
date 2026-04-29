from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_login_logs_do_not_pollute_authorization_header(tmp_path: Path):
    source_script = ROOT / "scripts" / "sendTask_webhook.sh"
    script_copy = tmp_path / "sendTask_webhook.sh"
    shutil.copy2(source_script, script_copy)
    script_copy.chmod(0o755)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture_file = tmp_path / "auth.txt"
    fake_curl = fake_bin / "curl"
    fake_curl.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
out=''
auth=''
url=''
while (($#)); do
  case "$1" in
    -o)
      out=$2
      shift 2
      ;;
    -w)
      shift 2
      ;;
    -H)
      if [[ "$2" == Authorization:* ]]; then
        auth="${2#Authorization: Bearer }"
      fi
      shift 2
      ;;
    -d|-F|-X)
      shift 2
      ;;
    -sS)
      shift 1
      ;;
    http://*)
      url=$1
      shift 1
      ;;
    *)
      shift 1
      ;;
  esac
done

if [[ "$url" == "http://mock/api/auth/login" ]]; then
  printf '%s' '{"access_token":"abc123"}' > "$out"
  printf '200'
elif [[ "$url" == "http://mock/sendTask" ]]; then
  printf '%s' "$auth" > "$TMP_AUTH_CAPTURE"
  printf '%s' '{"ok":true}' > "$out"
  printf '200'
else
  printf '%s' '{"detail":"unexpected url"}' > "$out"
  printf '500'
fi
""",
        encoding="utf-8",
    )
    fake_curl.chmod(0o755)

    conf_file = tmp_path / "sendTask.conf"
    conf_file.write_text(
        "BASE_URL=\"http://mock\"\n"
        "USERNAME=\"admin\"\n"
        "PASSWORD=\"secret\"\n"
        "declare -A GROUP_MAP=([cat]=\"剧场动画\")\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["TMP_AUTH_CAPTURE"] = str(capture_file)

    completed = subprocess.run(
        ["bash", str(script_copy), "-g", "cat", "-d", "demo", "-c", str(conf_file)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert capture_file.read_text(encoding="utf-8") == "abc123"
    assert "token 已缓存" in completed.stderr
    assert "token 已缓存" not in completed.stdout
    cache_files = list(tmp_path.glob(".token_*"))
    assert len(cache_files) == 1
    assert cache_files[0].read_text(encoding="utf-8").strip() == "abc123"