#!/usr/bin/env bash
# Install a verified improved-exp revision on the policy-trackr systemd VPS.
set -Eeuo pipefail
umask 022

unit=streamlit.service
repo=/srv/policy-trackr
override=/etc/systemd/system/streamlit.service.d/zz-policy-trackr-release.conf
live_port=8501

fail() { printf '%s\n' "$*" >&2; exit 1; }

healthy() {
    curl --noproxy '*' -fsS --max-time 2 "http://127.0.0.1:$1/${url_prefix:+$url_prefix/}_stcore/health" |
        grep -qx 'ok'
}

wait_for_release() {
    local service=$1 port=$2 expected_dir=$3 pid attempt
    for ((attempt=0; attempt<30; attempt++)); do
        pid=$(systemctl show "$service" -p MainPID --value)
        if [[ "$pid" =~ ^[1-9][0-9]*$ && "$(readlink -f "/proc/$pid/cwd")" == "$expected_dir" ]] &&
            healthy "$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

restore_previous() {
    local state=$1
    cmp -s "$state/release.conf" "$override" || {
        printf 'The active drop-in differs from this deployment record. Stop here.\n' >&2
        return 1
    }
    if [[ -f "$state/previous.conf" ]]; then
        install -m 0644 "$state/previous.conf" "$override.pending" || return 1
        mv -fT -- "$override.pending" "$override" || return 1
    else
        rm -- "$override" || return 1
    fi
    systemctl daemon-reload || return 1
    systemctl restart "$unit" || return 1
    wait_for_release "$unit" "$live_port" "$(<"$state/previous-directory")" || return 1
    printf 'Previous tracker release is running and healthy.\n'
}

[[ "$EUID" == 0 ]] || fail 'Run this installer as root.'
exec 9>/run/lock/policy-trackr-release.lock
flock -n 9 || fail 'Another tracker deployment is running.'
[[ ! -L "$override" ]] || fail 'The release drop-in is a symbolic link. Inspect it before continuing.'

if [[ "${1:-}" == --rollback ]]; then
    [[ $# == 2 && "$2" =~ ^/root/policy-trackr-release\.[[:alnum:]]+$ && -d "$2" && ! -L "$2" ]] ||
        fail 'Supply the rollback directory printed by the installer.'
    url_prefix=$(<"$2/url-prefix")
    restore_previous "$2" || fail 'Rollback health check failed. Inspect streamlit.service on the VPS.'
    exit 0
fi

[[ $# == 1 && "$1" =~ ^[0-9a-f]{40}$ ]] || fail 'Supply the full tested Git commit from improved-exp.'
revision=$1
short=${revision:0:7}
release="/srv/policy-trackr-improved-exp-$short"
entry=legiscan_git_sync_update_8_7.py
[[ "$(systemctl show "$unit" -p User --value)" == streamlit &&
   "$(systemctl show "$unit" -p Group --value)" == streamlit ]] || fail 'The tracker service account differs from the expected setup.'
previous=$(systemctl show "$unit" -p WorkingDirectory --value)
[[ "$previous" == /srv/policy-trackr* && -d "$previous" ]] || fail 'Could not identify the live tracker checkout.'
[[ "$previous" != "$release" ]] || fail 'This release is already selected by the live service.'
pid=$(systemctl show "$unit" -p MainPID --value)
url_prefix=$(python3.12 - "$previous" "$pid" <<'PY'
import sys
import tomllib
from pathlib import Path

directory, pid = sys.argv[1:]
process = Path("/proc") / pid
if pid == "0" or (process / "cwd").resolve() != Path(directory).resolve():
    raise SystemExit("The running process is not in the expected checkout.")
environment = dict(item.split(b"=", 1) for item in (process / "environ").read_bytes().split(b"\0") if b"=" in item)
if environment.get(b"DATA_DIR") != b"/srv/policy-trackr/data":
    raise SystemExit("The live persistent data path differs from the expected setup.")
config = Path(directory, ".streamlit", "config.toml")
settings = tomllib.loads(config.read_text()) if config.is_file() else {}
print(settings.get("server", {}).get("baseUrlPath", "").strip("/"))
PY
)
healthy "$live_port" || fail 'The current tracker did not pass its health check.'

git -C "$repo" fetch --no-tags origin refs/heads/improved-exp
git -C "$repo" merge-base --is-ancestor "$revision" FETCH_HEAD || fail 'That commit is outside improved-exp.'
[[ ! -L "$release" ]] || fail 'The staged checkout path is a symbolic link.'
if [[ -e "$release" ]]; then
    [[ "$(git -C "$release" rev-parse --show-toplevel)" == "$release" &&
       "$(git -C "$release" rev-parse HEAD)" == "$revision" ]] || fail 'The staged path already contains a different checkout.'
    git -C "$release" diff --quiet HEAD -- || fail 'The staged checkout has local edits.'
else
    git -C "$repo" worktree add --detach "$release" "$revision"
fi
python3.12 -m venv "$release/.venv"
python="$release/.venv/bin/python"
"$python" -m pip install --require-hashes -r "$release/requirements.lock"
"$python" -m pip check
install -d -m 0755 "$release/.streamlit"
if [[ -f "$previous/.streamlit/config.toml" ]]; then
    install -o root -g streamlit -m 0640 "$previous/.streamlit/config.toml" "$release/.streamlit/config.toml"
fi

smoke_port=18501
[[ -z "$(ss -H -ltn "sport = :$smoke_port")" ]] || fail 'The isolated smoke-test port is occupied.'
smoke_name="policy-trackr-smoke-$short-$$"
state=$(mktemp -d /root/policy-trackr-release.XXXXXXXX)
changed=0
complete=0
finish() {
    local result=$?
    trap - EXIT
    systemctl stop "$smoke_name.service" >/dev/null 2>&1 || true
    if (( changed && !complete )); then
        printf 'Cutover failed. Restoring the previous release.\n' >&2
        restore_previous "$state" || printf 'Rollback needs attention. Inspect %s and %s.\n' "$unit" "$state" >&2
    fi
    exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

systemd-run --quiet --unit="$smoke_name" --property=User=streamlit --property=Group=streamlit \
    --property="WorkingDirectory=$release" --property="RuntimeDirectory=$smoke_name" \
    --setenv="DATA_DIR=/run/$smoke_name/data" --setenv="REPO_DIR=/run/$smoke_name/publication" \
    --setenv=API_KEY= --setenv=CONGRESS_API_KEY= --setenv=LEGISCAN_BACKGROUND_JOBS=0 \
    --setenv=NOTIFICATION_DATABASE_URL= --setenv=RESEND_API_KEY= \
    "$python" -m streamlit run "$entry" --server.address=127.0.0.1 --server.port="$smoke_port" \
    --server.headless=true --browser.gatherUsageStats=false
wait_for_release "$smoke_name.service" "$smoke_port" "$release" || fail 'The staged release failed its isolated health check.'
printf 'Staged release passed the health check.\n'

if [[ -f "$previous/.streamlit/secrets.toml" ]]; then
    install -o root -g streamlit -m 0640 "$previous/.streamlit/secrets.toml" "$release/.streamlit/secrets.toml"
fi
[[ "$(systemctl show "$unit" -p MainPID --value)" == "$pid" ]] || fail 'The live service changed during staging. Run the installer again.'
printf '%s\n' "$previous" > "$state/previous-directory"
printf '%s\n' "$url_prefix" > "$state/url-prefix"
if [[ -f "$override" ]]; then
    install -m 0600 "$override" "$state/previous.conf"
fi
cat > "$state/release.conf" <<EOF
[Service]
WorkingDirectory=$release
ExecStart=
ExecStart=$python -m streamlit run $entry --server.address=0.0.0.0 --server.port=$live_port
EOF
printf 'Rollback directory: %s\n' "$state"
install -D -m 0644 "$state/release.conf" "$override.pending"
mv -fT -- "$override.pending" "$override"
changed=1
systemctl daemon-reload
[[ "$(systemctl show "$unit" -p WorkingDirectory --value)" == "$release" ]] || fail 'A different systemd override takes precedence.'
systemctl restart "$unit"
wait_for_release "$unit" "$live_port" "$release" || fail 'The new release failed its production health check.'
complete=1
printf 'Live tracker is running %s on port %s.\n' "$revision" "$live_port"
