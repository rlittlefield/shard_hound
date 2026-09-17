#!/usr/bin/env bash
#
# Builds the pgdog docker image the compose file expects, from the
# move-keys-broadcast-null-v3 branch of the pgdog fork (the stack of
# PRs adding ADD SHARD, MOVE KEYS and hybrid tables). See docs/pgdog.md
# and docs/resharding.md.
#
# Usage:
#   scripts/build-pgdog-image.sh
#
# Overridable via environment:
#   PGDOG_REPO    git URL of the fork    (default: rlittlefield/pgdog)
#   PGDOG_BRANCH  branch to build        (default: move-keys-broadcast-null-v3)
#   PGDOG_DIR     checkout location      (default: ~/.cache/shard_hound/pgdog)
#   PGDOG_IMAGE   image tag              (default: pgdog:move-keys-broadcast-null-v3)
#
# Point PGDOG_DIR at an existing checkout to build local, unpushed work.
set -euo pipefail

PGDOG_REPO="${PGDOG_REPO:-https://github.com/rlittlefield/pgdog.git}"
PGDOG_BRANCH="${PGDOG_BRANCH:-move-keys-broadcast-null-v3}"
DEFAULT_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/shard_hound/pgdog"
PGDOG_DIR="${PGDOG_DIR:-$DEFAULT_DIR}"
PGDOG_IMAGE="${PGDOG_IMAGE:-pgdog:move-keys-broadcast-null-v3}"

# Hybrid tables (`kind = "hybrid"` in pgdog.toml): without these two
# commits pgdog rejects the custom_packages entry, ADD SHARD skips the
# NULL-key default rows, and MOVE KEYS refuses the table's nullable
# sharding column.
REQUIRED_FEATURES=(
  "ADD SHARD copies hybrid tables' NULL-key rows to the new shard"
  "MOVE KEYS on hybrid tables"
)

if [ "$PGDOG_DIR" != "$DEFAULT_DIR" ]; then
  # A caller-provided checkout is built exactly as it stands, so
  # local, unpushed work is never touched.
  echo "==> Using existing checkout $PGDOG_DIR as-is"
elif [ -d "$PGDOG_DIR/.git" ]; then
  echo "==> Updating $PGDOG_BRANCH in $PGDOG_DIR"
  git -C "$PGDOG_DIR" fetch origin "$PGDOG_BRANCH"
  git -C "$PGDOG_DIR" checkout -B "$PGDOG_BRANCH" "origin/$PGDOG_BRANCH"
else
  echo "==> Cloning $PGDOG_REPO ($PGDOG_BRANCH) into $PGDOG_DIR"
  mkdir -p "$(dirname "$PGDOG_DIR")"
  git clone --branch "$PGDOG_BRANCH" "$PGDOG_REPO" "$PGDOG_DIR"
fi

echo "==> Building $PGDOG_IMAGE from $(git -C "$PGDOG_DIR" rev-parse --short HEAD)"

for feature in "${REQUIRED_FEATURES[@]}"; do
  if ! git -C "$PGDOG_DIR" log --oneline --fixed-strings --grep "$feature" | grep -q .; then
    echo "WARNING: this checkout is missing the '$feature' commit;" >&2
    echo "         the hybrid custom_packages table won't work end to end." >&2
  fi
done

docker build -t "$PGDOG_IMAGE" "$PGDOG_DIR"

echo "==> Done: $PGDOG_IMAGE"
