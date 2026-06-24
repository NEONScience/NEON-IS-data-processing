#!/usr/bin/env bash
set -euo pipefail

# Merges all remote origin branches matching a glob
# into a local branch named deployment-candidate-consolidated.
#
# Usage:
#   ./merge-deployment-candidates.sh [base_ref] [--prefix-glob GLOB] [--reset] [--dry-run]
#
# Examples:
#   ./merge-deployment-candidates.sh
#   ./merge-deployment-candidates.sh origin/master
#   ./merge-deployment-candidates.sh --prefix-glob 'origin/deployment-candidate-*'
#   ./merge-deployment-candidates.sh origin/main --reset
#   ./merge-deployment-candidates.sh --dry-run

REMOTE="origin"
PREFIX_GLOB="origin/deployment-candidate-*files*"
TARGET_BRANCH="deployment-candidate-consolidated-early"
RESET_TARGET=false
DRY_RUN=false
BASE_REF=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset)
      RESET_TARGET=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --prefix-glob)
      if [[ -z "${2:-}" ]]; then
        echo "Error: --prefix-glob requires a value."
        exit 1
      fi
      PREFIX_GLOB="$2"
      shift 2
      ;;
    --prefix-glob=*)
      PREFIX_GLOB="${1#*=}"
      shift
      ;;
    -*)
      echo "Error: Unknown option '$1'"
      echo "Usage: $0 [base_ref] [--prefix-glob GLOB] [--reset] [--dry-run]"
      exit 1
      ;;
    *)
      if [[ -z "$BASE_REF" ]]; then
        BASE_REF="$1"
        shift
      else
        echo "Error: Unexpected argument '$1'"
        echo "Usage: $0 [base_ref] [--prefix-glob GLOB] [--reset] [--dry-run]"
        exit 1
      fi
      ;;
  esac
done

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: This script must be run inside a git repository."
  exit 1
fi

echo "Fetching latest refs from $REMOTE..."
git fetch "$REMOTE" --prune

if [[ -z "$BASE_REF" ]]; then
  if BASE_REF=$(git symbolic-ref --quiet --short "refs/remotes/$REMOTE/HEAD" 2>/dev/null); then
    :
  elif git show-ref --verify --quiet "refs/remotes/$REMOTE/master"; then
    BASE_REF="$REMOTE/master"
  elif git show-ref --verify --quiet "refs/remotes/$REMOTE/main"; then
    BASE_REF="$REMOTE/main"
  else
    echo "Error: Could not determine base ref. Provide one explicitly, e.g. '$0 origin/master'."
    exit 1
  fi
fi

if ! git show-ref --verify --quiet "refs/remotes/${BASE_REF#refs/remotes/}" && ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null; then
  echo "Error: Base ref '$BASE_REF' does not exist locally after fetch."
  exit 1
fi

mapfile -t CANDIDATE_BRANCHES < <(
  git for-each-ref --format='%(refname:short)' "refs/remotes/$REMOTE" \
    | while IFS= read -r branch; do
        case "$branch" in
          $PREFIX_GLOB)
            if [[ "$branch" != "$REMOTE/$TARGET_BRANCH" ]]; then
              printf '%s\n' "$branch"
            fi
            ;;
        esac
      done \
    | sort
)

if [[ ${#CANDIDATE_BRANCHES[@]} -eq 0 ]]; then
  echo "No remote branches found matching glob '$PREFIX_GLOB'. Nothing to merge."
  exit 0
fi

echo "Found ${#CANDIDATE_BRANCHES[@]} branch(es):"
printf '  - %s\n' "${CANDIDATE_BRANCHES[@]}"

if [[ "$DRY_RUN" == true ]]; then
  echo ""
  echo "Dry run enabled. No branches were created or merged."
  exit 0
fi

echo "Preparing target branch '$TARGET_BRANCH' from '$BASE_REF'..."
if git show-ref --verify --quiet "refs/heads/$TARGET_BRANCH"; then
  if [[ "$RESET_TARGET" == true ]]; then
    git branch -D "$TARGET_BRANCH"
  else
    echo "Error: Local branch '$TARGET_BRANCH' already exists."
    echo "Re-run with --reset to recreate it from '$BASE_REF'."
    exit 1
  fi
fi

git checkout -b "$TARGET_BRANCH" "$BASE_REF"

for branch in "${CANDIDATE_BRANCHES[@]}"; do
  echo "Merging '$branch' into '$TARGET_BRANCH'..."
  if ! git merge --no-ff --no-edit "$branch"; then
    echo ""
    echo "Merge conflict encountered while merging '$branch'."
    echo "Resolve conflicts, then run 'git merge --continue' or abort with 'git merge --abort'."
    exit 1
  fi
done

echo ""
echo "Success: merged all deployment-candidate branches into '$TARGET_BRANCH'."
