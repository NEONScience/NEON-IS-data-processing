#!/usr/bin/env bash
# bump_image_tags.sh
#
# Creates incremented semver git tags for all images whose Dockerfiles reference
# a given base image.
#
# Usage:
#   bump_image_tags.sh <base_image> <major|minor|patch> [--dry-run]
#
# Arguments:
#   base_image        - Substring to match in Dockerfile FROM lines (e.g. "neon-is-pack-cal-r"
#                       or a full image URI). Case-sensitive.
#   major|minor|patch - Which semver component to increment.
#   --dry-run         - Print what would be done without creating or pushing any tags.
#
# Tag format created: <folder>/vM.m.p
#   where <folder> is the parent directory of the Dockerfile (last component of MODULE_PATH
#   in the matching build_push_update_* GitHub Actions workflow).
#
# The baseline version for each image is the highest semver found across all YAML files
# in the pipe/ directory. If no version is found, v0.0.0 is used as the baseline.

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 2 ]]; then
  echo "Usage: $(basename "$0") <base_image> <major|minor|patch> [--dry-run]" >&2
  exit 1
fi

BASE_IMAGE="$1"
SEMVER_COMPONENT="$2"
DRY_RUN=false

if [[ "${3:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

case "$SEMVER_COMPONENT" in
  major|minor|patch) ;;
  *)
    echo "Error: semver component must be 'major', 'minor', or 'patch'" >&2
    exit 1
    ;;
esac

# ---------------------------------------------------------------------------
# Locate repo root
# ---------------------------------------------------------------------------
REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
WORKFLOWS_DIR="$REPO_ROOT/.github/workflows"
PIPE_DIR="$REPO_ROOT/pipe"
PACK_DIR="$REPO_ROOT/pack"
FLOW_DIR="$REPO_ROOT/flow"
MODULES_DIR="$REPO_ROOT/modules"
MODULES_COMBINED_DIR="$REPO_ROOT/modules_combined"

# ---------------------------------------------------------------------------
# Helper: increment a semver string
# Resets lower components to 0 when a higher component is bumped.
# ---------------------------------------------------------------------------
bump_version() {
  local version="$1"   # e.g. "1.2.3"
  local component="$2" # major | minor | patch
  local major minor patch

  IFS='.' read -r major minor patch <<< "$version"
  major="${major:-0}"
  minor="${minor:-0}"
  patch="${patch:-0}"

  case "$component" in
    major)
      major=$((major + 1))
      minor=0
      patch=0
      ;;
    minor)
      minor=$((minor + 1))
      patch=0
      ;;
    patch)
      patch=$((patch + 1))
      ;;
  esac

  echo "${major}.${minor}.${patch}"
}

# ---------------------------------------------------------------------------
# Helper: find the highest semver tag for a given image name across:
#   - pipe/ and pack/ YAML files (image: .../name:vX.Y.Z)
#   - Dockerfiles in pipe/, pack/, flow/, modules/, and modules_combined/ (FROM .../name:vX.Y.Z)
# Returns an empty string if no version is found.
# ---------------------------------------------------------------------------
highest_pipe_version() {
  local image_name="$1"
  local highest

  highest=$(
    {
      # YAML files: match lines like  image: .../neon-is-foo:v1.2.3
      grep -rh "image:.*${image_name}:v[0-9]" "$PIPE_DIR" "$PACK_DIR" 2>/dev/null \
        | grep -oP "(?<=${image_name}:v)\d+\.\d+\.\d+"

      # Dockerfiles: match FROM lines like  FROM .../neon-is-foo:v1.2.3
      grep -rh --include="Dockerfile" "FROM.*${image_name}:v[0-9]" \
        "$PIPE_DIR" "$PACK_DIR" "$FLOW_DIR" "$MODULES_DIR" "$MODULES_COMBINED_DIR" 2>/dev/null \
        | grep -oP "(?<=${image_name}:v)\d+\.\d+\.\d+"
    } \
      | sort -t. -k1,1n -k2,2n -k3,3n \
      | tail -1
  )

  echo "${highest:-}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "Base image  : $BASE_IMAGE"
echo "Increment   : $SEMVER_COMPONENT"
echo "Dry run     : $DRY_RUN"
echo ""

# Find every Dockerfile whose FROM line references the base image
mapfile -t DOCKERFILES < <(
  grep -rl "FROM.*${BASE_IMAGE}" "$REPO_ROOT" \
    --include="Dockerfile" 2>/dev/null \
  | sort
)

if [[ ${#DOCKERFILES[@]} -eq 0 ]]; then
  echo "No Dockerfiles found referencing base image: $BASE_IMAGE"
  exit 0
fi

echo "Found ${#DOCKERFILES[@]} Dockerfile(s) referencing '${BASE_IMAGE}':"
printf '  %s\n' "${DOCKERFILES[@]}"
echo ""

# Build a lookup: MODULE_PATH (normalised, no leading ./) -> workflow file
# We only consider build_push_update_* workflows.
declare -A WORKFLOW_BY_MODULE_PATH

while IFS= read -r WORKFLOW_FILE; do
  # Extract non-commented MODULE_PATH line
  MODULE_PATH=$(
    grep "MODULE_PATH:" "$WORKFLOW_FILE" \
      | grep -v "^\s*#" \
      | sed 's/.*MODULE_PATH:[[:space:]]*//' \
      | tr -d ' \r\n' \
      | sed 's|^\./||'   # strip leading ./
  )
  [[ -z "$MODULE_PATH" ]] && continue
  WORKFLOW_BY_MODULE_PATH["$MODULE_PATH"]="$WORKFLOW_FILE"
done < <(find "$WORKFLOWS_DIR" -name "build_push_update_*.yml" -type f | sort)

NEW_TAGS=()
SKIPPED=()

for DOCKERFILE in "${DOCKERFILES[@]}"; do
  DOCKERFILE_DIR="$(dirname "$DOCKERFILE")"
  REL_DIR="$(realpath --relative-to="$REPO_ROOT" "$DOCKERFILE_DIR")"

  # Look up the workflow for this Dockerfile's directory
  MATCHING_WORKFLOW="${WORKFLOW_BY_MODULE_PATH[$REL_DIR]:-}"

  if [[ -z "$MATCHING_WORKFLOW" ]]; then
    echo "  [SKIP] $REL_DIR — no matching build_push_update_* workflow found"
    echo ""
    SKIPPED+=("$REL_DIR (no matching workflow)")
    continue
  fi

  # Extract IMAGE_NAME (skip commented lines)
  IMAGE_NAME=$(
    grep "IMAGE_NAME:" "$MATCHING_WORKFLOW" \
      | grep -v "^\s*#" \
      | sed 's/.*IMAGE_NAME:[[:space:]]*//' \
      | tr -d ' \r\n' \
      | head -1
  )

  # The tag folder name is the last component of the module path
  FOLDER_NAME="$(basename "$REL_DIR")"

  # Find highest existing semver for this image across pipe/, pack/, flow/, modules/, modules_combined/
  HIGHEST_VERSION="$(highest_pipe_version "$IMAGE_NAME")"

  if [[ -z "$HIGHEST_VERSION" ]]; then
    echo "  [SKIP] $REL_DIR — no existing semver found for image '$IMAGE_NAME'; cannot increment"
    echo ""
    SKIPPED+=("$REL_DIR (no existing semver for '$IMAGE_NAME')")
    continue
  fi

  # Increment
  NEW_VERSION="$(bump_version "$HIGHEST_VERSION" "$SEMVER_COMPONENT")"
  NEW_TAG="${FOLDER_NAME}/v${NEW_VERSION}"

  echo "  Dockerfile : $DOCKERFILE"
  echo "  Workflow   : $(basename "$MATCHING_WORKFLOW")"
  echo "  Image name : $IMAGE_NAME"
  echo "  Tag folder : $FOLDER_NAME"
  echo "  Highest ver: v${HIGHEST_VERSION}"
  echo "  New tag    : $NEW_TAG"
  echo ""

  NEW_TAGS+=("$NEW_TAG")
done

if [[ ${#NEW_TAGS[@]} -eq 0 ]]; then
  echo "No new tags to create."
  exit 0
fi

echo "New tags to be created:"
printf '  %s\n' "${NEW_TAGS[@]}"
echo ""

if [[ ${#SKIPPED[@]} -gt 0 ]]; then
  echo "Skipped (no tag created):"
  printf '  %s\n' "${SKIPPED[@]}"
  echo ""
fi

if $DRY_RUN; then
  echo "Dry run — no tags created or pushed."
  exit 0
fi

# Create tags locally
echo "Creating tags..."
for TAG in "${NEW_TAGS[@]}"; do
  if git -C "$REPO_ROOT" tag --list "$TAG" | grep -q .; then
    echo "  WARNING: tag '$TAG' already exists — skipping"
  else
    git -C "$REPO_ROOT" tag "$TAG"
    echo "  Created: $TAG"
  fi
done

# Push tags individually so each push emits a distinct tag push event in GitHub Actions.
# GitHub may suppress workflow events when too many tags are pushed at once.
echo ""
echo "Pushing tags to origin (one-by-one)..."
PUSHED=()
FAILED=()

for TAG in "${NEW_TAGS[@]}"; do
  # Only attempt to push tags that exist locally (newly created this run or pre-existing).
  if git -C "$REPO_ROOT" tag --list "$TAG" | grep -q .; then
    if git -C "$REPO_ROOT" push origin "$TAG"; then
      echo "  Pushed: $TAG"
      PUSHED+=("$TAG")
    else
      echo "  ERROR: failed to push '$TAG'"
      FAILED+=("$TAG")
    fi
  else
    echo "  WARNING: local tag '$TAG' not found, skipping push"
    FAILED+=("$TAG")
  fi
done

echo ""
echo "Push summary:"
echo "  Succeeded: ${#PUSHED[@]}"
echo "  Failed   : ${#FAILED[@]}"

if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "Failed tags:"
  printf '  %s\n' "${FAILED[@]}"
  exit 1
fi

echo "Done."
