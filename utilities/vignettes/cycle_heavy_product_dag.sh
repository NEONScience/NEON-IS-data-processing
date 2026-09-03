#!/bin/bash
# Cycle the "heavy" (bulk data / reprocessing) pipelines of a source-type + product DAG
# while leaving the "base" (cheap, tick-driven metadata) pipelines running all the time.
#
# Base pipelines (kept loaded always): ingest_days, cron_daily_and_date_control, loaders,
# assignment modules, and other small tick-driven metadata pipelines (matched by BASE_PATTERN
# below - review with the "list" command before trusting on a new product).
#
# Heavy pipelines (torn down/stood back up on demand): data source loads, calibration/location
# group-and-convert/restructure, gap filling, stats/qm compute, srf consolidation, monthly cron
# and pub control, and pub pipelines.
#
# Usage:
#   ./cycle_heavy_product_dag.sh list     <source_type> <product> [product_suffix]
#   ./cycle_heavy_product_dag.sh base     <source_type> <product> [product_suffix]
#   ./cycle_heavy_product_dag.sh standup  <source_type> <product> [product_suffix]
#   ./cycle_heavy_product_dag.sh teardown <source_type> <product> [product_suffix]
#
# Example (prt -> tempSoil):
#   ./cycle_heavy_product_dag.sh list prt tempSoil _prel1
#   ./cycle_heavy_product_dag.sh base prt tempSoil _prel1      # one-time: stand up always-on pipelines
#   ./cycle_heavy_product_dag.sh teardown prt tempSoil _prel1  # tear down heavy pipelines
#   ./cycle_heavy_product_dag.sh standup prt tempSoil _prel1   # stand heavy pipelines back up
#
# IMPORTANT: pipe_list_<source_type>.txt and pipe_list_<product><product_suffix>.txt must have a
# trailing newline and reflect true creation order (dependency order), same requirement as the
# stand_up_product_dag_example.sh / delete_product_dag_example.sh vignettes this is based on.
#
# BEFORE running `base` (or any time you want to move the processing window forward): manually
# edit <source_type>_cron_daily_and_date_control.yaml's START_DATE/END_DATE to the desired range,
# since that pipeline is always-on and this script never touches its dates for you. Changing dates
# on an already-running cron_daily_and_date_control requires `pachctl update pipeline --reprocess`,
# which is a deliberate, manual step you should run yourself once the yaml is edited.

#Example usage:
# ./cycle_heavy_product_dag.sh teardown enviroscan concH2oSoilSalinity _prel1
# ./cycle_heavy_product_dag.sh teardown prt tempSoil _prel1

# manually edit dates:
#   pipe/enviroscan/enviroscan_cron_daily_and_date_control.yaml
#   (START_DATE / END_DATE)
# then apply it yourself, since cron_daily_and_date_control is a base
# pipeline (never deleted by teardown) - editing the yaml alone doesn't
# change anything running until you push the update:
# pachctl update pipeline --reprocess -f /home/tburlingame/github/NEON-IS-data-processing/pipe/enviroscan/enviroscan_cron_daily_and_date_control.yaml
# pachctl update pipeline --reprocess -f /home/tburlingame/github/NEON-IS-data-processing/pipe/prt/prt_cron_daily_and_date_control.yaml

# ./cycle_heavy_product_dag.sh standup prt tempSoil _prel1
# ./cycle_heavy_product_dag.sh standup enviroscan concH2oSoilSalinity _prel1

set -euo pipefail

git_path_pipelines='/home/tburlingame/github/NEON-IS-data-processing/pipe'
pipe_list_prefix='pipe_list_'

cmd=${1:?"Usage: $0 {list|base|standup|teardown} ..."}
source_type=${2:?"source_type required"}
product=${3:?"product required"}
pipe_list_suffix=${4:-_prel1}
spec_path_source_type="$git_path_pipelines/$source_type"
spec_path_product="$git_path_pipelines/$product"
source_type_list_file="$spec_path_source_type/${pipe_list_prefix}${source_type}.txt"
product_list_file="$spec_path_product/${pipe_list_prefix}${product}${pipe_list_suffix}.txt"

# Pipelines matching this pattern are treated as always-on base pipelines.
BASE_PATTERN='(ingest_days|cron_daily_and_date_control|_loader|_assignment|list_files|_asset\.yaml|_threshold\.yaml)'

get_pipelines() {
  # $1 = path to pipe_list file. Strips comments/blank lines, preserves order.
  grep -v '^\s*#' "$1" | grep -v '^\s*$'
}

# Populates global arrays `base` and `heavy` (in file order) from a pipe_list file.
split_lists() {
  local file=$1
  base=()
  heavy=()
  while IFS= read -r pipe; do
    if [[ $pipe =~ $BASE_PATTERN ]]; then
      base+=("$pipe")
    else
      heavy+=("$pipe")
    fi
  done < <(get_pipelines "$file")
}

create_pipelines() {
  # $1 = spec dir, remaining args = pipe file names, created in given order
  local dir=$1; shift
  for pipe in "$@"; do
    echo pachctl create pipeline -f "$dir/$pipe"
    pachctl create pipeline -f "$dir/$pipe"
  done
}

get_pipeline_name() {
  # Pipeline name inside the yaml doesn't always match the filename (e.g. *_prel1.yaml
  # files can map to a name without the _prel1 suffix), so read it from the file itself.
  grep -A1 '^pipeline:' "$1" | grep 'name:' | head -1 | sed 's/.*name:[[:space:]]*//'
}

delete_pipelines_reverse() {
  # $1 = spec dir, remaining args = pipe file names, deleted in reverse order
  local dir=$1; shift
  local pipes=("$@")
  for (( idx=${#pipes[@]}-1 ; idx>=0 ; idx-- )); do
    local pipe=${pipes[idx]}
    local name
    name=$(get_pipeline_name "$dir/$pipe")
    echo pachctl delete pipeline "$name"
    pachctl delete pipeline "$name" || echo "  (skip: $name not found)"
  done
}

case "$cmd" in
  list)
    for f in "$source_type_list_file" "$product_list_file"; do
      echo "--- $f ---"
      split_lists "$f"
      echo "  base (always-on):"
      printf '    %s\n' "${base[@]}"
      echo "  heavy (teardown/standup):"
      printf '    %s\n' "${heavy[@]}"
    done
    ;;

  base)
    echo "=== Standing up always-on base pipelines: $source_type ==="
    split_lists "$source_type_list_file"
    create_pipelines "$spec_path_source_type" "${base[@]}"

    echo "=== Standing up always-on base pipelines: $product ==="
    split_lists "$product_list_file"
    create_pipelines "$spec_path_product" "${base[@]}"
    ;;

  standup)
    echo "=== Standing up heavy pipelines: $source_type ==="
    split_lists "$source_type_list_file"
    create_pipelines "$spec_path_source_type" "${heavy[@]}"

    echo "=== Standing up heavy pipelines: $product ==="
    split_lists "$product_list_file"
    create_pipelines "$spec_path_product" "${heavy[@]}"
    ;;

  teardown)
    echo "=== Tearing down heavy pipelines: $product ==="
    split_lists "$product_list_file"
    delete_pipelines_reverse "$spec_path_product" "${heavy[@]}"

    echo "=== Tearing down heavy pipelines: $source_type ==="
    split_lists "$source_type_list_file"
    delete_pipelines_reverse "$spec_path_source_type" "${heavy[@]}"
    ;;

  *)
    echo "Usage: $0 {list|base|standup|teardown} <source_type> <product> [product_suffix]"
    exit 1
    ;;
esac
