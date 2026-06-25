# Refactored Calibration Group and Convert Workflow

This directory contains a refactored implementation of the NEON calibration group and convert workflow that addresses the criticisms of complexity and platform dependency. The refactoring combines three architectural patterns:

1. **Option 2: Structured Configuration Format**
2. **Option 3: Kustomize Overlays for Sensor Variants**
3. **Option 4: Init Container for Configuration Normalization**

## Architecture Overview

### Problem Statement

The original workflow template had two main issues:

- **Configuration Boilerplate**: Every ConfigMap key required explicit parameter extraction, duplicating knowledge and making the template complicated.
- **Platform Dependency**: Hardcoded assumptions about Kubernetes/Argo paths and resource structures made migration to other orchestrators difficult.

### Solution Design

```
┌─────────────────────────────────────────────┐
│  Sensor Overlay (Kustomize)                 │
│  ├─ kustomization.yaml                      │
│  └─ configmap.yaml (sensor-specific config) │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  Structured YAML ConfigMap                  │
│  └─ config.yaml (clean, readable config)    │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  Init Container (Config Normalizer)         │
│  └─ Python script translates YAML to        │
│     application-specific env files          │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  Workflow Containers                        │
│  ├─ load-data                               │
│  ├─ calibration-group-and-convert           │
│  └─ main (data upload)                      │
└─────────────────────────────────────────────┘
```

## Directory Structure

```
workflows/
├── base/
│   ├── calibration-group-and-convert.yaml    # Base workflow template (platform-agnostic)
│   ├── config-normalizer-entrypoint.py       # Init container script
│   └── Dockerfile                            # Container image for normalizer
│
└── overlays/
    ├── cmp22/
    │   ├── kustomization.yaml                # Kustomize configuration
    │   └── configmap.yaml                    # CMP22-specific structured config
    │
    ├── aepg600m/
    │   ├── kustomization.yaml                # Kustomize configuration
    │   └── configmap.yaml                    # AEPG600M-specific structured config
    │
    └── aepg600m_heated/
        ├── kustomization.yaml                # Kustomize configuration
        └── configmap.yaml                    # AEPG600M_HEATED-specific structured config
```

## Key Improvements

### 1. Reduced Template Complexity

**Before**: ConfigMap keys extracted individually with `valueFrom.configMapKeyRef` for each parameter
```yaml
inputs:
  parameters:
  - name: schema-repo-eng-url
    valueFrom:
      configMapKeyRef:
        name: "{{workflow.parameters.config}}"
        key: schema-repo-eng-url
  # ... 30+ more parameters ...
```

**After**: Single ConfigMap mount + init container handles normalization
```yaml
volumes:
  - name: config-vol
    configMap:
      name: "{{workflow.parameters.config-map-name}}"

initContainers:
  - name: config-normalizer
    # Reads /etc/config-in/config.yaml
    # Generates environment files for each container
```

### 2. Structured, Readable Configuration

**Before**: Flat key-value pairs mixed with inline YAML strings
```yaml
data:
  log-level: INFO
  filter-joiner-config: |
    ---
    input_paths:
    - path:
        name: DATA_PATH_ARCHIVE
        # ... 10+ lines ...
```

**After**: Clean hierarchical YAML with logical grouping
```yaml
data:
  config.yaml: |
    workflow:
      log_level: INFO
    data_loading:
      l0_bucket_name: ...
    processing:
      filter_joiner_config: |
        ...
```

### 3. Easy Sensor Variant Management

**Before**: Create a new workflow template file for each sensor variant
**After**: Single base workflow + lightweight Kustomize overlays
```bash
# Deploy CMP22
kubectl apply -k workflows/overlays/cmp22

# Deploy AEPG600M
kubectl apply -k workflows/overlays/aepg600m

# Deploy AEPG600M_HEATED
kubectl apply -k workflows/overlays/aepg600m_heated
```

### 4. Platform Abstraction

All platform-specific logic is isolated to:
- Init container (config normalization)
- Base workflow template (Argo-specific syntax)

Application containers receive normalized environment variables and don't know about:
- Kubernetes volume structure
- Argo-specific paths (`/pfs/`, `/inputs/`)
- ConfigMap internals

This makes it trivial to migrate to Airflow, Nextflow, or other orchestrators—only the init container logic needs to change.

## Deployment Guide

### Prerequisites

- `kustomize` CLI installed
- Kubernetes cluster with Argo Workflows
- Python 3.11+ (for config normalizer)

### Building the Config Normalizer Image

```bash
cd workflows/base

# Build the Docker image
docker build -t <your-registry>/config-normalizer:latest .

# Push to your registry
docker push <your-registry>/config-normalizer:latest
```

Update the `calibration-group-and-convert.yaml` template to use your registry:
```yaml
initContainers:
  - name: config-normalizer
    image: <your-registry>/config-normalizer:latest
```

### Deploying a Sensor Workflow

```bash
# Deploy CMP22 workflow
kubectl apply -k workflows/overlays/cmp22/

# Verify ConfigMap and WorkflowTemplate were created
kubectl get configmap -n argo-workflows-dev | grep calibration
kubectl get workflowtemplate -n argo-workflows-dev | grep calibration
```

### Submitting a Workflow

```bash
# Submit a workflow using CMP22 configuration
argo submit -n argo-workflows-dev \
  --from workflowtemplate/calibration-group-and-convert \
  -p datum-manifest='{"paths": ["cmp22/2025/10/01/11185"]}'
```

## Configuration Schema

Each sensor ConfigMap follows this schema (in YAML):

```yaml
workflow:
  log_level: INFO                           # Logging level
  error_path: /pfs/errored_datums          # Error output path

data_loading:
  l0_bucket_name: ...                      # GCS bucket for raw data
  l0_bucket_version_path: ...              # Versioning path
  calibration_bucket_name: ...             # GCS bucket for calibrations
  calibration_bucket_prefix: ...           # Prefix for cal data
  source_type_index: 0                     # Manifest path indices
  year_index: 1
  month_index: 2
  day_index: 3
  source_id_index: 4
  out_path_l0: /inputs/DATA_PATH_ARCHIVE
  out_path_calibration: /inputs/CALIBRATION_PATH

schemas:
  engineering:
    url: git@github.com:...                # Engineering schema repo
    revision: develop
  scientific:
    url: https://github.com/...            # Scientific schema repo
    revision: master

processing:
  filter_joiner_config: |                  # YAML config for filter-joiner
    ---
    input_paths:
    ...
  relative_path_index: 3
  link_type: SYMLINK
  parallelism_internal: 3
  out_path_joiner: /pfs/data_cal_joined
  out_path_kafka_comb: /pfs/kafka_combined
  out_path_calibration_conversion: /pfs/...
  kafka_combine_r_args: |                  # R command-line arguments
    DirIn=$OUT_PATH_JOINER ...
  calibration_conversion_r_args: |         # R command-line arguments
    DirIn=$OUT_PATH_KAFKA_COMB ...

data_output:
  out_path: /pfs/...
  output_bucket_name: ...                  # Output GCS bucket
  output_bucket_prefix: ...                # Output path prefix
```

## Adding a New Sensor

To add a new sensor to this workflow:

1. **Create a new overlay directory**:
   ```bash
   mkdir workflows/overlays/your-sensor
   ```

2. **Copy and customize the ConfigMap**:
   ```bash
   cp workflows/overlays/cmp22/configmap.yaml workflows/overlays/your-sensor/
   # Edit the config.yaml section with your sensor-specific values
   ```

3. **Create the Kustomization file**:
   ```bash
   cp workflows/overlays/cmp22/kustomization.yaml workflows/overlays/your-sensor/
   # Update the ConfigMap name and labels to reference your sensor
   ```

4. **Deploy**:
   ```bash
   kubectl apply -k workflows/overlays/your-sensor/
   ```

## Migration to Other Orchestrators

To migrate this workflow to a different orchestrator (e.g., Airflow, Nextflow):

1. **Adapt the base workflow template** to the target orchestrator's syntax
2. **Keep the ConfigMap structure identical** (already platform-agnostic)
3. **Adapt the init container** to your orchestrator's initialization system
4. **Reuse all Kustomize overlays** without modification

Example: Migrating to Airflow would require:
- Converting `calibration-group-and-convert.yaml` to a DAG definition
- Keeping `config-normalizer-entrypoint.py` and `configmap.yaml` files unchanged
- Using Airflow's task initialization to run the normalizer before main tasks

## Troubleshooting

### ConfigMap Not Being Applied

```bash
# Check if ConfigMap exists
kubectl get cm -n argo-workflows-dev | grep calibration

# Verify label
kubectl get cm -n argo-workflows-dev -L workflows.argoproj.io/configmap-type
```

### Init Container Failing

```bash
# Check init container logs
kubectl logs -n argo-workflows-dev <pod-name> -c config-normalizer

# Verify config file syntax
python3 -c "import yaml; yaml.safe_load(open('workflows/overlays/cmp22/configmap.yaml'))"
```

### Container Not Finding Environment Files

```bash
# Exec into container and check
kubectl exec -it -n argo-workflows-dev <pod-name> -c load-data -- \
  ls -la /etc/config-out/
```

## Future Enhancements

- **Schema Validation**: Add JSON Schema validation for ConfigMaps
- **Multi-Stage Processing**: Extend to handle chained workflows
- **Versioning**: Support multiple configuration versions simultaneously
- **Environment Substitution**: Add templating for environment-specific values
- **Documentation Generation**: Auto-generate docs from configuration structure
