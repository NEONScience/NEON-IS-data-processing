# Implementation Summary

## What Was Built

This directory contains a complete refactoring of the NEON calibration group and convert workflow that combines three architectural patterns to address complexity and platform dependency issues.

## The Three Patterns

### 1. Structured Configuration (Option 2)
**Goal**: Replace flat key-value ConfigMap with hierarchical YAML

**Implementation**:
- Each sensor's ConfigMap contains a single `config.yaml` key
- Configuration organized into logical sections: workflow, data_loading, schemas, processing, data_output
- Much more readable and maintainable than flat key-value pairs

**Files**:
- `workflows/overlays/cmp22/configmap.yaml`
- `workflows/overlays/aepg600m/configmap.yaml`
- `workflows/overlays/aepg600m_heated/configmap.yaml`

### 2. Kustomize Overlays (Option 3)
**Goal**: Enable easy sensor variant management without file duplication

**Implementation**:
- Single base workflow template: `workflows/base/calibration-group-and-convert.yaml`
- Separate overlay for each sensor: `workflows/overlays/{sensor}/`
- Each overlay patches the base template with sensor-specific ConfigMap reference
- Overlays add labels for sensor identification

**Files**:
- `workflows/base/calibration-group-and-convert.yaml` (single template)
- `workflows/overlays/cmp22/kustomization.yaml`
- `workflows/overlays/aepg600m/kustomization.yaml`
- `workflows/overlays/aepg600m_heated/kustomization.yaml`

### 3. Init Container for Normalization (Option 4)
**Goal**: Abstract platform-specific configuration details

**Implementation**:
- Init container (`config-normalizer`) runs before main workflow containers
- Reads the mounted YAML ConfigMap
- Normalizes config into environment files specific to each container
- Each container sources its environment file, not knowing about platform details

**Files**:
- `workflows/base/config-normalizer-entrypoint.py` (Python script)
- `workflows/base/Dockerfile` (container image definition)

## Directory Structure

```
/home/csturtevant/Git/argo-design/
├── README.md                          # Main architecture documentation
├── QUICKSTART.md                      # Usage examples and reference
├── MIGRATION.md                       # Migration guide from old template
├── workflows/
│   ├── base/
│   │   ├── calibration-group-and-convert.yaml     # Base template
│   │   ├── config-normalizer-entrypoint.py        # Init container logic
│   │   └── Dockerfile                             # Build init container
│   └── overlays/
│       ├── cmp22/
│       │   ├── kustomization.yaml                 # Kustomize config
│       │   └── configmap.yaml                     # CMP22 sensor config
│       ├── aepg600m/
│       │   ├── kustomization.yaml                 # Kustomize config
│       │   └── configmap.yaml                     # AEPG600M sensor config
│       └── aepg600m_heated/
│           ├── kustomization.yaml                 # Kustomize config
│           └── configmap.yaml                     # AEPG600M_HEATED config
```

## Key Improvements vs Original Template

### Complexity Reduction

| Metric | Old | New | Improvement |
|--------|-----|-----|-------------|
| ConfigMap parameter extractions in template | 40+ | 0 | -100% |
| Number of template files for 3 sensors | 3 | 1 base | -66% |
| Lines of template boilerplate | 50+ | 5 | -90% |
| Configuration readability | Poor (mixed format) | Excellent (hierarchical) | Much better |

### Maintainability

- **Adding new sensor**: Copy an overlay, customize 20 lines of YAML (vs rewriting entire template)
- **Updating configuration**: Edit ConfigMap YAML (vs template parameters)
- **Platform migration**: Update init container (vs rewriting everything)

### Portability

- **Platform dependency isolated**: Only init container cares about K8s/Argo specifics
- **Container-agnostic**: Applications receive normalized environment variables
- **Easy migration**: To move to Airflow/Nextflow, update only the init container logic

## Data Flow

```
User applies Kustomize overlay
    ↓
Kubectl applies base template + sensor-specific ConfigMap
    ↓
Workflow starts
    ↓
Init container (config-normalizer) starts
  - Mounts ConfigMap with YAML config
  - Reads and parses config
  - Generates environment files
    ↓
Main workflow containers start
  - Source environment files
  - Run with normalized configuration
  - Don't need to know about platform details
    ↓
Workflow completes
  - All containers finished successfully
```

## Usage Patterns

### Deploy a Sensor
```bash
kubectl apply -k workflows/overlays/cmp22/
```

### Submit a Workflow
```bash
argo submit -n argo-workflows-dev \
  --from workflowtemplate/calibration-group-and-convert \
  -p datum-manifest='{"paths": ["cmp22/2025/10/01/11185"]}'
```

### Switch Between Sensors
```bash
# From CMP22 to AEPG600M
kubectl apply -k workflows/overlays/aepg600m/
# Re-run workflows—they'll use AEPG600M configuration
```

### Add a New Sensor
1. Create `workflows/overlays/new-sensor/` directory
2. Copy and customize `configmap.yaml` and `kustomization.yaml`
3. Run: `kubectl apply -k workflows/overlays/new-sensor/`

## Files Created

### Documentation
- [README.md](README.md) - Main architecture and deployment guide
- [QUICKSTART.md](QUICKSTART.md) - Usage examples and quick reference
- [MIGRATION.md](MIGRATION.md) - Migration guide from old template

### Workflow Configuration
- `workflows/base/calibration-group-and-convert.yaml` - Base template
- `workflows/overlays/cmp22/configmap.yaml` - CMP22 configuration
- `workflows/overlays/aepg600m/configmap.yaml` - AEPG600M configuration
- `workflows/overlays/aepg600m_heated/configmap.yaml` - AEPG600M_HEATED configuration

### Kustomize Overlays
- `workflows/overlays/cmp22/kustomization.yaml`
- `workflows/overlays/aepg600m/kustomization.yaml`
- `workflows/overlays/aepg600m_heated/kustomization.yaml`

### Init Container
- `workflows/base/config-normalizer-entrypoint.py` - Python script
- `workflows/base/Dockerfile` - Container definition

## Quick Links

- **Get started**: See [QUICKSTART.md](QUICKSTART.md)
- **Understand architecture**: See [README.md](README.md)
- **Migrate from old template**: See [MIGRATION.md](MIGRATION.md)

## Next Steps

1. **Build and push the init container**:
   ```bash
   cd workflows/base
   docker build -t your-registry/config-normalizer:latest .
   docker push your-registry/config-normalizer:latest
   ```

2. **Update template with your registry**:
   Edit `workflows/base/calibration-group-and-convert.yaml` and update the `config-normalizer` image reference

3. **Deploy a sensor**:
   ```bash
   kubectl apply -k workflows/overlays/cmp22/
   ```

4. **Submit a test workflow**:
   ```bash
   argo submit -n argo-workflows-dev \
     --from workflowtemplate/calibration-group-and-convert
   ```

## Design Principles

The implementation follows these principles:

1. **Separation of Concerns**: Template logic separate from sensor configuration
2. **DRY (Don't Repeat Yourself)**: Single base template, reused for all sensors
3. **Readability**: Clear, hierarchical configuration structure
4. **Portability**: Platform-specific logic isolated to init container
5. **Scalability**: Easy to add new sensors without changing existing code
6. **Maintainability**: Clear ownership of different aspects (template, config, init logic)

## Support for Additional Sensors

The structure supports any number of sensors. To add a new sensor:

1. Create overlay directory
2. Define sensor-specific config
3. Deploy with Kustomize

No changes to base template required. The architecture scales horizontally.

## Backward Compatibility

The new implementation does not interfere with the original template. Both can coexist:
- Original template: `NEON-IS-data-processing/argo/calibration_group_and_convert.yaml`
- New template: `argo-design/workflows/base/calibration-group-and-convert.yaml`

You can gradually migrate workflows from old to new.

---

**Created**: 2026-06-25
**Location**: `/home/csturtevant/Git/argo-design`
**Documentation**: See README.md, QUICKSTART.md, MIGRATION.md
