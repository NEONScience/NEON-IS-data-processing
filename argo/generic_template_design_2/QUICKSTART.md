# Usage Examples and Quick Reference

## Quick Start

### 1. Deploy a Sensor Workflow

Deploy the CMP22 workflow template:
```bash
kubectl apply -k workflows/overlays/cmp22/
```

### 2. Verify Deployment

```bash
# Check WorkflowTemplate
kubectl get workflowtemplate -n argo-workflows-dev calibration-group-and-convert

# Check ConfigMap
kubectl get cm -n argo-workflows-dev cmp22-calibration-group-convert-config
```

### 3. Submit a Workflow Instance

```bash
# Submit with default datum manifest from CMP22 config
argo submit -n argo-workflows-dev \
  --from workflowtemplate/calibration-group-and-convert

# Or override with custom manifest
argo submit -n argo-workflows-dev \
  --from workflowtemplate/calibration-group-and-convert \
  -p datum-manifest='{"paths": ["cmp22/2025/10/15/11185"]}'
```

### 4. Monitor Workflow

```bash
# Watch workflow execution
argo watch -n argo-workflows-dev <workflow-name>

# Get workflow details
argo get -n argo-workflows-dev <workflow-name>

# View logs from a specific step
argo logs -n argo-workflows-dev <workflow-name> -c load-data
```

## Switching Between Sensors

Deploy different sensor versions without recreating anything—just apply the appropriate overlay:

```bash
# Switch to AEPG600M
kubectl apply -k workflows/overlays/aepg600m/

# All workflows now use AEPG600M configuration

# Switch to AEPG600M_HEATED
kubectl apply -k workflows/overlays/aepg600m_heated/
```

The WorkflowTemplate name stays the same (`calibration-group-and-convert`), but the ConfigMap reference changes automatically.

## Customizing Configuration

### Modifying a Sensor's Configuration

Edit the sensor's ConfigMap:

```bash
# For CMP22
nano workflows/overlays/cmp22/configmap.yaml

# Update the values in config.yaml section

# Apply changes
kubectl apply -k workflows/overlays/cmp22/
```

### Adding a New Sensor

1. Create new overlay:
```bash
mkdir workflows/overlays/my-new-sensor
```

2. Create `kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: argo-workflows-dev

resources:
  - ../../base/calibration-group-and-convert.yaml
  - configmap.yaml

patchesStrategicMerge:
  - |-
    apiVersion: argoproj.io/v1alpha1
    kind: WorkflowTemplate
    metadata:
      name: calibration-group-and-convert
    spec:
      arguments:
        parameters:
          - name: config-map-name
            value: my-new-sensor-calibration-group-convert-config

commonLabels:
  sensor: my-new-sensor
  workflow: calibration-group-and-convert
```

3. Create `configmap.yaml` with sensor-specific values

4. Deploy:
```bash
kubectl apply -k workflows/overlays/my-new-sensor/
```

## Configuration Reference

### Key Configuration Sections

#### Workflow Settings
```yaml
workflow:
  log_level: INFO                    # DEBUG, INFO, WARNING, ERROR
  error_path: /pfs/errored_datums   # Where error outputs go
```

#### Data Loading Configuration
```yaml
data_loading:
  l0_bucket_name: neon-dev-l0-ingest
  calibration_bucket_name: neon-dev-argo-workflow-test
  calibration_bucket_prefix: cmp22_calibration_assignment
```

#### Processing Configuration
```yaml
processing:
  out_path_calibration_conversion: /pfs/cmp22_calibration_group_and_convert
  kafka_combine_r_args: |
    DirIn=$OUT_PATH_JOINER \
    DirOut=$OUT_PATH_KAFKA_COMB ...
```

#### Data Output
```yaml
data_output:
  output_bucket_name: neon-dev-argo-workflow-test
  output_bucket_prefix: cmp22_calibration_group_and_convert
```

## Workflow Execution Flow

```
┌─────────────────────────────┐
│  Workflow Start             │
│  Config Map Name Injected   │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  Init Container Runs        │
│  config-normalizer          │
│  ├─ Mounts ConfigMap        │
│  ├─ Reads YAML              │
│  └─ Generates .env files    │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  Containers Start (Sequential)      │
│                                     │
│  1. load-data                       │
│     ├─ Sources /etc/config-out/     │
│     │   load-data.env               │
│     └─ Pulls L0 and calibrations    │
│                                     │
│  2. calibration-group-and-convert   │
│     ├─ Sources /etc/config-out/     │
│     │   calibration-group-and-      │
│     │   convert.env                 │
│     ├─ Joins data & calibrations    │
│     └─ Runs R processing modules    │
│                                     │
│  3. main                            │
│     ├─ Sources /etc/config-out/     │
│     │   data-upload.env             │
│     └─ Uploads results to GCS       │
│                                     │
└─────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  Workflow Complete          │
│  Results in Output Bucket   │
└─────────────────────────────┘
```

## Environment Variables in Containers

Each container receives environment variables from the init container:

### load-data Container
```bash
BUCKET_NAME                    # L0 data bucket
BUCKET_VERSION_PATH            # Version path
CAL_BUCKET_NAME               # Calibration bucket
CAL_BUCKET_PREFIX             # Cal prefix
OUT_PATH                      # L0 output
OUT_PATH_CAL                  # Calibration output
LOG_LEVEL                     # Log level
```

### calibration-group-and-convert Container
```bash
CONFIG                        # Filter-joiner YAML config
OUT_PATH_JOINER              # Joined data output
OUT_PATH_KAFKA_COMB          # Kafka combined output
OUT_PATH_CAL_CONV            # Calibrated conversion output
ERR_PATH                     # Error path
LOG_LEVEL                    # Log level
KFKA_COMB_R_ARGS             # R script arguments
CAL_CONV_R_ARGS              # R script arguments
```

### main Container
```bash
OUT_PATH                     # Output path
OUTPUT_BUCKET_NAME           # Destination bucket
OUTPUT_BUCKET_PREFIX         # Destination prefix
```

## Comparing Old vs New

| Aspect | Old | New |
|--------|-----|-----|
| Template Complexity | ~30+ parameter extractions | ~5 environment mounts |
| Configuration Format | Flat key-value | Hierarchical YAML |
| Sensor Variants | Separate template files | Single base + overlays |
| Adding New Sensors | Duplicate template file | Create new overlay |
| Platform Dependency | Tightly coupled to Argo paths | Abstracted to init container |
| Configuration Readability | Mixed inline YAML strings | Clean, organized structure |
| Maintenance Burden | High (multiple templates) | Low (single base) |

## Debugging Tips

### Inspect Generated Environment Files

```bash
# Get into a running container
kubectl exec -it <pod-name> -c load-data -- /bin/bash

# Check generated env file
cat /etc/config-out/load-data.env

# Source and verify variables
source /etc/config-out/load-data.env
echo $OUT_PATH_CAL
```

### Validate ConfigMap YAML

```bash
# Check syntax
python3 -c "import yaml; yaml.safe_load(open('workflows/overlays/cmp22/configmap.yaml'))"

# View as ConfigMap would see it
kubectl get cm -n argo-workflows-dev cmp22-calibration-group-convert-config -o yaml
```

### Test Init Container Locally

```bash
cd workflows/base

# Build image
docker build -t config-normalizer:test .

# Run with test config
docker run -v $(pwd)/../overlays/cmp22:/etc/config-in \
  -v /tmp/out:/etc/config-out \
  config-normalizer:test

# Check output
ls -la /tmp/out/
cat /tmp/out/load-data.env
```

## Performance Considerations

- **Init Container Overhead**: ~1-2 seconds for YAML parsing and file generation
- **ConfigMap Size**: Typical configs are <50KB; no performance impact
- **Volumes**: All volumes are emptyDir; no storage I/O bottlenecks

## Troubleshooting Common Issues

### ConfigMap Reference Not Found

```
Error: ConfigMapKeyRef ... not found
```

**Solution**: Ensure ConfigMap name matches the patched parameter:
```yaml
# In kustomization.yaml
value: cmp22-calibration-group-convert-config  # Must exist
```

### Init Container Fails with Import Error

```
ModuleNotFoundError: No module named 'yaml'
```

**Solution**: Rebuild the config-normalizer image after Dockerfile changes:
```bash
docker build --no-cache -t config-normalizer:latest workflows/base/
```

### Environment Variable Not Set in Container

```
/entrypoint.sh: line 5: $OUT_PATH: unbound variable
```

**Solution**: Verify init container completed successfully:
```bash
kubectl logs <pod-name> -c config-normalizer
```
