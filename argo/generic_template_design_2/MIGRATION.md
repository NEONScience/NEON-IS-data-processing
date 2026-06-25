# Migration Guide: Old Template to New Architecture

This document explains how to migrate from the original monolithic workflow template to the new refactored architecture.

## What's Changed

### The Problem with the Original Template

The original `calibration_group_and_convert.yaml` had several issues:

1. **Parameter Extraction Boilerplate**: ~40 lines of ConfigMap key references
   ```yaml
   inputs:
     parameters:
     - name: schema-repo-eng-url
       valueFrom:
         configMapKeyRef:
           name: "{{workflow.parameters.config}}"
           key: schema-repo-eng-url
     # ... repeated 30+ times for each ConfigMap key
   ```

2. **Platform-Specific Assumptions**:
   - Hardcoded paths: `/pfs/`, `/inputs/`, `/tmp`
   - Kubernetes ConfigMap internals exposed
   - Tightly coupled to Argo Workflows API

3. **Difficult to Maintain**:
   - Adding new parameters required template changes
   - No separation of concerns between template logic and sensor configuration
   - Duplication across sensor variants

4. **Hard to Migrate**:
   - Moving to Airflow/Nextflow would require rewriting everything
   - Configuration structure tied to Kubernetes API

## How the New Architecture Solves This

### 1. Configuration Abstraction Layer

**Old**: ConfigMap keys scattered throughout template
```yaml
- name: OUT_PATH_CAL_CONV
  valueFrom:
    configMapKeyRef:
      name: "{{workflow.parameters.config}}"
      key: out-path-cal-conv
```

**New**: Single structured ConfigMap mounted to init container
```yaml
volumes:
  - name: config-vol
    configMap:
      name: "{{workflow.parameters.config-map-name}}"
```

The init container reads the ConfigMap once and generates all environment files.

### 2. Platform Abstraction Layer

**Old**: Template directly references platform-specific paths
```bash
export OUT_PATH=$OUT_PATH_JOINER
python3 -m filter_joiner.filter_joiner_main
```

**New**: Template sources normalized environment files
```bash
source /etc/config-out/calibration-group-and-convert.env
export OUT_PATH=$OUT_PATH_JOINER
python3 -m filter_joiner.filter_joiner_main
```

The init container can be adapted for any platform without changing the template.

### 3. Configuration Format

**Old**: Flat key-value pairs with embedded YAML strings
```yaml
data:
  log-level: INFO
  filter-joiner-config: |
    ---
    input_paths:
    - path:
        name: DATA_PATH_ARCHIVE
  cal-conv-r-args: >
    DirIn=$OUT_PATH_KAFKA_COMB ...
```

**New**: Hierarchical, self-documenting YAML
```yaml
data:
  config.yaml: |
    workflow:
      log_level: INFO
    processing:
      filter_joiner_config: |
        input_paths:
        - path:
            name: DATA_PATH_ARCHIVE
      calibration_conversion_r_args: |
        DirIn=$OUT_PATH_KAFKA_COMB ...
```

### 4. Sensor Variant Management

**Old**: Create a new template file for each sensor
```
workflows/
├── cmp22_calibration_group_and_convert.yaml
├── aepg600m_calibration_group_and_convert.yaml
└── aepg600m_heated_calibration_group_and_convert.yaml
```

**New**: One template + Kustomize overlays
```
workflows/
├── base/
│   └── calibration-group-and-convert.yaml    # Single base
└── overlays/
    ├── cmp22/
    │   ├── kustomization.yaml
    │   └── configmap.yaml
    ├── aepg600m/
    │   ├── kustomization.yaml
    │   └── configmap.yaml
    └── aepg600m_heated/
        ├── kustomization.yaml
        └── configmap.yaml
```

## Step-by-Step Migration

### Step 1: Understanding the Data Flow

The old template had this flow:
```
ConfigMap (flat keys)
    ↓
Template extracts each key
    ↓
Containers use environment variables
```

The new template has this flow:
```
ConfigMap (structured YAML)
    ↓
Init container normalizes
    ↓
Environment files created
    ↓
Containers source environment files
```

### Step 2: Converting Your ConfigMap

**Old ConfigMap** (key-value):
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cmp22-cal-group-convert-config
data:
  log-level: INFO
  l0-bucket-name: neon-dev-l0-ingest
  out-path-cal-conv: /pfs/cmp22_calibration_group_and_convert
  kfka-comb-r-args: >
    DirIn=$OUT_PATH_JOINER ...
```

**New ConfigMap** (structured):
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cmp22-calibration-group-convert-config
data:
  config.yaml: |
    workflow:
      log_level: INFO
    data_loading:
      l0_bucket_name: neon-dev-l0-ingest
    processing:
      out_path_calibration_conversion: /pfs/cmp22_calibration_group_and_convert
      kafka_combine_r_args: |
        DirIn=$OUT_PATH_JOINER ...
```

**Migration Checklist**:
- [ ] Group related keys under logical sections
- [ ] Use snake_case for keys (was kebab-case)
- [ ] Keep multi-line values as YAML blocks
- [ ] Verify all required keys are present
- [ ] Test YAML syntax with `python3 -c "import yaml; yaml.safe_load(open('file.yaml'))"`

### Step 3: Creating Kustomize Overlays

For each sensor, create a Kustomize overlay:

```bash
mkdir workflows/overlays/cmp22
cat > workflows/overlays/cmp22/kustomization.yaml << 'EOF'
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
            value: cmp22-calibration-group-convert-config

commonLabels:
  sensor: cmp22
  workflow: calibration-group-and-convert
EOF
```

### Step 4: Updating Container Commands

**Old**: Extracting ConfigMap values in container
```bash
env:
  - name: OUT_PATH_CAL_CONV
    valueFrom:
      configMapKeyRef:
        name: "{{workflow.parameters.config}}"
        key: out-path-cal-conv
  
command:
  - bash
  - -c
  - |
    export OUT_PATH=$OUT_PATH_CAL_CONV
```

**New**: Source normalized environment file
```bash
command:
  - bash
  - -c
  - |
    source /etc/config-out/calibration-group-and-convert.env
    export OUT_PATH=$OUT_PATH_CAL_CONV
```

### Step 5: Building the Init Container

Create the init container that normalizes config:

```bash
cd workflows/base
docker build -t your-registry/config-normalizer:latest .
docker push your-registry/config-normalizer:latest
```

Update template to use your registry:
```yaml
initContainers:
  - name: config-normalizer
    image: your-registry/config-normalizer:latest
```

### Step 6: Testing the Migration

1. **Deploy the new overlay**:
   ```bash
   kubectl apply -k workflows/overlays/cmp22/
   ```

2. **Verify resources**:
   ```bash
   kubectl get workflowtemplate -n argo-workflows-dev
   kubectl get cm -n argo-workflows-dev | grep calibration
   ```

3. **Submit a test workflow**:
   ```bash
   argo submit -n argo-workflows-dev \
     --from workflowtemplate/calibration-group-and-convert \
     -p datum-manifest='{"paths": ["cmp22/2025/10/01/11185"]}'
   ```

4. **Monitor execution**:
   ```bash
   argo watch -n argo-workflows-dev <workflow-id>
   ```

5. **Verify init container ran**:
   ```bash
   kubectl logs -n argo-workflows-dev <pod-id> -c config-normalizer
   ```

## Comparing Side-by-Side

### Configuration Definition

| Aspect | Old | New |
|--------|-----|-----|
| ConfigMap lines | ~100+ | ~70 (more readable) |
| Key naming | `kebab-case` | `snake_case` |
| Structure | Flat | Hierarchical |
| Readability | Mixed | Organized |

### Template Definition

| Aspect | Old | New |
|--------|-----|-----|
| Parameter extractions | 40+ | 0 |
| Init containers | 0 | 1 (normalizer) |
| Container env vars | 20+ per container | 1 sourceCommand |
| Workflow files | 3-5 | 1 base |
| Variants | Duplicate files | Kustomize overlays |

### Deployment

| Aspect | Old | New |
|--------|-----|-----|
| Deploying new sensor | Create new template | Create new overlay |
| Switching sensors | Modify workflows | `kubectl apply -k` |
| Adding parameters | Edit template | Edit ConfigMap |
| Maintenance | High | Low |

## Rollback Plan

If you need to rollback to the original template:

```bash
# Keep the old template available
git checkout <old-ref> -- calibration_group_and_convert.yaml

# Recreate old ConfigMap
kubectl apply -f argo/cmp22/configmap-cmp22-cal-group-convert-config.yaml

# Delete new resources
kubectl delete workflowtemplate calibration-group-and-convert -n argo-workflows-dev
kubectl delete cm -l workflow=calibration-group-and-convert -n argo-workflows-dev
```

## FAQ

### Q: Can I use both old and new templates in parallel?

**A**: Yes. Give them different names (e.g., `calibration-group-and-convert-v2`) and deploy both. You can gradually migrate workflows.

### Q: What if my sensor has unique configuration needs?

**A**: The hierarchical config format makes this easy:
1. Add new section to `config.yaml`
2. Update init container script to handle new section
3. Create .env file for containers that need it

### Q: Do I need to rebuild the init container for each sensor?

**A**: No. The init container is generic and reads sensor-specific config from the ConfigMap.

### Q: How do I migrate existing workflow submissions?

**A**: Existing workflows using the old template will continue to run. New submissions should use the new template:
```bash
# Old
argo submit -f cmp22_calibration_group_and_convert.yaml
argo submit -f aepg600m_calibration_group_and_convert.yaml

# New
argo submit --from workflowtemplate/calibration-group-and-convert \
  -p config-map-name=cmp22-calibration-group-convert-config
# Or use Kustomize to auto-patch
argo submit -k workflows/overlays/cmp22/
```

### Q: What about backward compatibility?

**A**: The new ConfigMap format is incompatible with the old template. You need to either:
1. Keep both templates (old and new)
2. Migrate all sensors at once
3. Create a wrapper script that converts old to new format

## Performance Impact

- **Init container overhead**: ~1-2 seconds per workflow startup
- **ConfigMap parsing**: Negligible (~100ms for typical configs)
- **No impact on container execution times**: Once init completes, containers run normally

## Security Considerations

- **ConfigMap storage**: Unchanged—still stored in etcd
- **RBAC**: Ensure service accounts can read ConfigMaps
- **Secrets**: If needed, mount secrets separately (not in this example)

## Next Steps

1. Review the new template: `workflows/base/calibration-group-and-convert.yaml`
2. Examine sensor configs: `workflows/overlays/*/configmap.yaml`
3. Deploy a test sensor: `kubectl apply -k workflows/overlays/cmp22/`
4. Submit a test workflow and monitor execution
5. Migrate other sensors following the same pattern
