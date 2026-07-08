# Architecture Diagrams

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Cluster                           │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────┐     ┌─────────────────┐   ┌────────────────┐  │
│  │  Kustomize      │     │   ConfigMaps    │   │  Base Template │  │
│  │  Overlays       │────▶│  (structured    │──▶│                │  │
│  │                 │     │   YAML config)  │   │  calibration-  │  │
│  │ • cmp22         │     │                 │   │  group-and-    │  │
│  │ • aepg600m      │     ├─────────────────┤   │  convert.yaml  │  │
│  │ • aepg600m_     │     │ cmp22           │   │                │  │
│  │   heated        │     ├─────────────────┤   └────────┬───────┘  │
│  └─────────────────┘     │ aepg600m        │            │           │
│                          ├─────────────────┤            │           │
│                          │ aepg600m_heated │            ▼           │
│                          └─────────────────┘   ┌────────────────┐   │
│                                                │  WorkflowSpec  │   │
│                                                │                │   │
│                                                │ Volumes:       │   │
│                                                │  • config-vol  │   │
│                                                │  • data-vol    │   │
│                                                │  • tmp-vol     │   │
│                                                │                │   │
│                                                │ InitContainers:│   │
│                                                │  • config-     │   │
│                                                │    normalizer  │   │
│                                                │                │   │
│                                                │ Containers:    │   │
│                                                │  • load-data   │   │
│                                                │  • cal-grp-    │   │
│                                                │    and-conv    │   │
│                                                │  • main        │   │
│                                                └────────────────┘   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Workflow Execution Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Workflow Submission                              │
│  $ kubectl apply -k workflows/overlays/cmp22/                       │
└────────────────────────┬──────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Kustomize Generates (merged YAML):                                 │
│  • WorkflowTemplate: calibration-group-and-convert                  │
│  • ConfigMap: cmp22-calibration-group-convert-config                │
└────────────────────────┬──────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Kubectl applies resources to cluster                               │
│  • WorkflowTemplate registered with Argo                            │
│  • ConfigMap stored in etcd                                         │
└────────────────────────┬──────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Workflow Submission (Argo)                                         │
│  $ argo submit --from workflowtemplate/calibration-group-and-      │
│    convert -p config-map-name=cmp22-calibration-group-convert...   │
└────────────────────────┬──────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Pod Created                                                        │
│  • ConfigMap mounted: /etc/config-in/                               │
│  • emptyDir volumes created                                         │
└────────────────────────┬──────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    INIT CONTAINER: config-normalizer                │
│                                                                      │
│  1. Read: /etc/config-in/config.yaml                               │
│     ├─ Parse YAML                                                  │
│     └─ Extract configuration sections                              │
│                                                                      │
│  2. Generate environment files:                                     │
│     ├─ /etc/config-out/load-data.env                               │
│     ├─ /etc/config-out/calibration-group-and-convert.env           │
│     └─ /etc/config-out/data-upload.env                             │
│                                                                      │
│  3. Status: ✓ Environment files ready for containers               │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────┐
│        CONTAINER 1: load-data (Sequential)                │
│                                                            │
│  1. Source: /etc/config-out/load-data.env                │
│  2. Run: python3 -m l0_gcs_loader_by_manifest            │
│  3. Download L0 data from GCS                            │
│  4. Download calibrations from GCS                       │
│  5. Output → /data/DATA_PATH_ARCHIVE                   │
│  6. Output → /data/CALIBRATION_PATH                    │
└────────────────┬─────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────────┐
│     CONTAINER 2: calibration-group-and-convert            │
│                                                            │
│  1. Source: /etc/config-out/calibration-group-and-        │
│     convert.env                                            │
│  2. Run: filter_joiner (join data and calibrations)       │
│  3. Run: Rscript flow.kfka.comb.R (kafka combine)         │
│  4. Run: Rscript flow.cal.conv.R (calibration conversion) │
│  5. Output → /data/cmp22_calibration_group_and_convert     │
└────────────────┬─────────────────────────────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────────┐
│     CONTAINER 3: main (data upload)                        │
│                                                            │
│  1. Source: /etc/config-out/data-upload.env              │
│  2. Link output files to temporary directory              │
│  3. Run: rclone copy to GCS output bucket                 │
│  4. Cleanup temporary files                               │
│  5. Complete                                               │
└────────────────┬─────────────────────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────────────────────────┐
│  Workflow Complete                                           │
│                                                              │
│  ✓ Data loaded from GCS                                     │
│  ✓ Calibrations merged with data                            │
│  ✓ Calibration conversions applied                          │
│  ✓ Results uploaded to output bucket                        │
│  ✓ Pod cleaned up                                           │
└──────────────────────────────────────────────────────────────┘
```

## Configuration Normalization Detail

```
┌────────────────────────────────────────────────────────────┐
│    ConfigMap: cmp22-calibration-group-convert-config      │
│    (/etc/config-in/config.yaml inside container)           │
└────────────────────────────┬───────────────────────────────┘
                             │
                             │ YAML Structure:
                             │
                             ├─ workflow:
                             │   ├─ log_level
                             │   └─ error_path
                             │
                             ├─ data_loading:
                             │   ├─ l0_bucket_name
                             │   ├─ calibration_bucket_name
                             │   └─ ...
                             │
                             ├─ processing:
                             │   ├─ filter_joiner_config
                             │   ├─ kafka_combine_r_args
                             │   ├─ calibration_conversion_r_args
                             │   └─ ...
                             │
                             └─ data_output:
                                 ├─ output_bucket_name
                                 └─ output_bucket_prefix
                             │
                             ▼
┌────────────────────────────────────────────────────────────┐
│     Init Container: config-normalizer (Python script)     │
│                                                            │
│  1. Parse YAML                                             │
│  2. Extract sections                                       │
│  3. Map to environment variables                           │
│  4. Generate environment files                             │
└────────────┬─────────────────────────────────────┬─────────┘
             │                                     │
             ▼                                     ▼
     ┌──────────────────┐                ┌──────────────────┐
     │ load-data.env    │                │ calibration-     │
     ├──────────────────┤                │ group-and-       │
     │BUCKET_NAME=...   │                │ convert.env      │
     │BUCKET_VERSION... │                ├──────────────────┤
     │CAL_BUCKET_NAME.. │                │CONFIG=...        │
     │CAL_BUCKET_PREF.. │                │OUT_PATH_JOINER.. │
     │LOG_LEVEL=...     │                │KFKA_COMB_R_ARGS  │
     │...               │                │CAL_CONV_R_ARGS   │
     └──────────────────┘                │...               │
                                         └──────────────────┘
                                                  │
                                                  ▼
                                         ┌──────────────────┐
                                         │ data-upload.env  │
                                         ├──────────────────┤
                                         │OUT_PATH=...      │
                                         │OUTPUT_BUCKET_... │
                                         │OUTPUT_BUCKET_... │
                                         └──────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────┐
│    Workflow Containers                                     │
│                                                            │
│  load-data:                                                │
│  $ source /etc/config-out/load-data.env                   │
│  $ python3 -m l0_gcs_loader_by_manifest                   │
│                                                            │
│  calibration-group-and-convert:                            │
│  $ source /etc/config-out/calibration-group-and-          │
│    convert.env                                             │
│  $ python3 -m filter_joiner.filter_joiner_main            │
│                                                            │
│  main:                                                     │
│  $ source /etc/config-out/data-upload.env                │
│  $ rclone copy ... :gcs://...                             │
└────────────────────────────────────────────────────────────┘
```

### Architecture (Modular)
```
┌──────────────────────────────────────────────────────────┐
│  Base Template + Kustomize Overlays                                │
│                                                          │
│  workflows/base/                                         │
│  ├─ calibration-group-and-convert.yaml (clean!)         │
│  ├─ config-normalizer-entrypoint.py                      │
│  └─ Dockerfile                                           │
│                                                          │
│  workflows/overlays/                                     │
│  ├─ cmp22/                                               │
│  │  ├─ kustomization.yaml (patches)                     │
│  │  └─ configmap.yaml (structured)                      │
│  ├─ aepg600m/                                            │
│  │  ├─ kustomization.yaml (patches)                     │
│  │  └─ configmap.yaml (structured)                      │
│  └─ aepg600m_heated/                                     │
│     ├─ kustomization.yaml (patches)                     │
│     └─ configmap.yaml (structured)                      │
│                                                          │
│  ConfigMap (structured YAML):                            │
│  ├─ config.yaml: |                                       │
│  │   workflow:                                            │
│  │     log_level: INFO                                   │
│  │   data_loading:                                       │
│  │     l0_bucket_name: ...                               │
│  │     calibration_bucket_name: ...                      │
│  │   processing:                                         │
│  │     filter_joiner_config: |                           │
│  │       (properly formatted)                             │
│  │     kafka_combine_r_args: |                           │
│  │       (properly formatted)                             │
│  │   data_output:                                        │
│  │     output_bucket_name: ...                           │
│                                                          │
│  Benefits:                                               │
│  ✓ Clean, minimal template                              │
│  ✓ Hierarchical configuration                           │
│  ✓ Easy to read and maintain                            │
│  ✓ Sensor variants via Kustomize                        │
│  ✓ Platform logic isolated to init container            │
│  ✓ Easy to migrate (adapt init container only)          │
└──────────────────────────────────────────────────────────┘
```

## Volume Layout During Execution

```
Pod Volumes During Workflow Execution:

/etc/config-in/
├─ config.yaml          ← Mounted from ConfigMap

/etc/config-out/        ← emptyDir, written by init container
├─ load-data.env        ← Sourced by load-data container
├─ calibration-group-and-convert.env  ← Sourced by cal-grp container
└─ data-upload.env      ← Sourced by main container

/data/                ← emptyDir volume
├─ DATA_PATH_ARCHIVE/   ← Created by load-data, read by filter-joiner
│  └─ cmp22/2025/10/01/11185/
│     └─ [raw data files]
│─ CALIBRATION_PATH/    ← Created by load-data, read by filter-joiner
│  └─ cmp22/2025/10/01/11185/
│     └─ [calibration files]
├─ data_cal_joined/     ← Output from filter-joiner
├─ kafka_combined/      ← Output from kafka combine step
└─ cmp22_calibration_group_and_convert/  ← Final output, uploaded to GCS

/tmp/                   ← emptyDir volume, needed for R temp files
```

## Deployment Topology

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Namespace: argo-workflows-dev                              │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  WorkflowTemplate: calibration-group-and-convert    │   │
│  │  (Managed by Kustomize)                             │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  ConfigMap: cmp22-calibration-group-convert-config  │   │
│  │  ConfigMap: aepg600m-calibration-group-convert...   │   │
│  │  ConfigMap: aepg600m-heated-calibration-group...    │   │
│  │  (Managed by Kustomize overlays)                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  At Runtime:                                                │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Pod (from Workflow submission)                      │  │
│  │                                                      │  │
│  │  Init Container: config-normalizer                  │  │
│  │  - Reads ConfigMap YAML                             │  │
│  │  - Generates environment files                      │  │
│  │                                                      │  │
│  │  Container: load-data (exits, passes control)       │  │
│  │  Container: calibration-group-and-convert (exits)   │  │
│  │  Container: main (exits)                            │  │
│  │                                                      │  │
│  │  Pod Status: Succeeded                              │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

These diagrams illustrate the architectural improvements and data flow through the refactored workflow system.
