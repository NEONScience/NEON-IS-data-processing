Workflow templates are organized by family so each template can own its base
resources and its overlays.

Current pattern:

```text
workflows/
  <workflow-family>/
    base/
      <workflow-template>.yaml
      configmap-*.yaml
      kustomization.yaml
    overlays/
      <overlay-name>/
        configmap-env.yaml
        configmap-resource-request.yaml
        kustomization.yaml
```

The initial family is:

```text
workflows/calibration-group-and-convert/
```

To add a new workflow template, create a sibling directory under `workflows/`
with the same `base/` and `overlays/` shape.