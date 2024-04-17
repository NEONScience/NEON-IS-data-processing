# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=neon-is-troll-logfiles-fill-r
tag=$(git rev-parse --short HEAD)
cd ./flow/flow.troll.logfiles.fill
docker build -t $image_name:latest .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag
cd ../..

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"

#also need to update modules_combined/troll_logs_group_and_fill: neon-is-troll-logs-group-fill