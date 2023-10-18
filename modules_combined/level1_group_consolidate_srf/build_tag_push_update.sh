# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=neon-is-levl1-grp-cons-srf
tag=$(git rev-parse --short HEAD)
docker build -t $image_name:latest -f ./modules_combined/level1_group_consolidate_srf/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"
