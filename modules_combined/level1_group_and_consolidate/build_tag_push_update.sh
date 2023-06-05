# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=level1_group_and_consolidate
tag=$(git rev-parse HEAD)
docker build -t $image_name:latest -f ./modules_combined/level1_group_and_consolidate/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"