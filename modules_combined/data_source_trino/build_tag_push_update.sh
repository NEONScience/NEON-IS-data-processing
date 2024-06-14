# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=data_source_trino
tag=$(git rev-parse --short HEAD)
docker build --no-cache -t $image_name:latest -f ./modules_combined/data_source_trino/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag

Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"
