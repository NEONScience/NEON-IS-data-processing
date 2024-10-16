# Run from root repository (NEON-IS-data-processing)
#!/usr/bin/env bash
image_name=processed_datums_reader
tag=$(git rev-parse --short HEAD)
cd ./modules
docker build --no-cache -t $image_name:latest -f ./processed_datums_reader/Dockerfile .
docker tag $image_name quay.io/battelleecology/$image_name:$tag
docker push quay.io/battelleecology/$image_name:$tag
cd ..
Rscript ./utilities/flow.img.updt.R "./pipe" ".yaml" "quay.io/battelleecology/$image_name" "$tag"
