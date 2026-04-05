# Plan and apply for dev
terraform plan -var-file="envs/dev.tfvars"
terraform apply -var-file="envs/dev.tfvars"

# Plan and apply for staging
terraform plan -var-file="envs/staging.tfvars"
terraform apply -var-file="envs/staging.tfvars"

# Plan and apply for prod
terraform plan -var-file="envs/prod.tfvars"
terraform apply -var-file="envs/prod.tfvars"