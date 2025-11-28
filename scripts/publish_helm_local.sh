# Package the chart
helm package charts/datagen_api
# Install the chart
helm install datagen-api ./datagen-api-0.1.0.tgz --namespace dev --create-namespace
# install the chart with the values-dev.yaml file
helm install datagen-api ./datagen-api-0.1.0.tgz --namespace dev --create-namespace --values values-dev.yaml
# install without package
helm install datagen-api ./charts/datagen_api --namespace dev --set image.pullPolicy=Always

# delete the chart
helm uninstall datagen-api ./charts/datagen_api --namespace dev 

# delete a deployment
helm uninstall datagen-api -n dev

# Upgrade Helm deployment
helm upgrade datagen-api charts/datagen_api -n dev

