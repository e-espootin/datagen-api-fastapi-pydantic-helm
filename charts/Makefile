.PHONY: deploy dry-run kafka



kafka_api_producer_install:
	cd kafka_api_producer && helm install producer . \
		--set image.pullPolicy=Always \
		--set image.tag=latest
kafka_api_producer_upgrade:
	cd kafka_api_producer && helm upgrade producer .
kafka_api_producer_dry_run:
	cd kafka_api_producer && helm install kafka-apip . \
		--set image.pullPolicy=Always \
		--set image.tag=latest \
		--dry-run --debug

kafka_api_consumer_install:
	cd kafka_api_consumer && helm install consumer . \
		--set image.pullPolicy=Always \
		--set image.tag=latest
kafka_api_consumer_upgrade:
	cd kafka_api_consumer && helm upgrade consumer . \
		--set image.pullPolicy=Always \
		--set image.tag=latest

configmap:
	kubectl apply -f configmap.yaml -n dev 

aws-zoho-infra:
	kubectl apply -f secrets/configmap_aws_zoho.yaml -n dev

encode:
	echo -n "Pu6AXRqoBMAWMuKUeGQhPZL3FQtnI1XokkTz1gxB" | base64

kafkaui:
	cd kafka-ui && helm install kafkaui . -n dev
kafkaui-upgrade:
	cd kafka-ui && helm upgrade kafkaui . -n dev