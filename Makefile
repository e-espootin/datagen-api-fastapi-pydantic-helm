.PHONY: deploy dry-run


deploy:
	cd charts && helm install datagen-api . \
		--set image.pullPolicy=Always \
		--set image.tag=latest
	
upgrade:
    cd charts && helm upgrade datagen-api . \
        --set image.pullPolicy=Always \
        --set image.tag=latest \
        --force
dry-run:
	cd charts && helm install datagen-api . --debug --dry-run

delete:
	helm uninstall datagen-api 