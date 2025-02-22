.PHONY: deploy dry-run kafka


deploy:
	cd charts && helm install datagen-api . \
		--set image.pullPolicy=Always \
		--set image.tag=latest
	

dry-run:
	cd charts && helm install datagen-api . --debug --dry-run
