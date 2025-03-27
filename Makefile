.PHONY: deribit-subscription

deribit-subscription:
	@docker rm -f deribit-subscription
	docker build -t nq-rs/deribit-subscription --build-arg APP=deribit-subscription --build-arg PROXY=http://192.168.2.98:8890 .
	@docker run -d --name deribit-subscription --restart always nq-rs/deribit-subscription

FLUVIO_HOST=none

fluvio-http-source-docker:
	@if [ "$(FLUVIO_HOST)" = "none" ]; then echo "\nmissing fluvio host, please use 'FLUVIO_HOST=xxx'\n\n" && exit 1; fi
	@docker build -t nq-rs/fluvio-http-source --build-arg FLUVIO_HOST=$(FLUVIO_HOST) -f ./fluvio/docker/http-source.Dockerfile .

fluvio-deribit-rv: fluvio-http-source-docker
	@docker rm -f fluvio-deribit-rv-btc
	@docker rm -f fluvio-deribit-rv-eth
	@docker run -d --name fluvio-deribit-rv-btc \
		--restart always \
		-v ./fluvio/connectors/deribit-rv-btc-connector.yaml:/home/fluvio/connector/connector.yaml \
		nq-rs/fluvio-http-source
	@docker run -d --name fluvio-deribit-rv-eth \
		--restart always \
		-v ./fluvio/connectors/deribit-rv-eth-connector.yaml:/home/fluvio/connector/connector.yaml \
		nq-rs/fluvio-http-source