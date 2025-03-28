.PHONY: deribit-subscription

deribit-subscription:
	@docker rm -f deribit-subscription
	docker build -t nq-rs/deribit-subscription --build-arg APP=deribit-subscription --build-arg PROXY=http://192.168.2.98:8890 .
	@docker run -d --name deribit-subscription --restart always nq-rs/deribit-subscription

FLUVIO_HOST=none

fluvio-http-source-docker:
	@if [ "$(FLUVIO_HOST)" = "none" ]; then echo "\nmissing fluvio host, please use 'FLUVIO_HOST=xxx'\n\n" && exit 1; fi
	@docker build -t nq-rs/fluvio-http-source --build-arg FLUVIO_HOST=$(FLUVIO_HOST) -f ./fluvio/docker/http-source.Dockerfile .

fluvio-http-sink-docker:
	@if [ "$(FLUVIO_HOST)" = "none" ]; then echo "\nmissing fluvio host, please use 'FLUVIO_HOST=xxx'\n\n" && exit 1; fi
	@docker build -t nq-rs/fluvio-http-sink --build-arg FLUVIO_HOST=$(FLUVIO_HOST) -f ./fluvio/docker/http-sink.Dockerfile .


DERIBIT_PROXY=none

fluvio-deribit-rv: fluvio-http-source-docker
	@if [ "$(DERIBIT_PROXY)" = "none" ]; then echo "\nmissing deribit proxy, please use 'DERIBIT_PROXY=xxx'\n\n" && exit 1; fi
	@docker rm -f fluvio-deribit-rv-btc
	@docker rm -f fluvio-deribit-rv-eth
	@docker run -d --name fluvio-deribit-rv-btc \
		--restart always \
		-e HTTPS_PROXY=$(DERIBIT_PROXY) \
		-v ./fluvio/connectorconfs/deribit-rv-btc-connector.yaml:./connector.yaml \
		nq-rs/fluvio-http-source
	@docker run -d --name fluvio-deribit-rv-eth \
		--restart always \
		-e HTTPS_PROXY=$(DERIBIT_PROXY) \
		-v ./fluvio/connectorconfs/deribit-rv-eth-connector.yaml:./connector.yaml \
		nq-rs/fluvio-http-source

fluvio-deribit-tdengine-http-sink: fluvio-http-sink-docker
	@docker rm -f fluvio-deribit-tdengine-http-sink
	@docker run -d --name fluvio-deribit-tdengine-http-sink \
		--restart always \
		-v ./fluvio/connectorconfs/deribit-tdengine-sink-connector.yaml:./connector.yaml \
		-v $(HOME)/.nq/fluvio/secrets:./secrets \
		nq-rs/fluvio-http-sink
