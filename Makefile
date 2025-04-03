.PHONY: deribit-subscription

deribit-subscription:
	@docker rm -f deribit-subscription
	docker build -t nq-rs/deribit-subscription --build-arg APP=deribit-subscription --build-arg PROXY=http://192.168.2.98:8890 .
	@docker run -d --name deribit-subscription --restart always nq-rs/deribit-subscription

FLUVIO_HOST=none
FLUVIO_CONNECTOR_WORKDIR=/home/fluvio/connector
FLUVIO_CONNECTOR_CONFIG=$(FLUVIO_CONNECTOR_WORKDIR)/connector.yaml
FLUVIO_CONNECTOR_SECRET=$(FLUVIO_CONNECTOR_WORKDIR)/secrets

fluvio-http-source-docker:
	@if [ "$(FLUVIO_HOST)" = "none" ]; then echo "\nmissing fluvio host, please use 'FLUVIO_HOST=xxx'\n\n" && exit 1; fi
	@docker build -t nq-rs/fluvio-http-source --build-arg FLUVIO_HOST=$(FLUVIO_HOST) -f ./fluvio/docker/http-source.Dockerfile .

fluvio-http-sink-docker:
	@if [ "$(FLUVIO_HOST)" = "none" ]; then echo "\nmissing fluvio host, please use 'FLUVIO_HOST=xxx'\n\n" && exit 1; fi
	@docker build -t nq-rs/fluvio-http-sink --build-arg FLUVIO_HOST=$(FLUVIO_HOST) -f ./fluvio/docker/http-sink.Dockerfile .


DERIBIT_PROXY=none
DERIBIT_CURRENCIES := btc eth sol
fluvio-deribit-rv: fluvio-http-source-docker
	@if [ "$(DERIBIT_PROXY)" = "none" ]; then echo "\nmissing deribit proxy, please use 'DERIBIT_PROXY=xxx'\n\n" && exit 1; fi
	@for currency in $(DERIBIT_CURRENCIES); do \
		echo run fluvio-deribit-rv for $$currency; \
		docker rm -f fluvio-deribit-rv-$$currency; \
		docker run -d --name fluvio-deribit-rv-$$currency \
			--restart always \
			-e HTTPS_PROXY=$(DERIBIT_PROXY) \
			-v ./fluvio/connectorconfs/deribit-rv-$$currency-connector.yaml:$(FLUVIO_CONNECTOR_CONFIG) \
			nq-rs/fluvio-http-source; \
	done

fluvio-deribit-tdengine-http-sink: fluvio-http-sink-docker
	@docker rm -f fluvio-deribit-tdengine-http-sink
	@docker run -d --name fluvio-deribit-tdengine-http-sink \
		--restart always \
		-v ./fluvio/connectorconfs/deribit-tdengine-sink-connector.yaml:$(FLUVIO_CONNECTOR_CONFIG) \
		-v $(HOME)/.nq/fluvio/secrets:$(FLUVIO_CONNECTOR_SECRET) \
		nq-rs/fluvio-http-sink
