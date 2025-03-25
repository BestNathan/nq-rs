.PHONY: deribit-subscription

deribit-subscription:
	@docker rm -f deribit-subscription
	docker build -t nq-rs/deribit-subscription --build-arg APP=deribit-subscription --build-arg PROXY=http://192.168.2.98:8890 .
	@docker run -d --name deribit-subscription --restart always nq-rs/deribit-subscription