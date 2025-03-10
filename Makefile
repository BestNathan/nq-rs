deribit-subscription:
	docker build -t nq-rs/deribit-subscription --build-arg APP=deribit-subscription --build-arg ALL_PROXY=http://192.168.2.98:8890 .
	docker run -d --name deribit-subscription --restart always nq-rs/deribit-subscription