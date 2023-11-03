test:
	go test -race ./...
bench:
	go test -run=^$ -bench=. ./...
build:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		./servicepb/service.proto
doc:
	pkgsite -gorepo ./...