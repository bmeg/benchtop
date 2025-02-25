
benchtop: *.go cmdline/benchtop
	go build ./cmdline/benchtop

clean:
	rm benchtop

proto:
	@cd kvindex && protoc \
		-I ./ \
		--go_opt=paths=source_relative \
		--go_out=. \
		--go_opt paths=source_relative \
		index.proto