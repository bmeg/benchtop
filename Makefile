
benchtop: *.go cmdline/benchtop
	go build ./cmdline/benchtop

clean:
	rm benchtop