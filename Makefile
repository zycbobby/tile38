all: 
	@./build.sh
clean:
	rm -f tile38-server
	rm -f tile38-cli
	rm -f tile38-benchmark
test:
	@./build.sh test
cover:
	@./build.sh cover
install: all
	cp tile38-server /usr/local/bin
	cp tile38-cli /usr/local/bin
	cp tile38-benchmark /usr/local/bin
uninstall: 
	rm -f /usr/local/bin/tile38-server
	rm -f /usr/local/bin/tile38-cli
	rm -f /usr/local/bin/tile38-benchmark
package:
package:
	@./build.sh package
