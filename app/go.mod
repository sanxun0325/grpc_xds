module main

go 1.13

require (
	echo v0.0.0
	golang.org/x/net v0.0.0-20201110031124-69a78807bb2b
	google.golang.org/grpc v1.45.0
)

replace echo => ./src/echo
