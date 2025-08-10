module custom_header_example

go 1.14

replace github.com/nilorg/eventbus => ../..

require (
	github.com/go-redis/redis/v8 v8.11.5
	github.com/nilorg/eventbus v0.0.0-00010101000000-000000000000
)
