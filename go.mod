module go.rischmann.fr/fdbtest

go 1.18

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20220225233552-7d7762ba77d3
	github.com/peterbourgon/ff/v3 v3.1.2
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

require golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543 // indirect

replace github.com/apple/foundationdb/bindings/go => github.com/vrischmann/foundationdb/bindings/go v0.0.0-20220313231739-3be5b4d440e3
