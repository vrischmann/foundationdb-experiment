package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/peterbourgon/ff/v3/ffcli"
	"golang.org/x/sync/errgroup"
)

func incrementCounter(db fdb.Database, key string) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)

	_, err := fdb.GenericTransact(db, func(tx fdb.Transaction) (struct{}, error) {
		tx.Add(fdb.Key(key), buf[:])
		return struct{}{}, nil
	})

	return err
}

type rootCmdConfig struct {
	cpuProfile string

	db fdb.Database
}

func newRootCmd() (*ffcli.Command, *rootCmdConfig) {
	var cfg rootCmdConfig

	fs := flag.NewFlagSet("fdbtest", flag.ExitOnError)
	cfg.RegisterFlags(fs)

	return &ffcli.Command{
		Name:       "fdbtest",
		ShortUsage: "fdbtest [flags] <subcommand> [flags] [<arg>...]",
		FlagSet:    fs,
		Exec:       cfg.Exec,
	}, &cfg
}

func (c *rootCmdConfig) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.cpuProfile, "cpu-profile", "", "Create a CPU profile")
}

func (c *rootCmdConfig) Exec(ctx context.Context, args []string) error {
	return flag.ErrHelp
}

type incCounterTestCmdConfig struct {
	root *rootCmdConfig

	nbGoroutines int
	nbIterations int
}

func newIncCounterTestCmd(root *rootCmdConfig) *ffcli.Command {
	cfg := &incCounterTestCmdConfig{
		root: root,
	}

	fs := flag.NewFlagSet("fdbtest inc-counter-test", flag.ExitOnError)
	fs.IntVar(&cfg.nbGoroutines, "nb-goroutines", 1, "The number of goroutines to run")
	fs.IntVar(&cfg.nbIterations, "nb-iter", 1, "The number of iterations per goroutine to run")
	root.RegisterFlags(fs)

	return &ffcli.Command{
		Name:       "inc-counter-test",
		ShortUsage: "inc-counter-test [flags] <key>",
		FlagSet:    fs,
		Exec:       cfg.Exec,
	}
}

func (c *incCounterTestCmdConfig) Exec(ctx context.Context, args []string) error {
	if len(args) < 1 {
		fmt.Printf("Missing `key` argument\n\n")
		return flag.ErrHelp
	}

	key := args[0]

	var eg errgroup.Group
	for i := 0; i < c.nbGoroutines; i++ {
		eg.Go(func() error {
			for i := 0; i < c.nbIterations; i++ {
				err := incrementCounter(c.root.db, key)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

type getCmdConfig struct {
	root *rootCmdConfig

	decodeAsInt bool
}

func newGetCmd(root *rootCmdConfig) *ffcli.Command {
	cfg := &getCmdConfig{
		root: root,
	}

	fs := flag.NewFlagSet("fdbtest get", flag.ExitOnError)
	fs.BoolVar(&cfg.decodeAsInt, "as-int", false, "Decode the value as an 8 byte integer")
	root.RegisterFlags(fs)

	return &ffcli.Command{
		Name:       "get",
		ShortUsage: "get [flags] <key>",
		FlagSet:    fs,
		Exec:       cfg.Exec,
	}
}

func (c *getCmdConfig) Exec(ctx context.Context, args []string) error {
	if len(args) < 1 {
		fmt.Printf("Missing `key` argument\n\n")
		return flag.ErrHelp
	}

	key := args[0]

	if c.decodeAsInt {
		n, err := fdb.GenericTransact(c.root.db, func(tx fdb.Transaction) (int, error) {
			data := tx.Get(fdb.Key(key)).MustGet()
			return int(binary.LittleEndian.Uint64(data)), nil
		})
		if err != nil {
			return err
		}

		fmt.Printf("integer value: %d\n", n)
	} else {
		data, err := fdb.GenericTransact(c.root.db, func(tx fdb.Transaction) (string, error) {
			data := tx.Get(fdb.Key(key)).MustGet()
			return string(data), nil
		})
		if err != nil {
			return err
		}

		fmt.Printf("byte string value: %q\n", data)
	}

	return nil
}

func main() {
	var (
		rootCmd, root     = newRootCmd()
		incCounterTestCmd = newIncCounterTestCmd(root)
		getCmd            = newGetCmd(root)
	)

	rootCmd.Subcommands = []*ffcli.Command{
		incCounterTestCmd,
		getCmd,
	}

	if err := rootCmd.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error during Parse: %v\n", err)
		os.Exit(1)
	}

	// Setup profiling if necessary

	if root.cpuProfile != "" {
		f, err := os.Create(root.cpuProfile)
		if err != nil {
			log.Fatal(err)
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Initialize FDB

	fdb.MustAPIVersion(630)

	var err error
	root.db, err = fdb.OpenDefault()
	if err != nil {
		log.Fatal(err)
	}

	// Run the commands

	if err := rootCmd.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
