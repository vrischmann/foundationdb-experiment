package main

import (
	"context"
	"encoding/binary"
	"errors"
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
	tx, err := db.CreateTransaction()
	if err != nil {
		log.Fatal(err)
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)

	for {
		tx.Add(fdb.Key(key), buf[:])

		f := tx.Commit()

		err := f.Get()
		if err == nil {
			break
		}

		log.Printf("commit failed, err: %v", err)

		var fdbErr fdb.Error
		if errors.Is(err, &fdbErr) {
			err = tx.OnError(fdbErr).Get()
		}

		if err != nil {
			return err
		}
	}

	return nil
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
}

func newIncCounterTestCmd(root *rootCmdConfig) *ffcli.Command {
	cfg := &incCounterTestCmdConfig{
		root: root,
	}

	fs := flag.NewFlagSet("fdbtest inc-counter-test", flag.ExitOnError)
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
	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			for i := 0; i < 200000; i++ {
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

func main() {
	var (
		rootCmd, root     = newRootCmd()
		incCounterTestCmd = newIncCounterTestCmd(root)
	)

	rootCmd.Subcommands = []*ffcli.Command{
		incCounterTestCmd,
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
