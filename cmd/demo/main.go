// Command demo runs the seeded M7 chaos harness and prints a compact results
// table suitable for a repeatable local demonstration.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/0xciph3r/mini-verde/internal/harness"
)

func main() {
	scenarios := flag.Int("scenarios", 1_000, "number of seeded scenarios")
	seed := flag.Uint64("seed", 1, "chaos seed")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	summary, err := harness.Run(ctx, *scenarios, *seed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "demo: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("mini-verde chaos results")
	fmt.Println("metric                 value")
	fmt.Printf("scenarios              %d\n", summary.Scenarios)
	fmt.Printf("accepted               %d\n", summary.Accepted)
	fmt.Printf("rejected               %d\n", summary.Rejected)
	fmt.Printf("unresolved             %d\n", summary.Unresolved)
	fmt.Printf("disputes               %d\n", summary.Disputes)
	fmt.Printf("injected drops         %d\n", summary.InjectedDrops)
	fmt.Printf("injected delays        %d\n", summary.InjectedDelays)
	fmt.Printf("safety violations      %d\n", summary.SafetyViolations)
	fmt.Printf("false blames           %d\n", summary.FalseBlames)
	if summary.SafetyViolations != 0 || summary.FalseBlames != 0 {
		os.Exit(1)
	}
}
