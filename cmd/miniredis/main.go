import main

import  (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
)


func main(){
	ctx,cancel := signal.NotifyContext(context.Background(), os.Interrupt)

	err:= run(ctx, os.Args[1:], os.Stderr)

	cancel()

	if err!=nil {
		fmt.Fprintln(os.Stderr,err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	
}