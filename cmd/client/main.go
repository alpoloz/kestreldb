package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"kestreldb/internal/proto"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	// one-shot mode: kdb HSET key field value
	if args := flag.Args(); len(args) > 0 {
		if err := send(bw, args); err != nil {
			fmt.Fprintln(os.Stderr, "send:", err)
			os.Exit(1)
		}
		resp, err := proto.ReadResponse(r)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read:", err)
			os.Exit(1)
		}
		printResponse(resp, "")
		return
	}

	// interactive REPL
	repl(r, bw)
}

func repl(r *bufio.Reader, bw *bufio.Writer) {
	stdin := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("kdb> ")
		if !stdin.Scan() {
			fmt.Println()
			return
		}

		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}

		// local quit without round-trip
		upper := strings.ToUpper(line)
		if upper == "QUIT" || upper == "EXIT" {
			fmt.Println("BYE")
			return
		}

		// send raw line — server does all parsing
		if _, err := fmt.Fprintf(bw, "%s\n", line); err != nil {
			fmt.Fprintln(os.Stderr, "send:", err)
			return
		}
		if err := bw.Flush(); err != nil {
			fmt.Fprintln(os.Stderr, "flush:", err)
			return
		}

		resp, err := proto.ReadResponse(r)
		if err != nil {
			fmt.Fprintln(os.Stderr, "read:", err)
			return
		}
		printResponse(resp, "")
	}
}

// send formats tokens as a KLP command line and writes it to bw.
func send(bw *bufio.Writer, tokens []string) error {
	line := proto.FormatCommand(tokens)
	if _, err := bw.WriteString(line); err != nil {
		return err
	}
	return bw.Flush()
}

func printResponse(resp proto.Response, prefix string) {
	switch resp.Type {
	case proto.RSimpleString, proto.RBlobString:
		fmt.Println(prefix + resp.Str)

	case proto.RInteger:
		fmt.Printf("%s(integer) %d\n", prefix, resp.Int)

	case proto.RFloat:
		fmt.Printf("%s%g\n", prefix, resp.Float)

	case proto.RNil:
		fmt.Println(prefix + "(nil)")

	case proto.RError:
		fmt.Println(prefix + resp.Str)

	case proto.RArray:
		if len(resp.Elements) == 0 {
			fmt.Println(prefix + "(empty)")
			return
		}
		for i, elem := range resp.Elements {
			fmt.Printf("%s%d) ", prefix, i+1)
			printResponse(elem, "   ")
		}
	}
}
