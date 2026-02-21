package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"kestreldb/internal/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	flag.Parse()

	c, err := client.Dial(*addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer c.Close()

	args := flag.Args()
	if len(args) > 0 {
		if _, err := executeCommand(context.Background(), c, args); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

	repl(c)
}

func repl(c *client.Client) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("kdb> ")
		if !scanner.Scan() {
			fmt.Println()
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		quit, err := executeCommand(context.Background(), c, strings.Fields(line))
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			continue
		}
		if quit {
			return
		}
	}
}

func executeCommand(ctx context.Context, c *client.Client, parts []string) (bool, error) {
	if len(parts) == 0 {
		return false, nil
	}

	command := strings.ToUpper(parts[0])
	args := parts[1:]

	switch command {
	case "PING":
		if err := requireArgCount(command, args, 0); err != nil {
			return false, err
		}
		resp, err := c.Ping(ctx)
		if err != nil {
			return false, err
		}
		fmt.Println(resp.GetMessage())
		return false, nil
	case "QUIT", "EXIT":
		fmt.Println("BYE")
		return true, nil
	case "HSET":
		if err := requireArgCount(command, args, 3); err != nil {
			return false, err
		}
		resp, err := c.HashSet(ctx, args[0], args[1], []byte(args[2]))
		if err != nil {
			return false, err
		}
		fmt.Println(boolToInt(resp.GetCreated()))
		return false, nil
	case "HGET":
		if err := requireArgCount(command, args, 2); err != nil {
			return false, err
		}
		resp, err := c.HashGet(ctx, args[0], args[1])
		if err != nil {
			return false, err
		}
		if !resp.GetFound() {
			fmt.Println("(nil)")
			return false, nil
		}
		fmt.Println(string(resp.GetValue()))
		return false, nil
	case "HDEL":
		if err := requireArgCount(command, args, 2); err != nil {
			return false, err
		}
		resp, err := c.HashDelete(ctx, args[0], args[1])
		if err != nil {
			return false, err
		}
		fmt.Println(boolToInt(resp.GetDeleted()))
		return false, nil
	case "HLEN":
		if err := requireArgCount(command, args, 1); err != nil {
			return false, err
		}
		resp, err := c.HashLen(ctx, args[0])
		if err != nil {
			return false, err
		}
		fmt.Println(resp.GetLength())
		return false, nil
	case "HGETALL":
		if err := requireArgCount(command, args, 1); err != nil {
			return false, err
		}
		resp, err := c.HashGetAll(ctx, args[0])
		if err != nil {
			return false, err
		}
		if len(resp.GetEntries()) == 0 {
			fmt.Println("(empty)")
			return false, nil
		}
		for i, entry := range resp.GetEntries() {
			fmt.Printf("%d) %s=%s\n", i+1, entry.GetField(), string(entry.GetValue()))
		}
		return false, nil
	case "ZADD":
		if err := requireArgCount(command, args, 3); err != nil {
			return false, err
		}
		score, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return false, errors.New("invalid score")
		}
		resp, err := c.SortedSetAdd(ctx, args[0], score, args[2])
		if err != nil {
			return false, err
		}
		fmt.Println(boolToInt(resp.GetAdded()))
		return false, nil
	case "ZREM":
		if err := requireArgCount(command, args, 2); err != nil {
			return false, err
		}
		resp, err := c.SortedSetRemove(ctx, args[0], args[1])
		if err != nil {
			return false, err
		}
		fmt.Println(boolToInt(resp.GetRemoved()))
		return false, nil
	case "ZSCORE":
		if err := requireArgCount(command, args, 2); err != nil {
			return false, err
		}
		resp, err := c.SortedSetScore(ctx, args[0], args[1])
		if err != nil {
			return false, err
		}
		if !resp.GetFound() {
			fmt.Println("(nil)")
			return false, nil
		}
		fmt.Println(resp.GetScore())
		return false, nil
	case "ZCARD":
		if err := requireArgCount(command, args, 1); err != nil {
			return false, err
		}
		resp, err := c.SortedSetCardinality(ctx, args[0])
		if err != nil {
			return false, err
		}
		fmt.Println(resp.GetCount())
		return false, nil
	case "ZRANGE":
		if err := requireArgCount(command, args, 3); err != nil {
			return false, err
		}
		start, stop, err := parseRange(args[1], args[2])
		if err != nil {
			return false, err
		}
		resp, err := c.SortedSetRange(ctx, args[0], start, stop)
		if err != nil {
			return false, err
		}
		for i, item := range resp.GetItems() {
			fmt.Printf("%d) %s\n", i+1, item.GetMember())
		}
		return false, nil
	case "ZRANGEWITHSCORES":
		if err := requireArgCount(command, args, 3); err != nil {
			return false, err
		}
		start, stop, err := parseRange(args[1], args[2])
		if err != nil {
			return false, err
		}
		resp, err := c.SortedSetRange(ctx, args[0], start, stop)
		if err != nil {
			return false, err
		}
		if len(resp.GetItems()) == 0 {
			fmt.Println("(empty)")
			return false, nil
		}
		for i, item := range resp.GetItems() {
			fmt.Printf("%d) %s (score=%g)\n", i+1, item.GetMember(), item.GetScore())
		}
		return false, nil
	default:
		return false, fmt.Errorf("unknown command: %s", command)
	}
}

func requireArgCount(command string, args []string, expected int) error {
	if len(args) != expected {
		return fmt.Errorf("%s expects %d argument(s)", command, expected)
	}
	return nil
}

func parseRange(start string, stop string) (int64, int64, error) {
	startValue, err := strconv.ParseInt(start, 10, 64)
	if err != nil {
		return 0, 0, errors.New("invalid start")
	}
	stopValue, err := strconv.ParseInt(stop, 10, 64)
	if err != nil {
		return 0, 0, errors.New("invalid stop")
	}
	return startValue, stopValue, nil
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
