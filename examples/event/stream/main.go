// Command streamtest opens an event stream against an ONVIF camera and
// prints decoded events as they arrive. Useful for verifying the
// classifier against real-camera topics; not intended as a production
// tool.
//
// Example:
//
//	go run ./examples/event/stream \
//	  -xaddr 192.168.1.10 \
//	  -username root -password admin \
//	  -duration 60s
//
// The xaddr is the camera's host or host:port (the library appends
// /onvif/device_service); pass with no protocol prefix.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kerberos-io/onvif"
	"github.com/kerberos-io/onvif/event/stream"
)

func main() {
	xaddr := flag.String("xaddr", "", "camera host or host:port (required)")
	username := flag.String("username", "", "ONVIF user (required)")
	password := flag.String("password", "", "ONVIF password (required)")
	deviceID := flag.String("device-id", "", "logical name printed with each event (default: xaddr)")
	filter := flag.String("filter", "", "raw ONVIF ConcreteSet topic filter (empty = all topics, works on AXIS)")
	pullTimeout := flag.Duration("pull-timeout", 5*time.Second, "server-side wait per PullMessages call")
	duration := flag.Duration("duration", 0, "stop after this long (0 = run until Ctrl-C)")
	flag.Parse()

	if *xaddr == "" || *username == "" || *password == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *deviceID == "" {
		*deviceID = *xaddr
	}

	dev, err := onvif.NewDevice(onvif.DeviceParams{
		Xaddr:    *xaddr,
		Username: *username,
		Password: *password,
		AuthMode: onvif.UsernameTokenAuth,
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *duration > 0 {
		var done context.CancelFunc
		ctx, done = context.WithTimeout(ctx, *duration)
		defer done()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	s, err := stream.NewStream(ctx, dev, stream.Options{
		DeviceID:       *deviceID,
		RawTopicFilter: *filter,
		PullTimeout:    *pullTimeout,
	})
	if err != nil {
		log.Fatalf("open stream: %v", err)
	}
	defer s.Close()

	log.Printf("streaming from %s (device-id=%s, filter=%q)", *xaddr, *deviceID, *filter)
	for {
		select {
		case <-ctx.Done():
			log.Printf("done (%v)", ctx.Err())
			return
		case ev, ok := <-s.Events():
			if !ok {
				return
			}
			fmt.Printf("%s  kind=%-15s state=%-9s op=%-12s topic=%s",
				ev.Timestamp.Format(time.RFC3339), ev.Kind, ev.State, ev.Operation, ev.Topic)
			if len(ev.Source) > 0 {
				fmt.Printf("  source=%v", ev.Source)
			}
			if len(ev.Data) > 0 {
				fmt.Printf("  data=%v", ev.Data)
			}
			fmt.Println()
		case e := <-s.Errors():
			log.Printf("stream error: %v", e)
		}
	}
}
