// Copyright 2016-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package natspub

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/go-nats-streaming"
)

/* var usageStr = `
Usage: stan-pub [options] <subject> <message>

Options:
	-s, --server   <url>            NATS Streaming server URL(s)
	-c, --cluster  <cluster name>   NATS Streaming cluster name
	-id,--clientid <client ID>      NATS Streaming client ID
	-a, --async                     Asynchronous publish mode
` */

// NOTE: Use tls scheme for TLS, e.g. stan-pub -s tls://demo.nats.io:4443 foo hello
/* func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
} */

//Pubber is a modiication of the go nats publish client example to pass messages from msgchan to the nats url
func Pubber(clusterID string, clientID string, async bool, URL string, msgchan chan string, subj string) {

	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(URL))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}
	defer sc.Close()

	ch := make(chan bool)
	var glock sync.Mutex
	var guid string
	acb := func(lguid string, err error) {
		glock.Lock()
		log.Printf("Received ACK for guid %s\n", lguid)
		defer glock.Unlock()
		if err != nil {
			log.Fatalf("Error in server ack for guid %s: %v\n", lguid, err)
		}
		if lguid != guid {
			log.Fatalf("Expected a matching guid in ack callback, got %s vs %s\n", lguid, guid)
		}
		ch <- true
	}
	for {
		select {
		case msg, ok := <-msgchan:
			if ok {
				splitmsg := strings.Split(msg, ";")
				_, text := splitmsg[0], splitmsg[1]
				if !async {

					err = sc.Publish(subj, []byte(text))
					if err != nil {
						log.Fatalf("Error during publish: %v\n", err)
					}
					log.Printf("Published [%s] : '%s'\n", subj, text)
				} else {
					glock.Lock()
					guid, err = sc.PublishAsync(subj, []byte(text), acb)
					if err != nil {
						log.Fatalf("Error during async publish: %v\n", err)
					}
					glock.Unlock()
					if guid == "" {
						log.Fatal("Expected non-empty guid to be returned.")
					}
					log.Printf("Published [%s] : '%s' [guid: %s]\n", subj, text, guid)

					select {
					case <-ch:
						break
					case <-time.After(5 * time.Second):
						log.Fatal("timeout")
					}

				}
			}
		default:
			log.Println("No message to process on queue sleeping 5 sec...")
			time.Sleep(5 * time.Second)
		}
	}
}
