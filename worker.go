package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type worker struct {
	closeCh   chan bool
	consumers []*consumer
}

type consumer struct {
	id     int
	msgCh  chan int
	exitCh chan bool
}

func (w *worker) Close() {
	close(w.closeCh)
}

func (w *worker) produce() {
	go func() {
		i := 0
		for {
			select {
			case <-w.closeCh:
				goto end
			default:
				i++
				index := rand.Int() % 2
				w.consumers[index].msgCh <- i
				fmt.Printf("send:%d\n", i)
			}
		}
	end:
		for _, c := range w.consumers {
			close(c.exitCh)
		}
	}()
}

func (w *worker) Run() {
	w.produce()
	for _, c := range w.consumers {
		go c.consume()
	}
}

func (c *consumer) consume() {
	go func() {
		for {
			select {
			case rev := <-c.msgCh:
				c.do(rev)
				time.Sleep(1 * time.Second)
			case <-c.exitCh:
				goto end
			}
		}
	end:
		close(c.msgCh)
		fmt.Printf("close msg ch:%d\n", c.id)
		for rev := range c.msgCh {
			c.do(rev)
		}
	}()
}

func (c *consumer) do(i int) {
	fmt.Printf("consumer:%d, rev:%d\n", c.id, i)
}

func main() {
	w := &worker{
		make(chan bool),
		[]*consumer{&consumer{1, make(chan int, 10), make(chan bool)}, &consumer{2, make(chan int, 10), make(chan bool)}},
	}

	w.Run()

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig

	w.Close()

	println("closed")
	time.Sleep(5 * time.Second)
}
