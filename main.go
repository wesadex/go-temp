package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const topic = "events"

// Итоги одного консьюмера. Считаем локально и складываем в конце,
// чтобы общий счётчик не стал ещё одной точкой конкуренции.
type consumerStats struct {
	received   int64
	duplicates int64
	emptyPolls int64 // Recv вернул «пусто» — холостой опрос
}

func main() {
	// total := 100_000_000
	// producers := 1000
	// consumers := 800
	// cpuProf := ""
	// mutexProf := ""
	// topics := 100 // число топиков; продюсер p пишет в топик p%topics, консьюмер c читает топик c%topics

	var (
		totalFlag     = flag.Int("total", 100_000_000, "сколько сообщений отправить")
		producersFlag = flag.Int("producers", 1000, "число горутин-продюсеров")
		consumersFlag = flag.Int("consumers", 800, "число горутин-консьюмеров")
		topicsFlag    = flag.Int("topics", 100, "число топиков; продюсер p пишет в топик p%topics, консьюмер c читает топик c%topics")
		cpuProfFlag   = flag.String("cpuprofile", "", "записать CPU-профиль в файл")
		mutexProfFlag = flag.String("mutexprofile", "", "записать профиль ожидания мьютексов в файл")
	)
	flag.Parse()

	// Дальше по коду используются обычные переменные, а не указатели.
	total, producers, consumers, topics := *totalFlag, *producersFlag, *consumersFlag, *topicsFlag
	cpuProf, mutexProf := *cpuProfFlag, *mutexProfFlag

	// У каждого топика должен быть хотя бы один продюсер и один консьюмер,
	// иначе топик либо пуст, либо его никто не вычитает и тест зависнет.
	if topics < 1 || topics > producers || topics > consumers {
		log.Fatalf("topics=%d: нужно 1 <= topics <= min(producers=%d, consumers=%d)", topics, producers, consumers)
	}
	topicNames := make([]string, topics) // имена заранее, чтобы не аллоцировать в цикле
	for i := range topicNames {
		topicNames[i] = "events-" + strconv.Itoa(i)
	}

	if cpuProf != "" {
		f := must(os.Create(cpuProf))
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	if mutexProf != "" {
		runtime.SetMutexProfileFraction(1)
		defer writeProfile("mutex", mutexProf)
	}

	broker := NewGPTBroker()

	// Проверка доставки: бит на каждое сообщение (250 млн сообщений — ~30 МБ).
	seen := make([]atomic.Uint64, (total+63)/64)
	var producersDone atomic.Bool

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Все горутины стартуют одновременно по сигналу, чтобы время
	// запуска 100k горутин не попадало в замер.
	startGate := make(chan struct{})
	var ready sync.WaitGroup

	// Консьюмеры.
	stats := make([]consumerStats, consumers)
	var consWG sync.WaitGroup
	ready.Add(consumers)
	consWG.Add(consumers)
	for c := range consumers {
		go func() {
			defer consWG.Done()
			st := &stats[c]
			topic := topicNames[c%topics]
			ready.Done()
			<-startGate
			for {
				// Флаг читаем ДО Recv: если продюсеры уже закончили,
				// а его топик после этого пуст, значит, топик вычитан окончательно.
				finished := producersDone.Load()
				msg, err := broker.Recv(topic)
				if err != nil {
					if !errors.Is(err, ErrTopicEmpty) && !errors.Is(err, ErrTopicNotFound) {
						log.Fatal(err)
					}
					if finished {
						return
					}
					st.emptyPolls++
					runtime.Gosched() // пусто — уступаем ядро продюсерам
					continue
				}
				id, err := strconv.Atoi(msg) // без аллокаций
				if err != nil || id < 0 || id >= total {
					log.Fatalf("битое сообщение %q", msg)
				}
				bit := uint64(1) << (id % 64)
				if seen[id/64].Or(bit)&bit != 0 {
					st.duplicates++
				}
				st.received++
			}
		}()
	}

	// Продюсеры: каждый отправляет свой диапазон id.
	var prodWG sync.WaitGroup
	ready.Add(producers)
	prodWG.Add(producers)
	for p := range producers {
		from := total * p / producers
		to := total * (p + 1) / producers
		go func() {
			defer prodWG.Done()
			buf := make([]byte, 0, 20)
			topic := topicNames[p%topics]
			ready.Done()
			<-startGate
			for id := from; id < to; id++ {
				buf = strconv.AppendInt(buf[:0], int64(id), 10)
				broker.Send(topic, string(buf)) // одна аллокация точного размера
			}
		}()
	}

	ready.Wait()
	start := time.Now()
	close(startGate)

	prodWG.Wait()
	sendDur := time.Since(start)
	producersDone.Store(true)

	consWG.Wait()
	totalDur := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	var received, duplicates, emptyPolls int64
	for _, st := range stats {
		received += st.received
		duplicates += st.duplicates
		emptyPolls += st.emptyPolls
	}

	n := float64(total)
	fmt.Printf("GOMAXPROCS=%d topics=%d producers=%d consumers=%d total=%d\n",
		runtime.GOMAXPROCS(0), topics, producers, consumers, total)
	fmt.Printf("запись:          %-10v %6.2f млн msg/s\n",
		sendDur.Round(time.Millisecond), n/sendDur.Seconds()/1e6)
	fmt.Printf("дочитка хвоста:  %v\n", (totalDur - sendDur).Round(time.Millisecond))
	fmt.Printf("всего:           %-10v %6.2f млн msg/s, %.0f ns/msg\n",
		totalDur.Round(time.Millisecond), n/totalDur.Seconds()/1e6, float64(totalDur.Nanoseconds())/n)
	fmt.Printf("холостых Recv:   %d (%.1f%% от полезных)\n", emptyPolls, 100*float64(emptyPolls)/max(n, 1))
	fmt.Printf("GC: циклов %d, пауз %v, выделено %d МБ\n",
		memAfter.NumGC-memBefore.NumGC,
		time.Duration(memAfter.PauseTotalNs-memBefore.PauseTotalNs).Round(time.Microsecond),
		(memAfter.TotalAlloc-memBefore.TotalAlloc)>>20)

	if received != int64(total) || duplicates != 0 {
		fmt.Printf("ОШИБКА: получено %d из %d, дубликатов %d\n", received, total, duplicates)
		os.Exit(1)
	}
	fmt.Printf("OK: получено %d, дубликатов нет\n", received)
}

func writeProfile(name, path string) {
	f := must(os.Create(path))
	defer f.Close()
	if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
		log.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}
