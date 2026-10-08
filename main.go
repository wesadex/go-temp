package main

import (
	"errors"
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

var ErrTopicNotFound = errors.New("topic not found")
var ErrTopicEmpty = errors.New("topic is empty")

const topicBlockSize int = 16

type IBroker interface {
	Send(string, string)
	Recv(string) (string, error)
}

var _ IBroker = (*Broker)(nil)

type TopicBlock struct {
	data *[topicBlockSize]string
	next *TopicBlock
}

type Topic struct {
	head       *TopicBlock
	tail       *TopicBlock
	headOffset int
	tailOffset int
	tmu        sync.Mutex
}

type Broker struct {
	storage map[string]*Topic
	mu      sync.Mutex
}

func NewMQBroker() *Broker {
	m := Broker{
		storage: make(map[string]*Topic),
	}
	return &m
}

func (m *Broker) createTopic(topic string) *Topic {
	tb := &TopicBlock{
		data: new([topicBlockSize]string),
	}

	t := &Topic{
		head: tb,
		tail: tb,
	}
	m.storage[topic] = t
	return t
}

func (m *Broker) ensureTopicExists(topic string) *Topic {
	// Проверяем есть ли такой топик
	// Мьютекс нужен в вызывающей функции!
	t, ok := m.storage[topic]
	if !ok {
		// и создаем, если нет
		return m.createTopic(topic)
	}
	return t
}

func (m *Broker) sendToTopic(topicAddr *Topic, message string) {
	// Проверка заполненности, расширение и запись — одна критическая секция
	topicAddr.tmu.Lock()
	defer topicAddr.tmu.Unlock()

	if topicAddr.tailOffset == topicBlockSize { // То есть у нас заполнен очередной блок, нужен новый
		tb := &TopicBlock{
			data: new([topicBlockSize]string),
		}
		topicAddr.tail.next = tb // прицепляем к цепочке, иначе читатель его не найдет
		topicAddr.tail = tb
		topicAddr.tailOffset = 0
	}
	topicAddr.tail.data[topicAddr.tailOffset] = message
	topicAddr.tailOffset++
}

func (m *Broker) recvFromTopic(topicAddr *Topic) (string, error) {
	topicAddr.tmu.Lock()
	defer topicAddr.tmu.Unlock()

	if topicAddr.headOffset == topicBlockSize { // Достигли конца блока с чтением
		if topicAddr.head.next == nil {
			return "", ErrTopicEmpty
		}
		topicAddr.head = topicAddr.head.next
		topicAddr.headOffset = 0
	}

	if topicAddr.head == topicAddr.tail {
		if topicAddr.headOffset == topicAddr.tailOffset { // голова и хвост - одно и то же, оба указателя - одно и то же
			return "", ErrTopicEmpty
		}
	}

	res := topicAddr.head.data[topicAddr.headOffset]
	topicAddr.head.data[topicAddr.headOffset] = "" // не держим прочитанную строку
	topicAddr.headOffset++
	return res, nil
}

func (m *Broker) Send(topic string, message string) {
	// Лок на мапу
	m.mu.Lock()
	defer m.mu.Unlock()

	top := m.ensureTopicExists(topic) // Получили топик
	m.sendToTopic(top, message)
}

func (m *Broker) Recv(topic string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Проверяем есть ли такой топик
	t, ok := m.storage[topic]
	if !ok {
		return "", ErrTopicNotFound
	}
	return m.recvFromTopic(t)
}

// TESTS

const topic = "events"

// Итоги одного консьюмера. Считаем локально и складываем в конце,
// чтобы общий счётчик не стал ещё одной точкой конкуренции.
type consumerStats struct {
	received   int64
	duplicates int64
	emptyPolls int64 // Recv вернул «пусто» — холостой опрос
}

func main() {
	total := 10_000_000
	producers := 1000
	consumers := 500
	cpuProf := ""
	mutexProf := ""
	topics := 16 // число топиков; продюсер p пишет в топик p%topics, консьюмер c читает топик c%topics

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

	broker := NewMQBroker()

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
