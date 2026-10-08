package main

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrTopicNotFound = errors.New("topic not found")
var ErrTopicEmpty = errors.New("topic is empty")

const topicBlockSize int = 256

// Размер кэш-линии на Apple Silicon. На x86 — 64, но 128 безопасно и там.
const cacheLineSize = 128

type IBroker interface {
	Send(string, string)
	Recv(string) (string, error)
}

var _ IBroker = (*Broker)(nil)

// Блок — единственное место, через которое общаются продюсеры и консьюмеры.
// Всё, что одна сторона пишет, а другая читает, — атомики.
type TopicBlock struct {
	data    [topicBlockSize]string
	written atomic.Int32               // сколько слотов опубликовано; пишет продюсер, читает консьюмер
	next    atomic.Pointer[TopicBlock] // следующий блок; пишет продюсер, читает консьюмер
}

// Указатель на блок со своим локом: один для головы, один для хвоста.
type TopicBlockPointer struct {
	mu         sync.Mutex
	topicBlock *TopicBlock
	offset     int // позиция чтения; используется только головой, под mu
	// Добиваем до кэш-линии, чтобы лок головы и лок хвоста
	// не делили одну линию (false sharing).
	_ [cacheLineSize - 24]byte
}

type Topic struct {
	head TopicBlockPointer // только консьюмеры
	tail TopicBlockPointer // только продюсеры
}

type Broker struct {
	storage sync.Map // string -> *Topic
}

func newTopic() *Topic {
	tb := &TopicBlock{}
	t := &Topic{}
	t.head.topicBlock = tb
	t.tail.topicBlock = tb
	return t
}

func (m *Broker) ensureTopicExists(topic string) *Topic {
	// Быстрый путь: топик уже есть, без аллокаций
	if t, ok := m.storage.Load(topic); ok {
		return t.(*Topic)
	}
	// Медленный путь: создаем. Если другой Send успел раньше — берем его топик, наш выбрасываем
	t, _ := m.storage.LoadOrStore(topic, newTopic())
	return t.(*Topic)
}

func (m *Broker) sendToTopic(t *Topic, message string) {
	// Под локом хвоста: голову не трогаем вообще
	t.tail.mu.Lock()
	defer t.tail.mu.Unlock()

	tb := t.tail.topicBlock
	w := tb.written.Load()        // пишем только мы, но читаем атомарно, т.к. поле атомарное
	if int(w) == topicBlockSize { // блок заполнен, нужен новый
		nb := &TopicBlock{}
		tb.next.Store(nb)      // СНАЧАЛА публикуем новый блок для консьюмеров
		t.tail.topicBlock = nb // потом переставляем хвост
		tb, w = nb, 0
	}
	tb.data[w] = message    // СНАЧАЛА пишем слот...
	tb.written.Store(w + 1) // ...потом публикуем: консьюмер увидит слот только после этого
}

func (m *Broker) recvFromTopic(t *Topic) (string, error) {
	// Под локом головы: хвост не трогаем вообще
	t.head.mu.Lock()
	defer t.head.mu.Unlock()

	tb, r := t.head.topicBlock, t.head.offset
	if r == topicBlockSize { // дочитали блок до конца
		nb := tb.next.Load()
		if nb == nil { // следующего блока ещё нет — читать нечего
			return "", ErrTopicEmpty
		}
		// Переходим на следующий блок. Старый больше никто не держит — его заберёт GC.
		t.head.topicBlock, t.head.offset = nb, 0
		tb, r = nb, 0
	}

	if r >= int(tb.written.Load()) { // всё опубликованное в блоке уже прочитано
		return "", ErrTopicEmpty
	}

	res := tb.data[r]
	tb.data[r] = "" // не держим прочитанную строку
	t.head.offset = r + 1
	return res, nil
}

// Public methods

func NewMQBroker() *Broker {
	return &Broker{}
}

func (m *Broker) Send(topic string, message string) {
	if message == "" { // игнорим пустые сообщения
		return
	}
	top := m.ensureTopicExists(topic) // Получили топик
	m.sendToTopic(top, message)
}

func (m *Broker) Recv(topic string) (string, error) {
	// Проверяем есть ли такой топик
	t, ok := m.storage.Load(topic)
	if !ok {
		return "", ErrTopicNotFound
	}
	return m.recvFromTopic(t.(*Topic))
}
