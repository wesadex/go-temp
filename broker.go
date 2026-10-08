package main

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrTopicNotFound = errors.New("topic not found")
var ErrTopicEmpty = errors.New("topic is empty")

const topicBlockSize int = 256

type IBroker interface {
	Send(string, string)
	Recv(string) (string, error)
}

var _ IBroker = (*Broker)(nil)

type TopicBlock struct {
	data *[topicBlockSize]string
	next *TopicBlock
}

type TopicBlockPointer struct {
	topicBlock *TopicBlock
	offset     atomic.Int32
	tbmu       sync.Mutex
}

type Topic struct {
	head       *TopicBlock
	tail       *TopicBlock
	headOffset int
	tailOffset int
	tmu        sync.Mutex
}

type Broker struct {
	storage sync.Map // string -> *Topic
}

func newTopic() *Topic {
	tb := &TopicBlock{
		data: new([topicBlockSize]string),
	}

	return &Topic{
		head: tb,
		tail: tb,
	}
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

func (m *Broker) sendToTopic(topicAddr *Topic, message string) {
	// Проверка заполненности, расширение и запись — одна критическая секция
	topicAddr.tmu.Lock()
	defer topicAddr.tmu.Unlock()

	if topicAddr.tailOffset == topicBlockSize { // То есть у нас заполнен очередной блок, нужен новый
		tb := &TopicBlock{
			data: new([topicBlockSize]string),
		}
		topicAddr.tail.next = tb // прицепляем к цепочке, иначе читатель его не найдет
		topicAddr.tail = tb      // хвост теперь - новый блок
		topicAddr.tailOffset = 0 // указатель записи - в начало нового блока-хвоста
	}
	topicAddr.tail.data[topicAddr.tailOffset] = message
	topicAddr.tailOffset++
}

func (m *Broker) recvFromTopic(topicAddr *Topic) (string, error) {
	topicAddr.tmu.Lock()
	defer topicAddr.tmu.Unlock()

	if topicAddr.headOffset == topicBlockSize { // Достигли конца блока с чтением
		if topicAddr.head.next == nil { // если дальше блоков нет - ошибка, нечего читать
			return "", ErrTopicEmpty
		}
		topicAddr.head = topicAddr.head.next // переставляем указатель - голова теперь следуюший блок
		topicAddr.headOffset = 0
	}

	if topicAddr.head == topicAddr.tail { //  если голова и хвост - один блок...
		if topicAddr.headOffset == topicAddr.tailOffset { // и оба указателя - одинаковые, то читать нечего больше
			return "", ErrTopicEmpty
		}
	}

	res := topicAddr.head.data[topicAddr.headOffset]
	topicAddr.head.data[topicAddr.headOffset] = "" // не держим прочитанную строку
	topicAddr.headOffset++
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
