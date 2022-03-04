package memory

import (
	"github.com/NICEXAI/go-eventbus"
	"reflect"
	"sync"
)

type Manager struct {
	mutex       *sync.Mutex
	Handles     []eventbus.SubscribeHandler
	HandlesOnce []eventbus.SubscribeHandler
}

func (e *Manager) Subscribe(handle eventbus.SubscribeHandler) {
	e.mutex.Lock()
	e.Handles = append(e.Handles, handle)
	e.mutex.Unlock()
}

func (e *Manager) Unsubscribe(handle eventbus.SubscribeHandler) {
	curPoint := reflect.ValueOf(handle).Pointer()
	e.mutex.Lock()
	for i := range e.Handles {
		if curPoint == reflect.ValueOf(e.Handles[i]).Pointer() {
			e.Handles = append(e.Handles[:i], e.Handles[i+1:]...)
			break
		}
	}
	e.mutex.Unlock()
}

func (e *Manager) SubscribeOnce(handle eventbus.SubscribeHandler) {
	e.mutex.Lock()
	e.HandlesOnce = append(e.HandlesOnce, handle)
	e.mutex.Unlock()
}

func (e *Manager) Publish(msg *eventbus.Message) {
	for _, cHandle := range e.Handles {
		cHandle(msg)
	}
	e.mutex.Lock()
	for _, oHandle := range e.HandlesOnce {
		oHandle(msg)
	}
	e.HandlesOnce = make([]eventbus.SubscribeHandler, 0)
	e.mutex.Unlock()
}

func NewManager() *Manager {
	return &Manager{
		mutex:       &sync.Mutex{},
		Handles:     make([]eventbus.SubscribeHandler, 0),
		HandlesOnce: make([]eventbus.SubscribeHandler, 0),
	}
}
