package memory

import (
	"context"
	"github.com/NICEXAI/go-eventbus"
	"sync"
)

type Memory struct {
	managerCenter sync.Map
}

func (m *Memory) Subscribe(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	manager := m.getManager(topic)
	manager.Subscribe(handle)
	m.managerCenter.Store(topic, manager)
	return nil
}

func (m *Memory) Unsubscribe(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	manager := m.getManager(topic)
	manager.Unsubscribe(handle)
	m.managerCenter.Store(topic, manager)
	return nil
}

func (m *Memory) SubscribeOnce(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	manager := m.getManager(topic)
	manager.SubscribeOnce(handle)
	m.managerCenter.Store(topic, manager)
	return nil
}

func (m *Memory) Publish(ctx context.Context, topic string, msg *eventbus.Message) (err error) {
	manager := m.getManager(topic)
	manager.Publish(msg)
	return nil
}

func (m *Memory) HandleCount(topic string) int {
	manager := m.getManager(topic)
	return len(manager.Handles) + len(manager.HandlesOnce)
}

func (m *Memory) getManager(topic string) *Manager {
	var manager *Manager

	originManager, ok := m.managerCenter.Load(topic)
	if !ok {
		manager = NewManager()
		m.managerCenter.Store(topic, manager)
		return manager
	}

	manager, _ = originManager.(*Manager)
	return manager
}

func NewEventBusByMemory() *Memory {
	return &Memory{managerCenter: sync.Map{}}
}
