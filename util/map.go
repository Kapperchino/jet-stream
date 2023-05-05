package util

import "sync"

type Map[K comparable, V any] struct {
	internalMap map[K]V
	mutex       sync.RWMutex
}

func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		internalMap: map[K]V{},
		mutex:       sync.RWMutex{},
	}
}

func (m *Map[K, V]) Get(key K) V {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.internalMap[key]
}

func (m *Map[K, V]) Set(key K, val V) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.internalMap[key] = val
}

func (m *Map[K, V]) Del(key K) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.internalMap, key)
}

func (m *Map[K, V]) Len() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.internalMap)
}

func (m *Map[K, V]) ForEach(f func(key K, val V) bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for k, v := range m.internalMap {
		res := f(k, v)
		if res == false {
			return
		}
	}
}
