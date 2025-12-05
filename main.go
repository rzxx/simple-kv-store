package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Item — это то, что мы храним.
// Теги `json:"..."` объясняют Go, как превращать это в JSON и обратно.
type Item struct {
	Value     string `json:"value"`
	ExpiresAt int64  `json:"expires_at"` // Unix nano timestamp
}

// Storage — наше ядро.
// Поля с маленькой буквы (private), потому что мы не хотим, чтобы
// кто-то лез в map мимо наших методов Set/Get.
type Storage struct {
	items map[string]Item
	mu    sync.RWMutex // Встраиваем мьютекс
}

// NewStorage — конструктор.
// Возвращает *Storage (УКАЗАТЕЛЬ), чтобы все работали с одним экземпляром.
func NewStorage() *Storage {
	return &Storage{
		items: make(map[string]Item),
		// mu инициализируется сама нулем (разблокирована)
	}
}

// Set — запись данных (требует полной блокировки Lock)
func (s *Storage) Set(key string, value string, ttlSeconds int) {
	// Вычисляем время смерти
	expires := time.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()

	s.mu.Lock()         // <--- ЗАКРЫВАЕМ ВСЕМ ДОСТУП
	defer s.mu.Unlock() // <--- ЗАПЛАНИРОВАЛИ ОТКРЫТИЕ

	s.items[key] = Item{
		Value:     value,
		ExpiresAt: expires,
	}
}

// Get — чтение данных (требует RLock — блокировки только для писателей)
func (s *Storage) Get(key string) (Item, bool) {
	s.mu.RLock()         // <--- ЧИТАТЕЛИ МОГУТ ЗАХОДИТЬ, ПИСАТЕЛИ НЕТ
	defer s.mu.RUnlock() // <--- ОТКРОЕМ, КОГДА ЗАКОНЧИМ

	item, ok := s.items[key]

	// Если ключа нет ИЛИ он протух
	if !ok {
		return Item{}, false
	}

	// Пассивная проверка (Lazy Expiration):
	// Если время текущее > времени смерти — считаем, что ключа нет.
	if time.Now().UnixNano() > item.ExpiresAt {
		return Item{}, false
	}

	return item, true
}

// Delete — удаление (требует Lock)
func (s *Storage) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

// StartCleanup запускает вечный цикл очистки в фоне
func (s *Storage) StartCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)

	// Запускаем горутину (аналог detached thread / background worker)
	go func() {
		// Цикл будет висеть тут и ждать "тика" от ticker.C
		for range ticker.C {
			s.cleanup()
		}
	}()
}

// cleanup — внутренняя функция, пробегает по мапе и удаляет старье
func (s *Storage) cleanup() {
	now := time.Now().UnixNano()

	s.mu.Lock() // Блокируем ВСЁ хранилище на время уборки
	defer s.mu.Unlock()

	// Пробегаем по всей мапе
	for key, item := range s.items {
		if now > item.ExpiresAt {
			delete(s.items, key)
			fmt.Printf("🧹 Janitor: удалил протухший ключ '%s'\n", key)
		}
	}
}

func main() {
	// 1. Создаем хранилище
	store := NewStorage()

	// 2. Запускаем уборщика (пусть чистит раз в 5 секунд)
	store.StartCleanup(5 * time.Second)

	// 3. Настраиваем HTTP роуты

	// Хендлер для GET /get/ключ
	http.HandleFunc("/get/", func(w http.ResponseWriter, r *http.Request) {
		// Парсим ключ из URL. Отрезаем "/get/" (5 символов)
		key := r.URL.Path[len("/get/"):]

		item, ok := store.Get(key)
		if !ok {
			http.Error(w, "Key not found or expired", http.StatusNotFound)
			return
		}

		// Отдаем JSON
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(item)
	})

	// Хендлер для POST /set
	// Ожидает JSON: {"key": "foo", "value": "bar", "ttl": 10}
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		// Временная структурка, чтобы распарсить входящий JSON
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			TTL   int    `json:"ttl"`
		}

		// Декодируем тело запроса
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad JSON", http.StatusBadRequest)
			return
		}

		// Сохраняем
		store.Set(req.Key, req.Value, req.TTL)

		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "Key '%s' saved with TTL %d seconds", req.Key, req.TTL)
	})

	// 4. Запускаем сервер
	fmt.Println("🚀 Server running on :8080")
	// ListenAndServe блокирует выполнение main, программа висит тут
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
