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
	Value     any   `json:"value"`
	ExpiresAt int64 `json:"expires_at"` // Unix nano timestamp
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
func (s *Storage) Set(key string, value any, ttlSeconds int) {
	expires := time.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

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
			s.cleanupProbabilistic()
		}
	}()
}

// cleanupProbabilistic — Реализация алгоритма вероятностной очистки.
// НЕ удаляет всё сразу, а щиплет по кусочкам, чтобы не грузить CPU
func (s *Storage) cleanupProbabilistic() {
	// Константы алгоритма
	const sampleSize = 20
	const maxExpiredPercentage = 25 // Если > 25% протухло, повторяем

	for {
		expiredCount := 0
		processedCount := 0
		now := time.Now().UnixNano()

		s.mu.Lock() // Блокируем на короткое время выборки

		// В Go итерация по мапе рандомная.
		// range по мапе — это и есть случайная выборка.
		for key, item := range s.items {
			if processedCount >= sampleSize {
				break
			}

			if now > item.ExpiresAt {
				delete(s.items, key)
				expiredCount++
			}
			processedCount++
		}

		s.mu.Unlock() // Быстро разблокируем

		// Логирование для наглядности (можно убрать в проде)
		if expiredCount > 0 {
			fmt.Printf("🧹 Janitor: проверил %d ключей, удалил %d\n", processedCount, expiredCount)
		}

		// Если мы проверили меньше чем sampleSize (мапа почти пустая), выходим
		if processedCount < sampleSize {
			break
		}

		// Вычисляем процент мусора
		// Если expiredCount (например 6) > 25% от 20 (это 5) — значит мусора много
		// Повторяем цикл СРАЗУ ЖЕ, не дожидаясь тикера
		if expiredCount*100/sampleSize <= maxExpiredPercentage {
			break
		}
		// Если дошли сюда — loop повторяется
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
			Value any    `json:"value"`
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
