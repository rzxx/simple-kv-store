package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// --- CONSTANTS ---
const (
	LevelError = 0
	LevelInfo  = 1 // Базовые события (Persistence, Upstream calls, Cleanup summary)
	LevelDebug = 2 // Шум (HTTP запросы, детали циклов)
)

// --- CONFIG ---

type Config struct {
	Server      ServerConfig   `json:"server"`
	Cleanup     CleanupConfig  `json:"cleanup"`
	Upstream    UpstreamConfig `json:"upstream"`
	Persistence PersistConfig  `json:"persistence"`
}

type ServerConfig struct {
	Port     string `json:"port"`
	LogLevel int    `json:"log_level"` // UPDATED: int вместо bool
}

type CleanupConfig struct {
	Enabled               bool                `json:"enabled"`
	IntervalSeconds       int                 `json:"interval_seconds"`
	Mode                  string              `json:"mode"`
	ProbabilisticSettings ProbabilisticConfig `json:"probabilistic_settings"`
}

type ProbabilisticConfig struct {
	SampleSize       int `json:"sample_size"`
	ThresholdPercent int `json:"threshold_percent"`
}

type UpstreamConfig struct {
	Enabled           bool   `json:"enabled"`
	URL               string `json:"url"`
	DefaultTTLSeconds int    `json:"default_ttl_seconds"`
}

type PersistConfig struct {
	Enabled             bool   `json:"enabled"`
	Filename            string `json:"filename"`
	SaveIntervalSeconds int    `json:"save_interval_seconds"`
}

func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	cfg := &Config{}
	if err := json.NewDecoder(file).Decode(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// --- STORAGE ---

type Item struct {
	Value     any   `json:"value"`
	ExpiresAt int64 `json:"expires_at"`
}

type Storage struct {
	items map[string]Item
	mu    sync.RWMutex
	cfg   *Config
}

func NewStorage(cfg *Config) *Storage {
	return &Storage{
		items: make(map[string]Item),
		cfg:   cfg,
	}
}

// Log теперь принимает уровень важности (level)
func (s *Storage) Log(level int, format string, args ...any) {
	if s.cfg.Server.LogLevel >= level {
		// Добавляем красивый префикс и время
		prefix := "[INFO]"
		if level == LevelDebug {
			prefix = "[DEBUG]"
		}

		timestamp := time.Now().Format("15:04:05")
		// Пример: 18:30:00 [INFO] Сообщение...
		fmt.Printf("%s %s %s\n", timestamp, prefix, fmt.Sprintf(format, args...))
	}
}

// --- LOGIC ---

func (s *Storage) Set(key string, value any, ttlSeconds int) {
	expires := time.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = Item{Value: value, ExpiresAt: expires}

	// Логируем установку ключа только на уровне Debug
	s.Log(LevelDebug, "SET key='%s', ttl=%ds", key, ttlSeconds)
}

func (s *Storage) Get(key string) (Item, bool) {
	s.mu.RLock()
	item, ok := s.items[key]
	s.mu.RUnlock()

	if ok && time.Now().UnixNano() <= item.ExpiresAt {
		return item, true
	}

	if !ok && s.cfg.Upstream.Enabled && s.cfg.Upstream.URL != "" {
		// Это важное событие — хождение во внешнюю сеть. Level Info.
		s.Log(LevelInfo, "🌐 Miss! Fetching '%s' from upstream...", key)
		return s.fetchFromUpstream(key)
	}

	return Item{}, false
}

func (s *Storage) fetchFromUpstream(key string) (Item, bool) {
	start := time.Now()
	resp, err := http.Get(s.cfg.Upstream.URL + "/" + key)
	if err != nil || resp.StatusCode != http.StatusOK {
		s.Log(LevelDebug, "Upstream error for '%s': %v", key, err)
		return Item{}, false
	}
	defer resp.Body.Close()

	var remoteValue any
	if err := json.NewDecoder(resp.Body).Decode(&remoteValue); err != nil {
		return Item{}, false
	}

	s.Set(key, remoteValue, s.cfg.Upstream.DefaultTTLSeconds)

	s.Log(LevelDebug, "Upstream success for '%s' in %v", key, time.Since(start))

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.items[key], true
}

// --- CLEANUP ---

func (s *Storage) StartBackgroundJobs() {
	if s.cfg.Cleanup.Enabled {
		go func() {
			ticker := time.NewTicker(time.Duration(s.cfg.Cleanup.IntervalSeconds) * time.Second)
			for range ticker.C {
				s.cleanupProbabilistic()
			}
		}()
	}
	if s.cfg.Persistence.Enabled && s.cfg.Persistence.SaveIntervalSeconds > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(s.cfg.Persistence.SaveIntervalSeconds) * time.Second)
			for range ticker.C {
				s.SaveToFile()
			}
		}()
	}
}

func (s *Storage) cleanupProbabilistic() {
	sampleSize := s.cfg.Cleanup.ProbabilisticSettings.SampleSize
	threshold := s.cfg.Cleanup.ProbabilisticSettings.ThresholdPercent

	totalDeleted := 0
	loops := 0

	for {
		expiredCount := 0
		processedCount := 0
		now := time.Now().UnixNano()

		s.mu.Lock()
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
		s.mu.Unlock()

		totalDeleted += expiredCount
		loops++

		if processedCount < sampleSize {
			break
		}
		if expiredCount*100/sampleSize <= threshold {
			break
		}
		// Если Level 2, то пишем, что заходим на второй круг
		s.Log(LevelDebug, "🧹 Janitor: high load, repeating loop...")
	}

	// Если что-то удалили, пишем INFO. Если нет - молчим (чтобы не засорять логи на Level 1)
	if totalDeleted > 0 {
		s.Log(LevelInfo, "🧹 Janitor: removed %d keys (in %d loops)", totalDeleted, loops)
	}
}

// --- PERSISTENCE ---

func (s *Storage) SaveToFile() error {
	if !s.cfg.Persistence.Enabled {
		return nil
	}
	filename := s.cfg.Persistence.Filename

	s.mu.RLock()
	defer s.mu.RUnlock()

	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("❌ Error creating dump: %v\n", err) // Ошибки всегда пишем
		return err
	}
	defer file.Close()

	// Level 1: Важное системное событие
	s.Log(LevelInfo, "💾 Saving snapshot to '%s'...", filename)
	return json.NewEncoder(file).Encode(s.items)
}

func (s *Storage) LoadFromFile() error {
	if !s.cfg.Persistence.Enabled {
		return nil
	}
	filename := s.cfg.Persistence.Filename

	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			s.Log(LevelInfo, "📂 No dump file found, starting fresh.")
			return nil
		}
		return err
	}
	defer file.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Log(LevelInfo, "📂 Loading snapshot from '%s'...", filename)
	return json.NewDecoder(file).Decode(&s.items)
}

// --- MIDDLEWARE ---

// loggingMiddleware оборачивает любой Handler и добавляет логирование
func loggingMiddleware(next http.HandlerFunc, store *Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Вызываем настоящий хендлер
		next(w, r)

		// Логируем ПОСЛЕ выполнения, чтобы знать время работы
		// Level 2: Нам интересны все запросы только в режиме отладки
		store.Log(LevelDebug, "HTTP %s %s took %v", r.Method, r.URL.Path, time.Since(start))
	}
}

// --- MAIN ---

func main() {
	cfg, err := LoadConfig("config.json")
	if err != nil {
		// Это Level 0 (Критическая ошибка)
		fmt.Printf("❌ Failed to load config: %v\n", err)
		return
	}

	store := NewStorage(cfg)
	if err := store.LoadFromFile(); err != nil {
		fmt.Printf("❌ Error loading dump: %v\n", err)
	}

	store.StartBackgroundJobs()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n🛑 Shutting down...")
		store.SaveToFile()
		os.Exit(0)
	}()

	// Оборачиваем хендлеры в Middleware
	http.HandleFunc("/get/", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/get/"):]
		item, ok := store.Get(key)
		if !ok {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(item)
	}, store))

	http.HandleFunc("/set", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
			TTL   int    `json:"ttl"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad JSON", http.StatusBadRequest)
			return
		}
		store.Set(req.Key, req.Value, req.TTL)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "Saved '%s'", req.Key)
	}, store))

	// Level 0: Это сообщение должно быть всегда
	fmt.Printf("🚀 Server running on port %s (Log Level: %d)\n", cfg.Server.Port, cfg.Server.LogLevel)
	if err := http.ListenAndServe(cfg.Server.Port, nil); err != nil {
		panic(err)
	}
}
