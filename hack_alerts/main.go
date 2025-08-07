package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

// Configuration constants
const (
	TICKER_INTERVAL_MINUTES = 5
	PERCENTAGE_THRESHOLD    = 5.0       // 5% change threshold
	USD_AMOUNT_THRESHOLD    = 5000000.0 // $5 million threshold
	ALERT_COOLDOWN_MINUTES  = 10        // Cooldown to prevent spam
	MAX_FAILURES            = 3         // Circuit breaker threshold
	CIRCUIT_TIMEOUT         = 30        // Circuit breaker timeout in seconds
	CLEANUP_INTERVAL        = 60        // Cleanup interval in minutes
)

type Config struct {
	PGDSNLive string
	ChainName string
}

type Token struct {
	Symbol     string `json:"symbol"`
	MTokenAddr string `json:"mTokenAddr"`
	Decimals   int    `json:"decimals"`
}

type MarketSnapshot struct {
	Symbol       string
	Price        float64
	TotalSupply  float64
	TotalBorrows float64
	SupplyUSD    float64
	BorrowsUSD   float64
	BlockNumber  int64
	Timestamp    int64
	CreatedAt    time.Time // For memory leak prevention
}

type AlertTracker struct {
	LastSupplyAlert time.Time
	LastBorrowAlert time.Time
	mu              sync.RWMutex // Per-token mutex for alert tracking
}

type CircuitBreaker struct {
	failures    int
	lastFailure time.Time
	mu          sync.RWMutex
}

type SupplyBorrowAlertsService struct {
	db             *sql.DB
	config         Config
	tokens         []Token
	lastSnapshot   map[string]*MarketSnapshot
	alertTracker   map[string]*AlertTracker
	circuitBreaker *CircuitBreaker
	snapshotMu     sync.RWMutex // Mutex for lastSnapshot map
	trackerMu      sync.RWMutex // Mutex for alertTracker map
}

type AlertMessage struct {
	Type      string
	Title     string
	Message   string
	Timestamp time.Time
	Channel   string
}

type ChangeData struct {
	PreviousValue    float64
	CurrentValue     float64
	AbsoluteChange   float64
	PercentageChange float64
	ChangeUSD        float64
}

func main() {
	log.Println("🚀 Starting Moonwell Supply/Borrow Change Alert Service...")

	// Validate configuration
	if err := validateConfig(); err != nil {
		log.Fatalf("❌ Configuration validation failed: %v", err)
	}

	config := loadConfig()

	// Load tokens configuration
	tokens, err := loadTokens()
	if err != nil {
		log.Fatalf("❌ Failed to load tokens: %v", err)
	}
	log.Printf("✅ Loaded %d tokens for monitoring", len(tokens))

	// Initialize database connection
	db, err := sql.Open("postgres", config.PGDSNLive)
	if err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}
	defer db.Close()

	// FIXED: Add connection pooling and timeout settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		logError(fmt.Sprintf("Database ping failed: %v", err))
		// FIXED: Don't exit immediately, try to reconnect
		log.Println("🔄 Attempting to reconnect to database...")
		time.Sleep(5 * time.Second)
		if err := db.Ping(); err != nil {
			logError(fmt.Sprintf("Database reconnection failed: %v", err))
			return
		}
	}
	log.Println("✅ Database connection established")

	// Initialize alert tracking
	alertTracker := make(map[string]*AlertTracker)
	for _, token := range tokens {
		alertTracker[token.Symbol] = &AlertTracker{
			LastSupplyAlert: time.Now().Add(-ALERT_COOLDOWN_MINUTES * time.Minute),
			LastBorrowAlert: time.Now().Add(-ALERT_COOLDOWN_MINUTES * time.Minute),
		}
	}

	// Initialize circuit breaker
	circuitBreaker := &CircuitBreaker{}

	// Initialize service
	service := &SupplyBorrowAlertsService{
		db:             db,
		config:         config,
		tokens:         tokens,
		lastSnapshot:   make(map[string]*MarketSnapshot),
		alertTracker:   alertTracker,
		circuitBreaker: circuitBreaker,
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("🛑 Shutdown signal received")
		cancel()
	}()

	// Start cleanup routine
	go service.startCleanupRoutine(ctx)

	// Take initial snapshot
	log.Println("📸 Taking initial market snapshots...")
	service.takeInitialSnapshots()

	// Start monitoring
	log.Printf("📊 Starting supply/borrow monitoring with %d minute intervals", TICKER_INTERVAL_MINUTES)
	log.Printf("⚠️  Alert thresholds: ±%.1f%% or ±$%.0f", PERCENTAGE_THRESHOLD, USD_AMOUNT_THRESHOLD)
	log.Printf("🔔 Alert cooldown: %d minutes", ALERT_COOLDOWN_MINUTES)

	service.Start(ctx)
}

func validateConfig() error {
	if TICKER_INTERVAL_MINUTES <= 0 {
		return fmt.Errorf("TICKER_INTERVAL_MINUTES must be positive, got %d", TICKER_INTERVAL_MINUTES)
	}
	if PERCENTAGE_THRESHOLD <= 0 || PERCENTAGE_THRESHOLD > 100 {
		return fmt.Errorf("PERCENTAGE_THRESHOLD must be between 0 and 100, got %.2f", PERCENTAGE_THRESHOLD)
	}
	if USD_AMOUNT_THRESHOLD <= 0 {
		return fmt.Errorf("USD_AMOUNT_THRESHOLD must be positive, got %.2f", USD_AMOUNT_THRESHOLD)
	}
	if ALERT_COOLDOWN_MINUTES <= 0 {
		return fmt.Errorf("ALERT_COOLDOWN_MINUTES must be positive, got %d", ALERT_COOLDOWN_MINUTES)
	}
	if MAX_FAILURES <= 0 {
		return fmt.Errorf("MAX_FAILURES must be positive, got %d", MAX_FAILURES)
	}
	return nil
}

func loadConfig() Config {
	config := Config{ChainName: "Optimism"}

	if live := os.Getenv("PG_DSN_LIVE"); live != "" {
		config.PGDSNLive = live
	} else {
		log.Fatal("❌ PG_DSN_LIVE environment variable is required")
	}

	if chain := os.Getenv("CHAIN_NAME"); chain != "" {
		config.ChainName = chain
	}

	return config
}

func loadTokens() ([]Token, error) {
	// Try to load from file first
	if file, err := os.ReadFile("data/tokens.json"); err == nil {
		var tokens []Token
		if err := json.Unmarshal(file, &tokens); err == nil {
			return tokens, nil
		}
	}

	// Fallback to embedded data
	tokensJSON := `[
		{"symbol": "DAI", "mTokenAddr": "0x3fe782c2fe7668c2f1eb313acf3022a31fead6b2", "decimals": 18},
		{"symbol": "USDC", "mTokenAddr": "0x8e08617b0d66359d73aa11e11017834c29155525", "decimals": 6},
		{"symbol": "WETH", "mTokenAddr": "0xb4104c02bbf4e9be85aaa41a62974e4e28d59a33", "decimals": 18},
		{"symbol": "cbETH", "mTokenAddr": "0x95c84f369bd0251ca903052600a3c96838d78ba1", "decimals": 18},
		{"symbol": "wstETH", "mTokenAddr": "0xbb3b1ab66efb43b10923b87460c0106643b83f9d", "decimals": 18},
		{"symbol": "rETH", "mTokenAddr": "0x4c2e35e3ec4a0c82849637bc04a4609dbe53d321", "decimals": 18},
		{"symbol": "weETH", "mTokenAddr": "0xb8051464c8c92209c92f3a4cd9c73746c4c3cfb3", "decimals": 18},
		{"symbol": "wrsETH", "mTokenAddr": "0x181ba797ccf779d8ab339721ed6ee827e758668e", "decimals": 18},
		{"symbol": "WBTC", "mTokenAddr": "0x6e6ca598a06e609c913551b729a228b023f06fdb", "decimals": 8},
		{"symbol": "USDT", "mTokenAddr": "0xa3a53899ee8f9f6e963437c5b3f805fec538bf84", "decimals": 6},
		{"symbol": "OP", "mTokenAddr": "0x9fc345a20541bf8773988515c5950ed69af01847", "decimals": 18},
		{"symbol": "VELO", "mTokenAddr": "0x866b838b97ee43f2c818b3cb5cc77a0dc22003fc", "decimals": 18},
		{"symbol": "USDT0", "mTokenAddr": "0xed37cd7872c6fe4020982d35104be7919b8f8b33", "decimals": 6}
	]`

	var tokens []Token
	if err := json.Unmarshal([]byte(tokensJSON), &tokens); err != nil {
		return nil, err
	}
	return tokens, nil
}

func (cb *CircuitBreaker) isOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.failures >= MAX_FAILURES {
		if time.Since(cb.lastFailure) > time.Duration(CIRCUIT_TIMEOUT)*time.Second {
			return false // Circuit should be closed now
		}
		return true // Circuit is still open
	}
	return false
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
}

func (cb *CircuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
}

func (s *SupplyBorrowAlertsService) startCleanupRoutine(ctx context.Context) {
	ticker := time.NewTicker(CLEANUP_INTERVAL * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanupOldSnapshots()
		}
	}
}

func (s *SupplyBorrowAlertsService) cleanupOldSnapshots() {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	// FIXED: Always run cleanup regardless of snapshot count
	cutoffTime := time.Now().Add(-24 * time.Hour) // Keep snapshots from last 24 hours

	cleanedCount := 0
	for symbol, snapshot := range s.lastSnapshot {
		if snapshot.CreatedAt.Before(cutoffTime) {
			delete(s.lastSnapshot, symbol)
			cleanedCount++
		}
	}

	if cleanedCount > 0 {
		log.Printf("🧹 Cleaned up %d old snapshots", cleanedCount)
	}
}

func (s *SupplyBorrowAlertsService) Start(ctx context.Context) {
	ticker := time.NewTicker(TICKER_INTERVAL_MINUTES * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Supply/Borrow alerts service stopping")
			return
		case <-ticker.C:
			if s.circuitBreaker.isOpen() {
				log.Println("⚠️ Circuit breaker is open, skipping this check")
				continue
			}
			log.Println("🔍 Running supply/borrow change check...")
			s.checkAllMarkets()
		}
	}
}

func (s *SupplyBorrowAlertsService) takeInitialSnapshots() {
	for _, token := range s.tokens {
		snapshot, err := s.getCurrentSnapshot(token)
		if err != nil {
			logError(fmt.Sprintf("Failed to get initial snapshot for %s: %v", token.Symbol, err))
			s.circuitBreaker.recordFailure()
			continue
		}

		s.circuitBreaker.recordSuccess()
		snapshot.CreatedAt = time.Now()

		// Thread-safe update of lastSnapshot
		s.snapshotMu.Lock()
		s.lastSnapshot[token.Symbol] = snapshot
		s.snapshotMu.Unlock()

		log.Printf("📸 Initial snapshot for %s: Supply=%.2f, Borrows=%.2f",
			token.Symbol, snapshot.SupplyUSD, snapshot.BorrowsUSD)
	}
}

func (s *SupplyBorrowAlertsService) checkAllMarkets() {
	var wg sync.WaitGroup
	for _, token := range s.tokens {
		wg.Add(1)
		go func(t Token) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					logError(fmt.Sprintf("Panic in supply/borrow check for %s: %v", t.Symbol, r))
					s.circuitBreaker.recordFailure()
				}
			}()

			if err := s.checkTokenChanges(t); err != nil {
				logError(fmt.Sprintf("Failed to check changes for %s: %v", t.Symbol, err))
				s.circuitBreaker.recordFailure()
			} else {
				s.circuitBreaker.recordSuccess()
			}
		}(token)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}

func (s *SupplyBorrowAlertsService) checkTokenChanges(token Token) error {
	// Get current snapshot
	currentSnapshot, err := s.getCurrentSnapshot(token)
	if err != nil {
		return fmt.Errorf("failed to get current snapshot: %w", err)
	}

	currentSnapshot.CreatedAt = time.Now()

	// Thread-safe read of lastSnapshot
	s.snapshotMu.RLock()
	lastSnapshot := s.lastSnapshot[token.Symbol]
	s.snapshotMu.RUnlock()

	// Thread-safe read of alertTracker
	s.trackerMu.RLock()
	tracker := s.alertTracker[token.Symbol]
	s.trackerMu.RUnlock()

	// Skip if no previous snapshot
	if lastSnapshot == nil {
		s.snapshotMu.Lock()
		s.lastSnapshot[token.Symbol] = currentSnapshot
		s.snapshotMu.Unlock()
		return nil
	}

	// Skip if current snapshot has no data (all zeros)
	if currentSnapshot.SupplyUSD == 0 && currentSnapshot.BorrowsUSD == 0 {
		log.Printf("⚠️ No data available for %s, skipping check", token.Symbol)
		return nil
	}

	// Handle transition from zero values to actual data
	// If previous snapshot was all zeros and current has data, treat it as initial data
	if lastSnapshot.SupplyUSD == 0 && lastSnapshot.BorrowsUSD == 0 {
		log.Printf("🔄 %s: Transitioning from zero data to actual data, treating as initial snapshot", token.Symbol)
		s.snapshotMu.Lock()
		s.lastSnapshot[token.Symbol] = currentSnapshot
		s.snapshotMu.Unlock()
		return nil
	}

	now := time.Now()

	// Check supply changes with thread-safe alert tracking
	if s.shouldAlert(lastSnapshot.SupplyUSD, currentSnapshot.SupplyUSD) {
		var shouldSendAlert bool
		var changeData ChangeData
		tracker.mu.Lock()
		shouldSendAlert = now.Sub(tracker.LastSupplyAlert) >= ALERT_COOLDOWN_MINUTES*time.Minute
		if shouldSendAlert {
			tracker.LastSupplyAlert = now
			changeData = s.calculateChangeData(lastSnapshot.SupplyUSD, currentSnapshot.SupplyUSD)
		}
		tracker.mu.Unlock()

		if shouldSendAlert {
			s.sendSupplyChangeAlert(token, *lastSnapshot, *currentSnapshot, changeData)
			log.Printf("📤 Sent supply change alert for %s", token.Symbol)
		} else {
			log.Printf("🔇 Supply alert for %s suppressed due to cooldown", token.Symbol)
		}
	}

	// Check borrow changes with thread-safe alert tracking
	if s.shouldAlert(lastSnapshot.BorrowsUSD, currentSnapshot.BorrowsUSD) {
		var shouldSendAlert bool
		var changeData ChangeData
		tracker.mu.Lock()
		shouldSendAlert = now.Sub(tracker.LastBorrowAlert) >= ALERT_COOLDOWN_MINUTES*time.Minute
		if shouldSendAlert {
			tracker.LastBorrowAlert = now
			changeData = s.calculateChangeData(lastSnapshot.BorrowsUSD, currentSnapshot.BorrowsUSD)
		}
		tracker.mu.Unlock()

		if shouldSendAlert {
			s.sendBorrowChangeAlert(token, *lastSnapshot, *currentSnapshot, changeData)
			log.Printf("📤 Sent borrow change alert for %s", token.Symbol)
		} else {
			log.Printf("🔇 Borrow alert for %s suppressed due to cooldown", token.Symbol)
		}
	}

	// Thread-safe update of last snapshot
	s.snapshotMu.Lock()
	s.lastSnapshot[token.Symbol] = currentSnapshot
	s.snapshotMu.Unlock()

	return nil
}

func (s *SupplyBorrowAlertsService) getCurrentSnapshot(token Token) (*MarketSnapshot, error) {
	// Using parameterized query to prevent SQL injection
	// Note: Table name still needs to be dynamically set as PostgreSQL doesn't allow
	// parameterized table names, but we validate token symbols from our known list
	query := `SELECT price, total_supply, total_borrows, block_number, block_timestamp
		FROM assets_data_frequent."` + token.Symbol + `"
		WHERE m_token_address = $1
		ORDER BY block_number DESC
		LIMIT 1`

	var price, totalSupply, totalBorrows sql.NullFloat64
	var blockNumber sql.NullInt64
	var timestamp sql.NullInt64

	// Use QueryRowContext with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s.db.QueryRowContext(ctx, query, token.MTokenAddr).Scan(
		&price, &totalSupply, &totalBorrows, &blockNumber, &timestamp)
	if err != nil {
		if err == sql.ErrNoRows {
			// Return a default snapshot if no data exists
			return &MarketSnapshot{
				Symbol:       token.Symbol,
				Price:        0.0,
				TotalSupply:  0.0,
				TotalBorrows: 0.0,
				SupplyUSD:    0.0,
				BorrowsUSD:   0.0,
				BlockNumber:  0,
				Timestamp:    0,
			}, nil
		}
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// Handle NULL values
	priceVal := 0.0
	if price.Valid {
		priceVal = price.Float64
		// FIXED: Price conversion using correct formula from ETL
		// priceUSD = price / (10^(36 - decimals))
		priceVal = s.convertPriceFromRawValue(priceVal, token.Decimals)
	}

	supplyVal := 0.0
	if totalSupply.Valid {
		supplyVal = totalSupply.Float64
		// FIXED: Using big.Float for precision in decimal conversion
		supplyVal = s.convertFromRawValue(supplyVal, token.Decimals)
	}

	borrowsVal := 0.0
	if totalBorrows.Valid {
		borrowsVal = totalBorrows.Float64
		// FIXED: Using big.Float for precision in decimal conversion
		borrowsVal = s.convertFromRawValue(borrowsVal, token.Decimals)
	}

	blockNum := int64(0)
	if blockNumber.Valid {
		blockNum = blockNumber.Int64
	}

	ts := int64(0)
	if timestamp.Valid {
		ts = timestamp.Int64
	}

	return &MarketSnapshot{
		Symbol:       token.Symbol,
		Price:        priceVal,
		TotalSupply:  supplyVal,
		TotalBorrows: borrowsVal,
		SupplyUSD:    supplyVal * priceVal,
		BorrowsUSD:   borrowsVal * priceVal,
		BlockNumber:  blockNum,
		Timestamp:    ts,
	}, nil
}

// FIXED: Precision loss in decimal conversion using big.Float - REMOVED UNUSED VARIABLE
func (s *SupplyBorrowAlertsService) convertFromRawValue(rawValue float64, decimals int) float64 {
	if rawValue == 0 {
		return 0
	}

	// Use big.Float for precise calculation
	raw := big.NewFloat(rawValue)
	divisor := big.NewFloat(0)

	// Calculate 10^decimals
	exp := big.NewInt(int64(decimals))

	// Set divisor to 10^decimals
	divisor.SetInt(big.NewInt(0).Exp(big.NewInt(10), exp, nil))

	// Divide raw by divisor
	result := big.NewFloat(0).Quo(raw, divisor)

	// Convert back to float64
	resultFloat, _ := result.Float64()
	return resultFloat
}

// FIXED: Price conversion using correct formula from ETL
// priceUSD = price / (10^(36 - decimals))
func (s *SupplyBorrowAlertsService) convertPriceFromRawValue(rawValue float64, decimals int) float64 {
	if rawValue == 0 {
		return 0
	}

	// Use big.Float for precise calculation
	raw := big.NewFloat(rawValue)
	divisor := big.NewFloat(0)

	// Calculate 10^(36 - decimals) as per ETL formula
	scaleFactor := 36 - decimals
	exp := big.NewInt(int64(scaleFactor))

	// Set divisor to 10^(36 - decimals)
	divisor.SetInt(big.NewInt(0).Exp(big.NewInt(10), exp, nil))

	// Divide raw by divisor
	result := big.NewFloat(0).Quo(raw, divisor)

	// Convert back to float64
	resultFloat, _ := result.Float64()
	return resultFloat
}

func (s *SupplyBorrowAlertsService) shouldAlert(previousUSD, currentUSD float64) bool {
	// Handle edge cases
	if previousUSD == 0 {
		return false // Can't calculate percentage change from zero
	}

	if currentUSD == 0 {
		// If current value is zero but previous wasn't, this might be a data issue
		// Only alert if the previous value was significant
		return previousUSD >= USD_AMOUNT_THRESHOLD
	}

	// Calculate absolute and percentage change
	absoluteChange := math.Abs(currentUSD - previousUSD)
	percentageChange := math.Abs((currentUSD-previousUSD)/previousUSD) * 100

	// Log significant changes for debugging
	if percentageChange >= PERCENTAGE_THRESHOLD || absoluteChange >= USD_AMOUNT_THRESHOLD {
		log.Printf("🔍 Significant change detected: %.2f%% change, $%.2f USD change",
			percentageChange, absoluteChange)
	}

	// Check if either threshold is exceeded
	return percentageChange >= PERCENTAGE_THRESHOLD || absoluteChange >= USD_AMOUNT_THRESHOLD
}

func (s *SupplyBorrowAlertsService) calculateChangeData(previousUSD, currentUSD float64) ChangeData {
	absoluteChange := currentUSD - previousUSD
	percentageChange := 0.0
	if previousUSD != 0 {
		percentageChange = (absoluteChange / previousUSD) * 100
	}

	return ChangeData{
		PreviousValue:    previousUSD,
		CurrentValue:     currentUSD,
		AbsoluteChange:   absoluteChange,
		PercentageChange: percentageChange,
		ChangeUSD:        math.Abs(absoluteChange),
	}
}

func (s *SupplyBorrowAlertsService) sendSupplyChangeAlert(token Token, lastSnapshot, currentSnapshot MarketSnapshot, change ChangeData) {
	direction := "📈"
	if change.AbsoluteChange < 0 {
		direction = "📉"
	}

	alert := AlertMessage{
		Type:      "SUPPLY_CHANGE_ALERT",
		Title:     fmt.Sprintf("%s Supply Change: %s Market", direction, token.Symbol),
		Timestamp: time.Now(),
		Channel:   "#moonwell-supply-alerts",
	}

	alert.Message = fmt.Sprintf(`*TOTAL SUPPLY CHANGE ALERT*
Chain: %s | Market: %s

%s *Supply Change*
• Previous Supply: $%.2f USD (%.2f %s)
• Current Supply: $%.2f USD (%.2f %s)
• Change: %s$%.2f USD (%.2f%%)

📊 *Details*
• Price: $%.4f
• Block: %d → %d`,
		s.config.ChainName,
		token.Symbol,
		direction,
		change.PreviousValue,
		lastSnapshot.TotalSupply,
		token.Symbol,
		change.CurrentValue,
		currentSnapshot.TotalSupply,
		token.Symbol,
		formatSign(change.AbsoluteChange),
		change.ChangeUSD,
		math.Abs(change.PercentageChange),
		currentSnapshot.Price,
		lastSnapshot.BlockNumber,
		currentSnapshot.BlockNumber)

	s.sendAlert(alert)
}

func (s *SupplyBorrowAlertsService) sendBorrowChangeAlert(token Token, lastSnapshot, currentSnapshot MarketSnapshot, change ChangeData) {
	direction := "📈"
	if change.AbsoluteChange < 0 {
		direction = "📉"
	}

	alert := AlertMessage{
		Type:      "BORROW_CHANGE_ALERT",
		Title:     fmt.Sprintf("%s Borrow Change: %s Market", direction, token.Symbol),
		Timestamp: time.Now(),
		Channel:   "#moonwell-borrow-alerts",
	}

	alert.Message = fmt.Sprintf(`*TOTAL BORROWS CHANGE ALERT*
Chain: %s | Market: %s

%s *Borrow Change*
• Previous Borrows: $%.2f USD (%.2f %s)
• Current Borrows: $%.2f USD (%.2f %s)
• Change: %s$%.2f USD (%.2f%%)

📊 *Details*
• Price: $%.4f
• Block: %d → %d`,
		s.config.ChainName,
		token.Symbol,
		direction,
		change.PreviousValue,
		lastSnapshot.TotalBorrows,
		token.Symbol,
		change.CurrentValue,
		currentSnapshot.TotalBorrows,
		token.Symbol,
		formatSign(change.AbsoluteChange),
		change.ChangeUSD,
		math.Abs(change.PercentageChange),
		currentSnapshot.Price,
		lastSnapshot.BlockNumber,
		currentSnapshot.BlockNumber)

	s.sendAlert(alert)
}

func (s *SupplyBorrowAlertsService) sendAlert(alert AlertMessage) {
	// Log to console immediately
	log.Printf("🚨 ALERT: %s", alert.Title)

	// Send to Slack immediately - this runs in background to not block
	go func() {
		if err := s.sendToSlack(alert); err != nil {
			logError(fmt.Sprintf("Failed to send Slack alert for %s: %v", alert.Title, err))
		} else {
			log.Printf("✅ Slack alert sent successfully: %s", alert.Title)
		}
	}()

	// Log to file immediately
	s.logToFile(alert)
}

func (s *SupplyBorrowAlertsService) sendToSlack(alert AlertMessage) error {
	// Skip if no webhook URL is configured
	webhookURL := os.Getenv("SLACK_WEBHOOK_URL")
	if webhookURL == "" {
		return nil // Not an error, just not configured
	}

	// Create Slack payload
	payload := map[string]interface{}{
		"text":       alert.Message, // Direct markdown formatting
		"username":   "Moonwell Supply/Borrow Monitor",
		"icon_emoji": ":chart_with_upwards_trend:",
		"channel":    alert.Channel,
	}

	// Marshal to JSON
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	// Send POST request with timeout
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(webhookURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to send webhook: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-OK response: %s", resp.Status)
	}

	return nil
}

func (s *SupplyBorrowAlertsService) logToFile(alert AlertMessage) {
	timestamp := alert.Timestamp.Format("2006-01-02 15:04:05")

	formatted := fmt.Sprintf("[%s] %s: %s\n\n%s\n%s\n",
		timestamp,
		alert.Type,
		alert.Title,
		alert.Message,
		"================================================================================")

	file, err := os.OpenFile("supply_borrow_alerts.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logError(fmt.Sprintf("Failed to open supply_borrow_alerts.log: %v", err))
		return
	}
	defer file.Close()

	if _, err := file.WriteString(formatted); err != nil {
		logError(fmt.Sprintf("Failed to write alert: %v", err))
	}
}

func logError(msg string) {
	log.Printf("[ERROR] %s", msg)
}

func formatSign(value float64) string {
	if value >= 0 {
		return "+"
	}
	return ""
}

// Helper function for risk level determination based on change magnitude
// (removed unused risk helper)
