package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

// Configuration constants - avoid magic numbers
const (
	TICKER_INTERVAL_HOURS        = 1 // Changed to 1 hour for frequent checks
	ALERT_COOLDOWN_DAYS          = 3 // Reduced from 7 to 3 days
	SUPPLY_CAP_THRESHOLD_PERCENT = 75.0
	BORROW_CAP_THRESHOLD_PERCENT = 75.0
	DECIMALS_DIVISOR             = 1e18
)

// Market utilization severity thresholds
var MARKET_UTIL_THRESHOLDS = []float64{0.91, 0.95, 0.99} // 91%, 95%, 99%

type Config struct {
	PGDSNDaily string
	PGDSNLive  string
	ChainName  string
}

type Token struct {
	Symbol     string `json:"symbol"`
	MTokenAddr string `json:"mTokenAddr"`
	Decimals   int    `json:"decimals"`
}

type MarketData struct {
	SupplyCap   float64
	BorrowCap   float64
	KinkIRM     float64
	TotalSupply float64
	TotalBorrow float64
	Utilization float64
	Price       float64
	Timestamp   int64
}

type AlertTracker struct {
	LastSupplyCapAlert time.Time
	LastBorrowCapAlert time.Time

	// Enhanced market utilization tracking
	MarketUtilState MarketUtilizationState
	mu              sync.RWMutex
}

// MarketUtilizationState tracks the complex utilization alert logic
type MarketUtilizationState struct {
	HighestThresholdReached float64               // Highest threshold reached since last alert
	ThresholdReachedTime    time.Time             // When the highest threshold was first reached
	LastUtilization         float64               // Last recorded utilization
	AlertsSent              map[float64]time.Time // Track when alerts were sent for each threshold
}

type MarketAlertsService struct {
	dbDaily      *sql.DB
	dbLive       *sql.DB
	config       Config
	tokens       []Token
	alertTracker map[string]*AlertTracker
	mu           sync.RWMutex
}

type AlertMessage struct {
	Type      string
	Title     string
	Message   string
	Timestamp time.Time
	Channel   string
}

func main() {
	log.Println("🚀 Starting Enhanced Moonwell Market Utilization Alerts Service...")

	config := loadConfig()

	// Load tokens configuration
	tokens, err := loadTokens()
	if err != nil {
		log.Fatalf("❌ Failed to load tokens: %v", err)
	}
	log.Printf("✅ Loaded %d tokens for monitoring", len(tokens))

	// Initialize database connections
	dbDaily, err := sql.Open("postgres", config.PGDSNDaily)
	if err != nil {
		log.Fatalf("❌ Daily database connection failed: %v", err)
	}
	defer dbDaily.Close()

	dbLive, err := sql.Open("postgres", config.PGDSNLive)
	if err != nil {
		log.Fatalf("❌ Live database connection failed: %v", err)
	}
	defer dbLive.Close()

	// Test connections
	if err := dbDaily.Ping(); err != nil {
		log.Fatalf("❌ Daily database ping failed: %v", err)
	}
	if err := dbLive.Ping(); err != nil {
		log.Fatalf("❌ Live database ping failed: %v", err)
	}
	log.Println("✅ Database connections established")

	// Initialize alert tracking
	alertTracker := make(map[string]*AlertTracker)
	for _, token := range tokens {
		alertTracker[token.Symbol] = &AlertTracker{
			LastSupplyCapAlert: time.Now().Add(-ALERT_COOLDOWN_DAYS * 24 * time.Hour),
			LastBorrowCapAlert: time.Now().Add(-ALERT_COOLDOWN_DAYS * 24 * time.Hour),
			MarketUtilState: MarketUtilizationState{
				HighestThresholdReached: 0,
				ThresholdReachedTime:    time.Now(),
				LastUtilization:         0,
				AlertsSent:              make(map[float64]time.Time),
			},
		}
	}

	// Initialize service
	service := &MarketAlertsService{
		dbDaily:      dbDaily,
		dbLive:       dbLive,
		config:       config,
		tokens:       tokens,
		alertTracker: alertTracker,
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

	// Start monitoring
	log.Printf("📊 Starting market monitoring with %d hour intervals", TICKER_INTERVAL_HOURS)
	log.Printf("⚠️  Alert thresholds: Supply Cap %g%%, Borrow Cap %g%%",
		SUPPLY_CAP_THRESHOLD_PERCENT, BORROW_CAP_THRESHOLD_PERCENT)
	log.Printf("📈 Market utilization thresholds: %v", MARKET_UTIL_THRESHOLDS)
	log.Printf("🔔 Alert cooldown: %d days", ALERT_COOLDOWN_DAYS)

	service.Start(ctx)
}

func loadConfig() Config {
	config := Config{ChainName: "Optimism"}

	if daily := os.Getenv("PG_DSN_DAILY"); daily != "" {
		config.PGDSNDaily = daily
	} else {
		log.Fatal("❌ PG_DSN_DAILY environment variable is required")
	}

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
	file, err := os.ReadFile("data/tokens.json")
	if err != nil {
		return nil, err
	}

	var tokens []Token
	if err := json.Unmarshal(file, &tokens); err != nil {
		return nil, err
	}

	return tokens, nil
}

func (s *MarketAlertsService) Start(ctx context.Context) {
	ticker := time.NewTicker(TICKER_INTERVAL_HOURS * time.Hour)
	defer ticker.Stop()

	// Run initial check
	log.Println("🔍 Running initial market check...")
	s.checkAllMarkets()

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Market alerts service stopping")
			return
		case <-ticker.C:
			log.Println("🔍 Running scheduled market check...")
			s.checkAllMarkets()
		}
	}
}

func (s *MarketAlertsService) checkAllMarkets() {
	var wg sync.WaitGroup
	for _, token := range s.tokens {
		wg.Add(1)
		// Check each token in a separate goroutine for resilience
		go func(t Token) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					logError(fmt.Sprintf("Panic in market check for %s: %v", t.Symbol, r))
				}
			}()

			if err := s.checkTokenMarket(t); err != nil {
				logError(fmt.Sprintf("Failed to check market for %s: %v", t.Symbol, err))
			}
		}(token)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}

func (s *MarketAlertsService) checkTokenMarket(token Token) error {
	// Get market data for this token
	marketData, err := s.getMarketData(token)
	if err != nil {
		return fmt.Errorf("failed to get market data: %w", err)
	}

	// FIXED: Thread-safe access to alertTracker
	s.mu.RLock()
	tracker := s.alertTracker[token.Symbol]
	s.mu.RUnlock()

	if tracker == nil {
		return fmt.Errorf("no alert tracker found for token %s", token.Symbol)
	}

	now := time.Now()

	// Check supply cap utilization
	if marketData.SupplyCap > 0 {
		supplyUtilization := (marketData.TotalSupply / marketData.SupplyCap) * 100
		if supplyUtilization >= SUPPLY_CAP_THRESHOLD_PERCENT {
			tracker.mu.Lock()
			shouldSendAlert := now.Sub(tracker.LastSupplyCapAlert) >= ALERT_COOLDOWN_DAYS*24*time.Hour
			if shouldSendAlert {
				tracker.LastSupplyCapAlert = now
				tracker.mu.Unlock()
				s.sendSupplyCapAlert(token, supplyUtilization, marketData)
				log.Printf("📤 Sent supply cap alert for %s (%.2f%%)", token.Symbol, supplyUtilization)
			} else {
				tracker.mu.Unlock()
			}
		}
	}

	// Check borrow cap utilization
	if marketData.BorrowCap > 0 {
		borrowUtilization := (marketData.TotalBorrow / marketData.BorrowCap) * 100
		if borrowUtilization >= BORROW_CAP_THRESHOLD_PERCENT {
			tracker.mu.Lock()
			shouldSendAlert := now.Sub(tracker.LastBorrowCapAlert) >= ALERT_COOLDOWN_DAYS*24*time.Hour
			if shouldSendAlert {
				tracker.LastBorrowCapAlert = now
				tracker.mu.Unlock()
				s.sendBorrowCapAlert(token, borrowUtilization, marketData)
				log.Printf("📤 Sent borrow cap alert for %s (%.2f%%)", token.Symbol, borrowUtilization)
			} else {
				tracker.mu.Unlock()
			}
		}
	}

	// Enhanced market utilization check with sophisticated logic
	kinkThreshold := marketData.KinkIRM / DECIMALS_DIVISOR
	if kinkThreshold > 0 {
		s.checkEnhancedMarketUtilization(token, marketData.Utilization, kinkThreshold, now)
	}

	return nil
}

// Enhanced market utilization logic
func (s *MarketAlertsService) checkEnhancedMarketUtilization(token Token, utilization, kinkThreshold float64, now time.Time) {
	s.mu.Lock()
	tracker := s.alertTracker[token.Symbol]
	s.mu.Unlock()

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	state := &tracker.MarketUtilState

	// First check if utilization is above kink
	if utilization < kinkThreshold {
		// Below kink - reset everything
		state.HighestThresholdReached = 0
		state.ThresholdReachedTime = now
		state.LastUtilization = utilization
		return
	}

	// Find current threshold level (including kink threshold)
	currentThreshold := s.getCurrentThreshold(utilization, kinkThreshold)

	log.Printf("🔍 %s: Util=%.2f%%, Kink=%.2f%%, CurrentThreshold=%.2f%%, HighestReached=%.2f%%",
		token.Symbol, utilization*100, kinkThreshold*100, currentThreshold*100, state.HighestThresholdReached*100)

	// Handle threshold progression
	if currentThreshold > state.HighestThresholdReached {
		// Utilization went UP - reached a new higher threshold
		state.HighestThresholdReached = currentThreshold
		state.ThresholdReachedTime = now
		state.LastUtilization = utilization

		// Send immediate alert for new threshold
		s.sendEnhancedMarketUtilAlert(token, utilization, currentThreshold, "IMMEDIATE")
		state.AlertsSent[currentThreshold] = now

		log.Printf("📈 %s: New threshold reached %.2f%% - immediate alert sent", token.Symbol, currentThreshold*100)

	} else if currentThreshold < state.HighestThresholdReached {
		// Utilization went DOWN - check if we need to send 3-day alert
		timeSinceHighest := now.Sub(state.ThresholdReachedTime)

		if timeSinceHighest >= ALERT_COOLDOWN_DAYS*24*time.Hour {
			// 3 days have passed since reaching the highest threshold
			// Send alert for the highest threshold that is still below current utilization
			alertThreshold := s.getHighestValidThreshold(utilization, state.HighestThresholdReached, kinkThreshold)

			if alertThreshold > 0 {
				lastSent, exists := state.AlertsSent[alertThreshold]
				if !exists || now.Sub(lastSent) >= ALERT_COOLDOWN_DAYS*24*time.Hour {
					s.sendEnhancedMarketUtilAlert(token, utilization, alertThreshold, "COOLDOWN")
					state.AlertsSent[alertThreshold] = now

					log.Printf("⏰ %s: 3-day cooldown alert sent for threshold %.2f%%", token.Symbol, alertThreshold*100)
				}
			}

			// Reset tracking for new cycle
			state.HighestThresholdReached = currentThreshold
			state.ThresholdReachedTime = now
		}

		state.LastUtilization = utilization

	} else {
		// Same threshold level - check for 3-day alert
		timeSinceHighest := now.Sub(state.ThresholdReachedTime)

		if timeSinceHighest >= ALERT_COOLDOWN_DAYS*24*time.Hour {
			// 3 days at same level
			lastSent, exists := state.AlertsSent[currentThreshold]
			if !exists || now.Sub(lastSent) >= ALERT_COOLDOWN_DAYS*24*time.Hour {
				s.sendEnhancedMarketUtilAlert(token, utilization, currentThreshold, "SUSTAINED")
				state.AlertsSent[currentThreshold] = now

				log.Printf("🔄 %s: Sustained threshold alert sent for %.2f%%", token.Symbol, currentThreshold*100)
			}

			// Reset timer for next cycle
			state.ThresholdReachedTime = now
		}

		state.LastUtilization = utilization
	}
}

// Get current threshold based on utilization (including kink threshold)
func (s *MarketAlertsService) getCurrentThreshold(utilization, kinkThreshold float64) float64 {
	// Check if utilization is above kink threshold first
	if utilization >= kinkThreshold {
		// Check specific thresholds (91%, 95%, 99%)
		for i := len(MARKET_UTIL_THRESHOLDS) - 1; i >= 0; i-- {
			if utilization >= MARKET_UTIL_THRESHOLDS[i] {
				return MARKET_UTIL_THRESHOLDS[i]
			}
		}
		// If above kink but below other thresholds, return kink threshold
		return kinkThreshold
	}
	return 0 // Below kink threshold
}

// Get highest valid threshold for alerting when utilization drops (including kink threshold)
func (s *MarketAlertsService) getHighestValidThreshold(currentUtil, highestReached, kinkThreshold float64) float64 {
	// Check specific thresholds first (91%, 95%, 99%)
	for i := len(MARKET_UTIL_THRESHOLDS) - 1; i >= 0; i-- {
		threshold := MARKET_UTIL_THRESHOLDS[i]
		// Return highest threshold that is below current utilization but was reached
		if threshold <= currentUtil && threshold <= highestReached {
			return threshold
		}
	}
	// If no specific threshold found, check kink threshold
	if kinkThreshold <= currentUtil && kinkThreshold <= highestReached {
		return kinkThreshold
	}
	return 0
}

func (s *MarketAlertsService) getMarketData(token Token) (*MarketData, error) {
	data := &MarketData{}

	// Get daily data (supply_cap, borrow_cap, kink_irm)
	dailyQuery := fmt.Sprintf(`
		SELECT supply_cap, borrow_cap, kink_irm, block_timestamp
		FROM assets_data_historical."%s"
		WHERE m_token_address = $1
		ORDER BY block_number DESC
		LIMIT 1
	`, token.Symbol)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s.dbDaily.QueryRowContext(ctx, dailyQuery, token.MTokenAddr).Scan(
		&data.SupplyCap, &data.BorrowCap, &data.KinkIRM, &data.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("daily data query failed: %w", err)
	}

	// Apply decimal conversion to daily data
	data.SupplyCap = s.convertFromRawValue(data.SupplyCap, token.Decimals)
	data.BorrowCap = s.convertFromRawValue(data.BorrowCap, token.Decimals)

	// Get live data (total_supply, total_borrows, utilization, price)
	liveQuery := fmt.Sprintf(`
		SELECT total_supply, total_borrows, utilization, price
		FROM assets_data_frequent."%s"
		WHERE m_token_address = $1
		ORDER BY block_number DESC
		LIMIT 1
	`, token.Symbol)

	err = s.dbLive.QueryRowContext(ctx, liveQuery, token.MTokenAddr).Scan(
		&data.TotalSupply, &data.TotalBorrow, &data.Utilization, &data.Price)
	if err != nil {
		return nil, fmt.Errorf("live data query failed: %w", err)
	}

	// Apply decimal conversion to live data
	data.TotalSupply = s.convertFromRawValue(data.TotalSupply, token.Decimals)
	data.TotalBorrow = s.convertFromRawValue(data.TotalBorrow, token.Decimals)
	// FIXED: Price conversion using correct formula from ETL
	// priceUSD = price / (10^(36 - decimals))
	data.Price = s.convertPriceFromRawValue(data.Price, token.Decimals)

	return data, nil
}

// Add decimal conversion function
func (s *MarketAlertsService) convertFromRawValue(rawValue float64, decimals int) float64 {
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
func (s *MarketAlertsService) convertPriceFromRawValue(rawValue float64, decimals int) float64 {
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

func (s *MarketAlertsService) sendSupplyCapAlert(token Token, utilization float64, data *MarketData) {
	alert := AlertMessage{
		Type:      "SUPPLY_CAP_ALERT",
		Title:     fmt.Sprintf("⚠️ Supply Cap Alert: %s Market", token.Symbol),
		Timestamp: time.Now(),
		Channel:   "#moonwell-alerts",
	}
	alert.Message = fmt.Sprintf(`*SUPPLY CAP UTILIZATION ALERT*
Chain: %s | Market: %s

📊 *Current Status*
• Supply cap utilization: *%.2f%%* (Threshold: %.0f%%)`,
		s.config.ChainName,
		token.Symbol,
		utilization,
		SUPPLY_CAP_THRESHOLD_PERCENT)

	s.sendAlert(alert)
}

func (s *MarketAlertsService) sendBorrowCapAlert(token Token, utilization float64, data *MarketData) {
	alert := AlertMessage{
		Type:      "BORROW_CAP_ALERT",
		Title:     fmt.Sprintf("🚨 Borrow Cap Alert: %s Market", token.Symbol),
		Timestamp: time.Now(),
		Channel:   "#moonwell-alerts",
	}
	alert.Message = fmt.Sprintf(`*BORROW CAP UTILIZATION ALERT*
Chain: %s | Market: %s

📊 *Current Status*
• Borrow cap utilization: *%.2f%%* (Threshold: %.0f%%)`,
		s.config.ChainName,
		token.Symbol,
		utilization,
		BORROW_CAP_THRESHOLD_PERCENT)

	s.sendAlert(alert)
}

func (s *MarketAlertsService) sendEnhancedMarketUtilAlert(token Token, utilization, threshold float64, alertType string) {
	var emoji, alertReason string

	switch {
	case threshold >= 0.99:
		emoji = "🚨🔥"
	case threshold >= 0.95:
		emoji = "🚨"
	case threshold >= 0.91:
		emoji = "⚠️"
	default:
		emoji = "📈"
	}

	switch alertType {
	case "IMMEDIATE":
		alertReason = fmt.Sprintf("Utilization crossed %.0f%% threshold", threshold*100)
	case "COOLDOWN":
		alertReason = fmt.Sprintf("Sustained utilization at %.0f%%", threshold*100)
	case "SUSTAINED":
		alertReason = fmt.Sprintf("Sustained at %.0f%% for 3 days", threshold*100)
	}

	alert := AlertMessage{
		Type:      "ENHANCED_MARKET_UTILIZATION_ALERT",
		Title:     fmt.Sprintf("%s %s Utilization: %.0f%%", emoji, token.Symbol, threshold*100),
		Timestamp: time.Now(),
		Channel:   "#moonwell-alerts",
	}

	alert.Message = fmt.Sprintf(`*MARKET UTILIZATION ALERT*
Chain: %s | Market: %s

📊 *Alert Details*
• Alert Reason: %s
• Current Utilization: *%.2f%%*
• Threshold Triggered: *%.0f%%*
• Alert Type: %s`,
		s.config.ChainName,
		token.Symbol,
		alertReason,
		utilization*100,
		threshold*100,
		alertType)

	s.sendAlert(alert)
}

func (s *MarketAlertsService) sendAlert(alert AlertMessage) {
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

func (s *MarketAlertsService) sendToSlack(alert AlertMessage) error {
	// Skip if no webhook URL is configured
	webhookURL := os.Getenv("SLACK_WEBHOOK_URL")
	if webhookURL == "" {
		return nil // Not an error, just not configured
	}

	// Create Slack payload
	payload := map[string]interface{}{
		"text":       alert.Message,
		"username":   "Moonwell Alerts",
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

func (s *MarketAlertsService) logToFile(alert AlertMessage) {
	timestamp := alert.Timestamp.Format("2006-01-02 15:04:05")

	formatted := fmt.Sprintf("[%s] %s: %s\n\n%s\n%s\n",
		timestamp,
		alert.Type,
		alert.Title,
		alert.Message,
		"================================================================================")

	file, err := os.OpenFile("alerts.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logError(fmt.Sprintf("Failed to open alerts.txt: %v", err))
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

// Helper function for risk level determination
func getRiskLevel(utilization float64) string {
	if utilization >= 99 {
		return "CRITICAL"
	} else if utilization >= 95 {
		return "HIGH"
	} else if utilization >= 91 {
		return "ELEVATED"
	} else if utilization >= 80 {
		return "MODERATE"
	}
	return "LOW"
}
