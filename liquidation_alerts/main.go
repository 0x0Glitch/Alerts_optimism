package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

type Config struct {
	PGDSN                   string
	LiquidationThresholdUSD float64
	AmountThreshold4H       float64
	AmountThreshold8H       float64
	AmountThreshold24H      float64
	ChainName               string
}

type AlertService struct {
	db     *sql.DB
	config Config

	// Last processed liquidation timestamps for each window
	lastProcessedTime4H  int64
	lastProcessedTime8H  int64
	lastProcessedTime24H int64

	// Last processed timestamp for individual threshold alerts
	lastProcessedTimeThreshold int64

	mu sync.RWMutex // mutex for thread‑safety
}

type Liquidation struct {
	ID                  string
	Timestamp           int64
	CollateralAsset     string
	DebtAsset           string
	BorrowerAddress     string
	LiquidatorAddress   string
	CollateralSeized    float64
	CollateralSeizedUSD float64
	DebtRepaid          float64
	DebtRepaidUSD       float64
	TransactionHash     string
}

type AlertMessage struct {
	Type      string
	Title     string
	Message   string
	Timestamp time.Time
	Channel   string
}

func main() {
	config := loadConfig()

	// DB connection --------------------------------------------------------------------
	db, err := sql.Open("postgres", config.PGDSN)
	if err != nil {
		logError(fmt.Sprintf("Database connection failed: %v", err))
		return
	}
	defer db.Close()

	// FIXED: Add connection pooling and timeout settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

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

	// Get latest liquidation timestamp from database
	var latestTimestamp int64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.QueryRowContext(ctx, "SELECT COALESCE(MAX(timestamp), 0) FROM public.liquidations").Scan(&latestTimestamp)
	if err != nil {
		log.Printf("⚠️ Failed to get latest liquidation timestamp: %v. Using current time.", err)
		latestTimestamp = time.Now().Unix()
	} else {
		log.Printf("📊 Latest liquidation timestamp: %d (%s)",
			latestTimestamp, time.Unix(latestTimestamp, 0).Format("2006-01-02 15:04:05"))
	}

	// Service struct -------------------------------------------------------------------
	service := &AlertService{
		db:                         db,
		config:                     config,
		lastProcessedTime4H:        latestTimestamp,
		lastProcessedTime8H:        latestTimestamp,
		lastProcessedTime24H:       latestTimestamp,
		lastProcessedTimeThreshold: latestTimestamp,
	}

	// Graceful shutdown ----------------------------------------------------------------
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("🛑 Shutdown signal received")
		cancel()
	}()

	// Kick‑off -------------------------------------------------------------------------
	log.Println("🚀 Moonwell Liquidation Alert Service started")
	log.Printf(
		"📊 Monitoring thresholds: Individual: $%.0f | 4H: $%.0f | 8H: $%.0f | 24H: $%.0f",
		config.LiquidationThresholdUSD,
		config.AmountThreshold4H,
		config.AmountThreshold8H,
		config.AmountThreshold24H,
	)

	service.Start(ctx)
}

// ----------------------------------------------------------------------------- CONFIG
func loadConfig() Config {
	cfg := Config{ChainName: "Optimism"}

	if dsn := os.Getenv("PG_DSN"); dsn != "" {
		cfg.PGDSN = dsn
	} else {
		log.Fatal("❌ PG_DSN environment variable is required")
	}

	parseEnvFloat := func(key string, def float64) float64 {
		if v := os.Getenv(key); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
			log.Fatalf("❌ Invalid value for %s", key)
		}
		return def
	}

	cfg.LiquidationThresholdUSD = parseEnvFloat("LIQUIDATION_THRESHOLD_USD", 50000)
	cfg.AmountThreshold4H = parseEnvFloat("AMOUNT_THRESHOLD_4H_USD", 100000)
	cfg.AmountThreshold8H = parseEnvFloat("AMOUNT_THRESHOLD_8H_USD", 250000)
	cfg.AmountThreshold24H = parseEnvFloat("AMOUNT_THRESHOLD_24H_USD", 500000)

	if chain := os.Getenv("CHAIN_NAME"); chain != "" {
		cfg.ChainName = chain
	}
	return cfg
}

// ---------------------------------------------------------------------------- SERVICE
func (s *AlertService) Start(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(4)

	// High‑value liquidation alerts (every 30 min)
	go func() { defer wg.Done(); s.runThresholdAlertsWorker(ctx) }()

	// Volume alerts (4 h / 8 h / 24 h)
	go func() { defer wg.Done(); s.runVolumeAlertsWorker(ctx, 4*time.Hour, "4H") }()
	go func() { defer wg.Done(); s.runVolumeAlertsWorker(ctx, 8*time.Hour, "8H") }()
	go func() { defer wg.Done(); s.runVolumeAlertsWorker(ctx, 24*time.Hour, "24H") }()

	wg.Wait()
	log.Println("🛑 All alert workers stopped")
}

// ------------------------ INDIVIDUAL‑LIQUIDATION WORKER ------------------------------
func (s *AlertService) runThresholdAlertsWorker(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	log.Println("📡 Threshold alerts worker started (30 min intervals)")

	// Check immediately on startup for any missed alerts
	s.checkThresholdAlerts()

	for {
		select {
		case <-ctx.Done():
			log.Println("🔴 Threshold alerts worker stopping")
			return
		case <-ticker.C:
			s.checkThresholdAlerts()
		}
	}
}

// ------------------------------ VOLUME WORKERS ---------------------------------------
func (s *AlertService) runVolumeAlertsWorker(ctx context.Context, interval time.Duration, windowName string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Printf("📡 Volume alerts worker started (%s intervals)", windowName)

	// Check immediately on startup
	s.checkVolumeAlert(interval, windowName)

	for {
		select {
		case <-ctx.Done():
			log.Printf("🔴 Volume alerts worker (%s) stopping", windowName)
			return
		case <-ticker.C:
			s.checkVolumeAlert(interval, windowName)
		}
	}
}

// -------------------------------------------------------------------  ALERT CHECKERS
func (s *AlertService) checkThresholdAlerts() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get high-value liquidations since last processed timestamp
	query := `
		SELECT id, timestamp, collateral_asset, debt_asset, borrower_address,
		       liquidator_address, collateral_seized_usd, transaction_hash
		FROM   public.liquidations
		WHERE  timestamp > $1 AND collateral_seized_usd >= $2
		ORDER  BY timestamp ASC
	`

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, query, s.lastProcessedTimeThreshold, s.config.LiquidationThresholdUSD)
	if err != nil {
		logError(fmt.Sprintf("Threshold query failed: %v", err))
		return
	}
	defer rows.Close()

	count := 0
	maxTimestamp := s.lastProcessedTimeThreshold
	for rows.Next() {
		var l Liquidation
		if err := rows.Scan(
			&l.ID, &l.Timestamp, &l.CollateralAsset, &l.DebtAsset,
			&l.BorrowerAddress, &l.LiquidatorAddress,
			&l.CollateralSeizedUSD, &l.TransactionHash,
		); err != nil {
			logError(fmt.Sprintf("Row scan failed: %v", err))
			continue
		}

		// Update max timestamp to avoid processing this liquidation again
		if l.Timestamp > maxTimestamp {
			maxTimestamp = l.Timestamp
		}

		s.sendThresholdAlert(l)
		count++
	}

	if count > 0 {
		// Update the last processed timestamp to avoid double counting
		s.lastProcessedTimeThreshold = maxTimestamp
		log.Printf("📤 Sent %d threshold alerts (processed up to timestamp %d)",
			count, maxTimestamp)
	}
}

// Check volume alerts for specific time window
func (s *AlertService) checkVolumeAlert(window time.Duration, windowName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lastProcessedTime *int64

	switch windowName {
	case "4H":
		lastProcessedTime = &s.lastProcessedTime4H
	case "8H":
		lastProcessedTime = &s.lastProcessedTime8H
	case "24H":
		lastProcessedTime = &s.lastProcessedTime24H
	default:
		return
	}

	// Calculate the window start time (current time minus window duration)
	windowStartTime := time.Now().Add(-window).Unix()

	// Get liquidations in the time window
	total, breakdown, maxTimestamp, err := s.getTotalVolumesInWindow(windowName)
	if err != nil {
		logError(fmt.Sprintf("Volume query failed for %s: %v", windowName, err))
		return
	}

	// If no liquidations found, nothing to do
	if maxTimestamp == 0 {
		return
	}

	var threshold float64
	switch windowName {
	case "4H":
		threshold = s.config.AmountThreshold4H
	case "8H":
		threshold = s.config.AmountThreshold8H
	case "24H":
		threshold = s.config.AmountThreshold24H
	}

	if total >= threshold {
		s.sendVolumeAlert(total, threshold, windowName, breakdown, time.Unix(windowStartTime, 0))
		*lastProcessedTime = maxTimestamp // Set to current time after alert
		log.Printf("📤 Sent %s volume alert (total: $%.2f, threshold: $%.2f)",
			windowName, total, threshold)
	} else {
		// Update processed time even if no alert was sent
		*lastProcessedTime = maxTimestamp
	}
}

// ------------------------------------------------------------- DB HELPERS / BUILDERS
func (s *AlertService) getTotalVolumesInWindow(windowName string) (float64, map[string]float64, int64, error) {
	// For volume alerts, we look at the entire window, not just since last processed
	windowDuration := map[string]time.Duration{
		"4H":  4 * time.Hour,
		"8H":  8 * time.Hour,
		"24H": 24 * time.Hour,
	}

	duration, exists := windowDuration[windowName]
	if !exists {
		return 0, nil, 0, fmt.Errorf("unknown window: %s", windowName)
	}

	// Calculate the full window: now - duration to now
	endTime := time.Now().Unix()
	windowStart := time.Now().Add(-duration).Unix()

	query := `
		SELECT collateral_asset, SUM(collateral_seized_usd), MAX(timestamp)
		FROM   public.liquidations
		WHERE  timestamp >= $1 AND timestamp <= $2
		GROUP  BY collateral_asset
	`

	rows, err := s.db.Query(query, windowStart, endTime)
	if err != nil {
		return 0, nil, 0, err
	}
	defer rows.Close()

	total := 0.0
	breakdown := make(map[string]float64)
	var maxTimestamp int64

	for rows.Next() {
		var asset string
		var usd float64
		var timestamp int64
		if err := rows.Scan(&asset, &usd, &timestamp); err != nil {
			logError(fmt.Sprintf("Breakdown scan failed: %v", err))
			continue
		}
		breakdown[asset] = usd
		total += usd

		if timestamp > maxTimestamp {
			maxTimestamp = timestamp
		}
	}
	return total, breakdown, maxTimestamp, nil
}

// --------------------------------------------------------------------- ALERT SENDERS
func (s *AlertService) sendThresholdAlert(l Liquidation) {
	msg := AlertMessage{
		Type:      "HIGH_VALUE_LIQUIDATION",
		Title:     fmt.Sprintf("Large Liquidation: $%.2f", l.CollateralSeizedUSD),
		Timestamp: time.Now(),
		Channel:   "#moonwell-alerts",
	}
	msg.Message = fmt.Sprintf(`*HIGH VALUE LIQUIDATION EXECUTED*
Chain: %s

💰 *Liquidation Details*
• Value: *$%.2f USD*
• Collateral: %s
• Debt Asset: %s

👥 *Participants*
• Borrower: %s
• Liquidator: %s

🔗 *Transaction*
• Hash: %s`,
		s.config.ChainName,
		l.CollateralSeizedUSD,
		l.CollateralAsset,
		l.DebtAsset,
		formatAddress(l.BorrowerAddress),
		formatAddress(l.LiquidatorAddress),
		formatTxHash(l.TransactionHash))
	logAlert(msg)
}

// Send professional volume alert
func (s *AlertService) sendVolumeAlert(total, threshold float64, window string, breakdown map[string]float64, since time.Time) {
	msg := AlertMessage{
		Type:      "VOLUME_THRESHOLD",
		Title:     fmt.Sprintf("📊 %s Liquidation Volume: $%.2f", window, total),
		Timestamp: time.Now(),
		Channel:   "#moonwell-alerts",
	}

	// Sort assets by volume for better presentation
	type assetVolume struct {
		asset  string
		volume float64
	}
	var sorted []assetVolume
	for asset, vol := range breakdown {
		sorted = append(sorted, assetVolume{asset, vol})
	}
	// Sort by volume descending
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].volume > sorted[j].volume
	})

	var detail string
	for _, av := range sorted {
		percentage := (av.volume / total) * 100
		detail += fmt.Sprintf("• %s: $%.2f USD (%.1f%%)\n",
			av.asset, av.volume, percentage)
	}

	msg.Message = fmt.Sprintf(`*LIQUIDATION VOLUME THRESHOLD EXCEEDED*
	Chain: %s | Window: %s
	• Total Liquidations: *$%.2f USD*
	• Threshold: $%.2f USD
	• Excess: $%.2f USD (+%.1f%%)
	• Window: Last %s

	💰 *Asset Breakdown*
	%s
	• Assets Affected: %d`,
		s.config.ChainName,
		window,
		total,
		threshold,
		total-threshold,
		((total-threshold)/threshold)*100,
		window,
		detail,
		len(breakdown))
	logAlert(msg)
}

// --------------------------------------------------------------------- LOG HELPERS
func logError(msg string) {
	log.Printf("[ERROR] %s", msg)
}

// Log alert messages to file and console
func logAlert(a AlertMessage) {
	stamp := a.Timestamp.Format("2006-01-02 15:04:05")
	log.Printf("🚨 ALERT: %s", a.Title)

	// Send to Slack immediately if webhook is configured
	if err := sendToSlack(a); err != nil {
		logError(fmt.Sprintf("Failed to send Slack alert for %s: %v", a.Title, err))
	} else {
		log.Printf("✅ Slack alert sent successfully: %s", a.Title)
	}

	formatted := fmt.Sprintf(
		"[%s] %s: %s\n\n%s\n%s\n",
		stamp,
		a.Type,
		a.Title,
		a.Message,
		"================================================================================",
	)

	file, err := os.OpenFile("liquidation_alerts.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logError(fmt.Sprintf("Failed to open alerts log: %v", err))
		return
	}
	defer file.Close()

	if _, err := file.WriteString(formatted); err != nil {
		logError(fmt.Sprintf("Failed to write alert: %v", err))
	}
}

func sendToSlack(alert AlertMessage) error {
	// Skip if no webhook URL is configured
	webhookURL := os.Getenv("SLACK_WEBHOOK_URL")
	if webhookURL == "" {
		return nil // Not an error, just not configured
	}

	// Create Slack payload
	payload := map[string]interface{}{
		"text":       alert.Message, // Direct markdown formatting
		"username":   "Moonwell Liquidation Bot",
		"icon_emoji": ":warning:",
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

// Helper functions for formatting
func formatAddress(addr string) string {
	return addr
}

func formatTxHash(hash string) string {
	return hash
}

// (removed unused risk helper functions)
