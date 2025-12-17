//! Binary Option Pricing Model
//!
//! Uses Historical Volatility (HV) to price binary options on crypto markets.
//!
//! KEY INSIGHT: At market launch, the strike is set ATM (at-the-money),
//! meaning the current spot price ≈ strike price. This means:
//!   - Delta ≈ 0.50 for both YES and NO
//!   - Fair value ≈ 50¢ for each side
//!   - Total fair value = 100¢ (no arbitrage in theory)
//!
//! The edge comes from:
//!   1. Buying both sides below fair value (e.g., 48¢ each = 96¢ total)
//!   2. Market inefficiencies at launch when liquidity is thin
//!   3. Slow market makers who haven't updated prices

/// Configuration for the pricing model
#[derive(Debug, Clone)]
pub struct PricingConfig {
    /// BTC annualized volatility (e.g., 0.50 = 50%)
    pub btc_hv: f64,
    /// ETH annualized volatility (e.g., 0.60 = 60%)
    pub eth_hv: f64,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            btc_hv: 0.50, // 50% annual vol for BTC
            eth_hv: 0.60, // 60% annual vol for ETH
        }
    }
}

impl PricingConfig {
    pub fn from_env() -> Self {
        let btc_hv = std::env::var("BTC_HV")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(50.0) / 100.0;

        let eth_hv = std::env::var("ETH_HV")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(60.0) / 100.0;

        Self { btc_hv, eth_hv }
    }

    pub fn get_hv(&self, ticker: &str) -> f64 {
        if ticker.contains("BTC") {
            self.btc_hv
        } else if ticker.contains("ETH") {
            self.eth_hv
        } else {
            0.50 // Default 50% vol
        }
    }
}

// ============================================================================
// MATHEMATICAL FUNCTIONS
// ============================================================================

/// Standard normal CDF approximation (Abramowitz and Stegun)
/// Accuracy: ~1e-7
pub fn norm_cdf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x / 2.0).exp();

    0.5 * (1.0 + sign * y)
}

/// Standard normal PDF
pub fn norm_pdf(x: f64) -> f64 {
    const INV_SQRT_2PI: f64 = 0.3989422804014327;
    INV_SQRT_2PI * (-0.5 * x * x).exp()
}

// ============================================================================
// BINARY OPTION PRICER
// ============================================================================

/// Binary option pricing using simplified Black-Scholes
///
/// For a binary (digital) option that pays $1 if spot > strike at expiry:
///   P(YES) = N(d2) where d2 = [ln(S/K) + (r - σ²/2)T] / (σ√T)
///
/// For 15-minute crypto markets:
/// - S = current spot price
/// - K = strike price (from market title, usually ≈ S at launch)
/// - T = time to expiry in years (15 min = 15/525600)
/// - σ = annualized volatility (historical)
/// - r = 0 (no risk-free rate for short duration)
///
/// AT LAUNCH (T=15min, S≈K):
/// - d2 ≈ 0 (since ln(S/K) ≈ 0)
/// - N(0) = 0.50
/// - Both YES and NO are worth ~50¢
#[derive(Debug, Clone)]
pub struct BinaryOptionPricer {
    /// Current spot price
    pub spot: f64,
    /// Strike price
    pub strike: f64,
    /// Time to expiry in years
    pub time_years: f64,
    /// Annualized volatility
    pub volatility: f64,
}

impl BinaryOptionPricer {
    /// Create a new pricer
    ///
    /// # Arguments
    /// * `spot` - Current spot price
    /// * `strike` - Strike price
    /// * `minutes_to_expiry` - Time until expiration in minutes
    /// * `annual_vol` - Annualized volatility (e.g., 0.50 for 50%)
    pub fn new(spot: f64, strike: f64, minutes_to_expiry: f64, annual_vol: f64) -> Self {
        // Convert minutes to years: minutes / (365.25 * 24 * 60)
        let time_years = minutes_to_expiry / 525960.0;
        Self {
            spot,
            strike,
            time_years,
            volatility: annual_vol,
        }
    }

    /// Create pricer for ATM option (spot = strike)
    /// This is the typical case at market launch
    pub fn new_atm(spot: f64, minutes_to_expiry: f64, annual_vol: f64) -> Self {
        Self::new(spot, spot, minutes_to_expiry, annual_vol)
    }

    /// Calculate d2 for binary option
    fn d2(&self) -> f64 {
        if self.time_years <= 0.0 {
            // At expiry: binary outcome based on spot vs strike
            return if self.spot > self.strike { 10.0 } else { -10.0 };
        }

        if self.volatility <= 0.0 {
            // No volatility: deterministic outcome
            return if self.spot > self.strike { 10.0 } else { -10.0 };
        }

        let sqrt_t = self.time_years.sqrt();
        let log_ratio = (self.spot / self.strike).ln();

        // d2 = [ln(S/K) - σ²T/2] / (σ√T)
        // Note: r=0 for short-term crypto
        (log_ratio - 0.5 * self.volatility.powi(2) * self.time_years) / (self.volatility * sqrt_t)
    }

    /// Fair value of YES (spot finishes above strike)
    /// Returns probability 0.0-1.0
    pub fn yes_fair_value(&self) -> f64 {
        norm_cdf(self.d2())
    }

    /// Fair value of NO (spot finishes at or below strike)
    pub fn no_fair_value(&self) -> f64 {
        1.0 - self.yes_fair_value()
    }

    /// Fair value in cents (0-100)
    pub fn yes_fair_cents(&self) -> i64 {
        (self.yes_fair_value() * 100.0).round() as i64
    }

    /// Fair value in cents (0-100)
    pub fn no_fair_cents(&self) -> i64 {
        (self.no_fair_value() * 100.0).round() as i64
    }

    /// Delta of the binary option (sensitivity to spot price)
    /// For binary: delta = n(d2) / (S * σ * √T)
    pub fn delta(&self) -> f64 {
        if self.time_years <= 0.0 || self.volatility <= 0.0 {
            return 0.0;
        }
        let sqrt_t = self.time_years.sqrt();
        norm_pdf(self.d2()) / (self.spot * self.volatility * sqrt_t)
    }

    /// Expected 1-standard-deviation move in the time period (as percentage)
    /// σ√T gives the 1-std-dev move
    pub fn expected_move_pct(&self) -> f64 {
        self.volatility * self.time_years.sqrt() * 100.0
    }

    /// Expected move in dollar terms
    pub fn expected_move_dollars(&self) -> f64 {
        self.spot * self.volatility * self.time_years.sqrt()
    }

    /// Probability of spot being within X% of current price at expiry
    pub fn prob_within_range(&self, range_pct: f64) -> f64 {
        let upper = self.spot * (1.0 + range_pct / 100.0);
        let lower = self.spot * (1.0 - range_pct / 100.0);

        let mins = self.time_years * 525960.0;
        let pricer_upper = Self::new(self.spot, upper, mins, self.volatility);
        let pricer_lower = Self::new(self.spot, lower, mins, self.volatility);

        // P(lower < S_T < upper) = P(S_T > lower) - P(S_T > upper)
        pricer_lower.yes_fair_value() - pricer_upper.yes_fair_value()
    }

    /// Calculate the edge (in cents) if buying at given prices
    /// Positive = you have edge, negative = market has edge
    pub fn edge_cents(&self, yes_price_cents: i64, no_price_cents: i64) -> i64 {
        let fair_yes = self.yes_fair_cents();
        let fair_no = self.no_fair_cents();

        // Edge = (fair - paid) for each side
        let yes_edge = fair_yes - yes_price_cents;
        let no_edge = fair_no - no_price_cents;

        yes_edge + no_edge
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Parse strike price from market title
/// Examples:
///   "Bitcoin above $104,250?" -> Some(104250.0)
///   "Bitcoin above $104,250.50?" -> Some(104250.50)
///   "Ethereum above $3,850?" -> Some(3850.0)
pub fn parse_strike_from_title(title: &str) -> Option<f64> {
    // Look for dollar amount pattern: $XXX,XXX or $XXX,XXX.XX
    let dollar_idx = title.find('$')?;
    let after_dollar = &title[dollar_idx + 1..];

    // Extract numeric characters and decimal point, skip commas
    let mut num_str = String::new();
    for c in after_dollar.chars() {
        if c.is_ascii_digit() || c == '.' {
            num_str.push(c);
        } else if c == ',' {
            continue; // Skip commas in numbers
        } else {
            break;
        }
    }

    num_str.parse().ok()
}

/// Determine if market is "above" type (YES wins if spot > strike)
pub fn is_above_market(title: &str) -> bool {
    let title_lower = title.to_lowercase();
    title_lower.contains("above") || title_lower.contains("higher") || title_lower.contains("up")
}

/// Print a detailed valuation report
pub fn print_valuation_report(
    ticker: &str,
    title: &str,
    spot: f64,
    minutes_to_expiry: f64,
    annual_vol: f64,
    market_yes_cents: Option<i64>,
    market_no_cents: Option<i64>,
) {
    let strike = parse_strike_from_title(title).unwrap_or(spot);
    let pricer = BinaryOptionPricer::new(spot, strike, minutes_to_expiry, annual_vol);

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║ BINARY OPTION VALUATION                                          ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ Ticker: {:55} ║", ticker);
    println!("║ Title:  {:55} ║", &title[..title.len().min(55)]);
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ INPUTS:                                                          ║");
    println!("║   Spot Price:     ${:>12.2}                                  ║", spot);
    println!("║   Strike Price:   ${:>12.2}                                  ║", strike);
    println!("║   Time to Expiry: {:>6.1} minutes                                ║", minutes_to_expiry);
    println!("║   Annual Vol (σ): {:>6.1}%                                       ║", annual_vol * 100.0);
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║ MODEL OUTPUT:                                                    ║");
    println!("║   d2:             {:>8.4}                                       ║", pricer.d2());
    println!("║   YES Fair Value: {:>6.1}% ({:>2}¢)                                ║",
             pricer.yes_fair_value() * 100.0, pricer.yes_fair_cents());
    println!("║   NO Fair Value:  {:>6.1}% ({:>2}¢)                                ║",
             pricer.no_fair_value() * 100.0, pricer.no_fair_cents());
    println!("║   Delta:          {:>8.4}                                       ║", pricer.delta());
    println!("║   Expected Move:  {:>6.2}% (${:.2})                             ║",
             pricer.expected_move_pct(), pricer.expected_move_dollars());
    println!("╠══════════════════════════════════════════════════════════════════╣");

    if let (Some(yes_px), Some(no_px)) = (market_yes_cents, market_no_cents) {
        let edge = pricer.edge_cents(yes_px, no_px);
        let edge_sign = if edge > 0 { "+" } else { "" };
        println!("║ MARKET PRICES:                                                   ║");
        println!("║   YES Market:     {:>2}¢  (fair: {:>2}¢, diff: {:>+3}¢)              ║",
                 yes_px, pricer.yes_fair_cents(), pricer.yes_fair_cents() - yes_px);
        println!("║   NO Market:      {:>2}¢  (fair: {:>2}¢, diff: {:>+3}¢)              ║",
                 no_px, pricer.no_fair_cents(), pricer.no_fair_cents() - no_px);
        println!("║   Total Cost:     {:>3}¢                                          ║", yes_px + no_px);
        println!("║   YOUR EDGE:      {}{:>2}¢                                          ║", edge_sign, edge);
        if edge > 0 {
            println!("║   >>> FAVORABLE: Market underpriced! <<<                         ║");
        } else if edge < 0 {
            println!("║   >>> UNFAVORABLE: Market overpriced                             ║");
        }
    }

    println!("╚══════════════════════════════════════════════════════════════════╝");
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atm_option_is_50_50() {
        // At launch, S = K (ATM), so fair value should be ~50%
        let pricer = BinaryOptionPricer::new_atm(100000.0, 15.0, 0.50);

        let yes_pct = pricer.yes_fair_value();
        let no_pct = pricer.no_fair_value();

        // Should be very close to 50/50
        assert!((yes_pct - 0.5).abs() < 0.02, "YES should be ~50%, got {:.2}%", yes_pct * 100.0);
        assert!((no_pct - 0.5).abs() < 0.02, "NO should be ~50%, got {:.2}%", no_pct * 100.0);
        assert!((yes_pct + no_pct - 1.0).abs() < 0.001, "YES + NO should = 100%");
    }

    #[test]
    fn test_itm_option() {
        // Spot well above strike = YES is ITM
        let pricer = BinaryOptionPricer::new(105000.0, 100000.0, 15.0, 0.50);

        let yes_pct = pricer.yes_fair_value();
        assert!(yes_pct > 0.7, "ITM YES should be >70%, got {:.2}%", yes_pct * 100.0);
    }

    #[test]
    fn test_otm_option() {
        // Spot well below strike = YES is OTM
        let pricer = BinaryOptionPricer::new(95000.0, 100000.0, 15.0, 0.50);

        let yes_pct = pricer.yes_fair_value();
        assert!(yes_pct < 0.3, "OTM YES should be <30%, got {:.2}%", yes_pct * 100.0);
    }

    #[test]
    fn test_higher_vol_wider_distribution() {
        let low_vol = BinaryOptionPricer::new_atm(100000.0, 15.0, 0.30);
        let high_vol = BinaryOptionPricer::new_atm(100000.0, 15.0, 0.80);

        // Higher vol = larger expected move
        assert!(high_vol.expected_move_pct() > low_vol.expected_move_pct());
    }

    #[test]
    fn test_parse_strike() {
        assert_eq!(parse_strike_from_title("Bitcoin above $104,250?"), Some(104250.0));
        assert_eq!(parse_strike_from_title("Ethereum above $3,850.50?"), Some(3850.50));
        assert_eq!(parse_strike_from_title("BTC > $100000"), Some(100000.0));
    }

    #[test]
    fn test_edge_calculation() {
        let pricer = BinaryOptionPricer::new_atm(100000.0, 15.0, 0.50);

        // Fair is ~50/50, so buying at 48/48 should give positive edge
        let edge = pricer.edge_cents(48, 48);
        assert!(edge > 0, "Buying below fair should give positive edge");

        // Buying at 52/52 should give negative edge
        let edge = pricer.edge_cents(52, 52);
        assert!(edge < 0, "Buying above fair should give negative edge");
    }

    #[test]
    fn test_15_min_expected_move() {
        // BTC at $100k with 50% annual vol
        let pricer = BinaryOptionPricer::new_atm(100000.0, 15.0, 0.50);

        let move_pct = pricer.expected_move_pct();
        let move_dollars = pricer.expected_move_dollars();

        // 15 min = 15/525960 years ≈ 0.0000285 years
        // σ√T = 0.50 * sqrt(0.0000285) ≈ 0.27%
        assert!(move_pct > 0.1 && move_pct < 0.5, "Expected move should be 0.1-0.5%, got {:.2}%", move_pct);
        println!("15-min expected move: {:.2}% (${:.2})", move_pct, move_dollars);
    }
}
