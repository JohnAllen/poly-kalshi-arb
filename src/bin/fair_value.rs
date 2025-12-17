//! Fair Value Calculator for Kalshi Crypto Binary Options
//!
//! Given:
//! - Minutes until/into 15-minute market expiration
//! - Strike price (e.g., $90,000)
//! - Current BTC/ETH spot price
//!
//! Outputs:
//! - Fair value for YES and NO
//! - Expected move
//! - Trading recommendations
//!
//! Usage:
//!   cargo run --release --bin fair_value -- --spot 105000 --strike 104500 --minutes 10
//!   cargo run --release --bin fair_value -- --spot 105000 --strike 104500 --minutes 10 --vol 60
//!
//! Interactive mode:
//!   cargo run --release --bin fair_value

use std::io::{self, Write};

// ============================================================================
// MATHEMATICAL FUNCTIONS
// ============================================================================

/// Standard normal CDF approximation (Abramowitz and Stegun)
fn norm_cdf(x: f64) -> f64 {
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
fn norm_pdf(x: f64) -> f64 {
    const INV_SQRT_2PI: f64 = 0.3989422804014327;
    INV_SQRT_2PI * (-0.5 * x * x).exp()
}

// ============================================================================
// BINARY OPTION FAIR VALUE CALCULATOR
// ============================================================================

/// Calculate fair value for a binary option
///
/// # Arguments
/// * `spot` - Current spot price (e.g., 105000.0 for BTC)
/// * `strike` - Strike price (e.g., 104500.0)
/// * `minutes_remaining` - Minutes until expiration (0-15 typically)
/// * `annual_vol` - Annualized volatility as decimal (e.g., 0.50 for 50%)
///
/// # Returns
/// (yes_fair_value, no_fair_value) as probabilities 0.0-1.0
pub fn calc_fair_value(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> (f64, f64) {
    // Edge cases
    if minutes_remaining <= 0.0 {
        // At expiry: binary outcome
        if spot > strike {
            return (1.0, 0.0); // YES wins
        } else {
            return (0.0, 1.0); // NO wins
        }
    }

    if annual_vol <= 0.0 {
        // No volatility: deterministic
        if spot > strike {
            return (1.0, 0.0);
        } else {
            return (0.0, 1.0);
        }
    }

    // Convert minutes to years: minutes / (365.25 * 24 * 60)
    let time_years = minutes_remaining / 525960.0;

    // d2 = [ln(S/K) - σ²T/2] / (σ√T)
    // Note: r=0 for short-term crypto (no risk-free rate)
    let sqrt_t = time_years.sqrt();
    let log_ratio = (spot / strike).ln();
    let d2 = (log_ratio - 0.5 * annual_vol.powi(2) * time_years) / (annual_vol * sqrt_t);

    // P(YES) = N(d2) for binary option
    let yes_prob = norm_cdf(d2);
    let no_prob = 1.0 - yes_prob;

    (yes_prob, no_prob)
}

/// Calculate fair value in cents (0-100)
pub fn calc_fair_value_cents(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> (i64, i64) {
    let (yes_prob, no_prob) = calc_fair_value(spot, strike, minutes_remaining, annual_vol);
    let yes_cents = (yes_prob * 100.0).round() as i64;
    let no_cents = (no_prob * 100.0).round() as i64;
    (yes_cents, no_cents)
}

/// Calculate expected 1-sigma move in the time period
pub fn calc_expected_move(spot: f64, minutes_remaining: f64, annual_vol: f64) -> (f64, f64) {
    let time_years = minutes_remaining / 525960.0;
    let move_pct = annual_vol * time_years.sqrt() * 100.0;
    let move_dollars = spot * annual_vol * time_years.sqrt();
    (move_pct, move_dollars)
}

/// Calculate delta (sensitivity to spot price change)
pub fn calc_delta(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> f64 {
    if minutes_remaining <= 0.0 || annual_vol <= 0.0 {
        return 0.0;
    }

    let time_years = minutes_remaining / 525960.0;
    let sqrt_t = time_years.sqrt();
    let log_ratio = (spot / strike).ln();
    let d2 = (log_ratio - 0.5 * annual_vol.powi(2) * time_years) / (annual_vol * sqrt_t);

    norm_pdf(d2) / (spot * annual_vol * sqrt_t)
}

/// Calculate gamma (rate of change of delta)
pub fn calc_gamma(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> f64 {
    if minutes_remaining <= 0.0 || annual_vol <= 0.0 {
        return 0.0;
    }

    let time_years = minutes_remaining / 525960.0;
    let sqrt_t = time_years.sqrt();
    let log_ratio = (spot / strike).ln();
    let d2 = (log_ratio - 0.5 * annual_vol.powi(2) * time_years) / (annual_vol * sqrt_t);

    let pdf = norm_pdf(d2);
    let denom = spot.powi(2) * annual_vol.powi(2) * time_years;

    pdf * d2.abs() / denom
}

/// Print detailed fair value analysis
fn print_analysis(spot: f64, strike: f64, minutes: f64, vol: f64) {
    let (yes_prob, no_prob) = calc_fair_value(spot, strike, minutes, vol);
    let (yes_cents, no_cents) = calc_fair_value_cents(spot, strike, minutes, vol);
    let (move_pct, move_dollars) = calc_expected_move(spot, minutes, vol);
    let delta = calc_delta(spot, strike, minutes, vol);

    let moneyness = if (spot - strike).abs() < 0.01 * spot {
        "ATM (At-The-Money)"
    } else if spot > strike {
        "ITM (In-The-Money for YES)"
    } else {
        "OTM (Out-of-The-Money for YES)"
    };

    let pct_from_strike = ((spot - strike) / strike * 100.0).abs();

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                    BINARY OPTION FAIR VALUE CALCULATOR                       ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ INPUTS:                                                                      ║");
    println!("║   Spot Price:        ${:>12.2}                                         ║", spot);
    println!("║   Strike Price:      ${:>12.2}                                         ║", strike);
    println!("║   Time Remaining:    {:>6.1} minutes                                         ║", minutes);
    println!("║   Annual Volatility: {:>6.1}%                                                ║", vol * 100.0);
    println!("║   Moneyness:         {:42}   ║", moneyness);
    println!("║   Distance:          {:>6.2}% from strike                                    ║", pct_from_strike);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ FAIR VALUES:                                                                 ║");
    println!("║   ┌─────────────────────────────────────────────────────────────────────┐   ║");
    println!("║   │  YES Fair Value:   {:>6.2}%   =   {:>2}¢                              │   ║", yes_prob * 100.0, yes_cents);
    println!("║   │  NO  Fair Value:   {:>6.2}%   =   {:>2}¢                              │   ║", no_prob * 100.0, no_cents);
    println!("║   │  Total:            {:>6.2}%   =  {:>3}¢                              │   ║", (yes_prob + no_prob) * 100.0, yes_cents + no_cents);
    println!("║   └─────────────────────────────────────────────────────────────────────┘   ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ GREEKS & EXPECTED MOVE:                                                      ║");
    println!("║   Delta:             {:>8.4} (¢ per $1 spot move)                          ║", delta * 100.0);
    println!("║   Expected 1σ Move:  {:>6.3}% (${:.2})                                      ║", move_pct, move_dollars);
    println!("║   68% Range:         ${:.2} - ${:.2}                               ║", spot - move_dollars, spot + move_dollars);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ TRADING GUIDANCE:                                                            ║");

    // Provide trading guidance
    if yes_cents <= 48 && no_cents <= 48 {
        println!("║   BUY BOTH SIDES - Total < 96¢ = GUARANTEED PROFIT                          ║");
    } else if yes_cents + no_cents < 100 {
        println!("║   Fair total = {}¢ (< 100¢) - Market may be mispriced                       ║", yes_cents + no_cents);
    }

    // Recommend based on moneyness
    if spot > strike * 1.005 {
        let breakeven = 100 - yes_cents;
        println!("║   YES is favored (spot above strike)                                        ║");
        println!("║   Buy YES if market < {}¢, Sell NO if market > {}¢                          ║", yes_cents, breakeven);
    } else if spot < strike * 0.995 {
        let breakeven = 100 - no_cents;
        println!("║   NO is favored (spot below strike)                                         ║");
        println!("║   Buy NO if market < {}¢, Sell YES if market > {}¢                          ║", no_cents, breakeven);
    } else {
        println!("║   Near ATM - Fair value ~50/50, wait for better entry                       ║");
    }

    // Time decay warning
    if minutes < 3.0 {
        println!("║   ⚠️  CAUTION: <3 min remaining - high gamma, prices will move fast          ║");
    } else if minutes < 5.0 {
        println!("║   ⚠️  NOTE: <5 min remaining - moderate gamma, be quick                      ║");
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
}

/// Print a table showing fair values across different spot prices
fn print_spot_ladder(strike: f64, minutes: f64, vol: f64, spot_center: f64) {
    println!();
    println!("Fair Value Ladder (Strike=${:.0}, {}min, {}% vol):", strike, minutes, vol * 100.0);
    println!("─────────────────────────────────────────────────────────");
    println!("{:>12}  {:>8}  {:>8}  {:>10}", "Spot", "YES¢", "NO¢", "Δ from 50");
    println!("─────────────────────────────────────────────────────────");

    let range = spot_center * 0.02; // ±2%
    let step = range / 10.0;

    for i in -10..=10 {
        let spot = spot_center + (i as f64) * step;
        let (yes_cents, no_cents) = calc_fair_value_cents(spot, strike, minutes, vol);
        let delta_from_50 = yes_cents - 50;
        let marker = if i == 0 { " <-- center" } else { "" };

        println!("{:>12.0}  {:>8}  {:>8}  {:>+10}{}", spot, yes_cents, no_cents, delta_from_50, marker);
    }
    println!("─────────────────────────────────────────────────────────");
}

/// Print a table showing fair values across different time remaining
fn print_time_ladder(spot: f64, strike: f64, vol: f64) {
    println!();
    println!("Fair Value vs Time (Spot=${:.0}, Strike=${:.0}, {}% vol):", spot, strike, vol * 100.0);
    println!("─────────────────────────────────────────────────────────");
    println!("{:>8}  {:>8}  {:>8}  {:>10}  {:>12}", "Minutes", "YES¢", "NO¢", "Exp Move%", "Exp Move$");
    println!("─────────────────────────────────────────────────────────");

    for mins in [15.0, 12.0, 10.0, 8.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.5, 0.25, 0.0] {
        let (yes_cents, no_cents) = calc_fair_value_cents(spot, strike, mins, vol);
        let (move_pct, move_dollars) = calc_expected_move(spot, mins, vol);

        println!("{:>8.2}  {:>8}  {:>8}  {:>10.3}  {:>12.2}", mins, yes_cents, no_cents, move_pct, move_dollars);
    }
    println!("─────────────────────────────────────────────────────────");
}

// ============================================================================
// CLI PARSING
// ============================================================================

#[derive(Debug)]
struct Args {
    spot: Option<f64>,
    strike: Option<f64>,
    minutes: Option<f64>,
    vol: f64,
    ladder: bool,
    time_ladder: bool,
    interactive: bool,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;

    let mut spot = None;
    let mut strike = None;
    let mut minutes = None;
    let mut vol = 0.50; // Default 50% annual vol
    let mut ladder = false;
    let mut time_ladder = false;

    while i < args.len() {
        match args[i].as_str() {
            "--spot" | "-s" => {
                i += 1;
                if i < args.len() {
                    spot = args[i].replace(",", "").parse().ok();
                }
            }
            "--strike" | "-k" => {
                i += 1;
                if i < args.len() {
                    strike = args[i].replace(",", "").parse().ok();
                }
            }
            "--minutes" | "-m" | "--time" | "-t" => {
                i += 1;
                if i < args.len() {
                    minutes = args[i].parse().ok();
                }
            }
            "--vol" | "-v" => {
                i += 1;
                if i < args.len() {
                    vol = args[i].parse::<f64>().unwrap_or(50.0) / 100.0;
                }
            }
            "--ladder" | "-l" => {
                ladder = true;
            }
            "--time-ladder" | "--tl" => {
                time_ladder = true;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    let interactive = spot.is_none() || strike.is_none() || minutes.is_none();

    Args {
        spot,
        strike,
        minutes,
        vol,
        ladder,
        time_ladder,
        interactive,
    }
}

fn print_help() {
    println!(r#"
Fair Value Calculator for Kalshi Crypto Binary Options

USAGE:
    fair_value [OPTIONS]

OPTIONS:
    --spot, -s <PRICE>      Current spot price (e.g., 105000)
    --strike, -k <PRICE>    Strike price (e.g., 104500)
    --minutes, -m <MIN>     Minutes until expiration (e.g., 10)
    --vol, -v <PCT>         Annual volatility % (default: 50)
    --ladder, -l            Show fair value ladder across spot prices
    --time-ladder, --tl     Show fair value across different times
    --help, -h              Show this help

EXAMPLES:
    # Calculate fair value for specific inputs
    fair_value --spot 105000 --strike 104500 --minutes 10

    # With custom volatility (60%)
    fair_value --spot 105000 --strike 104500 --minutes 10 --vol 60

    # Show ladder of fair values across spot prices
    fair_value --spot 105000 --strike 104500 --minutes 10 --ladder

    # Interactive mode (no arguments)
    fair_value

FORMULAS:
    For binary option paying $1 if spot > strike at expiry:

    P(YES) = N(d2)

    where:
        d2 = [ln(S/K) - σ²T/2] / (σ√T)
        S = spot price
        K = strike price
        T = time to expiry in years
        σ = annualized volatility
        N() = standard normal CDF

    Expected move (1 standard deviation):
        Move% = σ × √T × 100
        Move$ = S × σ × √T
"#);
}

fn prompt(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}

fn run_interactive() {
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("  BINARY OPTION FAIR VALUE CALCULATOR - Interactive Mode");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!();

    loop {
        let spot_str = prompt("Enter spot price (e.g., 105000) or 'q' to quit: ");
        if spot_str.to_lowercase() == "q" {
            break;
        }
        let spot: f64 = match spot_str.replace(",", "").parse() {
            Ok(v) => v,
            Err(_) => {
                println!("Invalid input, try again.");
                continue;
            }
        };

        let strike_str = prompt("Enter strike price (e.g., 104500): ");
        let strike: f64 = match strike_str.replace(",", "").parse() {
            Ok(v) => v,
            Err(_) => {
                println!("Invalid input, try again.");
                continue;
            }
        };

        let minutes_str = prompt("Enter minutes until expiration (e.g., 10): ");
        let minutes: f64 = match minutes_str.parse() {
            Ok(v) => v,
            Err(_) => {
                println!("Invalid input, try again.");
                continue;
            }
        };

        let vol_str = prompt("Enter annual volatility % (default 50): ");
        let vol: f64 = if vol_str.is_empty() {
            0.50
        } else {
            match vol_str.parse::<f64>() {
                Ok(v) => v / 100.0,
                Err(_) => 0.50,
            }
        };

        print_analysis(spot, strike, minutes, vol);

        let show_ladder = prompt("Show spot ladder? (y/n): ");
        if show_ladder.to_lowercase() == "y" {
            print_spot_ladder(strike, minutes, vol, spot);
        }

        let show_time = prompt("Show time ladder? (y/n): ");
        if show_time.to_lowercase() == "y" {
            print_time_ladder(spot, strike, vol);
        }

        println!();
    }

    println!("Goodbye!");
}

fn main() {
    let args = parse_args();

    if args.interactive {
        run_interactive();
    } else {
        let spot = args.spot.unwrap();
        let strike = args.strike.unwrap();
        let minutes = args.minutes.unwrap();

        print_analysis(spot, strike, minutes, args.vol);

        if args.ladder {
            print_spot_ladder(strike, minutes, args.vol, spot);
        }

        if args.time_ladder {
            print_time_ladder(spot, strike, args.vol);
        }
    }
}
