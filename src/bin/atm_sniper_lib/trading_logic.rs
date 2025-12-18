//! Trading logic for ATM Sniper
//!
//! Contains pure functions for trading decisions that can be unit tested.

#![allow(dead_code)]

/// Calculate bid price based on fair value and edge requirement
/// Only caps at max_bid when near ATM (fair <= 55), otherwise bids up to fair - edge
pub fn calc_bid_price(fair: i64, edge: i64, max_bid: i64) -> i64 {
    if fair <= 55 {
        (fair - edge).min(max_bid).max(1)
    } else {
        (fair - edge).max(1)
    }
}

/// Determine if we should buy YES based on edge
pub fn should_buy_yes(
    fair_yes: i64,
    yes_ask: Option<i64>,
    required_edge: i64,
    has_order: bool,
    unmatched_yes: i64,
) -> bool {
    if has_order || unmatched_yes > 0 {
        return false;
    }
    match yes_ask {
        Some(ask) => (fair_yes - ask) >= required_edge,
        None => false,
    }
}

/// Determine if we should buy NO based on edge
pub fn should_buy_no(
    fair_no: i64,
    no_ask: Option<i64>,
    required_edge: i64,
    has_order: bool,
    unmatched_no: i64,
) -> bool {
    if has_order || unmatched_no > 0 {
        return false;
    }
    match no_ask {
        Some(ask) => (fair_no - ask) >= required_edge,
        None => false,
    }
}

/// Check if YES is cheap enough to buy (regardless of fair value)
pub fn is_yes_cheap(yes_ask: Option<i64>, threshold: i64, minutes_left: i64, min_minutes: i64) -> bool {
    if minutes_left < min_minutes {
        return false;
    }
    match yes_ask {
        Some(ask) => ask <= threshold,
        None => false,
    }
}

/// Check if NO is cheap enough to buy (regardless of fair value)
/// threshold is the YES price above which NO is considered cheap
/// e.g., if threshold=80, NO is cheap when YES >= 80 (meaning NO <= 20)
pub fn is_no_cheap(no_ask: Option<i64>, cheap_no_threshold: i64, minutes_left: i64, min_minutes: i64) -> bool {
    if minutes_left < min_minutes {
        return false;
    }
    // NO is cheap if NO ask <= (100 - threshold)
    // e.g., threshold=80 means buy NO when NO ask <= 20
    match no_ask {
        Some(ask) => ask <= (100 - cheap_no_threshold),
        None => false,
    }
}

/// Calculate edge: positive means we can buy below fair value
pub fn calc_edge(fair: i64, ask: Option<i64>) -> Option<i64> {
    ask.map(|a| fair - a)
}

/// Determine if edge is large enough for aggressive IOC order
/// penny_bid = true means no liquidity, so can't aggro (nothing to hit)
pub fn should_aggro(edge: Option<i64>, aggro_threshold: i64, cooldown_ok: bool, penny_bid: bool) -> bool {
    !penny_bid && cooldown_ok && edge.map(|e| e >= aggro_threshold).unwrap_or(false)
}

/// Check if we should place a penny bid (no liquidity, fair value reasonable)
/// Returns true if ask >= 100 (no liquidity) and fair is between min_fair and max_fair
pub fn should_penny_bid(ask: Option<i64>, fair: i64, min_fair: i64, max_fair: i64) -> bool {
    let no_liquidity = ask.map(|a| a >= 100).unwrap_or(true);
    no_liquidity && fair >= min_fair && fair <= max_fair
}

/// Determine if we need to place an order
/// Checks: has_edge OR penny_bid, AND no existing order, AND not on cancel cooldown
pub fn should_place_order(
    has_edge: bool,
    penny_bid: bool,
    has_existing_order: bool,
    cancel_cooldown_ok: bool,
) -> bool {
    (has_edge || penny_bid) && !has_existing_order && cancel_cooldown_ok
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // calc_bid_price tests
    // =========================================================================

    #[test]
    fn test_calc_bid_price_atm() {
        // At ATM (fair=50), bid should be capped at max_bid
        assert_eq!(calc_bid_price(50, 15, 45), 35); // 50-15=35, min(35,45)=35
        assert_eq!(calc_bid_price(50, 5, 45), 45);  // 50-5=45, min(45,45)=45
        assert_eq!(calc_bid_price(50, 3, 45), 45);  // 50-3=47, min(47,45)=45 (capped)
    }

    #[test]
    fn test_calc_bid_price_near_atm() {
        // Near ATM (fair <= 55), still capped
        assert_eq!(calc_bid_price(55, 10, 45), 45); // 55-10=45
        assert_eq!(calc_bid_price(55, 5, 45), 45);  // 55-5=50, capped to 45
    }

    #[test]
    fn test_calc_bid_price_otm() {
        // OTM (fair > 55), not capped by max_bid
        assert_eq!(calc_bid_price(70, 10, 45), 60); // 70-10=60, not capped
        assert_eq!(calc_bid_price(80, 5, 45), 75);  // 80-5=75, not capped
    }

    #[test]
    fn test_calc_bid_price_minimum() {
        // Should never go below 1
        assert_eq!(calc_bid_price(10, 15, 45), 1); // 10-15=-5, max(-5,1)=1
        assert_eq!(calc_bid_price(5, 10, 45), 1);
    }

    // =========================================================================
    // should_buy_yes tests
    // =========================================================================

    #[test]
    fn test_should_buy_yes_with_edge() {
        // Fair=50, ask=35, edge=15 → should buy (50-35=15 >= 15)
        assert!(should_buy_yes(50, Some(35), 15, false, 0));

        // Fair=50, ask=36, edge=15 → should NOT buy (50-36=14 < 15)
        assert!(!should_buy_yes(50, Some(36), 15, false, 0));
    }

    #[test]
    fn test_should_buy_yes_no_liquidity() {
        // No ask available
        assert!(!should_buy_yes(50, None, 15, false, 0));
    }

    #[test]
    fn test_should_buy_yes_has_order() {
        // Already have order
        assert!(!should_buy_yes(50, Some(30), 15, true, 0));
    }

    #[test]
    fn test_should_buy_yes_unmatched_position() {
        // Have unmatched YES position
        assert!(!should_buy_yes(50, Some(30), 15, false, 5));
    }

    // =========================================================================
    // should_buy_no tests
    // =========================================================================

    #[test]
    fn test_should_buy_no_with_edge() {
        // Fair=50, ask=47, edge=3 → should buy (50-47=3 >= 3)
        assert!(should_buy_no(50, Some(47), 3, false, 0));

        // Fair=50, ask=48, edge=3 → should NOT buy (50-48=2 < 3)
        assert!(!should_buy_no(50, Some(48), 3, false, 0));
    }

    #[test]
    fn test_should_buy_no_aggressive_edge() {
        // With low edge requirement (3¢), more aggressive
        assert!(should_buy_no(50, Some(47), 3, false, 0));
        assert!(should_buy_no(30, Some(27), 3, false, 0));
    }

    // =========================================================================
    // is_yes_cheap tests
    // =========================================================================

    #[test]
    fn test_is_yes_cheap_below_threshold() {
        // YES at 18¢ with threshold 20¢ and 10 min left (min 8)
        assert!(is_yes_cheap(Some(18), 20, 10, 8));
        assert!(is_yes_cheap(Some(20), 20, 10, 8)); // exactly at threshold
    }

    #[test]
    fn test_is_yes_cheap_above_threshold() {
        // YES at 25¢ with threshold 20¢
        assert!(!is_yes_cheap(Some(25), 20, 10, 8));
    }

    #[test]
    fn test_is_yes_cheap_not_enough_time() {
        // Only 5 min left, need 8
        assert!(!is_yes_cheap(Some(15), 20, 5, 8));
    }

    #[test]
    fn test_is_yes_cheap_no_liquidity() {
        assert!(!is_yes_cheap(None, 20, 10, 8));
    }

    // =========================================================================
    // is_no_cheap tests
    // =========================================================================

    #[test]
    fn test_is_no_cheap_below_threshold() {
        // threshold=80 means NO is cheap when NO ask <= 20
        // NO at 18¢
        assert!(is_no_cheap(Some(18), 80, 10, 8));
        assert!(is_no_cheap(Some(20), 80, 10, 8)); // exactly at 100-80=20
    }

    #[test]
    fn test_is_no_cheap_above_threshold() {
        // NO at 25¢, threshold says NO should be <= 20
        assert!(!is_no_cheap(Some(25), 80, 10, 8));
    }

    #[test]
    fn test_is_no_cheap_not_enough_time() {
        assert!(!is_no_cheap(Some(15), 80, 5, 8));
    }

    // =========================================================================
    // calc_edge tests
    // =========================================================================

    #[test]
    fn test_calc_edge_positive() {
        // Fair value above market → positive edge (profit opportunity)
        assert_eq!(calc_edge(50, Some(35)), Some(15));
        assert_eq!(calc_edge(70, Some(50)), Some(20));
    }

    #[test]
    fn test_calc_edge_negative() {
        // Fair value below market → negative edge (no opportunity)
        assert_eq!(calc_edge(50, Some(60)), Some(-10));
    }

    #[test]
    fn test_calc_edge_no_liquidity() {
        assert_eq!(calc_edge(50, None), None);
    }

    // =========================================================================
    // should_aggro tests
    // =========================================================================

    #[test]
    fn test_should_aggro_high_edge() {
        // Edge of 25 with threshold 20, not penny bid
        assert!(should_aggro(Some(25), 20, true, false));
        assert!(should_aggro(Some(20), 20, true, false)); // exactly at threshold
    }

    #[test]
    fn test_should_aggro_low_edge() {
        // Edge of 15 with threshold 20
        assert!(!should_aggro(Some(15), 20, true, false));
    }

    #[test]
    fn test_should_aggro_on_cooldown() {
        // High edge but on cooldown
        assert!(!should_aggro(Some(30), 20, false, false));
    }

    #[test]
    fn test_should_aggro_no_liquidity() {
        assert!(!should_aggro(None, 20, true, false));
    }

    #[test]
    fn test_should_aggro_penny_bid() {
        // High edge but penny_bid = true (no liquidity to hit)
        assert!(!should_aggro(Some(30), 20, true, true));
    }

    // =========================================================================
    // should_penny_bid tests
    // =========================================================================

    #[test]
    fn test_penny_bid_no_liquidity_fair_in_range() {
        // No liquidity (ask=100), fair=50 (in 20-80 range)
        assert!(should_penny_bid(Some(100), 50, 20, 80));
        assert!(should_penny_bid(None, 50, 20, 80)); // None = no liquidity
    }

    #[test]
    fn test_penny_bid_has_liquidity() {
        // Has liquidity (ask=45), should not penny bid
        assert!(!should_penny_bid(Some(45), 50, 20, 80));
    }

    #[test]
    fn test_penny_bid_fair_too_low() {
        // No liquidity but fair=10 (below 20)
        assert!(!should_penny_bid(Some(100), 10, 20, 80));
    }

    #[test]
    fn test_penny_bid_fair_too_high() {
        // No liquidity but fair=90 (above 80)
        assert!(!should_penny_bid(Some(100), 90, 20, 80));
    }

    #[test]
    fn test_penny_bid_edge_cases() {
        // Exactly at boundaries
        assert!(should_penny_bid(Some(100), 20, 20, 80)); // min boundary
        assert!(should_penny_bid(Some(100), 80, 20, 80)); // max boundary
        assert!(!should_penny_bid(Some(100), 19, 20, 80)); // just below min
        assert!(!should_penny_bid(Some(100), 81, 20, 80)); // just above max
    }

    // =========================================================================
    // should_place_order tests
    // =========================================================================

    #[test]
    fn test_place_order_has_edge() {
        // Has edge, no existing order, not on cooldown
        assert!(should_place_order(true, false, false, true));
    }

    #[test]
    fn test_place_order_penny_bid() {
        // No edge but penny bid, no existing order, not on cooldown
        assert!(should_place_order(false, true, false, true));
    }

    #[test]
    fn test_place_order_has_existing() {
        // Has edge but already has order
        assert!(!should_place_order(true, false, true, true));
    }

    #[test]
    fn test_place_order_on_cooldown() {
        // Has edge but on cancel cooldown
        assert!(!should_place_order(true, false, false, false));
    }

    #[test]
    fn test_place_order_no_reason() {
        // No edge, no penny bid
        assert!(!should_place_order(false, false, false, true));
    }

    // =========================================================================
    // Integration scenarios
    // =========================================================================

    #[test]
    fn test_scenario_atm_market() {
        // ATM scenario: spot ≈ strike, fair value ~50 for both sides
        let fair_yes = 50;
        let fair_no = 50;
        let yes_ask = Some(45i64);
        let no_ask = Some(48i64);

        // With YES edge=15, NO edge=3
        let yes_edge = calc_edge(fair_yes, yes_ask); // 50-45=5
        let no_edge = calc_edge(fair_no, no_ask);    // 50-48=2

        assert!(!should_buy_yes(fair_yes, yes_ask, 15, false, 0)); // 5 < 15
        assert!(!should_buy_no(fair_no, no_ask, 3, false, 0));     // 2 < 3

        // But if asks were lower:
        let cheap_yes = Some(30i64); // edge = 20
        let cheap_no = Some(45i64);  // edge = 5
        assert!(should_buy_yes(fair_yes, cheap_yes, 15, false, 0)); // 20 >= 15
        assert!(should_buy_no(fair_no, cheap_no, 3, false, 0));     // 5 >= 3
    }

    #[test]
    fn test_scenario_otm_yes_itm() {
        // Spot > strike: YES is ITM (high fair value), NO is OTM (low fair value)
        let fair_yes = 75;
        let fair_no = 25;

        // YES ask at 60 → edge = 15
        // NO ask at 20 → edge = 5
        assert!(should_buy_yes(fair_yes, Some(60), 15, false, 0)); // 15 >= 15
        assert!(should_buy_no(fair_no, Some(20), 3, false, 0));    // 5 >= 3

        // Bid prices should not be capped since fair > 55
        assert_eq!(calc_bid_price(fair_yes, 15, 45), 60); // 75-15=60, not capped
        assert_eq!(calc_bid_price(fair_no, 3, 45), 22);   // 25-3=22, not capped (fair <=55)
    }

    #[test]
    fn test_scenario_cheap_options_late_market() {
        // Late in market (2 min left), cheap options shouldn't trigger
        let yes_ask = Some(15i64);
        let no_ask = Some(12i64);

        assert!(!is_yes_cheap(yes_ask, 20, 2, 8)); // not enough time
        assert!(!is_no_cheap(no_ask, 80, 2, 8));   // not enough time

        // Early in market (10 min left), should trigger
        assert!(is_yes_cheap(yes_ask, 20, 10, 8));
        assert!(is_no_cheap(no_ask, 80, 10, 8));
    }
}
