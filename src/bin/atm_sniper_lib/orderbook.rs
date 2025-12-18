//! Orderbook management for Kalshi markets
//!
//! Kalshi sends BIDS for YES and NO separately:
//! - YES bid = highest price someone will pay for YES
//! - NO bid = highest price someone will pay for NO
//!
//! To BUY:
//! - YES ask = 100 - NO bid (you sell NO to the NO bidder)
//! - NO ask = 100 - YES bid (you sell YES to the YES bidder)

#![allow(dead_code)]

use std::collections::HashMap;

/// Orderbook for a single market
#[derive(Debug, Clone, Default)]
pub struct Orderbook {
    /// YES side: price -> qty
    pub yes_book: HashMap<i64, i64>,
    /// NO side: price -> qty
    pub no_book: HashMap<i64, i64>,
    /// Best YES bid (highest)
    pub yes_bid: Option<i64>,
    /// Best NO bid (highest)
    pub no_bid: Option<i64>,
}

impl Orderbook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Recalculate best bids from the books
    fn update_best_bids(&mut self) {
        self.yes_bid = self.yes_book.keys().max().copied();
        self.no_bid = self.no_book.keys().max().copied();
    }

    /// Process orderbook snapshot - replaces entire book
    pub fn process_snapshot(&mut self, yes_levels: &Option<Vec<Vec<i64>>>, no_levels: &Option<Vec<Vec<i64>>>) {
        // YES orderbook
        if let Some(levels) = yes_levels {
            self.yes_book.clear();
            for level in levels {
                if level.len() >= 2 {
                    let price = level[0];
                    let qty = level[1];
                    if qty > 0 {
                        self.yes_book.insert(price, qty);
                    }
                }
            }
        }
        // NO orderbook
        if let Some(levels) = no_levels {
            self.no_book.clear();
            for level in levels {
                if level.len() >= 2 {
                    let price = level[0];
                    let qty = level[1];
                    if qty > 0 {
                        self.no_book.insert(price, qty);
                    }
                }
            }
        }
        self.update_best_bids();
    }

    /// Process orderbook delta - updates existing book
    pub fn process_delta(&mut self, yes_levels: &Option<Vec<Vec<i64>>>, no_levels: &Option<Vec<Vec<i64>>>) {
        // YES delta
        if let Some(levels) = yes_levels {
            for level in levels {
                if level.len() >= 2 {
                    let price = level[0];
                    let qty = level[1];
                    if qty > 0 {
                        self.yes_book.insert(price, qty);
                    } else {
                        self.yes_book.remove(&price);
                    }
                }
            }
        }
        // NO delta
        if let Some(levels) = no_levels {
            for level in levels {
                if level.len() >= 2 {
                    let price = level[0];
                    let qty = level[1];
                    if qty > 0 {
                        self.no_book.insert(price, qty);
                    } else {
                        self.no_book.remove(&price);
                    }
                }
            }
        }
        self.update_best_bids();
    }

    /// Get YES ask price (cost to buy YES = 100 - NO bid)
    pub fn yes_ask(&self) -> Option<i64> {
        self.no_bid.map(|b| 100 - b)
    }

    /// Get NO ask price (cost to buy NO = 100 - YES bid)
    pub fn no_ask(&self) -> Option<i64> {
        self.yes_bid.map(|b| 100 - b)
    }

    /// Get YES ask with default (100 = no liquidity)
    pub fn yes_ask_or_100(&self) -> i64 {
        self.yes_ask().unwrap_or(100)
    }

    /// Get NO ask with default (100 = no liquidity)
    pub fn no_ask_or_100(&self) -> i64 {
        self.no_ask().unwrap_or(100)
    }

    /// Check if there's liquidity on both sides
    pub fn has_liquidity(&self) -> bool {
        self.yes_bid.is_some() && self.no_bid.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot() {
        let mut book = Orderbook::new();

        // Snapshot: YES bids at 45, 40; NO bids at 50, 48
        let yes_levels = Some(vec![vec![45, 100], vec![40, 50]]);
        let no_levels = Some(vec![vec![50, 100], vec![48, 50]]);

        book.process_snapshot(&yes_levels, &no_levels);

        assert_eq!(book.yes_bid, Some(45));
        assert_eq!(book.no_bid, Some(50));

        // YES ask = 100 - NO bid = 100 - 50 = 50
        assert_eq!(book.yes_ask(), Some(50));
        // NO ask = 100 - YES bid = 100 - 45 = 55
        assert_eq!(book.no_ask(), Some(55));
    }

    #[test]
    fn test_delta_add() {
        let mut book = Orderbook::new();

        // Initial snapshot
        book.process_snapshot(
            &Some(vec![vec![45, 100]]),
            &Some(vec![vec![50, 100]]),
        );

        // Delta: new YES bid at 47
        book.process_delta(
            &Some(vec![vec![47, 50]]),
            &None,
        );

        assert_eq!(book.yes_bid, Some(47)); // Updated to new best
        assert_eq!(book.no_bid, Some(50));  // Unchanged
    }

    #[test]
    fn test_delta_remove() {
        let mut book = Orderbook::new();

        // Initial snapshot
        book.process_snapshot(
            &Some(vec![vec![45, 100], vec![40, 50]]),
            &Some(vec![vec![50, 100]]),
        );

        assert_eq!(book.yes_bid, Some(45));

        // Delta: remove YES bid at 45 (qty = 0)
        book.process_delta(
            &Some(vec![vec![45, 0]]),
            &None,
        );

        assert_eq!(book.yes_bid, Some(40)); // Falls back to next best
    }

    #[test]
    fn test_no_liquidity() {
        let book = Orderbook::new();

        assert_eq!(book.yes_ask(), None);
        assert_eq!(book.no_ask(), None);
        assert_eq!(book.yes_ask_or_100(), 100);
        assert_eq!(book.no_ask_or_100(), 100);
        assert!(!book.has_liquidity());
    }
}
