This is how the log lines should roughly look. : 12-26T20:24:49.565229Z  INFO poly_atm_sniper: [🚀 LIVE] ETH 3:15PM-3:30PM | ETH=$2926.55 K=$2923.39 ITM dist=0.108% | Y=88¢/90¢ N=10¢/12¢ | 4m | HOLDING | Pos N=5@43¢ | Cost=$2.15 MTM=$0.50 PnL=$-1.65

- Try to condense everything into a single log line so our eyes don't have to read multiple different things.
- Do not cargo build --release everything. Just run cargo check --bin bin_name to preserve CPU. We only have 4 cores on this and having multiple things run cargo build --release hogs all 4 cores
- We absolutely have to have the underlying crypto price and the strike price of the 15 minute markets that we are trading in or else we shouldn't trade
- Print the binary name we are trading always. 
- Prin tthe mins to market expiration always
- Do not disconnect to anything in the middle of a market
- Default to info! log level
- Print what we are doing and why in the main log line

