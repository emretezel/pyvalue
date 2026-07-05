# Metrics Catalog

This file is the single source of truth for supported `pyvalue` metrics.

Columns:
- English Descriptive Name of the Metric
- `pyvalue` key
- How is it calculated
- Why is it important in identifying quality/value stocks

## Liquidity / Balance Sheet

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Working Capital | `working_capital` | Latest `AssetsCurrent - LiabilitiesCurrent`. | Positive working capital helps a business absorb near-term shocks without forced financing. |
| Current Ratio | `current_ratio` | Latest `AssetsCurrent / LiabilitiesCurrent`. | A business that can comfortably cover short-term obligations is less fragile. |
| Net Current Asset Value | `ncav` | Latest `AssetsCurrent - Liabilities` (total liabilities, not just current); negative values are emitted. | Graham's crudest liquidation proxy: what remains for shareholders after every claim is settled from current assets alone. |
| Price to NCAV | `price_to_ncav` | EODHD-oriented: `market_cap / ncav`, only when `ncav > 0`. | The Graham net-net test: `price_to_ncav <= 0.67` flags stocks trading below two-thirds of liquidation value, the classic deep-value margin of safety. |
| Net Working Capital (Most Recent Quarter) | `nwc_mqr` | EODHD-oriented: `(AssetsCurrent - Cash) - (LiabilitiesCurrent - ShortTermDebt)` using cash and short-debt fallbacks. | Focuses on operating working capital rather than cash hoards or financing noise. |
| Net Working Capital (Fiscal Year) | `nwc_fy` | EODHD-oriented FY version of the same adjusted NWC formula used by `nwc_mqr`. | Gives an annual baseline for working-capital intensity. |
| Delta Net Working Capital (TTM Style) | `delta_nwc_ttm` | EODHD-oriented: `NWC(MRQ) - NWC(same fiscal quarter last year)` with strict quarter matching. | Highlights whether the business is tying up more capital in operations year over year. |
| Delta Net Working Capital (Fiscal Year) | `delta_nwc_fy` | EODHD-oriented: `NWC(latest FY) - NWC(prior FY)`. | Shows annual change in operating capital demands. |
| Net Working Capital Maintenance | `delta_nwc_maint` | EODHD-oriented: `max(average(last 3 FY deltas of NWC), 0)`. The FY-series owner-earnings metrics reuse the same rule per fiscal year: `maint_nwc_FY(y)` is the trailing 3-delta average as of year `y`, floored at 0, and exists only when NWC points cover `y` through `y-3`. | Converts multi-year NWC drift into a conservative maintenance drag used in owner-earnings style analysis. |

## Leverage / Coverage

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Long-Term Debt | `long_term_debt` | Latest reported long-term debt fact (current and noncurrent components plus fallback rollups are resolved during EODHD normalization). | High debt can destroy value when a business hits a downturn or refinancing window. |
| Short-Term Debt Share | `short_term_debt_share` | EODHD-oriented: `ShortTermDebt / TotalDebt`, with denominator falling back to `TotalDebtFromBalanceSheet` when needed. | Measures refinancing pressure by showing how much debt matures soon. |
| Debt Paydown Years | `debt_paydown_years` | EODHD-oriented: `TotalDebt / FCF_TTM`, where debt uses layered fallback and `FCF_TTM = OCF_TTM - Capex_TTM`. | Lower values indicate the balance sheet could be repaired faster with current cash generation. |
| Free Cash Flow to Debt | `fcf_to_debt` | EODHD-oriented reciprocal of debt paydown years: `FCF_TTM / TotalDebt`. | Higher values show stronger debt-service capacity from internally generated cash. |
| Net Debt to EBITDA | `net_debt_to_ebitda` | EODHD-oriented: `NetDebt / EBITDA_TTM`, where `EBITDA_TTM = EBIT_TTM + D&A_TTM` and cash/debt use fallback chains. | A widely used leverage check that compares debt burden to operating cash-earnings power. |
| Interest Coverage | `interest_coverage` | EODHD-oriented: `EBIT_TTM / InterestExpense_TTM`, with rescue-only fallback from `interestIncome - netInterestIncome`. | Tests whether operating profit comfortably covers financing cost. |

## Cash Flow / Cash Conversion

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| CFO to Net Income (TTM) | `cfo_to_ni_ttm` | EODHD-oriented: `CFO_TTM / NetIncome_TTM`, where net income prefers `NetIncomeLoss` then `NetIncomeLossAvailableToCommonStockholdersBasic`. | High cash conversion suggests reported earnings are backed by real cash generation. |
| CFO to Net Income (10Y Median) | `cfo_to_ni_10y_median` | EODHD-oriented median of `CFO_FY / NetIncome_FY` over the latest consecutive joint CFO+NI FY chain, capped at 10 years and stopping at the first missing joint year. Loss years (`NetIncome_FY <= 0`) are skipped — their ratio is sign-ambiguous — and at least 6 positive-NI points are required. Freshness (400d) and `as_of` anchor on the latest joint year even when it is a skipped loss year. | Shows whether earnings quality is durable across a full cycle, not just one strong year — without letting a single loss year (cyclical trough, one-off writedown) void an otherwise healthy decade; loss years themselves are policed by the stability metrics. |
| Free Cash Flow Median (5Y FY) | `fcf_fy_median_5y` | EODHD-oriented median of latest 5 available FY points, where `FCF_FY = OCF_FY - Capex_FY` and missing capex is treated as `0`. | Normalizes free-cash-flow power instead of relying on a single possibly peak year. |
| Free Cash Flow Negative Years (10Y) | `fcf_neg_years_10y` | EODHD-oriented count of years with `FCF_FY < 0` across the latest strict 10 consecutive FY window. | Frequent negative FCF can indicate weak economics or heavy ongoing reinvestment needs. |
| Net Income Loss Years (10Y) | `ni_loss_years_10y` | Count of years with `NetIncome_FY < 0` across the latest strict 10 consecutive FY window, with FY net-income fallback to common-shareholders NI. | A quick resilience test: repeated loss years are a warning sign for quality and valuation stability. |
| Accruals Ratio | `accruals_ratio` | EODHD-oriented: `(NetIncome_TTM - CFO_TTM) / AvgTotalAssets`, where `AvgTotalAssets` uses strict same-quarter prior-year averaging. | Lower or negative accruals usually indicate cleaner, cash-backed earnings. |
| Stock-Based Compensation to Revenue | `sbc_to_revenue` | EODHD-oriented: `StockBasedCompensation_TTM / Revenues_TTM`. | Shows how much top-line output is offset by equity compensation and potential dilution. |
| Stock-Based Compensation to Free Cash Flow | `sbc_to_fcf` | EODHD-oriented: `StockBasedCompensation_TTM / FCF_TTM`, with `FCF_TTM = OCF_TTM - Capex_TTM` and capex missing treated as `0`. | Helps judge whether apparent cash generation is being offset by large stock comp. |
| Free Cash Flow to EBITDA | `fcf_to_ebitda` | EODHD-oriented: `FCF_TTM / EBITDA_TTM`, where `FCF_TTM = OCF_TTM - Capex_TTM` (missing capex treated as `0`) and `EBITDA_TTM = EBIT_TTM + D&A_TTM`; only when `EBITDA_TTM > 0`, negative FCF allowed. | Operationalizes Buffett's critique that EBITDA ignores capex: high conversion shows EBITDA is backed by real free cash flow, low or negative conversion flags capital-hungry earnings. |

## Profitability / Returns

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Gross Margin (TTM) | `gross_margin_ttm` | `(Revenue_TTM - COGS_TTM) / Revenue_TTM`, where `COGS_TTM` prefers normalized `CostOfRevenue` and falls back to `Revenue - GrossProfit`; clamped to `[-1, 1]`. | A high and stable gross margin usually points to pricing power or a structurally advantaged business model. |
| Operating Margin (TTM) | `operating_margin_ttm` | `EBIT_TTM / Revenue_TTM`. | Shows how much revenue survives normal operating costs before financing and tax noise. |
| Free Cash Flow Margin (TTM) | `fcf_margin_ttm` | `FCF_TTM / Revenue_TTM`, where `FCF_TTM = OCF_TTM - Capex_TTM` and capex missing is treated as `0`. | Tests whether accounting profitability is translating into real free cash generation. |
| Return on Equity (TTM) | `roe_ttm` | `NetIncome_TTM / AvgCommonEquity`, using quarterly same-quarter averaging first and strict FY fallback. | Highlights how efficiently management turns common equity into earnings. |
| Return on Assets (TTM) | `roa_ttm` | `NetIncome_TTM / AvgTotalAssets`, where assets use strict same-quarter prior-year averaging. | Helps compare earnings power across firms with different leverage. |
| Return on Tangible Common Equity (TTM) | `roetce_ttm` | `NetIncome_TTM / AvgTangibleCommonEquity`, where `TangibleCommonEquity = CommonEquity - Goodwill - Intangibles` with missing goodwill/intangibles treated as `0`. | Reduces goodwill-driven ROE distortion and is especially useful for acquisitive businesses. |
| Gross Profit to Assets (TTM) | `gross_profit_to_assets_ttm` | `(Revenue_TTM - COGS_TTM) / AvgTotalAssets`, using the same TTM gross-profit logic as `gross_margin_ttm` and the accruals-style asset average. | A strong ratio indicates the asset base is generating a lot of gross economic output before overhead. |

## Owner Earnings

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Owner Earnings Equity (TTM) | `oe_equity_ttm` | EODHD-oriented: `NI_TTM + D&A_TTM - MCapex_TTM - delta_nwc_maint`. | Approximates cash earnings available to equity after maintenance reinvestment and working-capital drag. |
| Owner Earnings Equity (5Y Average) | `oe_equity_5y_avg` | EODHD-oriented average of latest 5 available FY points of `NI_FY + D&A_FY - MCapex_FY - maint_nwc_FY(y)`, each year subtracting its own trailing 3-delta maintenance NWC (see `delta_nwc_maint`); a year without that chain yields no point, so up to 8 consecutive FY of NWC history is needed. | Smooths owner earnings over multiple years to reduce one-year noise. |
| Owner Earnings Enterprise (TTM) | `oe_ev_ttm` | EODHD-oriented: `NOPAT_TTM + D&A_TTM - MCapex_TTM - delta_nwc_maint`, where `NOPAT_TTM = EBIT_TTM * (1 - tax_rate)`. | Gives an unlevered owner-earnings view that is less distorted by capital structure. |
| Owner Earnings Enterprise (5Y Average) | `oe_ev_5y_avg` | EODHD-oriented average of latest 5 available FY points of `NOPAT_FY + D&A_FY - MCapex_FY - maint_nwc_FY(y)`, each year subtracting its own trailing 3-delta maintenance NWC (see `delta_nwc_maint`); a year without that chain yields no point, so up to 8 consecutive FY of NWC history is needed. | Normalizes enterprise owner earnings across the cycle. |
| Owner Earnings Enterprise Median (5Y FY) | `oe_ev_fy_median_5y` | EODHD-oriented median of latest 5 available FY enterprise owner-earnings points using the same FY OE formula as `oe_ev_5y_avg` (per-year maintenance NWC included). | Median is more robust than an average when one year is abnormally high or low. |
| Worst Owner Earnings Enterprise Year (10Y) | `worst_oe_ev_fy_10y` | EODHD-oriented minimum FY enterprise owner-earnings point over the latest strict 10 consecutive FY window; each point subtracts its own per-year maintenance NWC, so the strict window needs up to 13 consecutive FY of NWC history. | A direct bad-year stress test: it shows what the business looked like in its weakest decade year. |

## EV / Valuation

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Market Capitalization | `market_cap` | Computed on demand: latest shares-outstanding fact x the latest `market_data` price, so it floats with every price refresh. | Size matters for liquidity, survivability, and practical investability. |
| Earnings Yield | `earnings_yield` | `EPS_TTM / latest price`. | A simple inverse-PE view of how much earnings you get per dollar paid. |
| PEG Ratio | `peg_ratio` | EODHD-oriented: `(price / EPS_TTM) / (100 x g)`, where `g` is the Graham smoothed CAGR shared with `graham_eps_10y_cagr_3y_avg`; only when TTM EPS, price, and growth are all positive. | Lynch's growth-at-a-reasonable-price yardstick: PEG < 1 flags a multiple more than covered by earnings growth. |
| PEG Ratio (Dividend-Adjusted) | `peg_ratio_div_adj` | EODHD-oriented: `(price / EPS_TTM) / (100 x (g + dividend_yield_ttm))`; missing dividend data credits zero yield, and the combined growth+yield must be positive. | Lynch's refinement for slower growers with fat payouts: total return (growth plus yield) is what has to cover the multiple. |
| Price to Free Cash Flow | `price_to_fcf` | Latest market cap divided by `FCF_TTM`, where `FCF_TTM = OCF_TTM - Capex_TTM`. | Useful when accounting earnings understate or distort cash generation. |
| Graham Multiplier | `graham_multiplier` | `(Price / TTM EPS) * (Price / TBVPS)`. | Enforces discipline against overpaying for both earnings and balance-sheet value. |
| Price to Book | `price_to_book` | EODHD-oriented: `price / BVPS`, where `BVPS = CommonEquity / SharesOutstanding` and common equity prefers derived `CommonStockholdersEquity` with `StockholdersEquity` fallback; only when equity, shares, and price are positive. | Graham's balance-sheet criterion and the academic HML value factor: paying below accounting net worth builds in a margin of safety. |
| Price to Tangible Book | `price_to_tangible_book` | EODHD-oriented: `price / TBVPS`, where tangible book = common equity − `Goodwill` − `IntangibleAssetsNetExcludingGoodwill` (missing treated as `0`); only when tangible equity is positive. | Strips acquisition goodwill and intangibles from book value, pricing only assets that tend to hold value in liquidation or distress. |
| Owner Earnings Yield on Equity (TTM) | `oey_equity` | EODHD-oriented: `oe_equity_ttm / market_cap`. | Values the business against a maintenance-adjusted equity cash-earnings proxy. |
| Owner Earnings Yield on Equity (5Y) | `oey_equity_5y` | EODHD-oriented: `oe_equity_5y_avg / market_cap`. | Pairs current equity value with a normalized owner-earnings baseline. |
| Owner Earnings Yield on EV (TTM) | `oey_ev` | EODHD-oriented: `oe_ev_ttm / EV`, where EV is always computed as market cap + total debt − cash. | A capital-structure-neutral owner-earnings yield. |
| Owner Earnings Yield on EV (Normalized) | `oey_ev_norm` | EODHD-oriented: `oe_ev_fy_median_5y / EV`, using the same EV denominator policy as `oey_ev`. | Helps avoid buying a business on peak recent owner earnings. |
| EBIT Yield on EV | `ebit_yield_ev` | EODHD-oriented: `EBIT_TTM / EV`. | A simple enterprise earnings-yield lens before owner-earnings refinements. |
| Free Cash Flow Yield on EV | `fcf_yield_ev` | EODHD-oriented: `FCF_TTM / EV`. | Shows how much enterprise value is backed by trailing free cash flow. |
| EV to EBIT | `ev_to_ebit` | EODHD-oriented: `EV / EBIT_TTM`, only when `EBIT_TTM > 0`. | A practical operating multiple that compares companies independent of capital structure. |
| EV to EBITDA | `ev_to_ebitda` | EODHD-oriented: `EV / EBITDA_TTM`, where `EBITDA_TTM = EBIT_TTM + D&A_TTM`, only when positive. | A common acquisition-style multiple that uses operating cash-earnings proxy. |
| EV to Sales | `ev_to_sales` | EODHD-oriented: `EV / Revenues_TTM`, only when revenue is positive. | Values the enterprise against top-line output, staying usable for margin-trough cyclicals and temporarily unprofitable businesses where EV/EBIT is undefined. |

## ROIC / Capital Efficiency

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Return on Invested Capital (Legacy) | `return_on_invested_capital` | EODHD-oriented: after-tax TTM EBIT divided by average invested capital using the older invested-capital definition. | High returns on capital often indicate a strong business model or moat. |
| Invested Capital (Most Recent Quarter) | `ic_mqr` | EODHD-oriented: latest quarterly `TotalDebt + TotalEquity - Cash`, with debt/equity/cash fallback chains. | Gives a direct snapshot of operating capital committed to the business. |
| Invested Capital (Fiscal Year) | `ic_fy` | EODHD-oriented FY version of the same invested-capital formula used by `ic_mqr`. | Useful as an annual anchor for capital-efficiency analysis. |
| Average Invested Capital | `avg_ic` | EODHD-oriented: `(IC_now + IC_same_quarter_last_year) / 2`, with strict FY fallback when quarter pairing is unavailable. | Smooths balance-sheet timing noise for return-on-capital metrics. |
| ROIC (TTM) | `roic_ttm` | EODHD-oriented: `NOPAT_TTM / avg_ic`, where `NOPAT_TTM = EBIT_TTM * (1 - effective_tax_rate)`. | Measures current after-tax operating efficiency on invested capital. |
| ROIC 10Y Median | `roic_10y_median` | EODHD-oriented median of strict 10 FY ROIC values. Requires 10 consecutive ROIC years, which in practice means 11 consecutive FY invested-capital points. | Captures central tendency of long-cycle capital efficiency. |
| ROIC 7Y Median | `roic_7y_median` | EODHD-oriented median of strict 7 FY ROIC values. Requires 7 consecutive ROIC years and therefore 8 consecutive FY invested-capital points. | A shorter-horizon coverage alternative when the 10Y window is too sparse but you still want strict history. |
| ROIC Years Above 12% (10Y) | `roic_years_above_12pct` | EODHD-oriented count of strict-10Y FY ROIC values `> 12%`. | Tests persistence of strong returns rather than just average level. |
| ROIC 10Y Minimum | `roic_10y_min` | EODHD-oriented minimum FY ROIC in the latest strict 10-year window. Requires the same strict 10 ROIC / 11 invested-capital FY chain as `roic_10y_median`. | A resilience check for how bad capital efficiency got in weak years. |
| ROIC 7Y Minimum | `roic_7y_min` | EODHD-oriented minimum FY ROIC in the latest strict 7-year window. | A shorter-horizon downside check that preserves strict consecutive FY semantics. |
| Incremental ROIC (5Y) | `iroic_5y` | EODHD-oriented: `DeltaNOPAT_5Y / DeltaIC_5Y`, requiring positive and non-tiny `DeltaIC`. | Tests whether incremental capital deployed actually created incremental operating profit. |
| Greenblatt ROC (5Y Average) | `roc_greenblatt_5y_avg` | Average over up to 5 FY of `EBIT / TangibleCapital`, where tangible capital is based on net PPE and working-capital components. | Highlights businesses that earn well on the tangible capital they require. |
| Return on Capital Employed | `roce` | EODHD-oriented: `EBIT_TTM / AvgCapitalEmployed`, where capital employed = `Assets - LiabilitiesCurrent` averaged same-quarter-prior-year with strict prior-FY fallback; negative EBIT is emitted as a negative return. | Fundsmith's headline quality test: pre-tax operating return on all capital tied up in the business, comparable across capital structures and alongside `roic_ttm`. |
| Cash Return on Invested Capital | `croic` | EODHD-oriented: `FCF_TTM / avg_ic`, using the standard FCF policy (`OCF_TTM - Capex_TTM`, missing capex treated as `0`) over the same averaged invested capital as `roic_ttm`; negative FCF is emitted as a negative return. | The Smith/Akre cash lens on capital efficiency: a persistent CROIC-vs-ROIC gap exposes accrual-heavy earnings or capex hunger that NOPAT-based returns miss. |
| Greenblatt ROE (5Y Average) | `roe_greenblatt_5y_avg` | Average over up to 5 FY of net income available to common divided by average common equity. | Sustained high equity returns can point to good economics if leverage is controlled. |
| Maintenance Capex (FY) | `mcapex_fy` | EODHD-oriented proxy: `min(Capex_FY, 1.1 * D&A_FY)` with single-input fallback and absolute-value handling. | Estimates recurring reinvestment needs rather than headline capex alone. |
| Maintenance Capex (5Y Average) | `mcapex_5y` | EODHD-oriented average of latest 5 available `mcapex_fy` values. | Smooths capital-spending noise when normalizing free cash flow or owner earnings. |
| Maintenance Capex (TTM) | `mcapex_ttm` | EODHD-oriented TTM version of the same maintenance-capex proxy. | Useful when evaluating recent owner earnings and cash flow quality. |

## Margin Stability / Quality

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Gross Margin Standard Deviation (10Y) | `gm_10y_std` | EODHD-oriented population standard deviation of FY gross margin over the latest strict 10 consecutive FY years. | Lower variability suggests more stable economics and easier valuation work. |
| Operating Margin Standard Deviation (10Y) | `opm_10y_std` | EODHD-oriented population standard deviation of FY operating margin over the latest strict 10 consecutive FY years. | Stable operating margins usually indicate a more predictable business. |
| Operating Margin Minimum (10Y) | `opm_10y_min` | EODHD-oriented minimum FY operating margin in the latest strict 10-year window. | Shows how bad operating profitability got in the toughest observed year. |
| Operating Margin Minimum (7Y) | `opm_7y_min` | EODHD-oriented minimum FY operating margin in the latest strict 7-year window. | A shorter-horizon bad-year check that preserves strict consecutive FY semantics. |

## Share Count / Capital Allocation

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Share Count CAGR (5Y) | `share_count_cagr_5y` | Uses point-in-time outstanding shares only: `((Shares_t / Shares_t-5)^(1/5)) - 1`, preferring MRQ then falling back to FY. | Flags more recent dilution or buyback behavior that may matter more than decade-old capital allocation. |
| Share Count CAGR (10Y) | `share_count_cagr_10y` | Uses point-in-time outstanding shares only: `((Shares_t / Shares_t-10)^(1/10)) - 1`, preferring MRQ then falling back to FY. | Long-run dilution or shrinkage materially changes per-share value compounding. |
| Share Count Percentage Change (10Y) | `shares_10y_pct_change` | Exact 10-year point-in-time share-count change: `(Shares_t / Shares_t-10) - 1`, using the same pairing rules as `share_count_cagr_10y`. | Gives a direct measure of dilution or buyback behavior over a decade. |
| Net Buyback Yield | `net_buyback_yield` | EODHD-oriented: primary `-TTM(SalePurchaseOfStock) / market_cap`, with issuance-only fallback and 1Y share-count fallback. | Captures whether management is shrinking or diluting the share base in value-relevant terms. |

## Shareholder Returns / Distribution

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Dividend Yield (TTM) | `dividend_yield_ttm` | Primary `abs(CommonStockDividendsPaid_TTM) / market_cap`; fallback `CommonStockDividendsPerShareCashPaid / latest price` when the cash-dividend path is unavailable. | Separates actual cash returned to owners from forward-looking provider yield fields. |
| Shareholder Yield (TTM) | `shareholder_yield_ttm` | `dividend_yield_ttm + net_buyback_yield`, emitted only when both inputs are available. | Combines cash dividends and buybacks into one capital-allocation return measure. |
| Dividend Payout Ratio (TTM) | `dividend_payout_ratio_ttm` | `abs(CommonStockDividendsPaid_TTM) / NetIncome_TTM`, only when `NetIncome_TTM > 0`. | Flags whether the dividend is comfortably covered by trailing earnings. |

## Growth / Compounding

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| Revenue CAGR (10Y) | `revenue_cagr_10y` | Strict FY endpoint CAGR: `((Revenue_FY0 / Revenue_FY-10)^(1/10)) - 1`. | Long-run sales compounding is a basic test of market opportunity and business relevance. |
| Free Cash Flow per Share CAGR (10Y) | `fcf_per_share_cagr_10y` | Strict FY endpoint CAGR of `(FCF_FY / DilutedShares_FY)`, where `FCF_FY = OCF_FY - Capex_FY`. | Filters out growth that came from dilution rather than improving per-share economics. |
| Owner Earnings CAGR (10Y) | `owner_earnings_cagr_10y` | EODHD-oriented enterprise owner-earnings CAGR over a strict window of 10 consecutive FY OE points (each subtracting its own per-year maintenance NWC, so up to 13 consecutive FY of NWC history is needed), with 3-year average endpoints, mirroring the Graham-style EPS CAGR approach. Both endpoint windows must be strictly positive — a compound growth rate has no real solution from a non-positive base. | Tests whether maintenance-adjusted operating cash earnings are compounding over time, not just reported accounting profit. |

## Screening Utility / Misc

| English Descriptive Name of the Metric | pyvalue key | How is it calculated | Why is it important in identifying quality/value stocks |
| --- | --- | --- | --- |
| EPS Streak | `eps_streak` | Counts consecutive FY periods with positive diluted EPS. | Persistent profitability usually signals a more durable business. |
| EPS (TTM) | `eps_ttm` | Sum of the latest four quarterly EPS values. | A quick recent earnings-power view used in valuation and screen rules. |
| EPS 6Y Average | `eps_6y_avg` | Average of the latest six FY EPS values. | Smooths cyclicality and provides a normalized per-share earnings baseline. |
| Graham EPS CAGR (10Y, 3Y Average Endpoints) | `graham_eps_10y_cagr_3y_avg` | 10-year EPS CAGR using 3-year average EPS at both the start and end of the measurement window. | Rewards long-run earnings compounding while reducing endpoint noise. |
| Altman Z-Score | `altman_z` | EODHD-oriented classic 1968 score: `1.2*(WC/TA) + 1.4*(RetainedEarnings/TA) + 3.3*(EBIT_TTM/TA) + 0.6*(MarketCap/TL) + 1.0*(Revenues_TTM/TA)`; stocks from the latest balance sheet, flows TTM with FY fallback; all five factors required. | The standard bankruptcy / value-trap filter, calibrated for public manufacturers: Z < 1.81 flags distress, Z > 2.99 the safe zone -- essential when screening statistically cheap stocks. |
| Piotroski F-Score | `piotroski_f_score` | EODHD-oriented canonical 9-signal score, FY t vs t-1 with beginning-of-year asset denominators (needs 3 consecutive FY balance sheets): profitability (ROA > 0, CFO > 0, dROA > 0, CFO > NI), leverage/liquidity (LTD/avgAssets down or debt-free, current ratio up, no dilution), efficiency (gross margin up, asset turnover up). Strict: all 9 computable or no score; missing `LongTermDebt` counts as zero debt. | The canonical quality gate for cheap stocks (Piotroski 2000, ~+7.5%/yr on high book-to-market): `piotroski_f_score >= 7` separates recovering value from value traps. |
