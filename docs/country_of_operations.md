# Country of Operations: How It Is Derived

Source notebook: `notebooks/02_silver_enrichment`
Output table: `ibm-aml`.silver.aml_silver_accounts_small

## Purpose

The IBM AML synthetic dataset has no country or location attribute for accounts. The silver layer adds a proxy jurisdiction to each account, `country_of_operations`, based on the currency the account transacts in most often. This is an inference, not ground truth.

## Inputs

| Table | Columns used |
| --- | --- |
| `ibm-aml`.bronze.aml_bronze_transactions_small | `from_account`, `payment_currency`, `to_account`, `receiving_currency` |
| `ibm-aml`.bronze.aml_bronze_accounts_small | `account_number` (plus all other columns, passed through) |

## Step-by-step logic

### 1. Currency reference maps

Two static Spark map literals (`F.create_map`) translate a currency name, as spelled in the IBM data, into:

* `currency_country_map`: a country / jurisdiction name
* `currency_iso_map`: an ISO-3 style code for downstream reporting and graph features

| Currency | country_of_operations | country_iso |
| --- | --- | --- |
| US Dollar | United States | USA |
| Euro | Eurozone | EUR |
| Yen | Japan | JPN |
| UK Pound | United Kingdom | GBR |
| Canadian Dollar | Canada | CAN |
| Australian Dollar | Australia | AUS |
| Swiss Franc | Switzerland | CHE |
| Brazil Real | Brazil | BRA |
| Saudi Riyal | Saudi Arabia | SAU |
| Shekel | Israel | ISR |
| Rupee | India | IND |
| Mexican Peso | Mexico | MEX |
| Yuan | China | CHN |
| Ruble | Russia | RUS |
| Bitcoin | Decentralized / Crypto | BTC |

Notes:
* `EUR` and `BTC` are not ISO 3166 country codes. They stand in for a multi-country zone and for crypto, which has no jurisdiction.
* All 15 currency values in the transactions table match a key in the map exactly, so no transaction currency goes unmapped.

### 2. Stack sender and receiver activity

Each transaction counts once for the sender and once for the receiver:

* Sender side: `from_account` is paired with `payment_currency`
* Receiver side: `to_account` is paired with `receiving_currency`

Rows with a null currency are dropped. The two sides are combined with `unionByName` into a single `(account_number, currency)` activity set. So an account's currency profile reflects both money it sends and money it receives.

### 3. Count activity per account and currency

The activity set is grouped by `(account_number, currency)` and counted (`tx_count`). This is a count of transactions, not a sum of amounts: a large payment and a small payment have the same weight.

### 4. Pick the dominant currency

A window partitioned by `account_number` and ordered by `tx_count` descending gives each currency a `row_number()`. Only rank 1 is kept. The two maps are then looked up for that currency:

* `dominant_currency` = the most frequent currency
* `country_of_operations` = `currency_country_map[dominant_currency]`, or `Unknown` if there is no match
* `country_iso` = `currency_iso_map[dominant_currency]`, or `UNK` if there is no match

### 5. Join to the accounts dimension

The bronze accounts table is left-joined to the dominant-currency result on `account_number`. Accounts that never appear in a transaction get these defaults:

* `dominant_currency` = `None` (the string, not null)
* `country_of_operations` = `Unknown`
* `country_iso` = `UNK`

The result overwrites `` `ibm-aml`.silver.aml_silver_accounts_small `` as a Delta table, with `overwriteSchema` set to true.

## Observed results (small dataset)

| Metric | Value |
| --- | --- |
| Silver rows | 712,688 |
| Distinct account numbers | 712,684 |
| Accounts with transaction activity | 705,903 |
| Accounts with no activity (Unknown / UNK / None) | 6,781 |
| Accounts that use more than one currency | 11,214 (about 1.6% of active accounts) |
| Accounts with a tie for the top currency | 619 |

Accounts per country of operations:

| country_of_operations | Accounts |
| --- | --- |
| United States | 263,811 |
| Eurozone | 165,796 |
| China | 48,687 |
| India | 33,564 |
| Decentralized / Crypto | 29,243 |
| Saudi Arabia | 26,761 |
| Australia | 21,651 |
| Japan | 21,116 |
| Brazil | 20,082 |
| Canada | 17,641 |
| Israel | 17,477 |
| Switzerland | 13,187 |
| United Kingdom | 9,612 |
| Russia | 9,123 |
| Mexico | 8,156 |
| Unknown | 6,781 |

All 6,781 `Unknown` accounts come from step 5: they have no transactions. None of them comes from an unmapped currency.

## Caveats and known limitations

* **Ties are non-deterministic.** The window orders only by `tx_count`. When two currencies have the same count (619 accounts), `row_number()` picks one arbitrarily, and the pick can change between runs. Adding a secondary sort key (for example, `currency`) would make the result reproducible.
* **Frequency, not value.** The ranking counts transactions and ignores `amount_paid` / `amount_received`.
* **Currency is not the same as country.** Euro maps to a zone, US Dollar is used widely outside the US, and Bitcoin has no jurisdiction. Treat the column as a currency-region proxy.
* **Duplicate account numbers.** The silver table has 4 more rows than distinct account numbers, so a few `account_number` values are duplicated in the bronze accounts table. The left join passes those duplicates through.
* **Placeholder strings rather than nulls.** `None`, `Unknown` and `UNK` are literal strings. Filter or separate them explicitly in downstream analysis.
