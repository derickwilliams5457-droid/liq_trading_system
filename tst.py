import time
import pandas as pd
import numpy as np
import ccxt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timezone, timedelta
from scipy import stats
import webbrowser
import os


class BinanceFuturesAnalyzer:
    def __init__(self, symbol='ATOMUSDT', chart_tf='15min'):
        self.symbol = symbol
        self.chart_tf = chart_tf
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })

    def fetch_anomaly_data(self, fetch_mode="today", target_date=None):
        now = datetime.now(timezone.utc)

        # Handle specific date input
        if target_date:
            try:
                # Expecting 'DD/MM/YYYY' format as requested (e.g., 11/04/2026)
                start_dt = datetime.strptime(target_date, "%d/%m/%Y").replace(tzinfo=timezone.utc)
            except ValueError:
                # Fallback to standard YYYY-MM-DD if needed
                start_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            
            # End at 23:59:59.999 UTC of that day
            end_dt = start_dt + timedelta(days=1) - timedelta(milliseconds=1)
            now_ms = int(end_dt.timestamp() * 1000)
            print(f"Fetching FULL data for historical date {target_date} ({start_dt} UTC to {end_dt} UTC)...")
        else:
            if fetch_mode == "yesterday":
                start_dt = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
            now_ms = int(now.timestamp() * 1000)
            print(f"Fetching FULL data from {start_dt} UTC to now...")

        start_ms = int(start_dt.timestamp() * 1000)

        # PAGINATED FETCH
        ohlcv = []
        since = start_ms

        while since < now_ms:
            batch = self.exchange.fetch_ohlcv(self.symbol, '1m', since=since, limit=1000)

            if not batch:
                break

            ohlcv.extend(batch)
            since = batch[-1][0] + 1

            # Break if we fetched past our target time window
            if since >= now_ms or len(batch) < 1000:
                break

            time.sleep(self.exchange.rateLimit / 1000)

        if not ohlcv:
            print("No data returned")
            return pd.DataFrame(), None

        df_1m = pd.DataFrame(ohlcv, columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
        
        # FIX: Localize 'Time' to UTC to prevent comparison exceptions with timezone-aware datetime objects
        df_1m['Time'] = pd.to_datetime(df_1m['Time'], unit='ms').dt.tz_localize('UTC')

        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            df_1m[col] = df_1m[col].astype(float)

        df_1m.set_index('Time', inplace=True)

        # CLEAN DATA
        df_1m = df_1m[~df_1m.index.duplicated(keep='last')]
        df_1m.sort_index(inplace=True)

        # Filter out any data beyond our target window if it snuck into the final batch
        if target_date:
            df_1m = df_1m[df_1m.index <= end_dt]

        if df_1m.empty:
            print("No data within the specified date range.")
            return pd.DataFrame(), None

        # PROPER RESAMPLING ALIGNMENT
        df = df_1m.resample(self.chart_tf, origin='start').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()

        # --- Anomaly Calculations ---
        def norm(s):
            return (s - s.min()) / (s.max() - s.min()) if (s.max() - s.min()) != 0 else s * 0

        df['price_chg'] = df['Close'].pct_change().abs()
        df['vol_surge'] = df['Volume'] / df['Volume'].rolling(10).mean()

        df['nv'] = norm(df['vol_surge'].fillna(0))
        df['np'] = norm(df['price_chg'].fillna(0))

        df['intensity'] = (df['np'] * 0.6) + (df['nv'] * 0.4)
        df['is_anomaly'] = df['intensity'] > 0.7

        df['intensity_zscore'] = stats.zscore(df['intensity'].fillna(0))

        # --- Buy/Sell Volume Split ---
        df['Buy_Vol'] = df['Volume'].copy()
        df['Sell_Vol'] = df['Volume'].copy()

        for i in range(len(df)):
            if i > 0:
                price_chg = (df['Close'].iloc[i] - df['Close'].iloc[i-1]) / df['Close'].iloc[i-1]
                buy_ratio = 0.5 + (price_chg * df['intensity'].iloc[i] * 0.3)
                buy_ratio = max(0.3, min(0.7, buy_ratio))

                df.iloc[i, df.columns.get_loc('Buy_Vol')] = df['Volume'].iloc[i] * buy_ratio
                df.iloc[i, df.columns.get_loc('Sell_Vol')] = df['Volume'].iloc[i] * (1 - buy_ratio)

        daily_open = df_1m.iloc[0]['Open']

        print(f"Final candles: {len(df)} | Last candle: {df.index[-1]}")

        return df.reset_index(), daily_open

    def fetch_recent_trades(self):
        try:
            trades = self.exchange.fetch_trades(self.symbol, limit=100)
            trades_df = pd.DataFrame(trades)
            trades_df['Time'] = pd.to_datetime(trades_df['timestamp'], unit='ms')
            trades_df['price'] = trades_df['price'].astype(float)
            return trades_df
        except Exception as e:
            print(f"Trade fetch error: {e}")
            return pd.DataFrame()


def plot_interactive(df, output_file="binance_chart.html"):
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.6, 0.2, 0.2],
        subplot_titles=("Price & Anomaly Intensity", "Volume Delta", "Intensity")
    )

    # Candles
    fig.add_trace(go.Candlestick(
        x=df['Time'], open=df['Open'], high=df['High'],
        low=df['Low'], close=df['Close'], opacity=0.3
    ), row=1, col=1)

    # Anomaly bubbles (top 20%)
    mask = df['intensity'] > df['intensity'].quantile(0.8)
    df_a = df[mask]

    if len(df_a) > 0:
        fig.add_trace(go.Scatter(
            x=df_a['Time'],
            y=(df_a['High'] + df_a['Low']) / 2,
            mode='markers',
            marker=dict(
                size=df_a['intensity'] * 80,
                color=df_a['intensity'],
                colorscale='Hot'
            ),
            name='Anomaly'
        ), row=1, col=1)

    # Volume
    fig.add_trace(go.Bar(x=df['Time'], y=df['Buy_Vol']), row=2, col=1)
    fig.add_trace(go.Bar(x=df['Time'], y=-df['Sell_Vol']), row=2, col=1)

    # Intensity
    fig.add_trace(go.Scatter(
        x=df['Time'], y=df['intensity'],
        fill='tozeroy'
    ), row=3, col=1)

    fig.update_layout(
        template='plotly_dark',
        height=900,
        xaxis_rangeslider_visible=False,
        showlegend=False,
        title=f"{df['Close'].iloc[-1]:.2f} USDT | Chart Analysis"
    )

    fig.write_html(output_file)
    print(f"Saved → {output_file}")
    return fig


def run_analysis(analyzer, target_date=None, update_interval=20):
    output_file = "binance_chart.html"
    
    if target_date:
        # One-time processing for historical data
        print(f"\nProcessing historical data for date: {target_date}...")
        df, _ = analyzer.fetch_anomaly_data(target_date=target_date)
        if len(df) > 0:
            plot_interactive(df, output_file=output_file)
            webbrowser.open(f'file://{os.path.abspath(output_file)}')
        else:
            print("No data found to plot.")
    else:
        # Live tracking loop for 'today' data
        first = True
        while True:
            print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Updating live data...")
            df, _ = analyzer.fetch_anomaly_data(fetch_mode="today")

            if len(df) > 0:
                plot_interactive(df, output_file=output_file)
                if first:
                    webbrowser.open(f'file://{os.path.abspath(output_file)}')
                    first = False

            time.sleep(update_interval)


# --- RUN ---
if __name__ == "__main__":
    analyzer = BinanceFuturesAnalyzer(symbol='ATOMUSDT', chart_tf='15min')
    
    # 💡 CHANGE DATE HERE: Use 'DD/MM/YYYY' format or set to None for live market processing
    TARGET_DATE = "21/06/2026" 
    
    run_analysis(analyzer, target_date=TARGET_DATE)