import yfinance as yf
import pandas as pd
from google.cloud import storage
import os
from datetime import datetime

# Configuration
PROJECT_ID = "zoomcamp-project-493610"
BUCKET_NAME = "zoomcamp-project-493610-stock-data-lake"
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "../credentials.json")

# 6 Global Stock Exchanges
TICKERS = {
    "NYSE": [
        "AAPL", "MSFT", "GOOGL", "JPM", "XOM",
        "AMZN", "TSLA", "NVDA", "BAC", "GS",
        "META", "BRK-B", "UNH", "JNJ", "V",
        "WMT", "PG", "MA", "HD", "CVX",
        "ABBV", "MRK", "LLY", "PEP", "KO",
        "AVGO", "COST", "TMO", "MCD", "ACN",
        "DHR", "NEE", "AMD", "TXN", "HON",
        "QCOM", "PM", "IBM", "GE", "CAT",
        "BA", "RTX", "SPGI", "BLK", "SCHW",
        "AXP", "CRM", "INTU", "ISRG", "NOW"
    ],
    "JSE": [
        "NPN.JO", "AGL.JO", "SBK.JO", "MTN.JO", "SOL.JO",
        "FSR.JO", "ABG.JO", "VOD.JO", "BHP.JO", "IMP.JO",
        "SHP.JO", "DSY.JO", "REM.JO", "GRT.JO", "MNP.JO",
        "CFR.JO", "BTI.JO", "GLN.JO", "AMS.JO", "TBS.JO",
        "CPI.JO", "MEI.JO", "SPP.JO", "WHL.JO", "TFG.JO",
        "SNH.JO", "LHC.JO", "MRP.JO", "PIK.JO", "TRU.JO",
        "BAW.JO", "BVT.JO", "CLS.JO", "EXX.JO", "HAR.JO",
        "INL.JO", "IPL.JO", "KIO.JO", "LBH.JO", "MCG.JO",
        "MDC.JO", "MML.JO", "MUR.JO", "NED.JO", "NPK.JO",
        "OMU.JO", "PAN.JO", "PSG.JO", "RBP.JO", "SLM.JO"
    ],
    "LSE": [
        "SHEL.L", "AZN.L", "HSBA.L", "ULVR.L", "BP.L",
        "RIO.L", "GSK.L", "REL.L", "NG.L", "LLOY.L",
        "BARC.L", "VOD.L", "BT-A.L", "MKS.L", "TSCO.L",
        "PRU.L", "LGEN.L", "STAN.L", "NWG.L", "INF.L",
        "DGE.L", "RKT.L", "CPG.L", "WPP.L", "IAG.L",
        "EZJ.L", "TUI.L", "JD.L", "NEXT.L", "M&S.L",
        "BATS.L", "ABF.L", "CNA.L", "ENT.L", "FRES.L",
        "GFS.L", "HIK.L", "IHG.L", "IMB.L", "KGF.L",
        "LAND.L", "MNG.L", "MNDI.L", "PCT.L", "PSN.L",
        "RTO.L", "SGE.L", "SMIN.L", "SSE.L", "SVT.L"
    ],
    "TSE": [
        "7203.T", "6758.T", "9984.T", "8306.T", "6861.T",
        "9432.T", "4063.T", "6954.T", "8035.T", "9433.T",
        "7974.T", "4519.T", "6902.T", "8316.T", "9022.T",
        "3382.T", "4661.T", "6367.T", "7751.T", "8411.T",
        "9020.T", "4452.T", "6501.T", "6702.T", "6752.T",
        "6762.T", "6857.T", "7267.T", "7270.T", "7741.T",
        "7832.T", "8001.T", "8002.T", "8031.T", "8058.T",
        "8801.T", "8802.T", "9531.T", "9613.T", "9735.T",
        "2914.T", "3659.T", "4543.T", "4568.T", "6098.T",
        "6146.T", "6594.T", "6645.T", "6723.T", "6770.T"
    ],
    "EURONEXT": [
        "MC.PA", "OR.PA", "SAN.PA", "AIR.PA", "TTE.PA",
        "BNP.PA", "SU.PA", "AI.PA", "DG.PA", "KER.PA",
        "RI.PA", "CS.PA", "ACA.PA", "GLE.PA", "SAF.PA",
        "DSY.PA", "CAP.PA", "VIE.PA", "SGO.PA", "MT.AS",
        "ASML.AS", "PHIA.AS", "RAND.AS", "WKL.AS", "HEIA.AS",
        "NN.AS", "AKZA.AS", "BESI.AS", "IMCD.AS", "KPN.AS",
        "OCI.AS", "PRX.AS", "SBMO.AS", "URW.AS", "VPK.AS",
        "AD.AS", "AGN.AS", "DSMF.AS", "FLOW.AS", "GLPG.AS",
        "LIGHT.AS", "TKWY.AS", "UMG.AS", "ABN.AS", "ADYEN.AS",
        "BAMNB.AS", "EXOR.AS", "INGA.AS", "RDSA.AS", "WDP.AS"
    ],
    "SSE": [
        "600519.SS", "601398.SS", "600036.SS", "601288.SS", "600276.SS",
        "601318.SS", "600900.SS", "601166.SS", "600028.SS", "601628.SS",
        "600050.SS", "601088.SS", "600309.SS", "601601.SS", "600030.SS",
        "601668.SS", "601939.SS", "600016.SS", "601857.SS", "600048.SS",
        "600104.SS", "600196.SS", "600547.SS", "601006.SS", "601186.SS",
        "601336.SS", "601390.SS", "601688.SS", "601766.SS", "601800.SS",
        "601818.SS", "601985.SS", "601988.SS", "602560.SS", "603259.SS",
        "603288.SS", "603501.SS", "603986.SS", "688111.SS", "688599.SS",
        "600000.SS", "600009.SS", "600011.SS", "600015.SS", "600019.SS",
        "600023.SS", "600025.SS", "600031.SS", "600038.SS", "600061.SS"
    ]
}

def fetch_stock_data(ticker, period="5y"):
    """Fetch stock data using yfinance"""
    print(f"  Fetching {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period)
        if df.empty:
            print(f"  WARNING: No data for {ticker}, skipping...")
            return None
        df.reset_index(inplace=True)
        df["ticker"] = ticker

        # Assign exchange based on ticker suffix
        if ticker.endswith(".JO"):
            exchange = "JSE"
        elif ticker.endswith(".L"):
            exchange = "LSE"
        elif ticker.endswith(".T"):
            exchange = "TSE"
        elif ticker.endswith(".PA") or ticker.endswith(".AS"):
            exchange = "EURONEXT"
        elif ticker.endswith(".SS"):
            exchange = "SSE"
        else:
            exchange = "NYSE"

        df["exchange"] = exchange
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        df["Date"] = df["Date"].dt.date.astype(str)
        df = df[["Date", "ticker", "exchange", "Open", "High", "Low", "Close", "Volume"]]
        df.columns = ["date", "ticker", "exchange", "open", "high", "low", "close", "volume"]
        df = df.dropna(subset=["close"])
        print(f"  Got {len(df)} rows for {ticker}")
        return df
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None

def upload_to_gcs(df, ticker):
    """Upload dataframe as CSV to GCS"""
    try:
        client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
        bucket = client.bucket(BUCKET_NAME)
        date_str = datetime.now().strftime("%Y-%m-%d")
        blob_name = f"raw/stocks/{date_str}/{ticker.replace('.', '_').replace('-', '_')}.csv"
        blob = bucket.blob(blob_name)
        csv_data = df.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type="text/csv")
        print(f"  Uploaded to gs://{BUCKET_NAME}/{blob_name}")
        return blob_name
    except Exception as e:
        print(f"  ERROR uploading {ticker}: {e}")
        return None

def main():
    print("=" * 60)
    print("  Global Stock Market Data Ingestion Pipeline")
    print("  Exchanges: NYSE, JSE, LSE, TSE, EURONEXT, SSE")
    print("=" * 60)

    successful = 0
    failed = []
    total_rows = 0

    for exchange, tickers in TICKERS.items():
        print(f"\nProcessing {exchange} ({len(tickers)} stocks)...")
        print("-" * 40)
        for ticker in tickers:
            df = fetch_stock_data(ticker)
            if df is not None:
                result = upload_to_gcs(df, ticker)
                if result:
                    successful += 1
                    total_rows += len(df)
            else:
                failed.append(ticker)

    print("\n" + "=" * 60)
    print(f"  Ingestion Complete!")
    print(f"  Successful: {successful} tickers")
    print(f"  Failed: {len(failed)} tickers")
    print(f"  Total rows uploaded: {total_rows:,}")
    if failed:
        print(f"  Failed tickers: {', '.join(failed)}")
    print("=" * 60)

if __name__ == "__main__":
    main()