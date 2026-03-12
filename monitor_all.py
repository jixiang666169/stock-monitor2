import requests
import sqlite3
import pandas as pd
import time
import schedule
from datetime import datetime

# ---------- 配置 ----------
BATCH_SIZE = 20          # 新浪接口一次最多请求20只
SCAN_INTERVAL = 10       # 扫描间隔（分钟）
STOCK_LIST_FILE = "stock_list.txt"  # 股票列表文件
DB_FILE = "stock_all.db" # 数据库文件

# ---------- 初始化数据库 ----------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS daily (
            symbol TEXT, date TEXT, close REAL,
            PRIMARY KEY (symbol, date)
        )
    ''')
    return conn

# ---------- 加载股票列表 ----------
def load_stock_list():
    with open(STOCK_LIST_FILE, 'r') as f:
        stocks = [line.strip() for line in f if line.strip()]
    return stocks

# ---------- 批量获取实时行情 ----------
def fetch_batch(stock_batch):
    # 新浪接口批量请求，格式：list=sh600001,sh600002,...
    # 需要为每个代码加上市场前缀：sh（沪市）或 sz（深市）
    symbol_str = ",".join([('sh' + code if code.startswith('6') else 'sz' + code) for code in stock_batch])
    url = f"https://hq.sinajs.cn/list={symbol_str}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://finance.sina.com.cn"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = 'gbk'
        lines = resp.text.strip().split('\n')
        results = {}
        for line in lines:
            if not line.startswith('var hq_str_'):
                continue
            # 解析每只股票
            # 格式：var hq_str_sh600089="特变电工,30.80,...
            code_with_prefix = line.split('var hq_str_')[1].split('=')[0]
            parts = line.split('="')[1].split(',')
            price = float(parts[1]) if parts[1] else None
            volume = float(parts[8]) if len(parts) > 8 and parts[8] else None  # 成交量（手）
            amount = float(parts[9]) if len(parts) > 9 and parts[9] else None  # 成交额（元）
            # 去掉市场前缀，只保留纯代码
            code = code_with_prefix[2:] if code_with_prefix.startswith(('sh', 'sz')) else code_with_prefix
            results[code] = (price, volume, amount)
        return results
    except Exception as e:
        print(f"批量获取失败: {e}")
        return {}

# ---------- 更新日线数据 ----------
def update_daily(symbol, price):
    conn = init_db()
    today = datetime.now().strftime('%Y-%m-%d')
    # 检查今天是否已存过
    cur = conn.execute('SELECT close FROM daily WHERE symbol=? AND date=?', (symbol, today))
    if cur.fetchone() is None:
        conn.execute('INSERT INTO daily (symbol, date, close) VALUES (?, ?, ?)',
                     (symbol, today, price))
        conn.commit()
    conn.close()

# ---------- 计算5日均线 ----------
def get_ma5(symbol):
    conn = init_db()
    # 获取最近5个交易日的数据（按日期倒序）
    df = pd.read_sql('SELECT close FROM daily WHERE symbol=? ORDER BY date DESC LIMIT 5',
                     conn, params=(symbol,))
    conn.close()
    if len(df) < 5:
        return None  # 数据不足
    return df['close'].mean()

# ---------- 检查买入条件（价格在5日线的99.8%~101.2%）----------
def check_buy_signal(symbol, price, ma5):
    if ma5 is None:
        return False
    lower = ma5 * 0.998
    upper = ma5 * 1.012
    return lower <= price <= upper

# ---------- 全市场扫描任务 ----------
def scan_all():
    stocks = load_stock_list()
    total = len(stocks)
    print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 开始扫描全市场 {total} 只主板股票...")
    hits = []
    for i in range(0, total, BATCH_SIZE):
        batch = stocks[i:i+BATCH_SIZE]
        data = fetch_batch(batch)
        for code, (price, vol, amt) in data.items():
            if price is None:
                continue
            # 更新历史数据
            update_daily(code, price)
            ma5 = get_ma5(code)
            if check_buy_signal(code, price, ma5):
                hits.append((code, price, ma5, lower, upper))
        # 控制请求频率，避免被封
        time.sleep(1)  # 每批间隔1秒
    # 输出符合条件的股票
    if hits:
        print("\n===== 触发买入条件的股票 =====")
        for code, price, ma5, lower, upper in hits:
            print(f"{code} 现价 {price:.2f}  5日线 {ma5:.2f}  区间 [{lower:.2f}~{upper:.2f}]")
        print("============================\n")
    else:
        print("本次扫描无符合条件的股票。")

# ---------- 初次运行，补全历史数据（可选）----------
# 可以手动执行一次，或者先运行几天积累数据

# ---------- 主程序 ----------
if __name__ == "__main__":
    print("全市场股票监控程序启动，每10分钟扫描一次...")
    # 立即执行一次
    scan_all()
    # 设置定时任务
    schedule.every(SCAN_INTERVAL).minutes.do(scan_all)
    while True:
        schedule.run_pending()
        time.sleep(1)
