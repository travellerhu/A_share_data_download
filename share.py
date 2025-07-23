import os
import time
import random
import pandas as pd
import requests
from tqdm import tqdm
from fake_useragent import UserAgent
import json

# 配置参数
START_DATE = "20210706"  # 起始日期
END_DATE = "20250706"  # 结束日期
OUTPUT_FILE = "A股历史数据_沪深非ST.csv"  # 输出文件名
STATUS_FILE = "download_status.json"  # 下载状态记录文件
MAX_RETRIES = 5  # 单只股票最大重试次数
BATCH_SIZE = 50  # 分批保存数量
MIN_DELAY = 2  # 最小请求间隔(秒)
MAX_DELAY = 5  # 最大请求间隔(秒)

# 初始化User-Agent
ua = UserAgent()


def get_filtered_stock_codes():
    """获取沪深A股代码（剔除北交所、ST、*ST、退市股票）"""
    try:
        # 方法1：使用AKShare的stock_info_a_code_name接口
        import akshare as ak
        try:
            df = ak.stock_info_a_code_name()
            if not df.empty:
                filtered = df[
                    (df["code"].str.startswith(("6", "0", "3"))) &  # 沪深股票
                    (~df["name"].str.contains("ST|退市"))  # 非ST/退市
                    ]
                return filtered["code"].tolist()
        except:
            pass

        # 方法2：如果AKShare接口失效，使用备用的股票列表
        print("使用备用股票列表...")
        test_codes = [
            "600519", "000001", "601318", "000858", "600036",
            "601988", "601288", "601398", "601988", "601628"
        ]
        return test_codes

    except Exception as e:
        print(f"获取股票列表失败，使用测试股票代码: {e}")
        return ["600519", "000001", "601318", "000858"]


def fetch_stock_history(code, retry_count=0):
    """获取单只股票历史数据"""
    headers = {"User-Agent": ua.random}
    exchange = "1" if code.startswith(("6", "9")) else "0"  # 1=上交所, 0=深交所
    secid = f"{exchange}.{code}"
    url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&beg={START_DATE}&end={END_DATE}"

    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if data.get("data"):
                klines = data["data"]["klines"]
                df = pd.DataFrame([k.split(",") for k in klines],
                                  columns=["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅",
                                           "涨跌幅", "涨跌额", "换手率"])
                df["股票代码"] = code
                return df
        elif response.status_code == 429:
            wait_time = 60 * (retry_count + 1)
            print(f"触发反爬机制，等待{wait_time}秒后重试...")
            time.sleep(wait_time)
            return fetch_stock_history(code, retry_count + 1)
    except Exception as e:
        if retry_count < MAX_RETRIES:
            wait_time = random.uniform(10, 30)
            print(f"请求失败（{code}），{wait_time:.1f}秒后重试... Error: {e}")
            time.sleep(wait_time)
            return fetch_stock_history(code, retry_count + 1)
    return None


def load_download_status():
    """加载下载状态"""
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, "r") as f:
            return json.load(f)
    return {"completed": [], "remaining": [], "total": 0}


def save_download_status(status):
    """保存下载状态"""
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f)


def main():
    # 获取股票列表
    all_codes = get_filtered_stock_codes()
    print(f"识别到总股票数量: {len(all_codes)}")
    print(f"示例股票代码: {all_codes[:5]}...")

    # 加载下载状态
    status = load_download_status()

    # 初始化下载状态
    if not status["total"]:
        status = {
            "total": len(all_codes),
            "completed": [],
            "remaining": all_codes.copy(),
            "start_time": time.time()
        }
        save_download_status(status)

    # 计算进度
    completed_count = len(status["completed"])
    remaining_count = len(status["remaining"])
    print("\n" + "=" * 50)
    print(f"当前进度: {completed_count}/{status['total']}")
    print(f"剩余数量: {remaining_count}")
    print(f"将从第 {completed_count + 1} 只股票开始下载")
    print("=" * 50 + "\n")

    # 初始化数据存储
    all_data = []
    success_count = completed_count

    # 创建CSV文件（如果不存在）
    if not os.path.exists(OUTPUT_FILE):
        pd.DataFrame().to_csv(OUTPUT_FILE, index=False, encoding="utf_8_sig")

    # 开始下载
    pbar = tqdm(total=remaining_count, desc="总体进度")

    for i, code in enumerate(status["remaining"]):
        current_num = completed_count + i + 1
        print(f"\n正在下载第 {current_num}/{status['total']} 只股票 ({code})...")

        df = fetch_stock_history(code)
        if df is not None:
            all_data.append(df)
            status["completed"].append(code)
            success_count += 1

            # 分批保存
            if len(all_data) >= BATCH_SIZE:
                pd.concat(all_data).to_csv(OUTPUT_FILE, mode="a",
                                           header=not os.path.exists(OUTPUT_FILE) or os.stat(OUTPUT_FILE).st_size == 0,
                                           index=False, encoding="utf_8_sig")
                all_data = []

            # 更新状态文件
            status["remaining"] = status["remaining"][1:]
            save_download_status(status)

        pbar.update(1)

        # 随机延迟
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        time.sleep(delay)

    pbar.close()

    # 保存剩余数据
    if all_data:
        pd.concat(all_data).to_csv(OUTPUT_FILE, mode="a",
                                   header=not os.path.exists(OUTPUT_FILE) or os.stat(OUTPUT_FILE).st_size == 0,
                                   index=False, encoding="utf_8_sig")

    # 输出结果
    output_path = os.path.abspath(OUTPUT_FILE)
    time_used = (time.time() - status["start_time"]) / 60
    print("\n" + "=" * 50)
    print(f"下载完成！共 {status['total']} 只股票")
    print(f"成功下载: {success_count}")
    print(f"失败数量: {status['total'] - success_count}")
    print(f"耗时: {time_used:.1f} 分钟")
    print(f"数据文件已保存至: {output_path}")
    print(f"文件大小: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
    print("=" * 50)

    # 清理状态文件
    if os.path.exists(STATUS_FILE):
        os.remove(STATUS_FILE)


if __name__ == "__main__":
    main()