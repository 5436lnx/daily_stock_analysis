#!/usr/bin/env python3
"""
每日A股复盘自动推送脚本
定时任务：每个交易日 12:00 / 18:00 执行
"""

import akshare as ak
import requests
import datetime
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# 强制不走代理，直连外网
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
os.environ['no_proxy'] = '*'

# Server酱配置（优先读环境变量，本地回退到硬编码）
SENDKEY = os.environ.get("SENDKEY", "SCT321741TL4LkqAQQkFFvnvndwcGPTEpB")

# 热门股票行业预置字典（避免逐只慢查）
STOCK_INDUSTRY = {
    '中国能建': '电力工程', '协鑫能科': '清洁能源', '金风科技': '风电设备',
    '节能风电': '风电运营', '金牛化工': '化学原料', '绿发电力': '清洁能源',
    '三安光电': '光电器件', '汉缆股份': '电线电缆', '华工科技': '激光设备',
    '华胜天成': 'IT服务', '金开新能': '新能源发电', '大唐发电': '电力',
    '协鑫集成': '光伏组件', '双一科技': '风电叶片', '华电能源': '电力',
    '中国电建': '电力工程', '中南文化': '文化传媒', '大地海洋': '环保',
    '宏景科技': 'IT服务', '博众精工': '自动化设备', '首航新能': '光伏',
    '华灿光电': 'LED芯片', '安达智能': '自动化设备', '金宏气体': '电子特气',
    '国能日新': '能源信息化', '三一重能': '风电整机', '优博讯': '智能终端',
    '派能科技': '储能电池', '潞化科技': '煤化工', '郑州煤电': '煤炭电力',
    '金煤科技': '煤矿机械', '法尔胜': '金属制品', '扬子新材': '新材料',
    '纳百川': '汽车零部件', '兖矿能源': '煤炭', '安道麦A': '农药',
    '德龙汇能': '能源贸易', '一拖股份': '农机', '陕西黑猫': '焦炭',
    '江瀚新材': '硅烷材料', '德力佳': '风电齿轮箱', '振江股份': '风电塔筒',
    '江苏新能': '新能源发电', '大金重工': '风电塔筒', '建投能源': '电力',
    '新天绿能': '风电运营', '洲际油气': '油气开采', '豫能控股': '电力热力',
    '顺钠股份': '输配电设备', '美利云': '造纸云计算',
}

STOCK_INTRO = {
    '中国能建': '全球最大电力工程承包商，主营电力、能源、交通等基础设施建设',
    '协鑫能科': '清洁能源发电及综合能源服务，光伏、风电项目开发运营',
    '金风科技': '全球风电整机龙头，主营大型风力发电机组研发制造',
    '节能风电': '央企中国节能旗下，主营风电项目开发、运营',
    '金牛化工': '主营甲醇、纯碱等化工产品生产销售',
    '绿发电力': '中国绿发投资集团旗下，主营清洁能源发电',
    '三安光电': 'LED芯片龙头，全球最大砷化镓芯片供应商',
    '汉缆股份': '电线电缆行业龙头，特高压电缆供应商',
    '华工科技': '光电子器件龙头，主营激光加工设备、光通信传感器',
    '华胜天成': 'IT服务提供商，主营软件开发和系统集成',
    '金开新能': '新能源发电运营商，主营风电、光伏项目开发',
    '大唐发电': '五大发电集团大唐旗下，电力生产与运营商',
    '协鑫集成': '协鑫集团旗下，主营光伏组件研发制造',
    '双一科技': '主营大型风电叶片研发制造',
    '华电能源': '主营火电、风电生产与供应',
    '中国电建': '电力工程建设龙头，主营电力、能源、交通等基建',
    '中南文化': '影视、游戏、文化传媒综合集团',
    '大地海洋': '环保企业，主营危险废物处置',
    '宏景科技': 'IT服务企业，主营软件和信息化服务',
    '博众精工': '主营自动化设备研发制造',
    '首航新能': '主营光伏组件',
    '华灿光电': '主营LED芯片',
    '安达智能': '主营智能装备',
    '金宏气体': '主营工业气体',
    '国能日新': '主营能源信息化服务',
    '三一重能': '风电设备企业，主营风电整机',
    '优博讯': '主营智能终端',
    '派能科技': '主营储能电池',
    '潞化科技': '主营煤化工产品',
    '郑州煤电': '主营煤炭开采和电力生产',
    '金煤科技': '主营煤矿机械',
    '法尔胜': '主营钢丝绳等金属制品',
    '扬子新材': '主营建筑新材料',
    '纳百川': '主营汽车散热器',
    '兖矿能源': '煤炭龙头企业，主营煤炭开采',
    '安道麦A': '主营农药生产',
    '德龙汇能': '主营能源贸易',
    '一拖股份': '拖拉机龙头，主营农机',
    '陕西黑猫': '主营焦炭生产',
    '江瀚新材': '功能性硅烷龙头，主营硅烷偶联剂',
    '德力佳': '主营风电齿轮箱',
    '振江股份': '主营风电塔筒',
    '江苏新能': '主营风电光伏发电',
    '大金重工': '主营风电支撑结构',
    '建投能源': '主营电力生产',
    '新天绿能': '主营风电运营',
    '洲际油气': '主营石油天然气开发',
    '豫能控股': '主营电力、热力生产供应',
    '顺钠股份': '主营输配电设备',
    '美利云': '央企中国诚通旗下，主营造纸、云计算',
}


def fmt_amount(val):
    """金额（元）→ 万亿/亿字符串"""
    try:
        v = float(val)
        if v >= 1e12:
            return f"{v/1e12:.2f}万亿"
        elif v >= 1e8:
            return f"{v/1e8:.0f}亿"
        else:
            return f"{v:.0f}"
    except:
        return "-"


def get_market_data():
    today = datetime.datetime.now().strftime("%Y%m%d")

    # ── 1. 主要指数近7日 ──────────────────────────────
    index_configs = [
        ('sh000001', '上证指数'),
        ('sz399001', '深证成指'),
        ('sz399006', '创业板指'),
    ]
    index_7d = {}    # {name: [(date_str, close), ...]}
    amt_by_index = {}  # {name: [(date_str, amount), ...]}  用于成交额
    sh_change = sz_change = cy_change = 0.0

    for code, name in index_configs:
        try:
            df = ak.stock_zh_index_daily_em(symbol=code)
            df = df.sort_values('date').reset_index(drop=True)
            recent = df.tail(7)
            index_7d[name] = list(zip(
                recent['date'].astype(str).tolist(),
                recent['close'].tolist()
            ))
            if 'amount' in df.columns:
                amt_by_index[name] = list(zip(
                    recent['date'].astype(str).tolist(),
                    recent['amount'].tolist()
                ))
            # 今日涨跌幅
            if len(df) >= 2:
                pct = (df.iloc[-1]['close'] - df.iloc[-2]['close']) / df.iloc[-2]['close'] * 100
                if name == '上证指数': sh_change = pct
                elif name == '深证成指': sz_change = pct
                elif name == '创业板指': cy_change = pct
        except Exception as e:
            print(f"获取{name}失败: {e}", file=sys.stderr)

    # ── 2. 涨停板数据 ─────────────────────────────────
    zt_count = 0
    lb_data = {}
    max_lb = 0
    max_stocks = []
    industry_table = ""
    try:
        df_zt = ak.stock_zt_pool_em(date=today)
        if df_zt is not None and not df_zt.empty:
            zt_count = len(df_zt)
            if '连板数' in df_zt.columns:
                max_lb = int(df_zt['连板数'].max())
                max_stocks = df_zt[df_zt['连板数'] == max_lb]['名称'].tolist()
                for lb in range(max_lb, 0, -1):
                    stocks = df_zt[df_zt['连板数'] == lb]['名称'].tolist()
                    if stocks:
                        lb_data[lb] = "、".join(stocks)
            if '所属行业' in df_zt.columns:
                top5 = df_zt['所属行业'].value_counts().head(5)
                lines = [f"| {i} | {ind} | {cnt}只 |"
                         for i, (ind, cnt) in enumerate(top5.items(), 1)]
                industry_table = "\n".join(lines)
    except Exception as e:
        print(f"获取涨停板失败: {e}", file=sys.stderr)

    # ── 3. 热门股票 Top20 ─────────────────────────────
    stock_table = ""
    try:
        df_hot = ak.stock_hot_rank_em()
        top20 = df_hot.head(20).copy()

        # 找出字典里缺少行业信息的股票，并发查询
        missing = {}  # code -> name
        for _, row in top20.iterrows():
            name = row['股票名称']
            if name not in STOCK_INDUSTRY:
                code = row['代码'].replace('SH', '').replace('SZ', '')
                missing[code] = name

        fetched_industry = {}  # name -> industry
        if missing:
            def fetch_industry(code_name):
                code, name = code_name
                try:
                    info = ak.stock_individual_info_em(symbol=code)
                    for _, r in info.iterrows():
                        if r['item'] == '行业':
                            return name, r['value']
                except:
                    pass
                return name, '-'

            with ThreadPoolExecutor(max_workers=8) as ex:
                futures = {ex.submit(fetch_industry, (c, n)): n for c, n in missing.items()}
                for f in as_completed(futures):
                    name, ind = f.result()
                    fetched_industry[name] = ind
                    # 同步更新字典，下次推送直接命中
                    STOCK_INDUSTRY[name] = ind

        lines = []
        for _, row in top20.iterrows():
            name = row['股票名称']
            price = row['最新价']
            change = row['涨跌幅']
            industry = STOCK_INDUSTRY.get(name) or fetched_industry.get(name, '-')
            intro = STOCK_INTRO.get(name, industry)  # 没有简介就用行业名代替
            lines.append(f"| {len(lines)+1} | {name} | {price:.2f} | {change:+.2f}% | {industry} | {intro} |")
        stock_table = "\n".join(lines)
    except Exception as e:
        print(f"获取热门股票失败: {e}", file=sys.stderr)

    return {
        'index_7d': index_7d,
        'amt_by_index': amt_by_index,
        'sh_change': sh_change,
        'sz_change': sz_change,
        'cy_change': cy_change,
        'zt_count': zt_count,
        'lb_data': lb_data,
        'max_lb': max_lb,
        'max_stocks': max_stocks,
        'industry_table': industry_table,
        'stock_table': stock_table,
    }


def build_index_table(index_7d):
    """动态生成指数近7日走势表"""
    names = ['上证指数', '深证成指', '创业板指']
    all_dates = sorted(set(
        str(d)[5:] for n in names if n in index_7d for d, _ in index_7d[n]
    ))[-7:]

    header = "| 指数 | " + " | ".join(all_dates) + " |"
    sep    = "|------|" + "-------|" * len(all_dates)
    rows = []
    for name in names:
        if name not in index_7d:
            continue
        d2c = {str(d)[5:]: c for d, c in index_7d[name]}
        cells = []
        for i, ds in enumerate(all_dates):
            v = d2c.get(ds)
            if v is None:
                cells.append("-")
            elif i == len(all_dates) - 1:
                cells.append(f"**{v:.2f}**")
            else:
                cells.append(f"{v:.2f}")
        rows.append(f"| {name} | " + " | ".join(cells) + " |")
    return "\n".join([header, sep] + rows)


def build_volume_table(amt_by_index):
    """动态生成成交额近7日表（沪+深）"""
    sh_data = amt_by_index.get('上证指数', [])
    sz_data = amt_by_index.get('深证成指', [])
    if not sh_data or not sz_data:
        return "_(成交额数据暂不可用)_"

    n = min(len(sh_data), len(sz_data), 7)
    dates   = [str(d)[5:] for d, _ in sh_data[-n:]]
    sh_amts = [a for _, a in sh_data[-n:]]
    sz_amts = [a for _, a in sz_data[-n:]]

    def row(label, vals):
        cells = []
        for i, v in enumerate(vals):
            s = fmt_amount(v)
            cells.append(f"**{s}**" if i == len(vals) - 1 else s)
        return f"| {label} | " + " | ".join(cells) + " |"

    total_amts = [sh + sz for sh, sz in zip(sh_amts, sz_amts)]
    header = "| 市场 | " + " | ".join(dates) + " |"
    sep    = "|------|" + "-------|" * n
    return "\n".join([header, sep,
                      row("沪市", sh_amts),
                      row("深市", sz_amts),
                      row("合计", total_amts)])


def build_lb_ladder(lb_data, max_lb):
    if not lb_data:
        return "  - 暂无连板数据"
    lines = []
    for lb in range(max_lb, 0, -1):
        stocks = lb_data.get(lb)
        if stocks:
            lines.append(f"  - {lb}连板：{stocks}")
        elif lb >= 2:
            lines.append(f"  - {lb}连板：无")
    return "\n".join(lines)


def build_summary(data):
    sh = data['sh_change']
    zt = data['zt_count']
    max_lb = data['max_lb']
    max_stocks = data['max_stocks']

    if sh >= 1.0:    trend = f"大盘强势上涨，沪指涨{sh:.2f}%"
    elif sh >= 0.2:  trend = f"大盘小幅收涨，沪指涨{sh:.2f}%"
    elif sh >= -0.2: trend = f"大盘震荡整理，沪指{sh:+.2f}%"
    elif sh >= -1.0: trend = f"大盘小幅收跌，沪指跌{abs(sh):.2f}%"
    else:            trend = f"大盘明显下跌，沪指跌{abs(sh):.2f}%"

    parts = [trend, f"今日涨停{zt}只"]
    if max_lb > 0 and max_stocks:
        parts.append(f"**{'、'.join(max_stocks)}** {max_lb}连板领涨")
    return "。".join(parts) + "。"


def send_notification(data):
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    max_stocks_str = "、".join(data['max_stocks']) if data['max_stocks'] else "无"

    index_table  = build_index_table(data['index_7d'])
    volume_table = build_volume_table(data['amt_by_index'])
    lb_ladder    = build_lb_ladder(data['lb_data'], data['max_lb'])
    summary      = build_summary(data)
    industry_tbl = data['industry_table'] or "_(暂无数据)_"
    stock_tbl    = data['stock_table'] or "_(暂无数据)_"

    content = f"""## 📈 主要指数（近7日走势）

{index_table}

**今日涨跌幅**：上证 {data['sh_change']:+.2f}% | 深证 {data['sz_change']:+.2f}% | 创业板 {data['cy_change']:+.2f}%

---

## 💰 全市场成交额（近7日）

{volume_table}

---

## 🔥 涨停板分析

- **涨停总数**：{data['zt_count']}只
- **最高连板**：{data['max_lb']}连板 — **{max_stocks_str}**
- **连板天梯**：
{lb_ladder}

---

## 🔥 热点板块 Top5（涨停家数）

| 排名 | 行业 | 涨停家数 |
|------|------|----------|
{industry_tbl}

---

## 🔥 热门股票 Top20（东方财富热门榜）

| 排名 | 名称 | 价格 | 涨跌幅 | 板块 | 公司简介 |
|------|------|------|--------|------|----------|
{stock_tbl}

---

## 📌 今日总结

{summary}

---
由 Sweet 自动推送 😜
"""

    url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
    r = requests.post(url, data={"title": f"📊 {today} A股复盘", "desp": content})
    print("发送结果:", r.text[:120])
    return r.json()


if __name__ == "__main__":
    print("开始获取市场数据...")
    data = get_market_data()
    print("数据获取完成，发送通知...")
    send_notification(data)
    print("完成!")
