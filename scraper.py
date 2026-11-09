# Created by 青云 on 2026/11/06.
"""MarketWhy 抓取脚本：行情分时 + 中外财经快讯，检测行情异动并关联同时段新闻，输出 data.json。只用标准库。

流程：
1. 东方财富分时（1 分钟一个点，统一北京时间）→ 检测 30 分钟内涨跌超过阈值的异动段
2. 中文快讯（新浪 7x24、华尔街见闻、东方财富、同花顺、格隆汇）+ 海外 RSS（彭博、WSJ、FT、CNBC、路透经 Google News 等）
3. 新闻打因素标签（货币政策 / 宏观数据 / 地缘 / 政策 / AI 科技 / 财报 / 大宗 / 汇率），和上一份合并，保留 48 小时
4. 每个异动段取前 25 分钟到后 10 分钟内的新闻，按「市场相关词 + 因素 + 重要度」打分，取前几条作为可能原因
5. 多家来源报道的相近标题合并成「今日大事」
"""
import html, json, re, time, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

OUT = Path(__file__).with_name("data.json")
OUT_A = Path(__file__).with_name("analysis.json")
THEMES = Path(__file__).with_name("themes.json")
CST = timezone(timedelta(hours=8))
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"}
EM_UT = "fa5fd1943c7b386f172d6893dbfba10b"   # 东方财富行情接口的公开 ut 参数
KEEP_HOURS = 48                               # 新闻保留时长
WINDOW_MIN = 30                               # 异动检测回看窗口（分钟）
NEWS_BEFORE, NEWS_AFTER = 25, 10              # 异动段前后多少分钟内的新闻算「同时段」

# ── 行情品种 ───────────────────────────────────────────────────────────────────
# id, 名称, 分组, 异动阈值（30 分钟内涨跌幅 %；unit=bp 时为基点）, 相关词组
MARKETS = [
    ("1.000001",   "上证指数",   "A股",   0.5, "cn"),
    ("0.399006",   "创业板指",   "A股",   0.8, "cn"),
    ("100.HSI",    "恒生指数",   "港股",  0.6, "hk"),
    ("124.HSTECH", "恒生科技",   "港股",  0.9, "hk"),
    ("100.NDX",    "纳斯达克100", "美股", 0.6, "us"),
    ("100.SPX",    "标普500",    "美股",  0.5, "us"),
    ("103.NQ00Y",  "纳指期货",   "美股",  0.5, "us"),
    ("100.N225",   "日经225",    "亚太欧", 0.7, "jp"),
    ("100.KS11",   "韩国KOSPI",  "亚太欧", 0.8, "kr"),
    ("100.GDAXI",  "德国DAX",    "亚太欧", 0.6, "eu"),
    ("122.XAU",    "伦敦金",     "商品汇率", 0.6, "gold"),
    ("112.B00Y",   "布伦特原油", "商品汇率", 1.0, "oil"),
    ("100.UDI",    "美元指数",   "商品汇率", 0.3, "fx"),
    ("133.USDCNH", "美元/离岸人民币", "商品汇率", 0.2, "cnh"),
    ("171.US10Y",  "美债10年收益率", "商品汇率", 4, "bond"),   # 收益率按基点算
]
BP_MARKETS = {"171.US10Y"}

# 每个市场的关键词分两级：
# 核心词 = 能确定说的就是这个市场（日经 / 日本 / 日元…），每个 +3 分；相关词 = 相关主题（AI、芯片、美元…），每个 +1 分
MARKET_CORE = {
    "cn":   r"A股|沪指|上证|深成指|深证成指|创业板|科创|两市|沪深|北向|汇金|国家队|互换便利|证监会|人民银行|中国央行|国家统计局",
    "hk":   r"港股|恒指|恒生|恒科|南向|港交所|香港|中概",
    "us":   r"美股|纳指|纳斯达克|标普|道指|华尔街|美股期货|Wall Street|Nasdaq|S&P 500|Dow|stock futures|US stocks|U.S. stocks",
    "jp":   r"日经|日本|日股|日元|日本央行|东京|亚太股市|亚洲股市|亚太市场|Nikkei|Japan|Japanese|yen|BOJ|Tokyo|Topix|Asian stocks|Asian markets|Asian shares|Asia stocks",
    "kr":   r"韩国|KOSPI|韩股|首尔|韩元|亚太股市|亚洲股市|亚太市场|Korea|Korean|Seoul|Asian stocks|Asian markets|Asian shares|Asia stocks",
    "eu":   r"欧股|欧洲|德国|DAX|欧央行|斯托克|Europe|European|German|ECB|STOXX",
    "gold": r"黄金|金价|贵金属|gold|bullion",
    "oil":  r"原油|油价|石油|OPEC|欧佩克|oil|crude|Brent|WTI",
    "fx":   r"美元指数|美元|dollar|DXY",
    "cnh":  r"人民币|离岸|在岸|中间价|yuan|renminbi|CNH",
    "bond": r"美债|国债收益率|Treasury|Treasuries|yield|yields|bond|bonds",
}
MARKET_REL = {
    "cn":   r"国务院|央行|降准|LPR|中国|国内|稳增长|中美|CPI|PPI|社融",
    "hk":   r"腾讯|阿里|美团|小米|中美|中国|内地",
    "us":   r"美联储|鲍威尔|英伟达|特斯拉|苹果|微软|科技股|芯片股|AI|美国|中期选举|特朗普|"
            r"Fed|Powell|Nvidia|Tesla|Apple|Microsoft|tech|AI|Treasury|Trump|midterm",
    "jp":   r"软银|SoftBank|AI|芯片|半导体|chip|亚洲|亚太|Asian|Asia",
    "kr":   r"三星|SK海力士|Samsung|SK Hynix|AI|芯片|半导体|存储|chip|亚洲|亚太|Asian|Asia",
    "eu":   r"欧元|euro",
    "gold": r"避险|美元|美联储|降息|safe-haven|safe haven|Fed|rate",
    "oil":  r"炼油|中东|伊朗|俄罗斯|沙特|库存|霍尔木兹|refinery|Iran|Saudi|Russia|Hormuz",
    "fx":   r"汇率|美联储|非农|CPI|欧元|日元|currency|Fed|yen|euro|payroll",
    "cnh":  r"汇率|央行|中美|PBOC",
    "bond": r"美联储|通胀|降息|财政部|Fed|inflation|rate",
}
MARKET_KW = {k: MARKET_CORE[k] + "|" + MARKET_REL[k] for k in MARKET_CORE}   # 合在一起：详情页「发布后的行情」、大事打分用
EQUITY = ("cn", "hk", "us", "jp", "kr", "eu")      # 股市：提到别的股市却没提本市场的新闻要降权

def kw_re(pattern):
    """英文词加词边界（AI 不能命中 Airbus，Fed 不能命中 Federal）；全大写的短缩写区分大小写。"""
    parts = []
    for tok in pattern.split("|"):
        if not re.match(r"[A-Za-z]", tok):           # 中文词原样
            parts.append(tok)
            continue
        prefix = tok.endswith("*")                    # 「regulat*」这类前缀词，后面不加边界
        core = tok.rstrip("*")
        if re.fullmatch(r"[A-Z0-9&]{2,5}", core):     # AI、CPI、FOMC 这类缩写区分大小写
            core = f"(?-i:{core})"
        parts.append(rf"(?<![A-Za-z]){core}" + ("" if prefix else r"(?![A-Za-z])"))
    return re.compile("|".join(parts), re.I)


MARKET_RE = {k: kw_re(v) for k, v in MARKET_KW.items()}
CORE_RE = {k: kw_re(v) for k, v in MARKET_CORE.items()}
REL_RE = {k: kw_re(v) for k, v in MARKET_REL.items()}
GLOBAL_RE = kw_re(MARKET_KW["us"] + r"|全球|外盘|隔夜|抛售|避险|selloff|sell-off|global|markets")
ALL_MKT_RE = kw_re("|".join(MARKET_KW.values()))

# ── 因素分类 ───────────────────────────────────────────────────────────────────
# key, 名称, 权重（越大越可能是推动大盘的因素）, 正则
FACTORS = [
    ("monetary",  "货币政策", 3, r"美联储|联储|央行|降息|加息|利率决议|议息|降准|LPR|MLF|逆回购|鲍威尔|缩表|欧央行|日本央行|英国央行|"
                                 r"Fed|FOMC|Powell|rate cut|rate hike|interest rate|central bank|ECB|BOJ|Bank of England"),
    ("macro",     "宏观数据", 3, r"CPI|PPI|PMI|GDP|非农|就业|失业率|零售销售|社融|M2|通胀|通缩|工业增加值|进出口|贸易顺差|初请|"
                                 r"inflation|payrolls?|jobs report|unemployment|retail sales|jobless|consumer prices|economic data"),
    ("geo",       "地缘冲突", 3, r"俄罗斯|乌克兰|以色列|伊朗|美伊|伊核|俄乌|哈马斯|中东|冲突|战争|袭击|导弹|空袭|停火|斡旋|和谈|制裁|朝鲜|台海|红海|霍尔木兹|"
                                 r"war|Iran|Israel|Ukraine|Russia|missile|strike|ceasefire|sanction|attack|Gaza|Middle East"),
    ("policy",    "政策政治", 2, r"国务院|证监会|发改委|财政部|商务部|关税|特朗普|白宫|选举|中期选举|国会|刺激政策|稳增长|专项债|降费|"
                                 r"tariff|Trump|White House|election|midterm|Congress|Senate|stimulus|Treasury Secretary|Bessent"),
    ("tech",      "AI科技",   2, r"AI|人工智能|大模型|芯片|半导体|英伟达|算力|存储|光模块|OpenAI|"
                                 r"Nvidia|chip|semiconductor|artificial intelligence|data center"),
    ("earnings",  "财报公司", 1, r"财报|业绩|营收|净利|季报|年报|指引|盈利|"
                                 r"earnings|revenue|guidance|profit|quarterly results|beats|misses"),
    ("commodity", "大宗商品", 2, r"原油|油价|黄金|金价|白银|OPEC|欧佩克|铜价|天然气|铁矿|"
                                 r"oil|crude|gold|silver|OPEC|copper|natural gas|commodit*"),
    ("fx",        "汇率",     2, r"人民币|美元指数|汇率|日元|欧元|英镑|dollar|yen|euro|currency|yuan"),
]
FACTOR_RE = [(k, n, w, kw_re(p)) for k, n, w, p in FACTORS]
FACTOR_W = {k: w for k, _, w, _ in FACTORS}
# 行情播报（「恒指收跌 1%」「创业板指高开 1.19%」这类）是结果不是原因
RESULT_RE = re.compile(r"(指数?|股|期货|价|ETF|板块|概念)[^，,。]{0,6}(涨|跌)\d|高开|低开|平开|报\d+(\.\d+)?点|午盘|早盘|尾盘|盘中|盘前|收市|报收|走弱|走强|震荡|收盘|收评|午评|开盘|"
                       r"收跌|收涨|收报|跌幅扩大|涨幅扩大|拉升|跳水|直线|走高|走低|翻红|翻绿|失守|跌破|站上|突破|日内|涨超|跌超|涨逾|跌逾|大涨|大跌|暴跌|暴涨|重挫|熊市|"
                       r"(多数|普遍|集体|全线|齐)(上涨|下跌|走高|走低|收涨|收跌|高开|低开)|涨停|跌停|异动|转涨|转跌|翻涨|翻跌|"
                       r"(上涨|下跌|涨|跌|升|降)(近|超|逾|约|了)?\d+(\.\d+)?%|至\d+(\.\d+)?点|"
                       r"\bclose[sd]?\b|\bopen(s|ed)?\b|\brall(y|ies)\b|\bslides?\b|\btumbles?\b|\bsurges?\b|\bplunges?\b|"
                       r"\bbounces?\b|\brebounds?\b|\bclimbs?\b|\bslips?\b|\bdips?\b|\bsinks?\b|\bjumps?\b|\bsoars?\b|\bedges? (higher|lower)\b|\b(rise|rises|fall|falls|gain|gains|drop|drops)\b", re.I)
# 带了原因的行情播报（「受伊朗局势影响，油价…」「Asian stocks bounce as AI jitters recede」）能解释异动，不排除
CAUSAL_RE = re.compile(r"受.{0,14}(影响|提振|拖累|推动)|因为?|由于|带动|拖累|提振|推动|获益于|在.{0,12}(之际|背景下)|"
                       r"\bas\b|\bafter\b|\bamid\b|\bfollowing\b|\bon (fears?|hopes?|worries|concerns?|bets?|optimism|news)\b", re.I)
# 例行操作（每天的逆回购、中间价公布等）：不是催化剂，降权
ROUTINE_RE = re.compile(r"逆回购操作|开展.{0,12}逆回购|中间价(报|调)|公开市场.{0,6}操作|MLF操作|国库现金")
# 早报、汇总类：一篇里什么都有，不是具体原因，直接排除
DIGEST_RE = re.compile(r"早餐|晚报|早报|午报|晚间要闻|要闻汇总|盘前必读|一文|速览|汇总|周报|月报|周前瞻|"
                       r"\bBriefing\b|What to watch|Things to Know|\bLive\b|Live updates|Latest .* News and Analysis|\bnewsletter\b", re.I)
# 观点、研报、点评、特稿：不是触发行情的事件，降权
OPINION_RE = re.compile(r"证券[:：]|研报|策略|展望|观点|点评|解读|分析师|首席|经济学家|专家称|复盘|前瞻|"
                        r"\bAnalysis\b|\bOpinion\b|\bCommentary\b|\bExplainer\b|\bInsight\b|\bcolumn\b|\bpodcast\b|Odd Lots|"
                        r"\bcould\b|\bmight\b|\bmay\b|^(How|Why|What|Inside|Here's|Here is)\b", re.I)
NOISE_RE = re.compile(r"辞职|减持计划|股东大会|回购进展|担保|董事会决议|获得.*专利|中标|签订.*合同|募集资金")


def fetch(url, timeout=15, headers=None, retries=3):
    """失败自动重试（东方财富偶尔会拒绝 GitHub Actions 的连接），每次间隔递增。"""
    for i in range(retries):
        try:
            return urllib.request.urlopen(urllib.request.Request(url, headers={**UA, **(headers or {})}), timeout=timeout).read()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.5 * (i + 1))


def fetch_json(url, **kw):
    return json.loads(fetch(url, **kw))


FULL = 20000          # 中文快讯本身就是全文，不截断（这个上限只防异常数据）


def clean(text, n=300):
    text = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", text or ""))).strip()
    return (text[:n] + "…") if len(text) > n else text


def fnv(s):
    """FNV-1a 32 位，用于生成稳定的新闻 id。"""
    h = 0x811c9dc5
    for b in s.encode("utf-8"):
        h = ((h ^ b) * 0x01000193) & 0xffffffff
    return format(h, "08x")


def split_title(text):
    """快讯没有标题时：优先用开头的【…】，否则取第一句。"""
    text = clean(text, FULL)
    m = re.match(r"^【([^】]{4,80})】\s*(.*)$", text)
    if m:
        return m.group(1), clean(m.group(2), FULL)
    text = re.sub(r"^(格隆汇|财联社|金十数据)\d+月\d+日[｜|丨]", "", text)
    m = re.match(r"^(.{8,70}?[。；！？])(.*)$", text)
    if m:
        return m.group(1).rstrip("。；"), clean(m.group(2), FULL)
    return clean(text, 70), ""


def news(src, title, url, ts, summary="", imp=0, lang="zh"):
    title = clean(title, 160)
    return {"id": fnv(src + title), "src": src, "title": title, "summary": clean(summary, FULL if lang == "zh" else 300),
            "url": url, "ts": int(ts), "imp": imp, "lang": lang}


# ── 中文快讯 ───────────────────────────────────────────────────────────────────

def ts_cst(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=CST).timestamp()


def sina():
    out = []
    for page in (1, 2):
        d = fetch_json(f"https://zhibo.sina.com.cn/api/zhibo/feed?page={page}&page_size=100&zhibo_id=152&tag_id=0&dire=f&dpc=1&type=0")
        for x in d["result"]["data"]["feed"]["list"]:
            tags = {t.get("name") for t in x.get("tag") or []}
            title, summary = split_title(x.get("rich_text", ""))
            imp = 2 if ("焦点" in tags or x.get("is_focus") == "1") else (0 if tags == {"公司"} else 1)
            out.append(news("sina", title, x.get("docurl") or f"https://finance.sina.com.cn/7x24/?id={x['id']}",
                            ts_cst(x["create_time"]), summary, imp))
    return out


def wscn():
    d = fetch_json("https://api-one-wscn.awtmt.com/apiv1/content/lives?channel=global-channel&limit=100")
    out = []
    for x in d["data"]["items"]:
        text = x.get("content_text") or clean(x.get("content"), FULL)
        title, summary = (x["title"], text) if x.get("title") else split_title(text)
        out.append(news("wscn", title, x.get("uri") or "https://wallstreetcn.com/live/global",
                        x["display_time"], summary, 2 if (x.get("score") or 1) >= 2 else 1))
    return out


def eastmoney():
    d = fetch_json("https://np-weblist.eastmoney.com/comm/web/getFastNewsList?client=web&biz=web_724&fastColumn=102"
                   "&sortEnd=&pageSize=200&req_trace=1")
    return [news("eastmoney", x["title"], f"https://finance.eastmoney.com/a/{x['code']}.html", ts_cst(x["showTime"]),
                 x.get("summary", ""), 2 if str(x.get("titleColor")) not in ("0", "") else 1)
            for x in d["data"]["fastNewsList"]]


def ths():
    d = fetch_json("https://news.10jqka.com.cn/tapp/news/push/stock/?page=1&tag=&track=website&pagesize=100")
    out = []
    for x in d["data"]["list"]:
        tags = (x.get("tag") or "").split(",")
        imp = 2 if x.get("color") == "2" else (0 if "公告" in tags else 1)
        out.append(news("ths", x["title"], x.get("url"), x["ctime"], x.get("digest", ""), imp))
    return out


def gelonghui():
    d = fetch_json("https://www.gelonghui.com/api/live-channels/all/lives/v4?category=all&limit=100")
    out = []
    for x in d["result"]:
        title, summary = (x["title"], x.get("content", "")) if x.get("title") else split_title(x.get("content", ""))
        out.append(news("gelonghui", title, x.get("route") or "https://www.gelonghui.com/live",
                        x["createTimestamp"], summary, 2 if (x.get("level") or 0) >= 1 else 1))
    return out


# ── 海外 RSS ──────────────────────────────────────────────────────────────────

def rss(src, url, imp=1, limit=40, strip_suffix=False):
    def run():
        root = ET.fromstring(fetch(url))
        items = list(root.iter("item")) or list(root.iter("{http://purl.org/rss/1.0/}item"))
        out = []
        for it in items[:limit]:
            def t(tag):
                return it.findtext(tag) or it.findtext("{http://purl.org/rss/1.0/}" + tag) or ""
            pub = t("pubDate") or it.findtext("{http://purl.org/dc/elements/1.1/}date") or ""
            est = False
            try:
                ts = parsedate_to_datetime(pub).timestamp() if "," in pub else datetime.fromisoformat(pub.replace("Z", "+00:00")).timestamp()
            except Exception:
                ts, est = time.time(), True       # 日经这类不带时间的，用首次抓到的时间，标记为估计值
            title = t("title")
            if src == "fed" and not re.search(r"FOMC|Federal Open Market|monetary policy|discount rate|statement|Powell|minutes", title, re.I):
                continue                          # 执法公告、人事任免等例行发布不要
            if strip_suffix:                      # Google News 标题带「 - Reuters」后缀
                title = re.sub(r"\s+-\s+[^-]+$", "", title)
            desc = "" if strip_suffix else t("description")
            n = news(src, title, t("link"), ts, desc, imp, "en")
            if est:
                n["est"] = True
            out.append(n)
        return out
    return run


def fallback(primary, backup):
    """主源失败或为空时用备用源（美联储官网会拦 GitHub Actions 的 IP，备用走 Google News）。"""
    def run():
        try:
            items = primary()
            if items:
                return items
        except Exception:
            pass
        return backup()
    return run


def gnews(q, days=1):
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode(
        {"q": f"when:{days}d {q}", "hl": "en-US", "gl": "US", "ceid": "US:en"})


# id, 名称, 地区, 缩写, 主题色, 是否收费墙, 抓取函数
SOURCES = [
    ("sina",      "新浪财经 7x24", "cn",   "新浪", "#e6162d", False, sina),
    ("wscn",      "华尔街见闻",   "cn",   "见闻", "#1a6dff", False, wscn),
    ("eastmoney", "东方财富",     "cn",   "东财", "#ff5f00", False, eastmoney),
    ("ths",       "同花顺",       "cn",   "同花", "#d6201b", False, ths),
    ("gelonghui", "格隆汇",       "cn",   "格隆", "#2e6be6", False, gelonghui),
    ("bloomberg", "Bloomberg",   "intl", "BBG",  "#7c4dff", True,  rss("bloomberg", "https://feeds.bloomberg.com/markets/news.rss")),
    ("reuters",   "Reuters",     "intl", "RTRS", "#ff8000", True,  rss("reuters", gnews("site:reuters.com markets OR stocks OR economy"), strip_suffix=True)),
    ("wsj",       "WSJ",         "intl", "WSJ",  "#9a9a9a", True,  rss("wsj", "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain")),
    ("ft",        "Financial Times", "intl", "FT", "#f2a5c0", True, rss("ft", "https://www.ft.com/markets?format=rss")),
    ("cnbc",      "CNBC",        "intl", "CNBC", "#2cb5e8", False, rss("cnbc", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114")),
    ("marketwatch", "MarketWatch", "intl", "MW", "#3dbb5c", False, rss("marketwatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories")),
    ("investing", "Investing.com", "intl", "INV", "#f5a623", False, rss("investing", "https://www.investing.com/rss/news_25.rss")),
    ("nikkei",    "Nikkei Asia", "intl", "NKA",  "#c8102e", False, rss("nikkei", "https://asia.nikkei.com/rss/feed/nar")),
    ("scmp",      "SCMP 商业",   "intl", "SCMP", "#ffc20e", False, rss("scmp", "https://www.scmp.com/rss/92/feed")),
    ("bbc",       "BBC Business", "intl", "BBC", "#bb1919", False, rss("bbc", "https://feeds.bbci.co.uk/news/business/rss.xml")),
    ("fed",       "美联储",      "intl", "FED",  "#3b82f6", False, fallback(
        rss("fed", "https://www.federalreserve.gov/feeds/press_all.xml", imp=2, limit=15),
        rss("fed", gnews("site:federalreserve.gov", days=7), imp=2, limit=15, strip_suffix=True))),
]


# ── 因素标签 ───────────────────────────────────────────────────────────────────

def tag(n):
    text = n["title"] + " " + n["summary"][:120]
    n["factors"] = [k for k, _, _, r in FACTOR_RE if r.search(text)]
    is_result = bool(RESULT_RE.search(n["title"]))
    # 带原因的行情播报：有「受…影响 / as / amid」这类连接词，或者同时带有地缘、货币、宏观、政策因素（「美伊斡旋再起、黄金重回 4100」）
    n["explain"] = is_result and (bool(CAUSAL_RE.search(n["title"])) or any(f in n["factors"] for f in ("geo", "monetary", "macro", "policy")))
    data_release = any(f in n["factors"] for f in ("macro", "monetary"))   # 「CPI 同比上涨」「Inflation rises」是数据发布，本身就是原因
    n["result"] = is_result and not n["explain"] and not data_release   # 纯行情播报
    n["opinion"] = bool(OPINION_RE.search(n["title"]))
    n["digest"] = bool(DIGEST_RE.search(n["title"]))
    n["routine"] = bool(ROUTINE_RE.search(n["title"]))
    n["mk"] = [m[0] for m in MARKETS if MARKET_RE[m[4]].search(text)][:6]   # 相关行情品种，详情页算发布后的行情反应
    if NOISE_RE.search(n["title"]):
        n["imp"] = 0
    return n


# ── 行情与异动 ─────────────────────────────────────────────────────────────────

def trends(secid):
    d = fetch_json(f"https://push2his.eastmoney.com/api/qt/stock/trends2/get?secid={secid}&ut={EM_UT}"
                   "&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f53&iscr=0&ndays=1")["data"]
    pts = []
    for row in d["trends"]:
        t, v = row.split(",")
        v = float(v)
        if v > 0:
            pts.append((int(ts_cst(t + ":00")), v))
    if not pts:
        raise RuntimeError("empty")
    return float(d["preClose"]), d.get("decimal", 2), pts


def change(secid, a, b, pre):
    """a → b 的变化：收益率品种用基点，其余用 % （相对昨收）。"""
    return (b - a) * 100 if secid in BP_MARKETS else (b - a) / pre * 100


def detect_moves(secid, pre, pts, th):
    """在 WINDOW_MIN 分钟回看窗口里找涨跌超过阈值的段，同方向重叠的合并，返回按幅度排序的异动段。"""
    segs = []
    j0 = 0
    for i, (ti, vi) in enumerate(pts):
        while pts[j0][0] < ti - WINDOW_MIN * 60:
            j0 += 1
        best = None
        for j in range(j0, i):
            c = change(secid, pts[j][1], vi, pre)
            if abs(c) >= th and (best is None or abs(c) > abs(best[1])):
                best = (j, c)
        if best:
            segs.append([best[0], i, 1 if best[1] > 0 else -1])
    # 合并：同方向且有重叠（或间隔不超过 5 分钟）
    merged = []
    for s in segs:
        if merged and merged[-1][2] == s[2] and s[0] <= merged[-1][1] + 5:
            merged[-1][1] = max(merged[-1][1], s[1])
            merged[-1][0] = min(merged[-1][0], s[0])
        else:
            merged.append(s)
    moves = []
    for a, b, d in merged:
        # 把起点收紧到区间内的极值点（下跌找最高点，上涨找最低点），终点同理
        seg = range(a, b + 1)
        a = (min if d > 0 else max)(seg, key=lambda k: pts[k][1])
        b = (max if d > 0 else min)(range(a, b + 1), key=lambda k: pts[k][1])
        c = change(secid, pts[a][1], pts[b][1], pre)
        if abs(c) < th:
            continue
        moves.append({"start": pts[a][0], "end": pts[b][0], "from": pts[a][1], "to": pts[b][1],
                      "chg": round(c, 2), "dir": d, "kind": "swing"})
    # 开盘跳空
    gap = change(secid, pre, pts[0][1], pre)
    if abs(gap) >= th:
        moves.append({"start": pts[0][0] - 60, "end": pts[0][0], "from": pre, "to": pts[0][1],
                      "chg": round(gap, 2), "dir": 1 if gap > 0 else -1, "kind": "gap"})
    moves.sort(key=lambda m: -abs(m["chg"]))
    kept = []                                     # 从大到小保留，和已保留段重叠超过一半的丢掉
    for m in moves:
        dup = False
        for k in kept:
            if m["kind"] != k["kind"]:
                continue
            ov = min(m["end"], k["end"]) - max(m["start"], k["start"])
            if ov > 0.5 * max(60, min(m["end"] - m["start"], k["end"] - k["start"])):
                dup = True
                break
        if not dup:
            kept.append(m)
    return kept[:6]


def rank_news(secid, move, pool, kw):
    """给异动段挑出可能的原因。

    时间：异动开始前 25 分钟到开始后 15 分钟（长异动放宽到 40 分钟）的新闻算「催化剂」，开始前 20 分钟到开始后 5 分钟最可能；
          带原因的行情播报（解释类）可以晚到异动结束后 10 分钟。跳空看休市期间 12 小时。
    打分：本市场核心词 ×3 + 相关词 ×1 + 因素权重 + 重要度 ×1.5 + 时间加分
          − 观点 / 研报 / 特稿 4 − 例行操作（逆回购、中间价）4 − 只提别的股市 4 − 带原因的行情播报 1；纯行情播报、早报汇总直接排除。
    入选：至少命中一个本市场核心词，或者是命中本市场相关词的高权重因素（货币 / 宏观 / 地缘）重要快讯；分数 ≥ 5。
    """
    start, end, gap = move["start"], move["end"], move["kind"] == "gap"
    lo = start - (12 * 3600 if gap else NEWS_BEFORE * 60)
    hi_cat = start if gap else start + max(15 * 60, min(end - start, 40 * 60))   # 催化剂：开始后 15 分钟，长异动放宽到 40 分钟
    hi_exp = end + NEWS_AFTER * 60
    scored = []
    for n in pool:
        ts = n["ts"]
        if n.get("est") or n.get("result") or n.get("digest") or not (lo <= ts <= max(hi_cat, hi_exp)):
            continue
        if ts > hi_cat and not n.get("explain"):          # 异动开始 15 分钟后才出来的，只收解释类
            continue
        text = n["title"] + " " + n["summary"][:120]
        core = len(set(x.lower() for x in CORE_RE[kw].findall(text)))
        rel = len(set(x.lower() for x in REL_RE[kw].findall(text)))
        if gap and core == 0:                             # 开盘跳空常常是外盘带的：隔夜美股 / 全球风险事件算相关词
            rel += len(set(x.lower() for x in GLOBAL_RE.findall(text)))
        fw = max([FACTOR_W[f] for f in n["factors"]], default=0)
        if core == 0 and not (fw >= 3 and n["imp"] >= 2 and rel >= 1) and not (gap and rel >= 2):
            continue
        score = core * 3 + min(rel, 3) + fw + n["imp"] * 1.5
        if n.get("opinion"):
            score -= 4
        if n.get("routine"):
            score -= 4
        if n.get("explain"):
            score -= 1
        if kw in EQUITY and core == 0 and not gap:        # 只提了别的股市（如评估日经时出现「创业板指…」）
            if any(CORE_RE[o].search(text) for o in EQUITY if o != kw):
                score -= 4
        if not gap:                                       # 时间加分：催化剂通常在异动开始前后
            dt = ts - start
            score += 2 if -1200 <= dt <= 300 else 1 if -1500 <= dt < -1200 else 0.5 if 300 < dt <= 900 else 0
        if score >= 5:
            scored.append((score, n))
    scored.sort(key=lambda x: (-x[0], -x[1]["ts"]))
    picked, seen = [], []
    for sc, n in scored:
        k = norm(n["title"])
        if any(similar(k, o) for o in seen):
            continue
        seen.append(k)
        picked.append({"id": n["id"], "score": round(sc, 1)})
        if len(picked) >= 6:
            break
    return picked


def link_moves(markets):
    """同一时段别的品种也朝同一方向异动：多半是同一个原因，作为联动提示。"""
    allm = [(m, mv) for m in markets for mv in m["moves"]]
    for m, mv in allm:
        linked = []
        for m2, mv2 in allm:
            if m2["id"] == m["id"] or mv2["dir"] != mv["dir"] or mv2["kind"] != mv["kind"]:
                continue
            ov = min(mv["end"], mv2["end"]) - max(mv["start"], mv2["start"])
            if ov > 0.5 * max(60, min(mv["end"] - mv["start"], mv2["end"] - mv2["start"])):
                if not any(x["id"] == m2["id"] for x in linked):
                    linked.append({"id": m2["id"], "name": m2["name"], "chg": mv2["chg"], "unit": m2["unit"]})
        mv["linked"] = sorted(linked, key=lambda x: -abs(x["chg"]))[:4]

def market_data(pool, prev_markets=()):
    out, errs = [], []
    prev_by = {m["id"]: m for m in prev_markets}

    def one(spec):
        secid, name, group, th, kw = spec
        try:
            pre, dec, pts = trends(secid)
        except Exception as e:
            errs.append(f"{name}：{str(e)[:60]}")
            old = prev_by.get(secid)
            if not old:
                return None
            # 分时取不到：沿用上一次的分时和异动段，但用最新的新闻库重新归因
            for mv in old["moves"]:
                mv["news"] = rank_news(secid, mv, pool, kw)
            return old
        moves = detect_moves(secid, pre, pts, th)
        for mv in moves:
            mv["news"] = rank_news(secid, mv, pool, kw)
        last = pts[-1][1]
        step = max(1, len(pts) // 240)              # 前端画图最多 240 个点，异动段起止点保留
        keep = {0, len(pts) - 1} | set(range(0, len(pts), step))
        idx = {p[0]: k for k, p in enumerate(pts)}
        for mv in moves:
            keep |= {idx.get(mv["start"], 0), idx.get(mv["end"], 0)}
        return {"id": secid, "name": name, "group": group, "th": th, "unit": "bp" if secid in BP_MARKETS else "%",
                "pre": pre, "last": last, "dec": dec, "chg": round(change(secid, pre, last, pre), 2),
                "pts": [[pts[k][0], pts[k][1]] for k in sorted(keep)], "moves": moves}

    with ThreadPoolExecutor(8) as ex:
        out = [r for r in ex.map(one, MARKETS) if r]
    link_moves(out)
    return out, errs


# ── 今日大事（跨来源聚合，复用 hot-news 的相似度算法）─────────────────────────────

def norm(title):
    return re.sub(r"[#【】\[\]「」『』“”\"'‘’《》（）()：:，,。.！!？?、\s·|_\-—…;；]", "", title.lower())


def bigrams(s):
    return {s[i:i + 2] for i in range(len(s) - 1)} if len(s) > 1 else {s}


def words(s):
    return set(re.findall(r"[a-z]{4,}", s))


def similar(a, b):
    if not a or not b:
        return False
    if re.search(r"[a-z]{4}", a) and re.search(r"[a-z]{4}", b) and not re.search(r"[一-鿿]", a + b):
        wa, wb = words(a), words(b)                # 英文按单词重合
        inter = len(wa & wb)
        return inter >= 3 and inter / max(1, min(len(wa), len(wb))) >= 0.4
    if len(a) >= 6 and len(b) >= 6 and (a in b or b in a):
        return True
    ga, gb = bigrams(a), bigrams(b)
    inter = len(ga & gb)
    ratio = inter / min(len(ga), len(gb))
    return (inter >= 4 and ratio >= 0.6) or (inter >= 7 and ratio >= 0.45)


def build_events(pool, now):
    cand = [n for n in pool if n["ts"] >= now - 24 * 3600 and n["imp"] >= 1]
    keys = [norm(n["title"]) for n in cand]
    parent = list(range(len(cand)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(len(cand)):
        for j in range(i + 1, len(cand)):
            if cand[i]["src"] != cand[j]["src"] and abs(cand[i]["ts"] - cand[j]["ts"]) < 8 * 3600 \
                    and similar(keys[i], keys[j]):
                parent[find(i)] = find(j)
    groups = {}
    for i, n in enumerate(cand):
        groups.setdefault(find(i), []).append(n)
    events = []
    for members in groups.values():
        srcs = {}
        for n in sorted(members, key=lambda n: n["ts"]):
            srcs.setdefault(n["src"], n)
        refs = list(srcs.values())
        imp = max(n["imp"] for n in members)
        factors = sorted({f for n in members for f in n["factors"]}, key=lambda f: -FACTOR_W[f])
        cn = sum(1 for n in refs if n["lang"] == "zh")
        intl = len(refs) - cn
        text = " ".join(n["title"] for n in members)
        mkt = len(set(x.lower() for x in ALL_MKT_RE.findall(text)))
        fw = max([FACTOR_W[f] for f in factors], default=0)
        result = sum(n["result"] for n in members) * 2 > len(members)
        # 国内各家互相转载很普遍，中文来源数封顶 3；外媒同时报道、命中市场词、属于高权重因素的更可能影响大盘
        score = min(cn, 3) * 1.5 + intl * 2 + imp * 1.5 + fw * 1.5 + min(mkt, 4) - (5 if result else 0)
        if (len(refs) < 2 and imp < 2) or (not factors and mkt == 0):
            continue
        lead = max(members, key=lambda n: (n["imp"], n["lang"] == "zh", len(n["summary"])))
        events.append({"id": "e" + lead["id"], "title": lead["title"], "summary": lead["summary"],
                       "ts": min(n["ts"] for n in members), "factors": factors, "imp": imp, "result": result,
                       "score": round(score, 1), "refs": [n["id"] for n in refs]})
    events.sort(key=lambda e: (-e["score"], -e["ts"]))
    return events[:60]


# ── 主题行情快照（页面平时直连东方财富，这份只在实时接口连不上时兜底）─────────────────

QFIELDS = "f2,f3,f4,f6,f8,f9,f12,f13,f14,f15,f16,f17,f18,f20,f23,f100,f115,f124"


def num(v):
    return v if isinstance(v, (int, float)) else None


def norm_quote(x):
    return {"name": x["f14"], "price": num(x["f2"]), "pct": num(x["f3"]), "chg": num(x["f4"]), "amt": num(x["f6"]),
            "turn": num(x["f8"]), "pe": num(x["f115"]) or num(x["f9"]), "pb": num(x["f23"]), "high": num(x["f15"]),
            "low": num(x["f16"]), "open": num(x["f17"]), "prev": num(x["f18"]), "cap": num(x["f20"]),
            "ind": x["f100"] if x.get("f100") not in (None, "-") else "", "time": num(x["f124"])}


def theme_quotes():
    cfg = json.loads(THEMES.read_text("utf-8"))
    ids = [it["id"] for tab in cfg["tabs"] for sec in tab["sections"] if sec["kind"] == "quote" for it in sec["items"]]
    for t in cfg["themes"].values():
        ids += t["stocks"] + [t[k] for k in ("quote", "board") if t.get(k)]
    ids = [i for i in dict.fromkeys(ids) if not i.startswith("cboe.")]
    out = {}
    for i in range(0, len(ids), 80):
        d = fetch_json(f"https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&ut={EM_UT}"
                       f"&fields={QFIELDS}&secids={','.join(ids[i:i + 80])}")
        for x in (d.get("data") or {}).get("diff") or []:
            if num(x.get("f2")) is not None:
                out[f"{x['f13']}.{x['f12']}"] = norm_quote(x)
    return out, len(ids)


def vix():
    """CBOE 官方延时行情（浏览器跨域不让调，只能由爬虫取）。"""
    d = fetch_json("https://cdn-api.cboe.com/api/global/delayed_quotes/quotes/_VIX.json")["data"]
    prev = d.get("prev_day_close") or d.get("close")
    return {"name": "恐慌指数 VIX", "price": d["current_price"], "pct": round(d["price_change_percent"], 2),
            "chg": d["price_change"], "prev": prev, "open": d.get("open"), "high": d.get("high"), "low": d.get("low"),
            "time": int(datetime.fromisoformat(d["last_trade_time"]).replace(tzinfo=timezone(timedelta(hours=-5))).timestamp())
            if d.get("last_trade_time") else None}


# ── 看盘（A 股，参考财联社看盘页）────────────────────────────────────────────────
# 东方财富：涨停 / 跌停 / 炸板池、昨日涨停今日表现、涨跌分布、成交额、板块、个股排行
# 同花顺：涨停原因、连板天数、涨停板块统计

UTZ = "7eea3edcaed734bea9cbfc24409ed989"     # 东方财富涨停池接口的公开 ut 参数
EM_REF = {"Referer": "https://quote.eastmoney.com/"}
THS_REF = {"Referer": "https://data.10jqka.com.cn/"}
A_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"    # 沪深京 A 股
CONCEPT_SKIP = re.compile(r"昨日|融资融券|沪股通|深股通|MSCI|标普|富时|预盈|预增|转债|破净|AH股|中特估|央企|国企|基金重仓|机构重仓|社保|QFII|HS300|上证|深证|创业板综|东方财富|题材股|次新股|昨日连板|含可转债")


def last_trade_days():
    """最近几个交易日的日期和两市成交额（上证 + 深证）。"""
    amt = {}
    for sid in ("1.000001", "0.399001"):
        d = fetch_json(f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={sid}&ut={EM_UT}&fields1=f1"
                       "&fields2=f51,f57&klt=101&fqt=0&lmt=3&end=20500101", headers=EM_REF)
        for k in d["data"]["klines"]:
            day, a = k.split(",")
            amt[day] = amt.get(day, 0) + float(a)
    days = sorted(amt)
    return days, amt


def em_pool(kind, date, sort, with_date=False):
    path = kind if kind.startswith("get") else "getTopic" + kind      # 昨日涨停的接口名是 getYesterdayZTPool，不带 Topic
    d = fetch_json(f"https://push2ex.eastmoney.com/{path}?ut={UTZ}&dpt=wz.ztzt&Pageindex=0&pagesize=300"
                   f"&sort={urllib.parse.quote(sort)}&date={date}", headers=EM_REF)
    data = d.get("data") or {}
    return (data.get("pool") or [], str(data.get("qdate") or date)) if with_date else data.get("pool") or []


def ths_reasons(date):
    """同花顺涨停池：代码 → (涨停原因, 连板描述)。"""
    out, page = {}, 1
    while page <= 5:
        d = fetch_json(f"https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool?page={page}&limit=100"
                       "&field=199112,10,9001,330323,330324,330325,9002,330329,133971,133970,1968584,3475914,9003,9004"
                       f"&filter=HS,GEM2STAR&order_field=330324&order_type=0&date={date}", headers=THS_REF)
        info = (d.get("data") or {}).get("info") or []
        for x in info:
            out[x["code"]] = (x.get("reason_type") or "", x.get("high_days") or "")
        if len(info) < 100:
            break
        page += 1
    return out


def ths_blocks(date):
    d = fetch_json(f"https://data.10jqka.com.cn/dataapi/limit_up/block_top?filter=HS,GEM2STAR&date={date}", headers=THS_REF)
    out = []
    for b in (d.get("data") or [])[:12]:
        stocks = sorted(b.get("stock_list") or [], key=lambda x: -(x.get("change_rate") or 0))
        out.append({"name": b.get("name"), "zt": b.get("limit_up_num") or 0, "pct": round((b.get("change") or 0), 2),
                    "high": b.get("high") or "",
                    "stocks": [{"id": ("1." if str(x.get("code", "")).startswith(("6", "9")) else "0.") + x.get("code", ""),
                                "name": x.get("name"), "pct": round(x.get("change_rate") or 0, 2)} for x in stocks[:4]]})
    return out


def em_clist(fs, fid, n, fields, po=1):
    d = fetch_json(f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz={n}&po={po}&np=1&fltt=2&invt=2&ut={EM_UT}"
                   f"&fid={fid}&fs={fs}&fields={fields}", headers=EM_REF)
    return [x for x in ((d.get("data") or {}).get("diff") or []) if isinstance(x.get("f3"), (int, float))]


def hhmm(t):
    t = int(t or 0)
    return f"{t // 10000:02d}:{t // 100 % 100:02d}"


def analysis():
    """返回看盘数据；各部分单独 try，某个接口失败不影响其余部分。"""
    # 交易日：涨停池接口按传入日期返回，非交易日会给出最近交易日的 qdate
    zt, date = em_pool("ZTPool", datetime.now(CST).strftime("%Y%m%d"), "fbt:asc", with_date=True)
    day = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    if date != datetime.now(CST).strftime("%Y%m%d"):
        zt = em_pool("ZTPool", date, "fbt:asc")
    A = {"date": day, "updated": datetime.now(CST).isoformat(timespec="seconds"), "errors": []}

    def part(name, fn):
        try:
            return fn()
        except Exception as e:
            A["errors"].append(f"{name}：{str(e)[:80]}")
            return None

    dt = part("跌停池", lambda: em_pool("DTPool", date, "fund:asc")) or []
    zb = part("炸板池", lambda: em_pool("ZBPool", date, "fbt:asc")) or []
    yzt = part("昨日涨停", lambda: em_pool("getYesterdayZTPool", date, "zs:desc")) or []
    rs = part("同花顺涨停原因", lambda: ths_reasons(date)) or {}

    def stock(x, extra=None):
        code = x["c"]
        r = rs.get(code, ("", ""))
        o = {"id": f"{x.get('m', 0)}.{code}", "name": x["n"], "price": round((x.get("p") or 0) / 1000, 2),
             "pct": round(x.get("zdp") or 0, 2), "ind": x.get("hybk") or "", "reason": r[0], "high": r[1]}
        o.update(extra or {})
        return o

    A["pools"] = {
        "zt": [stock(x, {"lbc": x.get("lbc") or 1, "fbt": hhmm(x.get("fbt")), "lbt": hhmm(x.get("lbt")),
                         "zbc": x.get("zbc") or 0, "fund": x.get("fund") or 0}) for x in zt],
        "zb": [stock(x, {"fbt": hhmm(x.get("fbt")), "zbc": x.get("zbc") or 0}) for x in zb],
        "dt": [stock(x) for x in dt],
        "yzt": [stock(x, {"ylbc": x.get("ylbc") or 1}) for x in yzt],
    }
    A["pools"]["lb"] = sorted([s for s in A["pools"]["zt"] if s["lbc"] >= 2], key=lambda s: -s["lbc"])

    # 涨跌分布：东方财富按涨跌幅取整分档，这里合并成财联社那样的 12 档
    fb = part("涨跌分布", lambda: fetch_json(f"https://push2ex.eastmoney.com/getTopicZDFenBu?ut={UTZ}&dpt=wz.ztzt", headers=EM_REF))
    counts = {}
    for it in ((fb or {}).get("data") or {}).get("fenbu") or []:
        for k, v in it.items():
            counts[int(k)] = counts.get(int(k), 0) + v
    nzt, ndt = len(zt), len(dt)
    bins = [("涨停", None), ("+10%", (9, 99)), ("+8%", (7, 8)), ("+6%", (5, 6)), ("+4%", (3, 4)), ("+2%", (1, 2)), ("0%", (0, 0)),
            ("-2%", (-2, -1)), ("-4%", (-4, -3)), ("-6%", (-6, -5)), ("-8%", (-8, -7)), ("-10%", (-99, -9)), ("跌停", None)]
    dist = []
    for label, rg in bins:
        if rg is None:
            dist.append({"label": label, "n": nzt if label == "涨停" else ndt})
        else:
            n = sum(v for k, v in counts.items() if rg[0] <= k <= rg[1])
            if label == "+10%":
                n = max(0, n - nzt)          # 分布里的 +10% 以上含涨停股，扣掉单列
            if label == "-10%":
                n = max(0, n - ndt)
            dist.append({"label": label, "n": n})
    up = sum(v for k, v in counts.items() if k > 0)
    down = sum(v for k, v in counts.items() if k < 0)
    flat = counts.get(0, 0)
    A["dist"] = dist
    A["breadth"] = {"up": up, "down": down, "flat": flat, "zt": nzt, "dt": ndt, "zb": len(zb)}

    # 成交额：今日从实时行情取（沪 + 深）；上一交易日要用 K 线接口，取不到就不显示「较上日」
    def today_amt():
        d = fetch_json(f"https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&ut={EM_UT}&fields=f6&secids=1.000001,0.399001", headers=EM_REF)
        return sum(x["f6"] for x in d["data"]["diff"] if isinstance(x.get("f6"), (int, float)))
    cur = part("成交额", today_amt)
    kl = part("上一交易日成交额", last_trade_days)
    prev = None
    if kl:
        days, amt = kl
        before = [d for d in days if d < day]
        prev = amt.get(before[-1]) if before else None
        cur = amt.get(day, cur)
    A["turnover"] = {"amt": cur, "prev": prev, "diff": (cur - prev) if (cur and prev) else None}

    # 封板率、昨日涨停今日表现
    seal = nzt / (nzt + len(zb)) if (nzt + len(zb)) else None
    ypct = [s["pct"] for s in A["pools"]["yzt"]]
    y_avg = sum(ypct) / len(ypct) if ypct else None
    y_win = sum(1 for p in ypct if p > 0) / len(ypct) if ypct else None
    A["seal"] = seal
    A["yzt"] = {"avg": y_avg, "win": y_win, "n": len(ypct)}

    # 市场热度（自定义，0～100）：上涨家数占比 40%、涨停 / (涨停 + 跌停) 20%、封板率 20%、昨日涨停今日平均涨幅 20%（-5%～+5% 映射到 0～1）
    parts = []
    if up + down:
        parts.append((0.4, up / (up + down)))
    if nzt + ndt:
        parts.append((0.2, nzt / (nzt + ndt)))
    if seal is not None:
        parts.append((0.2, seal))
    if y_avg is not None:
        parts.append((0.2, min(1, max(0, (y_avg + 5) / 10))))
    w = sum(p[0] for p in parts)
    A["heat"] = round(sum(p[0] * p[1] for p in parts) / w * 100, 1) if w else None

    # 分时图标注：涨停股按首次封板时间分到半小时一段，每段里涨停最多的题材（同花顺原因的第一个词，没有就用所属行业）
    buckets = {}
    for s in A["pools"]["zt"]:
        if s["fbt"] == "00:00":
            continue
        tag = (s["reason"].split("+")[0].strip(" “”\"'") if s["reason"] else "") or s["ind"]
        h, m = map(int, s["fbt"].split(":"))
        key = h * 60 + m // 30 * 30
        b = buckets.setdefault(key, {})
        b.setdefault(tag, []).append(s["fbt"])
    labels = []
    for key, tags in buckets.items():
        for tag, ts in sorted(tags.items(), key=lambda kv: -len(kv[1])):
            if len(ts) >= 2 or len(labels) < 4:
                labels.append({"t": min(ts), "tag": tag, "n": len(ts)})
            break
    merged = []                                   # 相邻时段是同一题材的合并成一个标注（如集合竞价和开盘都是「东方」）
    for l in sorted(labels, key=lambda x: x["t"]):
        if merged and merged[-1]["tag"] == l["tag"]:
            merged[-1]["n"] += l["n"]
        else:
            merged.append(dict(l, t=max(l["t"], "09:30")))   # 竞价阶段的涨停标在开盘处
    A["labels"] = merged[:10]

    A["blocks"] = part("同花顺涨停板块", lambda: ths_blocks(date)) or []

    def sectors(fs):
        rows = em_clist(fs, "f3", 60, "f12,f14,f3,f62,f128,f136,f140,f141")
        rows = [r for r in rows if not CONCEPT_SKIP.search(r["f14"]) and not r["f14"].endswith(("Ⅲ", "Ⅱ"))][:8]
        return [{"name": r["f14"], "pct": r["f3"], "flow": r.get("f62"), "lead": r.get("f128"),
                 "leadId": f"{r.get('f141', 0)}.{r.get('f140')}" if r.get("f140") else None, "leadPct": r.get("f136")} for r in rows]
    A["industry"] = part("行业板块", lambda: sectors("m:90+t:2")) or []
    A["concept"] = part("概念板块", lambda: sectors("m:90+t:3")) or []

    def rank(fid):
        rows = em_clist(A_FS, fid, 12, "f2,f3,f8,f12,f13,f14,f62")
        return [{"id": f"{r['f13']}.{r['f12']}", "name": r["f14"], "price": r.get("f2"), "pct": r["f3"],
                 "turn": r.get("f8"), "flow": r.get("f62")} for r in rows]
    A["rank"] = {"gain": part("涨幅榜", lambda: rank("f3")) or [], "flow": part("主力净流入榜", lambda: rank("f62")) or [],
                 "turn": part("换手率榜", lambda: rank("f8")) or []}
    return A


# ── 主流程 ────────────────────────────────────────────────────────────────────

def run_source(spec):
    sid, name, region, abbr, color, paywall, fn = spec
    info = {"id": sid, "name": name, "region": region, "abbr": abbr, "color": color, "paywall": paywall,
            "ok": True, "count": 0, "error": ""}
    try:
        items = fn()
        info["count"] = len(items)
        if not items:
            info.update(ok=False, error="empty")
        return info, items
    except Exception as e:
        info.update(ok=False, error=str(e)[:160])
        return info, []


def main():
    t0 = time.time()
    now = int(time.time())
    prev = {}
    if OUT.exists():
        try:
            prev = json.loads(OUT.read_text("utf-8"))
        except Exception:
            prev = {}

    with ThreadPoolExecutor(8) as ex:
        results = list(ex.map(run_source, SOURCES))
    sources = [r[0] for r in results]

    # 保留 48 小时；分时数据还停在更早的交易日时（周末、节假日），保留到那段行情开始前 12 小时，异动原因才找得到
    pts_from = [m["pts"][0][0] for m in prev.get("markets", []) if m.get("pts")]
    keep_from = min([now - KEEP_HOURS * 3600] + [t - 12 * 3600 for t in pts_from])
    pool = {n["id"]: n for n in prev.get("news", []) if n["ts"] >= keep_from}
    fresh = 0
    for _, items in results:
        for n in items:
            if n["ts"] < keep_from or n["ts"] > now + 600 or not n["title"]:
                continue
            old = pool.get(n["id"])
            if old is None:
                fresh += 1
            elif n.get("est"):
                n["ts"] = old["ts"]               # 估计时间以第一次见到为准
            pool[n["id"]] = n
    for n in pool.values():                       # 每次都重新打标签，改了关键词立即生效
        tag(n)
    news_list = sorted(pool.values(), key=lambda n: -n["ts"])

    markets, merrs = market_data(news_list, prev.get("markets", []))
    sources.append({"id": "quotes", "name": "东方财富分时", "region": "data", "abbr": "行情", "color": "#888",
                    "paywall": False, "ok": not merrs and bool(markets), "count": len(markets), "error": "；".join(merrs)})
    if not markets and prev.get("markets"):
        markets = prev["markets"]                  # 行情全挂时沿用上一份

    events = build_events(news_list, now)

    quotes = dict(prev.get("quotes", {}))
    try:
        got, total = theme_quotes()
        quotes.update(got)
        sources.append({"id": "themes", "name": "主题行情快照", "region": "data", "abbr": "主题", "color": "#888",
                        "paywall": False, "ok": bool(got), "count": len(got), "error": "" if len(got) >= total * 0.9 else f"{total - len(got)} 只取不到"})
    except Exception as e:
        sources.append({"id": "themes", "name": "主题行情快照", "region": "data", "abbr": "主题", "color": "#888",
                        "paywall": False, "ok": False, "count": 0, "error": str(e)[:160]})
    try:
        quotes["cboe.VIX"] = vix()
        sources.append({"id": "vix", "name": "CBOE 恐慌指数", "region": "data", "abbr": "VIX", "color": "#888",
                        "paywall": False, "ok": True, "count": 1, "error": ""})
    except Exception as e:
        sources.append({"id": "vix", "name": "CBOE 恐慌指数", "region": "data", "abbr": "VIX", "color": "#888",
                        "paywall": False, "ok": False, "count": 0, "error": str(e)[:160]})

    data = {
        "updated": datetime.now(CST).isoformat(timespec="seconds"),
        "factors": [{"key": k, "name": n, "weight": w} for k, n, w, _ in FACTORS],
        "sources": sources,
        "markets": markets,
        "events": events,
        "quotes": quotes,
        "news": news_list,
    }
    OUT.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), "utf-8")

    try:
        A = analysis()
        OUT_A.write_text(json.dumps(A, ensure_ascii=False, separators=(",", ":")), "utf-8")
        print(f"→ analysis.json：{A['date']} 涨停 {A['breadth']['zt']} 跌停 {A['breadth']['dt']} 炸板 {A['breadth']['zb']}，"
              f"热度 {A['heat']}，标注 {len(A['labels'])}，失败：{A['errors'] or '无'}")
    except Exception as e:
        print("✗ analysis.json 生成失败，沿用上一份：", str(e)[:160])
    nmoves = sum(len(m["moves"]) for m in markets)
    print(f"→ data.json：新闻 {len(news_list)}（新增 {fresh}），行情 {len(markets)}/{len(MARKETS)}，异动 {nmoves}，"
          f"大事 {len(events)}，用时 {time.time() - t0:.1f}s")
    for s in sources:
        print(f"  {'✓' if s['ok'] else '✗'} {s['name']:<16} {s['count']:>4}  {s['error']}")


if __name__ == "__main__":
    main()
