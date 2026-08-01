/* ReDNS documentation — page dictionaries for the 16 matcher pages. */
rednsI18nAdd({
  "zh-CN": {
    "matcher-qname": {
      "meta.title": "qname — ReDNS 文档",
      "page.lead": "按一组域名模式匹配查询名。这是屏蔽与路由规则的主力。",
      "s.usage.p": "写 <code>qname</code> 后跟空格分隔的域名表达式。裸域名匹配该域及其所有子域。表达式可以带类型前缀。",
      "p.expressions.d": "域名模式。裸写 <code>example.com</code> = 子域匹配（该域及其所有子域）。前缀：<code>domain:</code> 子域（显式）、<code>full:</code> 精确、<code>keyword:</code> 子串、<code>regexp:</code> 正则。<code>&amp;path</code> 从文件加载，每行一个模式。",
      "s.details.li1": "匹配不区分大小写；比较前会剥离首尾的点。",
      "s.details.li2": "用 <code>&amp;</code> 加载的文件在启动时读取；空行和以 <code>#</code> 开头的行会被忽略。",
      "s.details.li3": "基于字典树（trie）的匹配与列表大小无关，按标签数 O(labels) 查找——大型屏蔽列表开销极低。",
      "s.details.li4": "空参数永不匹配——需要正向默认时请使用 <code>has_resp</code>。",
    },

    "matcher-qtype": {
      "meta.title": "qtype — ReDNS 文档",
      "page.lead": "按数字 DNS 记录类型码匹配查询类型——A、AAAA、MX 等。",
      "p.types.d": "要接受的 DNS 记录类型码。查询类型在该集合中时匹配。空参数永不匹配。",
      "s.codes.t": "常用类型码",
    },

    "matcher-qclass": {
      "meta.title": "qclass — ReDNS 文档",
      "page.lead": "按数字 DNS 类别码匹配查询类别。实际上几乎所有查询都是类别 <code>1</code>（IN）。",
      "p.classes.d": "要接受的 DNS 类别码。查询类别在该集合中时匹配。常用：<code>1</code> IN、<code>3</code> CH、<code>254</code> NONE、<code>255</code> ANY。",
    },

    "matcher-client-ip": {
      "meta.title": "client_ip — ReDNS 文档",
      "page.lead": "将发送查询的客户端地址与一组 CIDR 范围匹配。非常适合按网络路由和限速。",
      "p.cidrs.d": "要接受的范围，例如 <code>192.168.0.0/16 10.0.0.1 ::1</code>。裸 IP 视为 /32（IPv4）或 /128（IPv6）。IPv4 和 IPv6 可以混用。",
      "s.details.li1": "源地址取自查询到达的套接字；经其他解析器转发的查询会显示为那个解析器的地址。",
      "s.details.li2": "查找复杂度为 O(不同前缀长度)——大型允许/屏蔽列表依然很快。",
    },

    "matcher-resp-ip": {
      "meta.title": "resp_ip — ReDNS 文档",
      "page.lead": "将当前响应中的任意 A/AAAA 地址与 CIDR 范围匹配。适合屏蔽或改路由解析到不受欢迎网络的应答。",
      "p.cidrs.d": "要与应答地址匹配的范围。裸 IP 视为 /32 或 /128。",
      "s.details.li1": "只检查 A 和 AAAA 记录；CNAME 链会沿应答记录追踪。",
      "s.details.li2": "匹配器检查的是<em>当前</em>响应——当响应由更早的规则产生时，请与 <code>has_resp</code> 组合使用。",
      "s.details.li3": "没有响应时返回 false。",
    },

    "matcher-cname": {
      "meta.title": "cname — ReDNS 文档",
      "page.lead": "匹配当前响应中出现的 CNAME 目标。适合捕获指向您想屏蔽的域名的别名。",
      "p.patterns.dflt": "永不匹配",
      "p.patterns.d": "CNAME 目标模式，转为小写。前导点（<code>.cdn.example</code>）匹配任何以该后缀结尾的目标；否则精确匹配。",
      "s.details.li1": "只检查应答区；比较时去掉末尾点。",
      "s.details.li2": "没有响应或没有 CNAME 记录时返回 false。",
    },

    "matcher-rcode": {
      "meta.title": "rcode — ReDNS 文档",
      "page.lead": "匹配当前响应的响应码——NOERROR、SERVFAIL、NXDOMAIN、REFUSED 等。",
      "p.codes.d": "要接受的 rcode 值。常用：<code>0</code> NOERROR、<code>1</code> FORMERR、<code>2</code> SERVFAIL、<code>3</code> NXDOMAIN、<code>4</code> NOTIMP、<code>5</code> REFUSED。",
      "s.details.li1": "尚未产生响应时返回 false。",
      "s.details.li2": "记住链的语义：来自上游的 SERVFAIL 是最终结果，<a href=\"exec-fallback.html\"><code>fallback</code></a> 不会重新解析——对它匹配让<em>您</em>来决定下一步。",
    },

    "matcher-random": {
      "meta.title": "random — ReDNS 文档",
      "page.lead": "以可配置的概率随机匹配每个查询。非常适合 A/B 测试、概率采样或对一部分流量限速。",
      "p.probability.d": "匹配概率，取值 <code>[0.0, 1.0]</code>。<code>0.0</code> 永不匹配，<code>1.0</code> 总是匹配，<code>0.5</code> 匹配一半查询。超出范围的值在启动时被拒绝。",
      "s.details.li1": "每个查询都会重新随机抽取，因此随时间推移匹配比例会收敛到该概率。",
      "s.details.li2": "与同一规则中的其他匹配器组合可实现「采样且……」的条件；「或」语义则写两条规则。",
    },

    "matcher-env": {
      "meta.title": "env — ReDNS 文档",
      "page.lead": "当环境变量已设置——或等于特定值时匹配。在启动时求值一次，因此可当作静态功能开关。",
      "p.key.d": "环境变量名。仅此一个 token 时，变量已设置即匹配，无论其值为何。",
      "p.value.d": "可选的第二个 token。给出时，仅当变量值与之完全相等时才匹配。其余 token 被忽略。",
      "s.details.li1": "检查发生在配置加载时——之后修改环境变量无效。",
      "s.details.li2": "无参数时永不匹配。",
      "s.details.li3": "需要逐查询的动态条件时，请参见使用 <code>$ENV_VAR</code> 来源的 <a href=\"matcher-string-exp.html\"><code>string_exp</code></a>。",
    },

    "matcher-ptr-ip": {
      "meta.title": "ptr_ip — ReDNS 文档",
      "page.lead": "从 PTR（反向 DNS）查询名中提取 IP 地址并与 CIDR 范围匹配——例如自行应答本地反向查询。",
      "p.cidrs.d": "要与内嵌 IP 匹配的范围。IPv4（<code>in-addr.arpa</code>）和 IPv6（<code>ip6.arpa</code>）PTR 名都会被解码。",
      "s.details.li1": "非 PTR 查询永不匹配（没有可提取的 IP）。",
      "s.details.li2": "与 <a href=\"exec-reverse-lookup.html\"><code>reverse_lookup</code></a> 组合，可以为自己的网段应答反向 DNS，无需上游往返。",
    },

    "matcher-string-exp": {
      "meta.title": "string_exp — ReDNS 文档",
      "page.lead": "对源值（环境变量或服务器元数据，如请求 URL 路径）应用字符串操作符。",
      "s.usage.p": "格式：<code>string_exp &lt;source&gt; &lt;op&gt; [values…]</code>。",
      "p.source.d": "从哪里读取字符串：<code>$ENV_VAR</code>（环境变量，启动时读取）、<code>url_path</code>（服务器元数据中的请求 URL 路径）或 <code>server_name</code>（元数据中的服务器名）。",
      "p.op.d": "操作符：<code>zl</code>（值为空）、<code>eq</code>（等于任一值）、<code>prefix</code>、<code>suffix</code>、<code>contains</code>、<code>regexp</code>（任一值是匹配的正则）。",
      "p.values.d": "一个或多个比较字符串。除 <code>zl</code> 外的所有操作符都需要；<code>eq</code>/<code>prefix</code>/<code>suffix</code>/<code>contains</code> 只要任一值匹配即触发，<code>regexp</code> 则任一值作为正则匹配即触发。",
      "s.details.li1": "<code>$ENV_VAR</code> 来源在配置加载时读取（与 <a href=\"matcher-env.html\"><code>env</code></a> 相同）；元数据来源逐查询求值。",
      "s.details.li2": "<code>url_path</code> 和 <code>server_name</code> 由服务器元数据填充——用于注入请求上下文的高级/嵌入式部署。",
    },

    "matcher-asn": {
      "meta.title": "asn — ReDNS 文档",
      "page.lead": "当响应中的任意 A/AAAA 地址属于给定自治系统（ASN）时匹配——例如屏蔽或改路由托管在特定网络上的应答。",
      "p.asns.d": "自治系统号，可带 <code>AS</code> 前缀（<code>13335</code> 或 <code>AS13335</code>）。任一应答地址属于任一列出的 ASN 时匹配。",
      "s.db.t": "数据库要求",
      "s.db.li1": "由 MaxMind DB 文件（GeoIP2/GeoLite2 或 sapics <code>*-asn</code> 数据库）支撑。请将顶层 <code>asn_db</code> 配置键指向您的文件。",
      "s.db.li2": "省略 <code>asn_db</code> 且使用了 <code>asn</code> 匹配器时，ReDNS 会在首次使用时自动下载默认的 <code>origin-asn.mmdb</code>（约 10 MB），并缓存在 <code>$XDG_CACHE_HOME/redns/</code> 或 <code>~/.cache/redns/</code> 下。",
      "s.db.li3": "可以列出多个文件；所有文件的并集参与查询。",
    },

    "matcher-has-resp": {
      "meta.title": "has_resp — ReDNS 文档",
      "page.lead": "当更早的规则已为当前查询产生响应时返回 true。「能答就答，否则放行」链的基石。",
      "s.usage.p": "不带参数。通常与 <a href=\"exec-accept.html\"><code>accept</code></a> 搭配，一旦有响应就终止链：",
      "p.none.d": "此匹配器不带参数。",
      "s.details.li1": "响应可来自 <a href=\"exec-cache.html\"><code>cache</code></a>、<a href=\"exec-hosts.html\"><code>hosts</code></a>、<a href=\"exec-black-hole.html\"><code>black_hole</code></a>、<a href=\"exec-forward.html\"><code>forward</code></a> 或任何设置了响应的动作。",
      "s.details.li2": "反向——<code>!has_resp</code>——只在尚无响应时匹配；是转发前的典型守卫。",
    },

    "matcher-has-wanted-ans": {
      "meta.title": "has_wanted_ans — ReDNS 文档",
      "page.lead": "当当前响应至少包含一条应答记录时返回 true——比 <a href=\"matcher-has-resp.html\"><code>has_resp</code></a> 更严格，忽略空 NOERROR 或错误响应。",
      "s.usage.p": "不带参数：",
      "p.none.d": "此匹配器不带参数。",
      "s.details.li1": "只有应答区计数——只有授权或附加记录的响应不匹配。",
      "s.details.li2": "没有响应时返回 false。",
    },

    "matcher-domain-set": {
      "meta.title": "domain_set — ReDNS 文档",
      "page.lead": "具名、可复用的域名表达式集合。用 tag 声明一次，即可从任何规则引用——与 <a href=\"matcher-qname.html\"><code>qname</code></a> 使用同一引擎。",
      "s.usage.p": "声明为具名插件，然后用 <code>$tag</code> 引用：",
      "p.exps.d": "内联域名表达式：裸域名（子域匹配）或带前缀（<code>domain:</code>、<code>full:</code>、<code>keyword:</code>、<code>regexp:</code>）。",
      "p.files.d": "每行一个表达式的文件；空行和 <code>#</code> 注释被忽略。适合共享屏蔽列表。",
      "s.args.note": "<code>args</code> 也可以是普通的空格分隔表达式字符串（快速形式），例如 <code>args: \"ads.example keyword:tracker &amp;/etc/redns/ads.txt\"</code>。应至少提供一个表达式或文件。",
      "s.details.li1": "匹配不区分大小写，并剥离首尾点。",
      "s.details.li2": "字典树查找复杂度为 O(标签数)——大列表每次查询的开销可忽略。",
      "s.details.li3": "同一个集合可被多个序列中的许多规则引用。",
    },

    "matcher-ip-set": {
      "meta.title": "ip_set — ReDNS 文档",
      "page.lead": "具名、可复用的 IP 与 CIDR 集合。与 <a href=\"matcher-resp-ip.html\"><code>resp_ip</code></a> 一样匹配当前响应中的地址——但只需声明一次即可跨规则共享。",
      "s.usage.t": "用法",
      "p.ips.d": "内联 IP 和 CIDR。裸 IP 视为 /32（IPv4）或 /128（IPv6）。",
      "p.files.d": "每行一个 IP/CIDR 的文件；空行和 <code>#</code> 注释被忽略。",
      "s.args.note": "<code>args</code> 也可以是普通的空格分隔字符串（快速形式），例如 <code>args: \"203.0.113.0/24 198.51.100.7\"</code>。",
      "s.details.li1": "匹配当前响应中的 A/AAAA 地址——当响应来自更早的规则时，请与 <code>has_resp</code> 组合。",
      "s.details.li2": "查找复杂度为 O(不同前缀长度)，与集合大小无关。",
    },
  },
});
