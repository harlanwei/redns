/* ReDNS documentation — page dictionaries for the executable pages and sequence. */
rednsI18nAdd({
  "zh-CN": {
    "exec-forward": {
      "meta.title": "forward — ReDNS 文档",
      "page.lead": "将查询转发给一个或多个上游解析器。这就是 ReDNS 真正解析域名的方式——大多数链末尾的插件。",
      "s.usage.quick": "快速形式——空格分隔的上游地址（裸 <code>host:port</code> 表示 UDP）：",
      "s.usage.yaml": "完整形式——带各上游选项的 YAML：",
      "p.upstreams.d": "上游列表。每个条目：<code>{addr, tag, dial_addr, bootstrap, pool_max_idle}</code>（见下文）。",
      "p.addr.d": "带任意支持协议的上游地址：<code>udp://</code>、<code>tcp://</code>、<code>tls://</code>、<code>https://</code>、<code>quic://</code>、<code>h3://</code>。裸 <code>host:port</code> 表示 UDP。协议表见<a href=\"upstreams.html\">上游</a>。",
      "p.tag.d": "可选标签，显示在指标和仪表盘的上游视图中。",
      "p.dial_addr.d": "连接此 IP:端口而非解析主机名。端口按协议取默认值（53/853/443）。系统解析器不可用时很有用。",
      "p.bootstrap.d": "用于解析上游主机名的 DNS 服务器（适用于 DoH/DoT）。",
      "p.pool_max_idle.d": "单个上游的空闲连接/套接字池上限。也可在 forward 层设置一次，应用于所有上游。",
      "p.concurrent.d": "同时查询的上游数量；延迟最低的最佳响应胜出。与延迟感知的选择机制配合。",
      "p.subprocess_suffix.d": "通过 Unix 套接字使用 DNS 的外部进程上游的后缀——面向自定义解析器的高级集成点。",
      "s.details.li1": "上游按 EMA 延迟和连续错误数评分；健康、快速的上游优先。",
      "s.details.li2": "启动时会探测 DoT 流水线（RFC 7766）支持，让查询共享一条 TLS 连接。",
      "s.details.li3": "带 TC 标志的 UDP 响应会自动改走 TCP 重试。",
      "s.details.li4": "DoH 请求使用浏览器风格 User-Agent，长查询会从 GET 自动回退到 POST。",
      "s.details.li5": "如果所有上游都无响应，链会继续——后面的规则（或 <code>best_effort</code> 选项）仍可处理该查询。",
    },

    "exec-cache": {
      "meta.title": "cache — ReDNS 文档",
      "page.lead": "分片 LRU 响应缓存，支持过期数据延迟刷新（stale-while-refresh）、请求合并和可选的磁盘持久化。放在 <code>forward</code> 之前，可让大多数重复查询无需触碰上游。",
      "s.usage.quick": "快速形式——仅条目数：",
      "s.usage.yaml": "完整形式——带磁盘持久化：",
      "p.size.d": "LRU 容量（条目数）。<code>0</code> 禁用缓存（插件变为直通）。",
      "p.cache_file.d": "设置后，缓存每隔 <code>dump_interval</code> 秒以及优雅退出时转储到此文件，启动时恢复。文件缺失或损坏时以空缓存启动。",
      "p.dump_interval.d": "设置 <code>cache_file</code> 时两次自动转储之间的间隔。",
      "s.details.li1": "<strong>分片 LRU</strong>——内存有界、锁竞争低；条目按 TTL 过期。",
      "s.details.li2": "<strong>延迟刷新</strong>——过期条目立即返回，同时后台刷新获取新副本（最多容忍 30 秒过期）。",
      "s.details.li3": "<strong>请求合并</strong>——同一键的并发未命中只触发一次上游查询。",
      "s.details.li4": "缓存键为小写 qname + qtype；响应以归一化 TTL 存储，命中时原地修补。",
      "s.details.li5": "命中时插件设置响应——之后用 <code>matches: has_resp</code> + <code>accept</code> 终止链。",
    },

    "exec-fallback": {
      "meta.title": "fallback — ReDNS 文档",
      "page.lead": "先运行主解析器，当主解析器太慢或拒绝查询时切换到备用——采用 DNSSEC 安全的语义。",
      "s.usage.p": "声明为具名插件，引用另外两个具名可执行插件（通常是 <code>forward</code> 组或序列），然后在规则中使用 <code>$tag</code>：",
      "p.primary.d": "主执行器的 tag，例如一个 <code>forward</code> 组。必须在 fallback 之前注册（ReDNS 会自动解析）。",
      "p.secondary.d": "备用执行器的 tag；当主解析器太慢、拒绝或无响应时被查询。",
      "p.threshold.d": "查询备用前等待主解析器的时间。<code>0</code> 也视为 500 ms。",
      "p.always_standby.d": "为 true 时，备用从一开始就并行运行（对冲），若它先返回则采用其响应。",
      "s.details.li1": "<strong>SERVFAIL 是最终结果。</strong>主解析器的 SERVFAIL 原样返回——重新解析 DNSSEC 无效应答会静默破坏 DNSSEC。",
      "s.details.li2": "REFUSED——或无响应——会落到备用。",
      "s.details.li3": "只有最快的可用响应会被写入上下文。",
    },

    "exec-ecs": {
      "meta.title": "ecs — ReDNS 文档",
      "page.lead": "在查询发往上游之前附加 EDNS 客户端子网（ECS）选项——使用真实客户端地址或固定预设——让 CDN 返回位置最优的应答。两个名字注册同一个插件。",
      "p.preset.dflt": "客户端地址",
      "p.preset.d": "省略时，将客户端的真实源地址作为 ECS 子网发送。给出时，每个查询都发送该固定 IP（尾部 <code>/mask</code> 后缀会被接受但忽略）。",
      "s.details.li1": "子网掩码固定：IPv4 /24、IPv6 /48。",
      "s.details.li2": "把规则放在 <a href=\"exec-forward.html\"><code>forward</code></a> 步骤之前；选项会附加到后续规则发往上游的查询上。",
      "s.details.li3": "如果查询已带 ECS 选项，则保持不变。",
      "s.details.li4": "并非所有解析器都支持 ECS——请查阅您的上游文档。",
    },

    "exec-redirect": {
      "meta.title": "redirect — ReDNS 文档",
      "page.lead": "将对一个域名的查询改写为另一个域名，在下游解析目标并用 CNAME 链接原名——经典的别名语义。",
      "s.usage.p": "每行映射一个域名到另一个：<code>from to</code>。多行请使用 YAML 块标量。空行和 <code>#</code> 注释被忽略；每行必须恰好两个字段。",
      "p.rules.d": "重定向映射。查询名匹配某个 <code>from</code> 条目时，用 <code>to</code> 的记录应答。匹配是精确的。",
      "s.details.li1": "查询名在链的其余部分（例如 <code>forward</code>）运行前被改写为目标，之后再恢复。",
      "s.details.li2": "收到响应后，问题区会被修正，并插入一条 CNAME 记录（TTL 1）把原名链接到目标——客户端可以跟随该链。",
      "s.details.li3": "没有匹配规则的查询原样通过。",
      "s.details.li4": "只改写查询名——qtype 和 qclass 保持不变。",
    },

    "exec-hosts": {
      "meta.title": "hosts — ReDNS 文档",
      "page.lead": "从静态 <code>hosts</code> 文件式映射提供 A/AAAA 应答——非常适合局域网名称和必须绕过上游的分裂视图（split-horizon）配置。",
      "s.usage.p": "每行将域名映射到一个或多个 IP（域名在前）：<code>domain IP [IP…]</code>。多行请使用 YAML 块标量。",
      "p.entries.d": "静态映射。每行至少两个字段；字段不足的行被跳过。每个域名的 IPv4 和 IPv6 地址可以混用。",
      "s.details.li1": "A 查询只获得 A 记录，AAAA 查询只获得 AAAA 记录；其他类型无应答。",
      "s.details.li2": "应答使用 TTL 300，rcode NOERROR。",
      "s.details.li3": "插件会设置响应但不会终止链——之后请加 <code>matches: has_resp</code> + <code>accept</code>，以免后面的规则覆盖应答。",
    },

    "exec-black-hole": {
      "meta.title": "black_hole — ReDNS 文档",
      "page.lead": "用固定 IP 应答 A/AAAA 查询——经典的「屏蔽此域名」动作。客户端得到一个指向空处的成功应答。",
      "p.ips.d": "IPv4 地址应答 A 查询；IPv6 地址应答 AAAA 查询。至少提供一个 IP。",
      "s.details.li1": "应答使用 TTL 300，rcode NOERROR。",
      "s.details.li2": "没有匹配 IP 族的查询类型会得到空 NOERROR 应答。",
      "s.details.li3": "插件会设置响应但不会终止链——如需防止后面的规则覆盖应答，请在其后加 <code>matches: has_resp</code> + <code>accept</code>。",
    },

    "exec-ttl": {
      "meta.title": "ttl — ReDNS 文档",
      "page.lead": "把响应中的每个 TTL 固定为单一值，或限制在某个区间内——控制缓存时长和客户端缓存的一根简单杠杆。",
      "p.value.d": "单个整数将所有 TTL 固定为该值（秒）。<code>min-max</code> 表示钳制：低于 <code>min</code> 的调高、高于 <code>max</code> 的调低，区间内保持不变。",
      "s.details.li1": "作用于应答、授权和附加记录；OPT 伪记录的 TTL 字段（承载 EDNS 标志）保持不动。",
      "s.details.li2": "把规则放在产生响应的动作（例如 <code>forward</code>）之后。",
    },

    "exec-reverse-lookup": {
      "meta.title": "reverse_lookup — ReDNS 文档",
      "page.lead": "从看到的 A/AAAA 应答中学习 IP→域名映射，并直接用该缓存应答 PTR（反向 DNS）查询——无需上游往返。",
      "s.usage.p": "不带参数时使用默认缓存大小。在规则中用 <code>$reverse</code> 引用。",
      "p.size.d": "内存中保留的 IP→域名条目数上限。满时淘汰最旧的条目。",
      "s.fixed.p": "其他行为固定：PTR 查询直接从缓存处理（<code>handle_ptr</code>），缓存映射 7200 秒后过期。",
      "s.details.li1": "每当 A/AAAA 响应经过时学习映射——不产生额外上游流量。",
      "s.details.li2": "IPv4（<code>in-addr.arpa</code>）和 IPv6（<code>ip6.arpa</code>）PTR 名都会被解码。",
      "s.details.li3": "没有缓存映射的 PTR 查询会落到后面的规则（例如 <code>forward</code>）。",
    },

    "exec-use-answer-of": {
      "meta.title": "use-answer-of — ReDNS 文档",
      "page.lead": "用单个目标域名的记录应答当前查询，并用 CNAME 链接两者——<a href=\"exec-redirect.html\"><code>redirect</code></a> 的单目标表亲。",
      "p.qname.d": "应答所用记录的目标域名。末尾点可选。若目标与原始查询名相同，插件什么都不做。",
      "s.details.li1": "查询名在后面的规则运行前被改写为目标——下游的 <code>forward</code> 解析目标——然后再恢复。",
      "s.details.li2": "响应的问题区会被修正，并插入一条 CNAME 记录把原名链接到目标。",
      "s.details.li3": "查询类型和类别保持不变。",
    },

    "exec-shuffle": {
      "meta.title": "shuffle — ReDNS 文档",
      "page.lead": "随机打乱应答、授权和附加区的顺序——把客户端分散到多 A 记录各地址上的经典轮询技术。",
      "s.usage.p": "不带参数。放在产生响应的动作之后：",
      "p.none.d": "此执行器不带参数。三个区（应答、授权、附加）都会被洗牌。",
      "s.details.li1": "每次查询都会重新洗牌，因此不同的客户端会得到不同的地址顺序。",
      "s.details.li2": "记录只会被重排——绝不丢弃或复制。",
    },

    "exec-accept": {
      "meta.title": "accept — ReDNS 文档",
      "page.lead": "终止当前查询的规则求值。迄今产生的响应——如果有——返回给客户端。",
      "s.usage.p": "不带参数。典型模式——能缓存则从缓存应答，否则转发：",
      "p.none.d": "此执行器不带参数。",
      "s.details.li1": "尚无响应时，服务器返回空 NOERROR 应答——因此链末尾无条件 <code>accept</code> 是安全的「到此为止」标记。",
      "s.details.li2": "与 <a href=\"exec-reject.html\"><code>reject</code></a> 对比：后者总是产生带 rcode 的响应。",
    },

    "exec-reject": {
      "meta.title": "reject — ReDNS 文档",
      "page.lead": "以错误 rcode 应答查询并终止链。拒绝不想解析的查询的礼貌方式。",
      "p.rcode.d": "应答的响应码。常用值：<code>0</code> NOERROR、<code>1</code> FORMERR、<code>2</code> SERVFAIL、<code>3</code> NXDOMAIN、<code>4</code> NOTIMP、<code>5</code> REFUSED。",
      "s.details.li1": "响应不带任何应答记录——只有 rcode。",
      "s.details.li2": "链立即终止；后面的规则不会为该查询运行。",
    },

    "exec-sleep": {
      "meta.title": "sleep — ReDNS 文档",
      "page.lead": "在链继续之前将查询暂停固定的毫秒数。适合对滥用客户端限速或模拟延迟。",
      "p.ms.d": "延迟查询的时长。延迟是异步的——不会阻塞其他查询。",
      "s.details.li1": "延迟在后面的规则运行前生效，因此也会推迟上游查询。",
      "s.details.li2": "超时较短的客户端会放弃——与 <code>client_ip</code> 或 <code>random</code> 组合以针对特定流量。",
    },

    "exec-debug-print": {
      "meta.title": "debug_print — ReDNS 文档",
      "page.lead": "以 debug 级别记录完整的查询上下文。当您需要看清规则收到的输入时，这是首选工具。",
      "s.usage.p": "不带参数。记得开启 debug 日志（<code>log.level: debug</code> 或 <code>RUST_LOG=debug</code>）：",
      "p.none.d": "此执行器不带参数。",
      "s.details.li1": "查询、当前响应和服务器元数据都会被写入日志。",
      "s.details.li2": "之后链正常继续——不做任何修改。",
      "s.details.li3": "与 <a href=\"matcher-random.html\"><code>random</code></a> 组合可以只采样一部分查询，而不是记录所有内容。",
    },

    "exec-drop-resp": {
      "meta.title": "drop_resp — ReDNS 文档",
      "page.lead": "丢弃迄今产生的任何响应。客户端收不到任何应答，最终超时——最隐蔽的屏蔽域名方式。",
      "s.usage.p": "不带参数：",
      "p.none.d": "此执行器不带参数。",
      "s.details.li1": "链继续，但上下文为空——后面的规则看不到响应。",
      "s.details.li2": "客户端会重试其他解析器，因此最好与 <code>accept</code> 组合，或放在匹配流量的最后。",
    },

    "exec-udp-server": {
      "meta.title": "udp_server — ReDNS 文档",
      "page.lead": "以插件形式声明 UDP DNS 监听器——对于把所有内容围绕插件组织的配置，这是 <code>servers</code> 段的替代方案。",
      "p.entry.d": "处理该监听器上查询的 sequence 插件 tag。运行时必填——缺失时 ReDNS 拒绝启动。",
      "p.listen.d": "绑定地址，例如 <code>\"0.0.0.0:53\"</code>。",
      "p.udp_workers.d": "epoll UDP 后端的接收循环工作线程数。",
      "p.udp_max_inflight.d": "并发进行中的 UDP 处理任务数上限。",
      "s.details.li1": "UDP 后端（<code>epoll</code> / <code>io-uring</code>）通过 <code>--udp-backend</code> CLI 标志选择，而非按插件设置。",
      "s.details.li2": "可以声明多个监听器；每个可使用不同的入口序列。",
      "s.details.li3": "等价于 <code>protocol: udp</code> 的 <code>servers</code> 条目。",
    },

    "exec-tcp-server": {
      "meta.title": "tcp_server — ReDNS 文档",
      "page.lead": "以插件形式声明 TCP DNS 监听器——<a href=\"exec-udp-server.html\"><code>udp_server</code></a> 的 TCP 对应物。",
      "p.entry.d": "处理该监听器上查询的 sequence 插件 tag。运行时必填——缺失时 ReDNS 拒绝启动。",
      "p.listen.d": "绑定地址，例如 <code>\"0.0.0.0:53\"</code>。",
      "p.udp.d": "为与 <code>udp_server</code> 保持一致而接受；对 TCP 监听器无影响。",
      "s.details.li1": "可以声明多个监听器；每个可使用不同的入口序列。",
      "s.details.li2": "等价于 <code>protocol: tcp</code> 的 <code>servers</code> 条目。",
      "s.details.li3": "要在一个地址上同时提供两种协议，请在 <code>servers</code> 段使用 <code>protocol: udp+tcp</code>。",
    },

    "plugin-sequence": {
      "meta.title": "sequence — ReDNS 文档",
      "page.lead": "ReDNS 核心的规则容器：为每个查询求值的有序规则列表。每个监听器通过 <code>entry</code> 指向一个序列。",
      "p.rules.d": "有序规则列表。每条规则：<code>{matches, exec}</code>。",
      "rules.matches.dflt": "始终触发",
      "p.matches.d": "匹配器指令；必须全部匹配规则才会触发。<code>!</code> 取反；<code>$tag</code> 引用具名匹配器。参见<a href=\"rules.html\">规则</a>页面。",
      "p.exec.d": "动作：<code>type args</code> 或 <code>$tag</code> 引用。",
      "s.details.li1": "序列最后构建，晚于所有其他具名插件，因此无论文件顺序如何，<code>$tag</code> 引用总能解析。",
      "s.details.li2": "监听器的 <code>entry</code> 必须指向序列的 tag；匿名（无 tag）序列不能作为入口。",
      "s.details.li3": "序列可以通过 <code>$tag</code> 引用另一个序列——复用公共「前奏」很方便。",
    },
  },
});
