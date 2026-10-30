# 购物车与订单 SKU/价格契约（PR8 修订）

## 变更记录与迁移

- **变更**：购物车增减和删除以 `user_id/product_id/sku_id` 定位；不再按商品合并或按客户端规格选 SKU。立即购买不再要求或信任 `price`，所有订单从锁定的 SKU 行读取价格，条件扣减库存。
- `quantity` 必须是 JSON 整数 1..2147483647；字符串、布尔、小数、零和负数均 422。数据库遗留非法数量也拒绝结算。
- `sku_id` 兼容缺失/null，但仅商品恰好一个 SKU 时自动确定。多 SKU 缺失返回 422；增减和下单时明确传入不属于商品的 SKU 返回 422。不根据规格猜选。删除显式 SKU 时按购物车三元组删除，即使目录中 SKU 已失效也可清理。新客户端始终传商品详情中的 SKU ID。`price` 为旧客户端保留可选输入，无弃用日期，但永不作为金额来源。
- 前端配送字段为 `custom_address.consignee_name/consignee_phone`，不是 `name/phone`；自提不需要地址。运费、积分和优惠券既有政策未变。
- 本文是 SKU/价格修订的权威契约；旧总接口文档中“第一条 SKU”“必须传 price/信任客户端价格”的说明被替代。

## 共用基本信息及安全边界

URL 前缀为部署服务根地址，路由为 `/cart`、`/order`。请求和响应为 `application/json`，成功 HTTP 200。本次未引入认证机制：这些现存路由**未强制 Bearer 登录，也没有 user_id 所有权校验**，增减和下单检查所选 SKU 属于商品，显式 SKU 删除仅匹配购物车三元组；不能据本次价格修复宣称可安全开放生产。前端业务上应先登录并取本人 user_id；服务端认证修复需另行完成。无新增角色/权限要求、限流或 Idempotency-Key Header。

### 添加购物车

**基本信息**：`POST /cart/add`，累加指定 SKU 数量。

**请求参数**：

| 参数 | 位置/类型 | 必填/默认 | 校验与含义 | 合法/非法示例 |
|---|---|---|---|---|
| user_id | body/integer | 是/无 | 现存用户 ID | 101/不存在的ID→404 |
| product_id | body/integer | 是/无 | 现存商品 ID | 20/不存在的ID→404 |
| sku_id | body/integer或null | 否/null | 正整数，必须归属该商品；缺失仅单 SKU | 21/别的商品SKU→422 |
| quantity | body/integer | 否/1 | 严格正整数，上限2147483647；累加量 | 2/true、"2"、1.5、0→422 |
| specifications | body/object或null | 否/null | 动态规格键值，仅展示，不选择SKU | {"规格":"10枚"}/[]→422 |

**请求体示例**：

```http
POST /cart/add
Content-Type: application/json

{"user_id":101,"product_id":20,"sku_id":21,"quantity":2,"specifications":{"规格":"10枚"}}
```

**返回参数**：`ok` boolean，必返且非空，true 表示写入提交成功。成功 `{"ok":true}`，无空列表形态。

**使用方法与业务规则**：先商品详情选 SKU 再添加；同一用户/商品/SKU 原子累加，不同 SKU 独立行。事务锁定目标购物车行及SKU，累计数量不得超过当时SKU库存；超量400，`detail="购物车累计数量超过 SKU 可用库存"`，不修改购物车。严格INT累计上限超出422。重复提交会继续增加，不幂等；入车不占库存，不保证最终结算库存。已选中标记不因累加重置。锁等待/死锁由数据库回滚，前端不得显示成功。

空购物车首次并发加入也受保护：add 与购物车结算统一按 `用户行 → cart行 → SKU行` 顺序加锁，用户行在 cart 为空时仍存在，避免 READ COMMITTED 下两个空行快照分别累加而超库存。同一用户的加入和购物车结算串行；不同用户不共享用户锁。减量和精确删除仅持 cart 写锁，不反向请求用户或 SKU 锁。新增真实 MySQL 回归设计为在 READ COMMITTED 下以事件暂停首请求于空 cart 读取后、SKU 锁之前，再令第二请求尝试，检查锁等待回滚后重试返回 400、最终 quantity 为 3 而非 6；该新增用例尚待运行。

立即购买同样先锁 users，再锁 SKU，避免插入 orders 时 users 外键共享锁与入车 users 独占锁形成反向依赖。新增真实 MySQL 测试表包含 orders/cart 的 users 外键；确定性交错测试设计为暂停入车于持用户锁、请求 SKU 锁之前，让立即购买竞争，仅允许正常锁等待回滚、不接受死锁 1213，释放后重试检查订单、购物车及库存结果；该新增用例尚待运行。过期取消为已有订单行→SKU，不插入 users 外键子记录；减量/删除不再请求 users 或 SKU 写锁。本次不声称全局资金服务与所有既有支付路径完全无死锁，相关既有跨账户锁另需专项验证。

**错误**：公共错误表适用，用户/商品/无 SKU 为404，多 SKU 未指定或错误归属422。

### 减少购物车

**基本信息**：`POST /cart/decrease`，仅减少指定 SKU。

**请求参数**：与添加完全相同，user_id/product_id 必填、quantity 默认1、sku_id/specifications 默认null；specifications 不再参与定位，sku_id 才是标识。

**请求体示例**：

```http
POST /cart/decrease
Content-Type: application/json

{"user_id":101,"product_id":20,"sku_id":21,"quantity":1}
```

非法示例 `{"user_id":101,"product_id":20,"sku_id":21,"quantity":-1}` →422。

**返回参数与示例**：`ok` 必返非空 boolean，`{"ok":true}`。无匹配行404，不返回空对象。

**使用方法与业务规则**：重复请求继续扣减，不幂等；锁定指定购物车行后计算余量，余量≤0删除，不允许负数遗留。并发增减由行锁串行处理。不会影响同商品其他 SKU。

**错误**：公共错误表适用；找不到目标购物车行404，重新获取购物车。

### 删除购物车 SKU

**基本信息**：`DELETE /cart/{user_id}/{product_id}?sku_id=21`。

**请求参数**：user_id、product_id 位于 path，integer必填无默认，含义同添加；sku_id 位于 query，可选integer/null默认null，必须>0。显式SKU按购物车三元组精确删除，不要求商品目录中SKU仍存在；缺失仍仅单SKU可确定。合法 `sku_id=21`；非法 `sku_id=0`→422。

**请求体示例**：`DELETE /cart/101/20?sku_id=21`，无请求体；无必须 Headers。

**返回参数与示例**：`ok` 必返非空 boolean，`{"ok":true}`。目标行不存在也为true；显式SKU即使已从目录删除仍可清理对应用户/商品行，不会删除其他用户或SKU。

**使用方法与业务规则**：幂等删除，不能省略多SKU商品的sku_id以删除整商品；无状态/积分/库存变动。

**错误**：公共错误表适用。显式SKU不存在不构成错误；缺省SKU且当前商品多SKU仍422。失效购物车行可凭之前取得的sku_id精确删除。

### 获取购物车

**基本信息**：`GET /cart/{user_id}`；只读，无分页、无请求体。path user_id 为必填integer，无默认，含义为购物车用户ID；非法非整数→422。

**请求示例**：`GET /cart/101`，无必须Headers。

**返回参数**：根为数组，无数据 `[]`。每行字段如下（当前表扩展列可能附加）：

| 字段 | 类型/必返/空值 | 含义/示例 |
|---|---|---|
| id,user_id,product_id,sku_id | integer/是/非空 | 行ID、用户、商品、SKU，如1、101、20、21 |
| quantity | integer/是/非空 | 数量，2 |
| specifications | object或null/是/null=无展示规格 | 动态规格键值，例如{"规格":"10枚"} |
| selected | integer/是/历史可null | 1选中、0未选中 |
| added_at,updated_at | datetime string/是/历史可null | 入车/更新时间 |
| product_name | string/是/非空 | 商品名称 |
| unit_price,total_price | number/是/非空 | 当前SKU价和数量乘价，元 |
| cash_only | integer或boolean/是/以商品配置为准 | 是否仅现金支付 |

**返回示例**：`[{"id":1,"user_id":101,"product_id":20,"sku_id":21,"quantity":2,"selected":1,"specifications":null,"added_at":"2026-10-30T10:00:00","updated_at":"2026-10-30T10:00:00","product_name":"商品","unit_price":20,"total_price":40,"cash_only":0}]`。

**使用方法与业务规则**：只读可重试；仅返回SKU归属与商品一致的记录。非法遗留行须先迁移处理，不应依赖查询隐藏行作为数据修复。

**错误**：公共错误表适用，参数类型422。

### 创建订单

**基本信息**：`POST /order/create`，立即购买或选中购物车结算。

**请求参数**（body，除特别说明外均可null）：

| 参数 | 类型 | 必填/默认 | 校验/含义/示例 |
|---|---|---|---|
| user_id | integer | 是/无 | 用户ID，101 |
| delivery_way | string | 否/platform | platform配送，pickup自提；express非法422 |
| address_id | integer/null | 否/null | 旧保留字段，单独传入不代替custom_address |
| custom_address | object/null | 配送必填/null | 收货地址对象 |
| custom_address.consignee_name | string | 配送需提供/无 | 收货人，如测试收货人 |
| custom_address.consignee_phone | string | 配送需提供/无 | 联系电话，如13800000000 |
| custom_address.province,city,district | string | 否/空串 | 省市区 |
| custom_address.detail | string | 否/空串 | 详细地址 |
| specifications | string/null | 否/null | 旧备注/规格字段，当前写refund_reason |
| buy_now | boolean | 否/false | true立即购买；false选中购物车 |
| buy_now_items | array/null | 立即购买需非空/null | 明细数组 |
| buy_now_items[].product_id | integer | 是/无 | 严格正整数商品ID |
| buy_now_items[].sku_id | integer/null | 否/null | 严格正整数SKU，归属匹配；缺失仅单SKU |
| buy_now_items[].quantity | integer | 是/无 | 严格1..2147483647，不接受"1"/true/1.5 |
| buy_now_items[].price | decimal number/string/null | 否/null | 旧兼容字段，**忽略**；不得决定金额 |
| points_to_use | decimal number/string | 否/0 | 积分使用量，沿用既有抵扣策略 |
| coupon_id | integer/null | 否/null | 旧单券ID |
| coupon_ids | integer array/null | 否/null | 多券优先、去重；不与单券合并 |
| idempotency_key | string/null | 否/null | 既有Redis幂等键；不是新增可靠性保证 |
| merchant_id | integer/null | 否/null | 旧商家字段；省略则从商品推导 |

现有外层camelCase别名、coupon规范化兼容保留；嵌套明细字段为snake_case。

**请求体示例**：

```http
POST /order/create
Content-Type: application/json

{"user_id":101,"delivery_way":"pickup","buy_now":true,"buy_now_items":[{"product_id":20,"sku_id":21,"quantity":2}],"points_to_use":0,"coupon_ids":[]}
```

非法示例 `buy_now_items:[{"product_id":20,"sku_id":21,"quantity":true}]`→422；多SKU缺sku_id→422。配送示例替换为 `"delivery_way":"platform","custom_address":{"consignee_name":"测试人","consignee_phone":"13800000000","province":"浙江省","city":"杭州市","district":"测试区","detail":"测试地址"}`。

**返回参数**：order_number string必返非空，订单号；need_pay boolean必返非空，true待支付，false创建时已完成零元结算。成功 `{"order_number":"<订单号>","need_pay":true}`，空购物车422而非空成功对象。

**使用方法与业务规则**：从商品详情/购物车选SKU后创建，再按need_pay决定是否调用支付。商品同商家限制、cash_only禁积分/券规则保留。总原价、折扣基数、订单明细单价均来自事务内锁定SKU的price，与客户端price无关。按SKU ID稳定顺序锁行并使用 `stock >= quantity` 条件扣减；重复SKU明细累计消耗，不会绕过库存；任何后续业务错误同事务回滚库存、订单和明细。NULL/负库存不允许购买。购物车结算还校验遗留SKU归属和严格正数量。入单后通常pending_pay，零元按配送方式进入pending_ship/pending_recv并走既有结算。超时取消逐订单事务重新锁定并检查pending_pay/过期状态，只返还实际SKU。缺失SKU或旧NULL多SKU不能确定时，该订单全部回滚、保持pending_pay、记录带order_id异常等待核查；其他健康订单继续独立提交，不向替代SKU猜还库存。不新增状态枚举；故障订单后续扫描继续尝试，运维需处理日志中的异常。

既有Redis创建锁、幂等缓存和一分钟购物车重复下单拦截保留，但不保证Redis故障下跨进程请求幂等，幂等key返回类型缺陷也非此次修复范围。不要因网络超时盲目重复下单；先查询订单。并发死锁由数据库回滚，不新增隐式重试。未新增频率/额度限制；不新增支付或真实微信调用。

**错误**：公共错误表适用；400库存不足/多商家/券或cash_only业务冲突，422空购物车、无地址、数量/SKU错误，429创建锁已占用。前端重新刷新SKU价格库存，不得用客户端价格恢复旧价。

## 公共错误契约

| HTTP | 业务码 | 条件 | 前端建议 |
|---|---|---|---|
| 400 | 无 | 库存不足或既有订单业务规则 | 刷新库存/优惠选择 |
| 404 | 无 | 用户、商品、可用SKU或购物车目标不存在 | 刷新资源 |
| 422 | 无 | Schema校验或SKU歧义/不匹配、遗留非法数量 | 修正输入，显式选择SKU |
| 429 | 无 | Redis创建锁占用 | 稍后查询订单再决定是否重试 |
| 500 | 无 | 未处理数据库错误/死锁/整数溢出/迁移未完成 | 不显示成功，保留上下文并排查 |

业务错误示例 `{"detail":"多 SKU 商品必须提供 sku_id"}`；Schema错误示例 `{"detail":[{"type":"int_type","loc":["body","quantity"],"msg":"Input should be a valid integer","input":true}]}`。没有统一业务code字段。

## 数据库迁移及验证（仅隔离环境）

先停写购物车/订单并备份，使用隔离MYSQL环境变量，确认绝非生产，再执行：

```powershell
uv run python scripts/migrate.py --dry-run
uv run python scripts/migrate.py
uv run python scripts/migrate.py
uv run --with pytest pytest tests/test_order_cart_security.py tests/test_cart_migration.py -q
```

003 SQL使用 information_schema + PREPARE 条件ALTER，兼容MySQL不支持 `ADD COLUMN IF NOT EXISTS`，仍可重复执行；runner原有按分号拆分可直接处理此文件。脚本随后调用 `ensure_cart_sku_key`：预检非法数量、孤立商品/错误SKU、无SKU或多SKU的NULL遗留行，以及解析后重复键；任何此类问题明确中止，必须人工核实，禁止猜选/合并。只有单SKU的NULL行安全回填。将sku_id设NOT NULL、quantity设NOT NULL，并移除旧(user_id,product_id)唯一键，保留/建立三列唯一键。MySQL DDL会自动提交，必须停写和备份；重复执行可继续，不假设整批DDL可回滚。

旧cart已存的合法非空sku_id保持不变；**早期代码猜错但形式合法的SKU无法自动恢复购买意图**，须用户重新选择或人工核对。已删除SKU或旧order_items中NULL多SKU保持不猜，仅隔离该订单取消失败并记录异常；上线前单独审计并依据原始业务证据修正。不要清空此类历史记录来“通过迁移”。回滚须恢复备份/评估新多SKU数据，不能直接恢复旧两列唯一键造成数据损失。

`test_order_cart_security.py`保留快速行为double；`test_cart_migration.py`改为显式启用的真实MySQL集成测试，创建随机`pr8_test_*`数据库并最终删除，仅使用独立`PR8_MYSQL_TEST_*`配置，不复用应用MYSQL_*。默认skip。主控必须确认测试实例非生产，并设置`PR8_MYSQL_TEST_ENABLE=1`、`PR8_MYSQL_TEST_HOST/PORT/USER/PASSWORD`后运行上述pytest命令；账号需要CREATE/DROP DATABASE。覆盖迁移首次/重跑结果、旧两列唯一键升级/NULL单SKU回填、非法/歧义/重复阻断且数据不变，以及真实双连接抢库存、累计入车并发、重复SKU和后续错误回滚、删除SKU后的故障/健康订单隔离、失效购物车精确清理后结算。不再断言SQL源码/DDL文本。支付与取消交错仍需主控专项验收。本文不声称这些测试已运行。
