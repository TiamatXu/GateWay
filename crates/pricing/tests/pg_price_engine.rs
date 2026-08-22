//! 价格引擎。用量向量 → 规则匹配 → 金额。

use chrono::{DateTime, TimeZone, Utc};
use gw_core::{ChannelId, Money, UsageVector, dims};
use gw_pricing::{EstimateCeilings, PgPriceEngine, PriceCtx, PriceEngine, PriceError};
use sqlx::PgPool;

const MILLION: i64 = 1_000_000;

fn at(y: i32, m: u32, d: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
}

/// 每个测试用独立的 model 名隔离规则
fn model() -> String {
    format!("m-{}", uuid::Uuid::new_v4().simple())
}

struct Rule<'a> {
    model: Option<&'a str>,
    endpoint: Option<&'a str>,
    dim: &'a str,
    /// 纳单位 per 百万用量单位
    unit_price: i64,
    cost_price: i64,
    effective_from: DateTime<Utc>,
}

async fn insert_rule(pool: &PgPool, r: Rule<'_>) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO price_rule
           (version, match_model, match_endpoint, dim, unit_price, cost_price, effective_from)
         VALUES (1, $1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(r.model)
    .bind(r.endpoint)
    .bind(r.dim)
    .bind(r.unit_price)
    .bind(r.cost_price)
    .bind(r.effective_from)
    .fetch_one(pool)
    .await
    .unwrap()
}

fn ctx(model: &str, at: DateTime<Utc>) -> PriceCtx<'_> {
    PriceCtx {
        model,
        channel: ChannelId(1),
        tier: "default",
        endpoint: "/v1/chat/completions",
        at,
        max_output_tokens: None,
        input_tokens: None,
    }
}

fn usage(pairs: &[(&str, i64)]) -> UsageVector {
    let mut u = UsageVector::new();
    for (d, v) in pairs {
        u.set(*d, *v);
    }
    u
}

async fn engine() -> (PgPriceEngine, PgPool) {
    let pool = gw_store::testkit::pool().await;
    (
        PgPriceEngine::new(pool.clone(), EstimateCeilings::default()),
        pool,
    )
}

/// $3 / 百万 token × 1500 token = $0.0045
#[tokio::test]
async fn prices_a_single_dimension() {
    let (eng, pool) = engine().await;
    let m = model();
    insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: None,
            dim: dims::INPUT_TOKENS,
            unit_price: 3 * MILLION * 1000,
            cost_price: 2 * MILLION * 1000,
            effective_from: at(2020, 1, 1),
        },
    )
    .await;

    let q = eng
        .quote(
            &ctx(&m, at(2026, 8, 23)),
            &usage(&[(dims::INPUT_TOKENS, 1500)]),
        )
        .await
        .unwrap();

    assert_eq!(q.amount, Money::from_nanos(4_500_000));
    assert_eq!(q.cost, Money::from_nanos(3_000_000));
    assert!(!q.estimated);
}

#[tokio::test]
async fn sums_across_dimensions_with_stable_breakdown() {
    let (eng, pool) = engine().await;
    let m = model();
    for (dim, price) in [
        (dims::INPUT_TOKENS, 3 * MILLION * 1000),
        (dims::OUTPUT_TOKENS, 15 * MILLION * 1000),
    ] {
        insert_rule(
            &pool,
            Rule {
                model: Some(&m),
                endpoint: None,
                dim,
                unit_price: price,
                cost_price: price / 2,
                effective_from: at(2020, 1, 1),
            },
        )
        .await;
    }

    let q = eng
        .quote(
            &ctx(&m, at(2026, 8, 23)),
            &usage(&[(dims::INPUT_TOKENS, 1000), (dims::OUTPUT_TOKENS, 500)]),
        )
        .await
        .unwrap();

    // 3e9*1000/1e6 + 15e9*500/1e6
    assert_eq!(q.amount, Money::from_nanos(3_000_000 + 7_500_000));
    let names: Vec<&str> = q.breakdown.iter().map(|(d, _, _)| d.as_str()).collect();
    assert_eq!(names, vec![dims::INPUT_TOKENS, dims::OUTPUT_TOKENS]);
    assert_eq!(q.breakdown[1].1, 500);
    assert_eq!(q.breakdown[1].2, Money::from_nanos(7_500_000));
}

/// 匹配维度越多的规则优先
#[tokio::test]
async fn more_specific_rule_wins() {
    let (eng, pool) = engine().await;
    let m = model();
    insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: None,
            dim: dims::INPUT_TOKENS,
            unit_price: 3 * MILLION * 1000,
            cost_price: 0,
            effective_from: at(2020, 1, 1),
        },
    )
    .await;
    let specific = insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: Some("/v1/chat/completions"),
            dim: dims::INPUT_TOKENS,
            unit_price: MILLION * 1000,
            cost_price: 0,
            effective_from: at(2020, 1, 1),
        },
    )
    .await;

    let q = eng
        .quote(
            &ctx(&m, at(2026, 8, 23)),
            &usage(&[(dims::INPUT_TOKENS, 1000)]),
        )
        .await
        .unwrap();

    assert_eq!(q.amount, Money::from_nanos(1_000_000));
    assert_eq!(q.rule_ids.as_slice(), &[gw_pricing::RuleId(specific)]);
}

/// `at` 决定命中哪个价格版本：改价不影响历史账单
#[tokio::test]
async fn historical_timestamp_selects_the_price_in_force_then() {
    let (eng, pool) = engine().await;
    let m = model();
    insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: None,
            dim: dims::INPUT_TOKENS,
            unit_price: 3 * MILLION * 1000,
            cost_price: 0,
            effective_from: at(2026, 1, 1),
        },
    )
    .await;
    insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: None,
            dim: dims::INPUT_TOKENS,
            unit_price: 9 * MILLION * 1000,
            cost_price: 0,
            effective_from: at(2026, 6, 1),
        },
    )
    .await;

    let old = eng
        .quote(
            &ctx(&m, at(2026, 3, 1)),
            &usage(&[(dims::INPUT_TOKENS, 1000)]),
        )
        .await
        .unwrap();
    let new = eng
        .quote(
            &ctx(&m, at(2026, 8, 1)),
            &usage(&[(dims::INPUT_TOKENS, 1000)]),
        )
        .await
        .unwrap();

    assert_eq!(old.amount, Money::from_nanos(3_000_000));
    assert_eq!(new.amount, Money::from_nanos(9_000_000));
}

/// 厂商随时新增用量维度，无规则的维度跳过而非报错——不能因此让请求失败
#[tokio::test]
async fn dimension_without_a_rule_is_skipped() {
    let (eng, pool) = engine().await;
    let m = model();
    insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: None,
            dim: dims::INPUT_TOKENS,
            unit_price: 3 * MILLION * 1000,
            cost_price: 0,
            effective_from: at(2020, 1, 1),
        },
    )
    .await;

    let q = eng
        .quote(
            &ctx(&m, at(2026, 8, 23)),
            &usage(&[(dims::INPUT_TOKENS, 1000), ("brand_new_dim", 99)]),
        )
        .await
        .unwrap();

    assert_eq!(q.amount, Money::from_nanos(3_000_000));
    assert_eq!(q.breakdown.len(), 1);
}

/// 一条规则都匹配不上必须报错，否则等于免费送
#[tokio::test]
async fn no_matching_rule_is_an_error() {
    let (eng, _pool) = engine().await;
    let err = eng
        .quote(
            &ctx(&model(), at(2026, 8, 23)),
            &usage(&[(dims::INPUT_TOKENS, 1000)]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, PriceError::NoRule { .. }), "{err:?}");
}

/// 用量为空同样不能静默通过
#[tokio::test]
async fn empty_usage_quotes_zero() {
    let (eng, pool) = engine().await;
    let m = model();
    insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: None,
            dim: dims::INPUT_TOKENS,
            unit_price: 3 * MILLION * 1000,
            cost_price: 0,
            effective_from: at(2020, 1, 1),
        },
    )
    .await;

    let q = eng
        .quote(&ctx(&m, at(2026, 8, 23)), &UsageVector::new())
        .await
        .unwrap();
    assert_eq!(q.amount, Money::from_nanos(0));
}

// ---------------------------------------------------------------- 预扣估算

#[tokio::test]
async fn estimate_max_prices_the_worst_case() {
    let (eng, pool) = engine().await;
    let m = model();
    for (dim, price) in [
        (dims::INPUT_TOKENS, 3 * MILLION * 1000),
        (dims::OUTPUT_TOKENS, 15 * MILLION * 1000),
    ] {
        insert_rule(
            &pool,
            Rule {
                model: Some(&m),
                endpoint: None,
                dim,
                unit_price: price,
                cost_price: 0,
                effective_from: at(2020, 1, 1),
            },
        )
        .await;
    }

    let mut c = ctx(&m, at(2026, 8, 23));
    c.max_output_tokens = Some(1_000);
    let est = eng.estimate_max(&c).await.unwrap();

    // 输入按上限 128k 估，输出按请求声明的 1000 估
    assert_eq!(est, Money::from_nanos(384_000_000 + 15_000_000));
}

/// 已知真实输入量时按实际值预扣，不再按上限。
/// 否则 $3/M 的模型每请求要冻结 $0.384，小额余额用户一个请求都发不出去。
#[tokio::test]
async fn estimate_max_uses_the_known_input_size() {
    let (eng, pool) = engine().await;
    let m = model();
    for (dim, price) in [
        (dims::INPUT_TOKENS, 3 * MILLION * 1000),
        (dims::OUTPUT_TOKENS, 15 * MILLION * 1000),
    ] {
        insert_rule(
            &pool,
            Rule {
                model: Some(&m),
                endpoint: None,
                dim,
                unit_price: price,
                cost_price: 0,
                effective_from: at(2020, 1, 1),
            },
        )
        .await;
    }

    let mut c = ctx(&m, at(2026, 8, 23));
    c.max_output_tokens = Some(1_000);
    c.input_tokens = Some(42);
    let est = eng.estimate_max(&c).await.unwrap();

    assert_eq!(est, Money::from_nanos(3 * 42 * 1000 + 15 * 1_000 * 1000));
}

/// 请求未声明 `max_tokens` 时按配置的输出上限估算
#[tokio::test]
async fn estimate_max_falls_back_to_configured_output_ceiling() {
    let (eng, pool) = engine().await;
    let m = model();
    insert_rule(
        &pool,
        Rule {
            model: Some(&m),
            endpoint: None,
            dim: dims::OUTPUT_TOKENS,
            unit_price: 15 * MILLION * 1000,
            cost_price: 0,
            effective_from: at(2020, 1, 1),
        },
    )
    .await;

    let est = eng.estimate_max(&ctx(&m, at(2026, 8, 23))).await.unwrap();

    // 默认输出上限 16384
    assert_eq!(est, Money::from_nanos(15 * 16_384 * 1000));
}
