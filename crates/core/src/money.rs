use core::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

const NANOS_PER_UNIT: u64 = 1_000_000_000;
const SCALE: u32 = 9;

/// 金额，纳单位（1e-9）。仅提供 checked 运算，禁止裸 `i64` 隐式转入。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Money(i64);

impl Money {
    #[must_use]
    pub const fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    /// `self * mul / div`，中间量用 i128 避免溢出，余数向零截断。
    #[must_use]
    pub fn checked_mul_div(self, mul: i64, div: i64) -> Option<Self> {
        if div == 0 {
            return None;
        }
        let wide = i128::from(self.0) * i128::from(mul) / i128::from(div);
        i64::try_from(wide).ok().map(Self)
    }

    #[must_use]
    pub const fn checked_sub(self, rhs: Self) -> Option<Self> {
        match self.0.checked_sub(rhs.0) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    #[must_use]
    pub const fn checked_add(self, rhs: Self) -> Option<Self> {
        match self.0.checked_add(rhs.0) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let abs = self.0.unsigned_abs();
        write!(
            f,
            "{sign}{}.{:0width$}",
            abs / NANOS_PER_UNIT,
            abs % NANOS_PER_UNIT,
            width = SCALE as usize
        )
    }
}

impl Serialize for Money {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

/// 解析定点十进制字符串为纳单位。小数位超过 9 位视为精度损失，报错而非截断。
fn parse_nanos(src: &str) -> Result<i64, ParseMoneyError> {
    let (negative, digits) = match src.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, src.strip_prefix('+').unwrap_or(src)),
    };

    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(ParseMoneyError);
    }
    if frac_part.len() > SCALE as usize {
        return Err(ParseMoneyError);
    }
    let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(int_part) || !all_digits(frac_part) {
        return Err(ParseMoneyError);
    }

    let units: u64 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().map_err(|_| ParseMoneyError)?
    };
    let frac: u64 = if frac_part.is_empty() {
        0
    } else {
        let scaled = format!("{frac_part:0<width$}", width = SCALE as usize);
        scaled.parse().map_err(|_| ParseMoneyError)?
    };

    let magnitude = units
        .checked_mul(NANOS_PER_UNIT)
        .and_then(|v| v.checked_add(frac))
        .ok_or(ParseMoneyError)?;

    // 经 i128 中转，i64::MIN 的绝对值超出 i64 范围也能正确表示
    let signed = if negative {
        -i128::from(magnitude)
    } else {
        i128::from(magnitude)
    };
    i64::try_from(signed).map_err(|_| ParseMoneyError)
}

#[derive(Debug)]
pub struct ParseMoneyError;

impl fmt::Display for ParseMoneyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("金额必须是小数位不超过 9 位的定点十进制字符串")
    }
}

impl core::str::FromStr for Money {
    type Err = ParseMoneyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_nanos(s).map(Self)
    }
}

struct MoneyVisitor;

impl de::Visitor<'_> for MoneyVisitor {
    type Value = Money;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("小数位不超过 9 位的定点十进制字符串")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Money, E> {
        parse_nanos(v).map(Money).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Money {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_str(MoneyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(Money::from_nanos(3_000_000_000), "3.000000000")]
    #[case(Money::from_nanos(0), "0.000000000")]
    #[case(Money::from_nanos(-1), "-0.000000001")]
    #[case(Money::from_nanos(4_500_000), "0.004500000")]
    #[case(Money::from_nanos(i64::MIN), "-9223372036.854775808")]
    fn serializes_as_decimal_string(#[case] m: Money, #[case] expected: &str) {
        assert_eq!(
            serde_json::to_string(&m).unwrap(),
            format!("\"{expected}\"")
        );
    }

    #[rstest]
    #[case("3", 3_000_000_000)]
    #[case("3.0", 3_000_000_000)]
    #[case("0.5", 500_000_000)]
    #[case("-0.000000001", -1)]
    #[case("-9223372036.854775808", i64::MIN)]
    fn deserializes_from_decimal_string(#[case] input: &str, #[case] nanos: i64) {
        let m: Money = serde_json::from_str(&format!("\"{input}\"")).unwrap();
        assert_eq!(m, Money::from_nanos(nanos));
    }

    /// figment 等配置源经 `Value` 中转，反序列化不能只接受借用的 &str
    #[test]
    fn deserializes_from_owned_value() {
        let v = serde_json::Value::String("1.25".into());
        assert_eq!(
            serde_json::from_value::<Money>(v).unwrap(),
            Money::from_nanos(1_250_000_000)
        );
    }

    /// 超过 9 位小数意味着精度损失，必须报错而非静默截断
    #[rstest]
    #[case("0.0000000001")]
    #[case("")]
    #[case("abc")]
    #[case("1.2.3")]
    #[case("9223372036.854775808")]
    fn rejects_invalid_decimal_string(#[case] input: &str) {
        assert!(serde_json::from_str::<Money>(&format!("\"{input}\"")).is_err());
    }

    /// $3 / 百万 token × 1500 token = $0.0045
    #[test]
    fn mul_div_computes_unit_price() {
        let unit = Money::from_nanos(3_000_000_000);
        assert_eq!(
            unit.checked_mul_div(1500, 1_000_000),
            Some(Money::from_nanos(4_500_000))
        );
    }

    /// 中间量超过 i64 时不得溢出
    #[test]
    fn mul_div_uses_wide_intermediate() {
        let m = Money::from_nanos(i64::MAX);
        assert_eq!(m.checked_mul_div(3, 3), Some(m));
    }

    #[test]
    fn mul_div_result_overflow_returns_none() {
        assert_eq!(Money::from_nanos(i64::MAX).checked_mul_div(2, 1), None);
    }

    #[test]
    fn mul_div_by_zero_returns_none() {
        assert_eq!(Money::from_nanos(1).checked_mul_div(1, 0), None);
    }

    /// 不足一纳单位的部分向零截断
    #[test]
    fn mul_div_truncates_toward_zero() {
        assert_eq!(
            Money::from_nanos(1).checked_mul_div(1, 3),
            Some(Money::from_nanos(0))
        );
        assert_eq!(
            Money::from_nanos(-1).checked_mul_div(1, 3),
            Some(Money::from_nanos(0))
        );
    }

    #[test]
    fn add_overflow_returns_none() {
        assert_eq!(
            Money::from_nanos(i64::MAX).checked_add(Money::from_nanos(1)),
            None
        );
    }

    #[test]
    fn subtracts_two_amounts() {
        let a = Money::from_nanos(3_500_000_000);
        let b = Money::from_nanos(2_000_000_000);
        assert_eq!(a.checked_sub(b), Some(Money::from_nanos(1_500_000_000)));
    }

    #[test]
    fn sub_overflow_returns_none() {
        assert_eq!(
            Money::from_nanos(i64::MIN).checked_sub(Money::from_nanos(1)),
            None
        );
    }

    #[test]
    fn adds_two_amounts() {
        let a = Money::from_nanos(1_500_000_000);
        let b = Money::from_nanos(2_000_000_000);
        assert_eq!(a.checked_add(b), Some(Money::from_nanos(3_500_000_000)));
    }
}
