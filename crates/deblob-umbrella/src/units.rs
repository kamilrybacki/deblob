//! Deterministic unit dimension + conversion registry (umbrella V1).
//!
//! The trust gate requires that any `unit_convert` op join two units of the
//! **same physical dimension** via a **known, machine-verifiable rule** — never
//! an SLM-invented equation (design §Trust gate / §SLM authority boundary). This
//! module is that registry: a closed table of dimensions per unit code and a
//! closed table of linear conversion rules. Currencies (ISO 4217) are a
//! dimension but are deliberately **non-convertible** here (a cross-currency rate
//! is time-varying data, not a static rule).

use deblob_core::semantic::{Unit, UnitSystem};

/// The physical dimension a unit measures. Two units may only convert when they
/// share a dimension. `Currency` is its own dimension with no static conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dimension {
    Temperature,
    Pressure,
    Power,
    Energy,
    Time,
    Length,
    Dimensionless,
    Currency,
}

/// A linear conversion `to = from * factor + offset`, addressed by a stable
/// `rule_id` the transform binding cites. Both endpoints carry their UCUM code so
/// static verification can confirm the rule actually maps the binding's units.
#[derive(Debug, Clone, Copy)]
pub struct ConversionRule {
    pub rule_id: &'static str,
    pub from_code: &'static str,
    pub to_code: &'static str,
    pub factor: f64,
    pub offset: f64,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum UnitError {
    #[error("unknown unit code: {0}")]
    UnknownCode(String),
    #[error("unknown conversion rule: {0}")]
    UnknownRule(String),
    #[error("rule {rule_id} does not map {from} -> {to}")]
    RuleMismatch { rule_id: String, from: String, to: String },
    #[error("dimension mismatch: {from} is {from_dim:?}, {to} is {to_dim:?}")]
    DimensionMismatch { from: String, from_dim: Dimension, to: String, to_dim: Dimension },
    #[error("currencies are not statically convertible: {0} -> {1}")]
    CurrencyNotConvertible(String, String),
}

/// Closed dimension table for the UCUM codes umbrella V1 recognises (weather +
/// grid + generic domains). Extend deliberately — an unknown code is a hard
/// error, never a guess.
const DIMENSIONS: &[(&str, Dimension)] = &[
    // temperature
    ("Cel", Dimension::Temperature),
    ("K", Dimension::Temperature),
    ("[degF]", Dimension::Temperature),
    // pressure
    ("Pa", Dimension::Pressure),
    ("hPa", Dimension::Pressure),
    ("kPa", Dimension::Pressure),
    ("bar", Dimension::Pressure),
    // power
    ("W", Dimension::Power),
    ("kW", Dimension::Power),
    ("MW", Dimension::Power),
    ("GW", Dimension::Power),
    // energy
    ("J", Dimension::Energy),
    ("kW.h", Dimension::Energy),
    ("MW.h", Dimension::Energy),
    // time
    ("s", Dimension::Time),
    ("min", Dimension::Time),
    ("h", Dimension::Time),
    // length
    ("m", Dimension::Length),
    ("km", Dimension::Length),
    // dimensionless (ratios, percent, counts)
    ("1", Dimension::Dimensionless),
    ("%", Dimension::Dimensionless),
];

/// Closed conversion table. Every rule is exact-linear and reversible; both
/// directions are listed explicitly so no code has to invert a rule at runtime.
const RULES: &[ConversionRule] = &[
    ConversionRule { rule_id: "ucum:Cel->K", from_code: "Cel", to_code: "K", factor: 1.0, offset: 273.15 },
    ConversionRule { rule_id: "ucum:K->Cel", from_code: "K", to_code: "Cel", factor: 1.0, offset: -273.15 },
    ConversionRule { rule_id: "ucum:hPa->Pa", from_code: "hPa", to_code: "Pa", factor: 100.0, offset: 0.0 },
    ConversionRule { rule_id: "ucum:Pa->hPa", from_code: "Pa", to_code: "hPa", factor: 0.01, offset: 0.0 },
    ConversionRule { rule_id: "ucum:kPa->Pa", from_code: "kPa", to_code: "Pa", factor: 1000.0, offset: 0.0 },
    ConversionRule { rule_id: "ucum:kW->W", from_code: "kW", to_code: "W", factor: 1000.0, offset: 0.0 },
    ConversionRule { rule_id: "ucum:MW->W", from_code: "MW", to_code: "W", factor: 1_000_000.0, offset: 0.0 },
    ConversionRule { rule_id: "ucum:GW->W", from_code: "GW", to_code: "W", factor: 1_000_000_000.0, offset: 0.0 },
    ConversionRule { rule_id: "ucum:W->kW", from_code: "W", to_code: "kW", factor: 0.001, offset: 0.0 },
    ConversionRule { rule_id: "ucum:W->MW", from_code: "W", to_code: "MW", factor: 0.000_001, offset: 0.0 },
    ConversionRule { rule_id: "ucum:km->m", from_code: "km", to_code: "m", factor: 1000.0, offset: 0.0 },
    ConversionRule { rule_id: "ucum:m->km", from_code: "m", to_code: "km", factor: 0.001, offset: 0.0 },
    ConversionRule { rule_id: "ucum:h->s", from_code: "h", to_code: "s", factor: 3600.0, offset: 0.0 },
    ConversionRule { rule_id: "ucum:min->s", from_code: "min", to_code: "s", factor: 60.0, offset: 0.0 },
];

/// The dimension a unit measures, or `None` if its code is not in the closed
/// table. ISO 4217 units are always [`Dimension::Currency`] regardless of code.
pub fn dimension_of(unit: &Unit) -> Option<Dimension> {
    match unit.system {
        UnitSystem::Iso4217 => Some(Dimension::Currency),
        UnitSystem::Ucum | UnitSystem::Registered => {
            DIMENSIONS.iter().find(|(c, _)| *c == unit.code).map(|(_, d)| *d)
        }
    }
}

/// True iff both units are recognised and share a dimension.
pub fn same_dimension(from: &Unit, to: &Unit) -> bool {
    match (dimension_of(from), dimension_of(to)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Look up a conversion rule by its stable id.
pub fn rule(rule_id: &str) -> Option<&'static ConversionRule> {
    RULES.iter().find(|r| r.rule_id == rule_id)
}

/// Find the conversion rule that maps `from`'s code to `to`'s code, if one exists
/// (used by correspondence enumeration to decide `SAME_QUANTITY_DIFFERENT_UNIT`).
pub fn find_rule(from: &Unit, to: &Unit) -> Option<&'static ConversionRule> {
    RULES.iter().find(|r| r.from_code == from.code && r.to_code == to.code)
}

/// Static check for a `unit_convert` op: the rule exists, its endpoints match the
/// binding's declared units, and those units share a dimension. Currencies are
/// rejected outright. Returns `Ok` when the op is verifiably sound.
pub fn verify_conversion(from: &Unit, to: &Unit, rule_id: &str) -> Result<(), UnitError> {
    if from.system == UnitSystem::Iso4217 || to.system == UnitSystem::Iso4217 {
        if from == to {
            return Ok(()); // same-currency passthrough (no scaling) is allowed
        }
        return Err(UnitError::CurrencyNotConvertible(from.code.clone(), to.code.clone()));
    }
    let from_dim = dimension_of(from).ok_or_else(|| UnitError::UnknownCode(from.code.clone()))?;
    let to_dim = dimension_of(to).ok_or_else(|| UnitError::UnknownCode(to.code.clone()))?;
    if from_dim != to_dim {
        return Err(UnitError::DimensionMismatch {
            from: from.code.clone(), from_dim, to: to.code.clone(), to_dim,
        });
    }
    let r = rule(rule_id).ok_or_else(|| UnitError::UnknownRule(rule_id.to_string()))?;
    if r.from_code != from.code || r.to_code != to.code {
        return Err(UnitError::RuleMismatch {
            rule_id: rule_id.to_string(), from: from.code.clone(), to: to.code.clone(),
        });
    }
    Ok(())
}

/// Apply a conversion rule to a value. Callers should have run
/// [`verify_conversion`] at bind-verification time; this re-checks the rule id so
/// execution can never apply an unknown rule.
pub fn convert(value: f64, rule_id: &str) -> Result<f64, UnitError> {
    let r = rule(rule_id).ok_or_else(|| UnitError::UnknownRule(rule_id.to_string()))?;
    Ok(value * r.factor + r.offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ucum(code: &str) -> Unit { Unit { system: UnitSystem::Ucum, code: code.into() } }
    fn iso(code: &str) -> Unit { Unit { system: UnitSystem::Iso4217, code: code.into() } }

    #[test]
    fn dimensions_resolve() {
        assert_eq!(dimension_of(&ucum("Cel")), Some(Dimension::Temperature));
        assert_eq!(dimension_of(&ucum("MW")), Some(Dimension::Power));
        assert_eq!(dimension_of(&iso("USD")), Some(Dimension::Currency));
        assert_eq!(dimension_of(&ucum("nonsense")), None);
    }

    #[test]
    fn celsius_to_kelvin_is_exact_and_offset() {
        assert!(verify_conversion(&ucum("Cel"), &ucum("K"), "ucum:Cel->K").is_ok());
        assert_eq!(convert(0.0, "ucum:Cel->K").unwrap(), 273.15);
        assert_eq!(convert(25.0, "ucum:Cel->K").unwrap(), 298.15);
    }

    #[test]
    fn megawatt_to_watt_scales() {
        assert!(verify_conversion(&ucum("MW"), &ucum("W"), "ucum:MW->W").is_ok());
        assert_eq!(convert(1.5, "ucum:MW->W").unwrap(), 1_500_000.0);
    }

    #[test]
    fn cross_dimension_is_rejected() {
        let err = verify_conversion(&ucum("Cel"), &ucum("W"), "ucum:Cel->K").unwrap_err();
        assert!(matches!(err, UnitError::DimensionMismatch { .. }));
    }

    #[test]
    fn rule_must_match_declared_units() {
        // right dimension, wrong rule for these endpoints
        let err = verify_conversion(&ucum("K"), &ucum("Cel"), "ucum:Cel->K").unwrap_err();
        assert!(matches!(err, UnitError::RuleMismatch { .. }));
    }

    #[test]
    fn unknown_rule_is_rejected() {
        assert!(matches!(verify_conversion(&ucum("Cel"), &ucum("K"), "ucum:made-up"), Err(UnitError::UnknownRule(_))));
        assert!(matches!(convert(1.0, "ucum:made-up"), Err(UnitError::UnknownRule(_))));
    }

    #[test]
    fn cross_currency_never_static_converts() {
        let err = verify_conversion(&iso("USD"), &iso("PLN"), "whatever").unwrap_err();
        assert!(matches!(err, UnitError::CurrencyNotConvertible(_, _)));
        // same currency passthrough is fine
        assert!(verify_conversion(&iso("USD"), &iso("USD"), "noop").is_ok());
    }
}
