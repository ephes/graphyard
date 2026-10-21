//! Pure logic of the graphyard agent: no I/O, no async runtime.
//!
//! Everything in this crate is deterministic and tested against fixtures
//! generated from the Python implementation (`agent-rs/fixtures`, PLAN.md
//! section 9.2).

pub mod conditions;
#[cfg(test)]
mod fixture;
pub mod line_protocol;
pub mod logline;
pub mod model;
pub mod normalize;
pub mod pycompat;
pub mod settings;
pub mod spec_config;

pub use conditions::{
    ConditionRule, EvaluationError, SampleSource, evaluate_condition, evaluate_samples,
};
pub use model::{
    ComparisonOperator, ConditionEvaluation, ConditionEvaluationRun, MetricCollectionSpecRun,
    MetricPoint, MetricSample, SpecOutcome, SpecType, StatusLevel, SubjectType, Tags,
};
