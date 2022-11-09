use std::{collections::HashMap, hash::Hash};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use enerdag_time::TimePeriod;
use enerdagsim::{mw_to_k_wh, HouseholdBatterySim, HouseholdDescription, Neighborhood};
use mosaik_rust_api::MosaikApi;

// pub fn criterion_benchmark(c: &mut Criterion) {
//     c.bench_function("mw to kwh 20 20", |b| {
//         b.iter(|| mw_to_k_wh(black_box(20.), black_box(20.)));
//         b.iter(|| mw_to_k_wh(black_box(-0.000359375), black_box(900.)))
//     });
// }

pub fn create_sim(c: &mut Criterion) {
    let mut nbhd = Neighborhood::initmodel(
        "eid01".to_string(),
        TimePeriod::last(),
        100.0,
        vec![HouseholdDescription::random(); 20],
        10,
    );
    c.bench_with_input(
        BenchmarkId::new("step", "30 nbhd eid01"),
        &mut nbhd,
        |b, nbhd| {
            b.iter(|| {
                let mut x: Neighborhood = nbhd.clone();
                x.step(30, HashMap::new())
            });

            // sim.create(num, model, model_params) mw_to_k_wh(black_box(20.), black_box(20.)));
            // b.iter(|| mw_to_k_wh(black_box(-0.000359375), black_box(900.)))
        },
    );
    c.bench_with_input(
        BenchmarkId::new("step", "10 nbhd eid01"),
        &mut nbhd,
        |b, nbhd| {
            b.iter(|| {
                let mut x: Neighborhood = nbhd.clone();
                x.step(10, HashMap::new())
            });

            // sim.create(num, model, model_params) mw_to_k_wh(black_box(20.), black_box(20.)));
            // b.iter(|| mw_to_k_wh(black_box(-0.000359375), black_box(900.)))
        },
    );
    c.bench_with_input(
        BenchmarkId::new("step", "5 nbhd eid01"),
        &mut nbhd,
        |b, nbhd| {
            b.iter(|| {
                let mut x: Neighborhood = nbhd.clone();
                x.step(5, HashMap::new())
            });

            // sim.create(num, model, model_params) mw_to_k_wh(black_box(20.), black_box(20.)));
            // b.iter(|| mw_to_k_wh(black_box(-0.000359375), black_box(900.)))
        },
    );
}

// criterion_group!(benches, criterion_benchmark);
criterion_group!(benches2, create_sim);
criterion_main!(benches2);
