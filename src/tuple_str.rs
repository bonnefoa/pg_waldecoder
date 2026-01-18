use std::{mem, num::NonZero};

use pgrx::prelude::*;

pub fn append_values(
    mut buffer: String,
    new_tuple: &PgHeapTuple<AllocatedByRust>,
    old_tuple: Option<&PgHeapTuple<AllocatedByRust>>,
) {
    for (i, attr) in new_tuple.attributes() {
        if attr.is_dropped() {
            continue;
        }

        if i.get() > 1 {
            buffer.push_str(", ");
        }
        let old_val = old_tuple.map(|e| e.get_by_index::<String>(i));
    }
}

pub fn generate_insert_query(
    relname: &str,
    heap_tuple: &PgHeapTuple<AllocatedByPostgres>,
) -> String {
    info!("Content: {0:?}", heap_tuple
                    .get_by_index::<String>(NonZero::new(1).unwrap())
                    .expect("value must exist"));

    let idxs = heap_tuple
        .attributes()
        .filter(|(i, att)| {
            !att.is_dropped()
                && heap_tuple
                    .get_by_index::<String>(*i)
                    .expect("value must exist")
                    .is_some()
        })
        .map(|(i, _)| i)
        .collect::<Vec<NonZero<usize>>>();

    let attributes = idxs
        .clone()
        .into_iter()
        .map(|i| heap_tuple.get_attribute_by_index(i).unwrap().name())
        .collect::<Vec<_>>();

    let values = idxs
        .into_iter()
        .map(|i| {
            heap_tuple
                .get_by_index(i)
                .expect("Value must exist")
                .expect("Value must be non null")
        })
        .collect::<Vec<String>>();

    format!(
        "INSERT INTO {relname} ({}) VALUES ({})",
        attributes.join(","),
        values.join(",")
    )
}
