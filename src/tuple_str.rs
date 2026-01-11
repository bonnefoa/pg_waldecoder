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

pub fn generate_insert_query(relname: &str, heap_tuple: &PgHeapTuple<AllocatedByRust>) -> String {
    // let natts = tuple_desc.len();
    // let mut is_null = (0..natts).map(|_| true).collect::<Vec<_>>();
    //        let heap_tuple_data = pg_sys::heap_form_tuple(
    //            tuple_desc.as_ptr(),
    //            std::ptr::null_mut(),
    //            is_null.as_mut_ptr(),
    //        );
    //    let heap_tuple = unsafe { PgHeapTuple::from_heap_tuple(tuple_desc, heap_tuple_data) };

    let attribute_names = heap_tuple
        .attributes()
        .filter(|(_, v)| v.is_dropped())
        .map(|(_, v)| v.name()).collect::<Vec<_>>();

    format!("INSERT INTO {relname} ({})", attribute_names.join(","))
}
