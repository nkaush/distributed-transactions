use super::object::{TimestampedObject, Diffable, Updateable};
use std::collections::HashMap;
use std::hash::Hash;

pub struct Shard<K, T, D> where K: Hash, T: Diffable<D>, D: Updateable {
    objects: HashMap<K, TimestampedObject<T, D>>
}