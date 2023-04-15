use super::object::{TimestampedObject, Diffable};
use std::collections::HashMap;
use std::hash::Hash;

pub struct Shard<K, T, D> where K: Hash, T: Diffable<D> {
    objects: HashMap<K, TimestampedObject<T, D>>
}