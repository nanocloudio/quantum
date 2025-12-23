/// Holds the constituent floors contributing to the product-level compaction bound.
#[derive(Debug, Clone, Copy)]
pub struct ProductFloors {
    pub clustor_floor: u64,
    pub local_dedup_floor: u64,
    pub earliest_offline_queue_index: u64,
    pub forward_chain_floor: u64,
}

impl ProductFloors {
    /// Compute effective_product_floor = max(clustor_floor, local_dedup_floor, earliest_offline_queue_index, forward_chain_floor)
    pub fn effective_product_floor(&self) -> u64 {
        self.clustor_floor
            .max(self.local_dedup_floor)
            .max(self.earliest_offline_queue_index)
            .max(self.forward_chain_floor)
    }
}
