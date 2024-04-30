use crate::{SequenceStorage};


pub struct ComposedStorage<RS, SS> {
    relations: RS,
    sequences: SS,
}

impl<RS, SS> SequenceStorage for ComposedStorage<RS, SS> where SS: SequenceStorage {
    type SequenceHandle<'s> = SS::SequenceHandle<'s> where Self: 's;

    fn get_sequence<'se, 'seq>(
        &'se self,
        name: &str,
    ) -> impl futures::prelude::Future<Output = Result<Option<Self::SequenceHandle<'seq>>, ()>>
    where
        'se: 'seq {
        self.sequences.get_sequence(name)
    }

    fn create_sequence(&self, name: &str) -> impl futures::prelude::Future<Output = Result<(), ()>> {
        self.sequences.create_sequence(name)
    }

    fn remove_sequence(&self, name: &str) -> impl futures::prelude::Future<Output = Result<(), ()>> {
        self.sequences.remove_sequence(name)
    }
}
