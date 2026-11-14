//! A barrier and a restore through each store, to measure what each adds to
//! a binary: build it with and without the `slatedb` feature.

use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use openetl_state::runs::{RunConfig, RunStore};
use openetl_state::{KeyGroupRange, Store};

#[tokio::main]
async fn main() -> openetl_state::Result<()> {
    // The engine links every provider, so both builds do.
    let providers: Vec<Option<Arc<dyn ObjectStore>>> = vec![
        object_store::aws::AmazonS3Builder::from_env()
            .build()
            .ok()
            .map(|s| Arc::new(s) as Arc<dyn ObjectStore>),
        object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .build()
            .ok()
            .map(|s| Arc::new(s) as Arc<dyn ObjectStore>),
        object_store::azure::MicrosoftAzureBuilder::from_env()
            .build()
            .ok()
            .map(|s| Arc::new(s) as Arc<dyn ObjectStore>),
        object_store::local::LocalFileSystem::new_with_prefix("/")
            .ok()
            .map(|s| Arc::new(s) as Arc<dyn ObjectStore>),
    ];
    let os: Arc<dyn ObjectStore> = providers
        .into_iter()
        .flatten()
        .find(|s| std::env::args().any(|a| a == s.to_string()))
        .unwrap_or_else(|| Arc::new(InMemory::new()));
    let range = KeyGroupRange::new(0, 255);
    let change = vec![(
        Bytes::from_static(b"\0\0\0\0vk"),
        Some(Bytes::from_static(b"v")),
    )];
    let base = Path::from("s");
    let mut runs = RunStore::open(
        os.clone(),
        os.clone(),
        base.clone(),
        1,
        range,
        &[],
        RunConfig::default(),
    )
    .await?;
    let snaps = runs.commit(1, 0, change.clone()).await?;
    let back = RunStore::open(
        os.clone(),
        os.clone(),
        base,
        2,
        range,
        &snaps,
        RunConfig::default(),
    )
    .await?;
    println!("{:?}", back.get_many(vec![change[0].0.clone()]).await?);
    #[cfg(feature = "slatedb")]
    {
        use openetl_state::slate::{SlateConfig, SlateStore, Stores};
        let stores = Stores {
            db: os.clone(),
            compactor: os.clone(),
            gc: os,
        };
        let base = Path::from("d");
        let mut s =
            SlateStore::open(stores.clone(), &base, 1, range, &[], SlateConfig::default()).await?;
        let snaps = s.commit(1, 0, change.clone()).await?;
        s.close().await?;
        let back =
            SlateStore::open(stores, &base, 2, range, &snaps, SlateConfig::default()).await?;
        println!("{:?}", back.get_many(vec![change[0].0.clone()]).await?);
        back.close().await?;
    }
    Ok(())
}
