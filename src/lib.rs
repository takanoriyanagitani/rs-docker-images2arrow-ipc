use std::io;
use std::sync::Arc;

use io::BufWriter;
use io::Write;

use arrow::record_batch::RecordBatch;

use bollard::ClientVersion;
use bollard::Docker;
use bollard::models::ImageSummary;
use bollard::query_parameters::ListImagesOptions;

pub async fn list_images(
    d: &Docker,
    opts: Option<ListImagesOptions>,
) -> Result<Vec<ImageSummary>, io::Error> {
    d.list_images(opts).await.map_err(io::Error::other)
}

use arrow::array::ArrayRef;
use arrow::array::builder::{Int64Builder, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};

pub fn summary_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("parent_id", DataType::Utf8, false),
        Field::new("repo_tags", DataType::new_list(DataType::Utf8, true), false),
        Field::new(
            "repo_digests",
            DataType::new_list(DataType::Utf8, true),
            false,
        ),
        Field::new("created", DataType::Int64, false),
        Field::new("size", DataType::Int64, false),
        Field::new("shared_size", DataType::Int64, false),
        Field::new("virtual_size", DataType::Int64, true), // Option<i64>
        // Field::new("labels", DataType::Utf8, false), // HashMap<String, String> - complex type, handle later
        Field::new("containers", DataType::Int64, false),
        // Field::new("manifests", DataType::Utf8, true), // Option<Vec<ImageManifestSummary>> - complex type, handle later
        // Field::new("descriptor", DataType::Utf8, true), // Option<OciDescriptor> - complex type, handle later
    ]))
}

pub fn images2batch(
    imgs: Vec<ImageSummary>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, io::Error> {
    let mut id_builder = StringBuilder::new();
    let mut parent_id_builder = StringBuilder::new();
    let mut repo_tags_builder = ListBuilder::new(StringBuilder::new());
    let mut repo_digests_builder = ListBuilder::new(StringBuilder::new());
    let mut created_builder = Int64Builder::new();
    let mut size_builder = Int64Builder::new();
    let mut shared_size_builder = Int64Builder::new();
    let mut virtual_size_builder = Int64Builder::new();
    let mut containers_builder = Int64Builder::new();

    for img in imgs {
        id_builder.append_value(img.id);
        parent_id_builder.append_value(img.parent_id);

        repo_tags_builder.append(true);
        for tag in img.repo_tags {
            repo_tags_builder.values().append_value(tag);
        }

        repo_digests_builder.append(true);
        for digest in img.repo_digests {
            repo_digests_builder.values().append_value(digest);
        }

        created_builder.append_value(img.created);
        size_builder.append_value(img.size);
        shared_size_builder.append_value(img.shared_size);
        containers_builder.append_value(img.containers);

        if let Some(vs) = img.virtual_size {
            virtual_size_builder.append_value(vs);
        } else {
            virtual_size_builder.append_null();
        }
    }

    let id_array = id_builder.finish();
    let parent_id_array = parent_id_builder.finish();
    let repo_tags_array = repo_tags_builder.finish();
    let repo_digests_array = repo_digests_builder.finish();
    let created_array = created_builder.finish();
    let size_array = size_builder.finish();
    let shared_size_array = shared_size_builder.finish();
    let virtual_size_array = virtual_size_builder.finish();
    let containers_array = containers_builder.finish();

    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(id_array),
        std::sync::Arc::new(parent_id_array),
        std::sync::Arc::new(repo_tags_array),
        std::sync::Arc::new(repo_digests_array),
        std::sync::Arc::new(created_array),
        std::sync::Arc::new(size_array),
        std::sync::Arc::new(shared_size_array),
        std::sync::Arc::new(virtual_size_array),
        std::sync::Arc::new(containers_array),
    ];

    RecordBatch::try_new(schema, columns).map_err(io::Error::other)
}

pub struct IpcStreamWriter<W>(pub arrow::ipc::writer::StreamWriter<BufWriter<W>>)
where
    W: Write;

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn finish(&mut self) -> Result<(), io::Error> {
        self.0.finish().map_err(io::Error::other)
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        self.0.flush().map_err(io::Error::other)
    }

    pub fn write_batch(&mut self, b: &RecordBatch) -> Result<(), io::Error> {
        self.0.write(b).map_err(io::Error::other)
    }
}

pub fn batch2writer<W>(b: &RecordBatch, mut wtr: W, sch: &Schema) -> Result<(), io::Error>
where
    W: Write,
{
    let swtr = arrow::ipc::writer::StreamWriter::try_new_buffered(&mut wtr, sch)
        .map_err(io::Error::other)?;
    let mut iw = IpcStreamWriter(swtr);
    iw.write_batch(b)?;
    iw.flush()?;
    iw.finish()?;

    drop(iw);

    wtr.flush()
}

pub fn imgs2writer<W>(
    imgs: Vec<ImageSummary>,
    mut wtr: W,
    sch: Arc<Schema>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let batch = images2batch(imgs, sch.clone())?;
    batch2writer(&batch, &mut wtr, &sch)
}

pub async fn images2writer<W>(
    d: &Docker,
    mut wtr: W,
    opts: Option<ListImagesOptions>,
) -> Result<(), io::Error>
where
    W: Write,
{
    let imgs = list_images(d, opts).await?;
    let schema = summary_schema();
    imgs2writer(imgs, &mut wtr, schema)
}

pub fn unix2docker(
    sock_path: &str,
    timeout_seconds: u64,
    client_version: &ClientVersion,
) -> Result<Docker, io::Error> {
    Docker::connect_with_unix(sock_path, timeout_seconds, client_version).map_err(io::Error::other)
}

pub const DOCKER_UNIX_PATH_DEFAULT: &str = "/var/run/docker.sock";
pub const DOCKER_CON_TIMEOUT_SECONDS_DEFAULT: u64 = 30;
pub const DOCKER_CLIENT_VERSION_DEFAULT: &ClientVersion = bollard::API_DEFAULT_VERSION;
