use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::SharedCredentialsProvider;
use aws_sdk_s3::config::Region;
use std::path::Path;
use configparser::ini::Ini;
use std::sync::Arc;
use tokio::task;
use image::ImageFormat;
use std::time::Instant;
use walkdir::WalkDir;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::put_object::PutObjectError;

#[tokio::main]
async fn main() {
    // Load configuration from config.ini file
    let mut config = Ini::new();
    config.load("config.ini").expect("Failed to load configuration file");

    let region = config.get("AWS", "region").expect("Missing region");
    let _endpoint = config.get("AWS", "endpoint").expect("Missing endpoint");
    let access_key = config.get("AWS", "access_key").expect("Missing access_key");
    let secret_key = config.get("AWS", "secret_key").expect("Missing secret_key");
    let bucket = config.get("AWS", "bucket").expect("Missing bucket");
    let source_path = config.get("Paths", "source_path").expect("Missing source_path");

    // Setup S3 client
    let region_provider = RegionProviderChain::first_try(Region::new(region)).or_default_provider();
    let credentials_provider = SharedCredentialsProvider::new(Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "static",
    ));
    let shared_config = aws_config::from_env()
        .region(region_provider)
        .credentials_provider(credentials_provider)
        .load()
        .await;
    let client = Client::new(&shared_config);
    let client = Arc::new(client);

    // Get all images from source path recursively
    let images: Vec<_> = WalkDir::new(&source_path)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path().to_path_buf();
            if path.is_file() && matches!(path.extension().and_then(|s| s.to_str()), Some("jpg") | Some("jpeg") | Some("png") | Some("gif")) {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    let start = Instant::now();

    // Process images concurrently
    let mut tasks = vec![];
    for image_path in images {
        let client = Arc::clone(&client);
        let bucket = bucket.clone();
        let task = task::spawn(async move {
            match convert_and_upload(&client, &bucket, &image_path).await {
                Ok(_) => println!("Successfully uploaded: {:?}", image_path.file_name().unwrap()),
                Err(e) => eprintln!("Failed to upload {:?}: {:?}", image_path.file_name().unwrap(), e),
            }
        });
        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    println!("Completed in: {:.2?}", start.elapsed());
}

async fn convert_and_upload(client: &Client, bucket: &str, image_path: &Path) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let webp_image_path = image_path.with_extension("webp");

    // Convert image to WebP
    let image = match image::open(&image_path) {
        Ok(img) => img,
        Err(e) => {
            eprintln!("Failed to open image {:?}: {:?}", image_path.file_name().unwrap(), e);
            return Err(SdkError::construction_failure(e));
        }
    };
    if let Err(e) = image.save_with_format(&webp_image_path, ImageFormat::WebP) {
        eprintln!("Failed to save WebP image {:?}: {:?}", webp_image_path.file_name().unwrap(), e);
        return Err(SdkError::construction_failure(e));
    }

    // Upload WebP image to S3
    let body = match ByteStream::from_path(&webp_image_path).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Failed to read WebP image {:?}: {:?}", webp_image_path.file_name().unwrap(), e);
            return Err(SdkError::construction_failure(e));
        }
    };
    let response = client
        .put_object()
        .bucket(bucket)
        .key(webp_image_path.file_name().unwrap().to_str().unwrap())
        .body(body)
        .send()
        .await;

    if let Err(e) = &response {
        eprintln!("Failed to upload {:?}: {:?}", webp_image_path.file_name().unwrap(), e);
    }

    response.map_err(|e| e.into())
}