mod db;
mod handlers;
mod services;

// Import necessary modules and crates
use actix_web::{web, App, HttpServer};
use deadpool_postgres::{Config, ConfigError, ManagerConfig, RecyclingMethod, Runtime};
use services::PersonService;
use std::{ptr::null, sync::Arc};
use tokio_postgres::{GenericClient, NoTls};
use tracing::{error, info};

//#[actix_web::main]
//async fn main() -> Result<(), Box<dyn std::error::Error>> {
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    //std::env::set_var("RUST_LOG", "DEBUG");
    tracing_subscriber::fmt::init();
    let mut config = Config::new();
    config.dbname = Some("Person".to_string());
    config.user = Some("postgres".to_string());
    config.password = Some("5k4t3rd4t3r".to_string());
    config.host = Some("localhost".to_string());
    config.port = Some(5432);
    config.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = config.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let mut client = pool.get().await.unwrap();
    let person_service: Arc<PersonService> = Arc::new(PersonService::new(&client).await.unwrap());
    //let person_service: PersonService = PersonService::new(&client).await.unwrap();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(person_service.clone()))
            //.app_data(person_service.clone())
            .app_data(web::Data::new(pool.clone()))
            .route(
                "/api/v1/person",
                web::post().to(handlers::person_handler::create_user),
            )
            .route(
                "/api/v1/person",
                web::get().to(handlers::person_handler::list_users),
            )
            .route(
                "/api/v1/person/{id}",
                web::get().to(handlers::person_handler::get_user_by_id),
            )
            .route(
                "/api/v1/person/{id}",
                web::put().to(handlers::person_handler::update_user),
            )
            .route(
                "/api/v1/person/{id}",
                web::delete().to(handlers::person_handler::delete_user),
            )
    })
    .bind("127.0.0.1:8080")?
    .run();

    println!("Server running at http://127.0.0.1:8080/");

    server.await
}
