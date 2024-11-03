// handlers/person_handler.rs

use std::default;
use std::sync::Arc;

use crate::db::{person::PersonDb, NewPersonRecord, PersonRecord};
use crate::services::person_service::PersonService;
use actix_web::{web, HttpResponse, Responder};
use chrono;
use deadpool_postgres::{Client, Pool};
use serde::{Deserialize, Serialize};
use tokio_postgres::GenericClient;
use tracing::error;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct CreateUserRequest {
    first_name: String,
    middle_name: Option<String>,
    last_name: String,
    suffix: Option<String>,
}

#[derive(Serialize)]
pub struct UserResponse {
    id: Uuid,
    first_name: String,
    middle_name: Option<String>,
    last_name: String,
    suffix: Option<String>,
    created_date_time: String,
    updated_date_time: Option<String>,
}

pub async fn get_user_by_id(
    service: web::Data<Arc<PersonService>>,
    db_pool: web::Data<Pool>,
    id: web::Path<Uuid>,
) -> impl Responder {
    let client = db_pool
        .get()
        .await
        .map_err(|err| {
            error!("Error getting db client from pool: {}", err);
            HttpResponse::InternalServerError().finish();
        })
        .unwrap();

    let user = service
        .get_user_by_id(db_pool.get_ref(), id.into_inner())
        .await
        .map_err(|err| {
            error!("Error retrieving record from database: {}", err);
            HttpResponse::InternalServerError().finish();
        })
        .unwrap();
    match (user) {
        Some(user) => HttpResponse::Ok().json(UserResponse {
            id: user.id,
            first_name: user.first_name,
            middle_name: user.middle_name,
            last_name: user.last_name,
            suffix: user.suffix,
            created_date_time: user.created_date_time.to_string(),
            updated_date_time: user.updated_date_time.map(|dt| dt.to_string()),
        }),
        None => HttpResponse::NotFound().finish(),
    }
}

pub async fn list_users(
    service: web::Data<PersonService>,
    db_pool: web::Data<Pool>,
    web::Query((offset, limit)): web::Query<(i64, i64)>,
) -> impl Responder {
    let client = db_pool
        .get()
        .await
        .map_err(|err| {
            error!("Error getting db client from pool: {}", err);
            HttpResponse::InternalServerError().finish();
        })
        .unwrap();

    match service.list_users(db_pool.get_ref(), offset, limit).await {
        Ok(users) => HttpResponse::Ok().json(
            users
                .into_iter()
                .map(|user| UserResponse {
                    id: user.id,
                    first_name: user.first_name,
                    middle_name: user.middle_name,
                    last_name: user.last_name,
                    suffix: user.suffix,
                    created_date_time: user.created_date_time.to_string(),
                    updated_date_time: user.updated_date_time.map(|dt| dt.to_string()),
                })
                .collect::<Vec<_>>(),
        ),
        Err(e) => {
            error!("Error listing users: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn create_user(
    service: web::Data<PersonService>,
    db_pool: web::Data<Pool>,
    new_user: web::Json<CreateUserRequest>,
) -> impl Responder {
    let new_person_record = NewPersonRecord {
        id: Uuid::new_v4(),
        first_name: new_user.first_name.to_string(),
        middle_name: new_user.middle_name.as_ref().map(|m| m.to_string()),
        last_name: new_user.last_name.to_string(),
        suffix: new_user.suffix.as_ref().map(|m| m.to_string()),
        created_date_time: chrono::offset::Utc::now().naive_local(),
    };

    match service
        .create_user(&db_pool.get_ref(), &new_person_record)
        .await
    {
        Ok(_) => {}
        Err(err) => {
            error!("Error creating user: {:?}", err);
            HttpResponse::InternalServerError().finish();
        }
    };

    match service
        .create_user(db_pool.get_ref(), &new_person_record)
        .await
    {
        Ok(created_user) => HttpResponse::Created().json(UserResponse {
            id: created_user.id,
            first_name: created_user.first_name,
            middle_name: created_user.middle_name,
            last_name: created_user.last_name,
            suffix: created_user.suffix,
            created_date_time: created_user.created_date_time.to_string(),
            updated_date_time: created_user.updated_date_time.map(|dt| dt.to_string()),
        }),
        Err(e) => {
            error!("Error creating user: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn update_user(
    service: web::Data<PersonService>,
    db_pool: web::Data<Pool>,
    user_id: web::Path<Uuid>,
    updated_user: web::Json<CreateUserRequest>,
) -> impl Responder {
    let user = PersonRecord {
        id: *user_id,
        first_name: updated_user.first_name.clone(),
        middle_name: updated_user.middle_name.clone(),
        last_name: updated_user.last_name.clone(),
        suffix: updated_user.suffix.clone(),
        created_date_time: chrono::Utc::now().naive_utc(),
        updated_date_time: Some(chrono::Utc::now().naive_utc()),
    };

    match service.update_user(db_pool.get_ref(), &user).await {
        Ok(user) => HttpResponse::Ok().json(UserResponse {
            id: user.id,
            first_name: user.first_name,
            middle_name: user.middle_name,
            last_name: user.last_name,
            suffix: user.suffix,
            created_date_time: user.created_date_time.to_string(),
            updated_date_time: user.updated_date_time.map(|dt| dt.to_string()),
        }),
        Err(e) => {
            error!("Error updating user: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

pub async fn delete_user(
    service: web::Data<PersonService>,
    db_pool: web::Data<Pool>,
    id: web::Path<Uuid>,
) -> impl Responder {
    match service.delete_user(db_pool.get_ref(), &id).await {
        Ok(true) => HttpResponse::Ok().finish(),
        Ok(false) => HttpResponse::NotFound().finish(),
        Err(e) => {
            error!("Error deleting user: {:?}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}
