// db/user_name.rs

use chrono::NaiveDateTime;
use opentelemetry::trace::Tracer;
use opentelemetry::{global, trace::Span};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, Error, Statement, Transaction};
use tracing::error;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NewPersonRecord {
    pub id: Uuid,
    pub first_name: String,
    pub middle_name: Option<String>,
    pub last_name: String,
    pub suffix: Option<String>,
    pub created_date_time: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PersonRecord {
    pub id: Uuid,
    pub first_name: String,
    pub middle_name: Option<String>,
    pub last_name: String,
    pub suffix: Option<String>,
    pub created_date_time: NaiveDateTime,
    pub updated_date_time: Option<NaiveDateTime>,
}

pub struct PersonDb {
    get_by_id_stmt: Statement,
    list_stmt: Statement,
    create_stmt: Statement,
    update_stmt: Statement,
    delete_stmt: Statement,
}

impl PersonDb {
    pub async fn new(client: &Client) -> Result<Self, Error> {
        let get_by_id_stmt = client
            .prepare("SELECT * FROM \"UserName\" WHERE Id = $1")
            .await?;

        let list_stmt = client
            .prepare("SELECT * FROM \"UserName\" ORDER BY CreatedDateTime DESC OFFSET $1 LIMIT $2")
            .await?;

        let create_stmt = client.prepare(
            "INSERT INTO \"UserName\" (Id, FirstName, MiddleName, LastName, Suffix) VALUES ($1, $2, $3, $4, $5) RETURNING *"
        ).await?;

        let update_stmt = client.prepare(
            "UPDATE \"UserName\" SET FirstName = $1, MiddleName = $2, LastName = $3, Suffix = $4, UpdatedDateTime = NOW() WHERE Id = $5 RETURNING *"
        ).await?;

        let delete_stmt = client
            .prepare("DELETE FROM \"UserName\" WHERE Id = $1")
            .await?;

        Ok(PersonDb {
            get_by_id_stmt,
            list_stmt,
            create_stmt,
            update_stmt,
            delete_stmt,
        })
    }

    pub async fn get_by_id(
        &self,
        client: &Client,
        id: Uuid,
    ) -> Result<Option<PersonRecord>, Error> {
        let mut span = global::tracer("person_db").start("get_by_id");

        //let result = client.query_opt(&self.get_by_id_stmt, &[&id]).await;
        let result = client
            .query_opt("SELECT * FROM \"UserName\" WHERE Id = $1", &[&id])
            .await;

        span.end();

        match result {
            Ok(Some(row)) => Ok(Some(PersonRecord {
                id: row.get("Id"),
                first_name: row.get("FirstName"),
                middle_name: row.get("MiddleName"),
                last_name: row.get("LastName"),
                suffix: row.get("Suffix"),
                created_date_time: row.get("CreatedDateTime"),
                updated_date_time: row.get("UpdatedDateTime"),
            })),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Error fetching user by id: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn list(
        &self,
        client: &Client,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<PersonRecord>, Error> {
        let mut span = global::tracer("person_db").start("list");

        let result = client.query(&self.list_stmt, &[&offset, &limit]).await;
        span.end();

        match result {
            Ok(rows) => Ok(rows
                .into_iter()
                .map(|row| PersonRecord {
                    id: row.get("Id"),
                    first_name: row.get("FirstName"),
                    middle_name: row.get("MiddleName"),
                    last_name: row.get("LastName"),
                    suffix: row.get("Suffix"),
                    created_date_time: row.get("CreatedDateTime"),
                    updated_date_time: row.get("UpdatedDateTime"),
                })
                .collect()),
            Err(e) => {
                error!("Error listing users: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn create<'a>(
        &self,
        txn: &Transaction<'a>,
        user: &NewPersonRecord,
    ) -> Result<PersonRecord, Error> {
        let mut span = global::tracer("person_db").start("create");

        let result = txn
            .query_one(
                &self.create_stmt,
                &[
                    &user.id,
                    &user.first_name,
                    &user.middle_name,
                    &user.last_name,
                    &user.suffix,
                ],
            )
            .await;
        span.end();

        match result {
            Ok(row) => Ok(PersonRecord {
                id: row.get("Id"),
                first_name: row.get("FirstName"),
                middle_name: row.get("MiddleName"),
                last_name: row.get("LastName"),
                suffix: row.get("Suffix"),
                created_date_time: row.get("CreatedDateTime"),
                updated_date_time: row.get("UpdatedDateTime"),
            }),
            Err(e) => {
                error!("Error creating user: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn update<'a>(
        &self,
        txn: &Transaction<'a>,
        user: &PersonRecord,
    ) -> Result<PersonRecord, Error> {
        let mut span = global::tracer("person_db").start("update");

        let result = txn
            .query_one(
                &self.update_stmt,
                &[
                    &user.first_name,
                    &user.middle_name,
                    &user.last_name,
                    &user.suffix,
                    &user.id,
                ],
            )
            .await;
        span.end();

        match result {
            Ok(row) => Ok(PersonRecord {
                id: row.get("Id"),
                first_name: row.get("FirstName"),
                middle_name: row.get("MiddleName"),
                last_name: row.get("LastName"),
                suffix: row.get("Suffix"),
                created_date_time: row.get("CreatedDateTime"),
                updated_date_time: row.get("UpdatedDateTime"),
            }),
            Err(e) => {
                error!("Error updating user: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn delete<'a>(&self, txn: &Transaction<'a>, id: &Uuid) -> Result<bool, Error> {
        let mut span = global::tracer("person_db").start("delete");

        let result = txn.execute(&self.delete_stmt, &[&id]).await;
        span.end();

        match result {
            Ok(count) => Ok(count > 0),
            Err(e) => {
                error!("Error deleting user: {:?}", e);
                Err(e)
            }
        }
    }
}
