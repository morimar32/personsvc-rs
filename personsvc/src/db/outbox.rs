// db/outbox.rs

use chrono::NaiveDateTime;
use deadpool_postgres::{Pool, Transaction};
use opentelemetry::trace::Tracer;
use opentelemetry::{global, trace::Span};
use tokio_postgres::{Client, Error, Statement};
use tracing::error;
use uuid::Uuid;

#[derive(Debug)]
pub struct Outbox {
    insert_stmt: Statement,
    get_pending_stmt: Statement,
    clear_event_stmt: Statement,
    errored_event_stmt: Statement,
}

#[derive(Debug)]
pub struct OutboxMessage {
    pub id: Uuid,
    pub topic: String,
    pub event_name: String,
    pub payload: String,
    pub status: String,
    pub created_date_time: NaiveDateTime,
    pub published_date_time: Option<NaiveDateTime>,
    pub error_count: i32,
    pub error_message: Option<String>,
}

impl Outbox {
    pub async fn new(client: &Client) -> Result<Self, Error> {
        let insert_stmt = client.prepare(
            "INSERT INTO \"Outbox\" (Id, Topic, EventName, Payload, \"Status\", CreatedDateTime) VALUES ($1, $2, $3, $4, $5, NOW())"
        ).await?;

        let get_pending_stmt = client
            .prepare(
                "SELECT * FROM \"Outbox\" WHERE PublishedDateTime IS NULL AND ErrorCount < 10 LIMIT 50",
            )
            .await?;

        let clear_event_stmt = client.prepare(
            "UPDATE \"Outbox\" SET \"Status\" = 'Published', ErrorCount = 0, PublishedDateTime = NOW() WHERE Id = $1"
        ).await?;

        let errored_event_stmt = client
            .prepare(
                "UPDATE \"Outbox\" SET \"Status\" = 'Error', ErrorCount = ErrorCount + 1 WHERE Id = $1",
            )
            .await?;

        Ok(Self {
            insert_stmt,
            get_pending_stmt,
            clear_event_stmt,
            errored_event_stmt,
        })
    }

    pub async fn insert<'a, T: serde::Serialize>(
        &self,
        txn: &Transaction<'a>,
        topic: &str,
        event_name: &str,
        payload: &T,
    ) -> Result<(), Error> {
        let mut span = global::tracer("outbox_db").start("insert");
        let payload_str = serde_json::to_string(payload).ok();
        let id = Uuid::new_v4();
        let status = "Unpublished";

        let result = txn
            .execute(
                &self.insert_stmt,
                &[&id, &topic, &event_name, &payload_str, &status],
            )
            .await;
        span.end();

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Error inserting into Outbox: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn get_pending_messages(&self, client: &Client) -> Result<Vec<OutboxMessage>, Error> {
        let mut span = global::tracer("outbox_db").start("get_pending_messages");

        let result = client.query(&self.get_pending_stmt, &[]).await;
        span.end();

        match result {
            Ok(rows) => Ok(rows
                .into_iter()
                .map(|row| OutboxMessage {
                    id: row.get("Id"),
                    topic: row.get("Topic"),
                    event_name: row.get("EventName"),
                    payload: row.get("Payload"),
                    status: row.get("Status"),
                    created_date_time: row.get("CreatedDateTime"),
                    published_date_time: row.get("PublishedDateTime"),
                    error_count: row.get("ErrorCount"),
                    error_message: row.get("ErrorMessage"),
                })
                .collect()),
            Err(e) => {
                error!("Error retrieving pending messages: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn clear_event(&self, client: &Client, id: Uuid) -> Result<(), Error> {
        let mut span = global::tracer("outbox_db").start("clear_event");

        let result = client.execute(&self.clear_event_stmt, &[&id]).await;
        span.end();

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Error clearing event: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn errored_event(&self, client: &Client, id: Uuid) -> Result<(), Error> {
        let mut span = global::tracer("outbox_db").start("errored_event");

        let result = client.execute(&self.errored_event_stmt, &[&id]).await;
        span.end();

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Error marking event as errored: {:?}", e);
                Err(e)
            }
        }
    }
}
