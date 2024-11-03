// services/person_service.rs

use std::error::Error;

use crate::db::outbox::Outbox;
use crate::db::person::{NewPersonRecord, PersonDb, PersonRecord};
use deadpool_postgres::{Pool, Transaction};
use opentelemetry::global::{self, ObjectSafeSpan};
use opentelemetry::trace::{Span, Tracer};
use serde_json::json;
use tokio_postgres::Client;
use tracing::{error, info};
use uuid::Uuid;

pub struct PersonService {
    person_db: PersonDb,
    outbox: Outbox,
}

impl PersonService {
    pub async fn new(client: &Client) -> Result<Self, Box<dyn Error>> {
        match PersonDb::new(&client).await {
            Ok(user_name_db) => match Outbox::new(&client).await {
                Ok(outbox) => Ok(PersonService {
                    person_db: user_name_db,
                    outbox,
                }),
                Err(err) => {
                    error!("Failed to initialize Outbox: {:?}", err);
                    return Err(Box::new(err));
                }
            },
            Err(err) => {
                error!("Failed to initialize UserNameDb: {:?}", err);
                return Err(Box::new(err));
            }
        }
    }

    pub async fn get_user_by_id(
        &self,
        db_pool: &Pool,
        id: Uuid,
    ) -> Result<Option<PersonRecord>, Box<dyn Error>> {
        let mut span = global::tracer("person_service").start("get_user_by_id");

        let mut client = match db_pool.get().await {
            Ok(c) => c,
            Err(err) => {
                error!("Error getting db client from pool: {:?}", err);
                return Err(Box::new(err));
            }
        };

        let user = match self.person_db.get_by_id(&client, id).await {
            Ok(u) => u,
            Err(err) => {
                error!("Failed to get user by ID: {:?}", err);
                ObjectSafeSpan::end(&mut span);
                return Err(Box::new(err));
            }
        };

        ObjectSafeSpan::end(&mut span);
        return Ok(user);
    }

    pub async fn list_users(
        &self,
        db_pool: &Pool,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<PersonRecord>, Box<dyn Error>> {
        let mut span = global::tracer("person_service").start("list_users");
        let client = match db_pool.get().await {
            Ok(c) => c,
            Err(err) => {
                error!("Error getting db client from pool: {:?}", err);
                return Err(Box::new(err));
            }
        };

        let users = match self.person_db.list(&client, offset, limit).await {
            Ok(u) => u,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Failed to list users: {:?}", err);
                return Err(Box::new(err));
            }
        };

        ObjectSafeSpan::end(&mut span);

        return Ok(users);
    }

    pub async fn create_user(
        &self,
        db_pool: &Pool,
        user: &NewPersonRecord,
    ) -> Result<PersonRecord, Box<dyn Error>> {
        let mut span = global::tracer("person_service").start("create_user");

        let mut client = match db_pool.get().await {
            Ok(c) => c,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error getting db client from pool: {:?}", err);
                return Err(Box::new(err));
            }
        };

        let tx = match client.build_transaction().start().await {
            Ok(t) => t,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error starting transaction for Create Person {:?}", err);
                return Err(Box::new(err));
            }
        };

        let created_user = match self.person_db.create(&tx, user).await {
            Ok(user) => user,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Failed to create user: {:?}", err);
                let _ = match tx.rollback().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Failed to rollback transaction for Create User: {:?}", e);
                        return Err(Box::new(e));
                    }
                };
                return Err(Box::new(err));
            }
        };

        let _ = match self
            .outbox
            .insert(&tx, "user_events", "user_added", &created_user)
            .await
        {
            Ok(o) => o,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Failed to create outbox record: {:?}", err);
                let _ = match tx.rollback().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Failed to rollback transaction for Create Person Outbox record: {:?}",
                            e
                        );
                        return Err(Box::new(e));
                    }
                };
                return Err(Box::new(err));
            }
        };

        let _ = match tx.commit().await {
            Ok(_) => {}
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error committing transaction for Create Person: {:?}", err);
                return Err(Box::new(err));
            }
        };
        ObjectSafeSpan::end(&mut span);
        return Ok(created_user);
    }

    pub async fn update_user(
        &self,
        db_pool: &Pool,
        user: &PersonRecord,
    ) -> Result<PersonRecord, Box<dyn Error>> {
        let mut span = global::tracer("person_service").start("update_user");

        let mut client = match db_pool.get().await {
            Ok(c) => c,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error getting db client from pool: {:?}", err);
                return Err(Box::new(err));
            }
        };

        let tx = match client.build_transaction().start().await {
            Ok(t) => t,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error starting transaction for Create Person {:?}", err);
                return Err(Box::new(err));
            }
        };

        let updated_user = match self.person_db.update(&tx, user).await {
            Ok(u) => u,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Failed to update user: {:?}", err);
                let _ = match tx.rollback().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error rolling transaction for Update Person {:?}", e);
                        return Err(Box::new(e));
                    }
                };
                return Err(Box::new(err));
            }
        };

        let _ = match self
            .outbox
            .insert(&tx, "user_events", "user_updated", &updated_user)
            .await
        {
            Ok(a) => a,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                let _ = match tx.rollback().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Failed to rollback transaction for Create Person Outbox record: {:?}",
                            e
                        );
                        return Err(Box::new(e));
                    }
                };
                error!(
                    "Failed to create outbox record For Update Person: {:?}",
                    err
                );
                return Err(Box::new(err));
            }
        };
        let _ = match tx.commit().await {
            Ok(_) => {}
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error committing transaction for Update Person: {:?}", err);
                return Err(Box::new(err));
            }
        };
        ObjectSafeSpan::end(&mut span);
        return Ok(updated_user);
    }

    pub async fn delete_user(&self, db_pool: &Pool, id: &Uuid) -> Result<bool, Box<dyn Error>> {
        let mut span = global::tracer("person_service").start("delete_user");

        let mut client = match db_pool.get().await {
            Ok(c) => c,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error getting db client from pool: {:?}", err);
                return Err(Box::new(err));
            }
        };

        let tx = match client.build_transaction().start().await {
            Ok(t) => t,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error starting transaction for Create Person {:?}", err);
                return Err(Box::new(err));
            }
        };

        let deleted = match self.person_db.delete(&tx, &id).await {
            Ok(d) => d,
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error deleting Person {:?}", err);
                let _ = match tx.rollback().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error rolling back transaction for Delete Person {:?}", err);
                        return Err(Box::new(e));
                    }
                };
                return Err(Box::new(err));
            }
        };
        if (deleted) {
            let _ = match self
                .outbox
                .insert(&tx, "user_events", "user_deleted", &id)
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    ObjectSafeSpan::end(&mut span);
                    error!("Error deleting Person {:?}", err);
                    let _ = match tx.rollback().await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Error committing transaction for Delete Person {:?}", e);
                            return Err(Box::new(e));
                        }
                    };
                    return Err(Box::new(err));
                }
            };
        }
        let _ = match tx.commit().await {
            Ok(_) => {}
            Err(err) => {
                ObjectSafeSpan::end(&mut span);
                error!("Error committing transaction for Delete Person: {:?}", err);
                return Err(Box::new(err));
            }
        };

        ObjectSafeSpan::end(&mut span);
        return Ok(deleted);
    }
}
