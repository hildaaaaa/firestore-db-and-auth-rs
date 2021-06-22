use super::*;
use std::vec::IntoIter;

///
/// Queries the database for specific documents, for example all documents in a collection of 'type' == "car".
///
/// Example:
/// ```rust
/// # use serde::{Serialize, Deserialize};
/// #[derive(Debug, Serialize, Deserialize)]
/// struct DemoDTO { a_string: String, an_int: u32, }
///
/// use firestore_db_and_auth::{documents, dto};
/// use std::collections::HashMap;
/// # use firestore_db_and_auth::{credentials::Credentials, ServiceSession, errors::Result};
///
/// # let credentials = Credentials::new(include_str!("../../firebase-service-account.json"),
///                                         &[include_str!("../../tests/service-account-for-tests.jwks")])?;
/// # let session = ServiceSession::new(credentials)?;
///
/// let mut orderby = vec![];
/// orderby.push(("age".to_owned(), true));
///
/// let values: documents::Query = documents::query(&session, "tests", Some(("Sam Weiss".into(), dto::FieldOperator::EQUAL, "id")), Some(orderby), None)?;
/// for metadata in values {
///     println!("id: {}, created: {}, updated: {}", &metadata.name, metadata.create_time.as_ref().unwrap(), metadata.update_time.as_ref().unwrap());
///     // Fetch the actual document
///     // The data is wrapped in a Result<> because fetching new data could have failed
///     let doc : DemoDTO = documents::read_by_name(&session, &metadata.name)?;
///     println!("{:?}", doc);
/// }
///
/// ```
///
/// ## Arguments
/// * 'auth' The authentication token
/// * 'collectionid' The collection id; "my_collection" or "a/nested/collection"
/// * 'select_value' The query / filter value. For example (value, field, operator)
/// * 'orderby_value The order by value. For example array of ("field_1": true) for order by field_1 ascendingly, ("a_map.`000`": true) for orderby query start with numbers
pub fn query(
    auth: &impl FirebaseAuthBearer,
    collection_id: &str,
    where_value: Option<Vec<(serde_json::Value, dto::FieldOperator, &str)>>,
    orderby_value: Option<Vec<(String, bool)>>,
    limit: Option<i32>,
) -> Result<Query> {
    let url = firebase_url_query(auth.project_id());

    let query_request = make_structured_query(collection_id, where_value, orderby_value, limit);

    let resp = exp_backoff(
        || {
            let resp = auth
                .client()
                .post(&url)
                .bearer_auth(auth.access_token().to_owned())
                .json(&query_request)
                .send()
                .map_err(|err| backoff::Error::Permanent(FirebaseError::from(err)))?;

            let status = resp.status().as_u16();

            match extract_google_api_error(resp, || collection_id.to_owned()) {
                Ok(new_resp) => Ok(new_resp),
                Err(err) => {
                    if retryable_http_status(status) {
                        Err(backoff::Error::Transient(err))
                    } else {
                        Err(backoff::Error::Permanent(err))
                    }
                }
            }
        },
        FIRESTORE_REQUEST_RETRY_MAX_ELAPSED_TIME,
    )?;

    let json: Option<Vec<dto::RunQueryResponse>> = resp.json()?;

    Ok(Query(json.unwrap_or_default().into_iter()))
}

/// [Async] Query
/// ## Arguments
/// * 'auth' The authentication token
/// * 'collectionid' The collection id; "my_collection" or "a/nested/collection"
/// * 'select_value' The query / filter value. For example (value, field, operator)
/// * 'orderby_value The order by value. For example array of ("field_1": true) for order by field_1 ascendingly, ("a_map.`000`": true) for orderby query start with numbers
pub async fn query_async(
    auth: &impl FirebaseAuthBearer,
    collection_id: &str,
    where_value: Option<Vec<(serde_json::Value, dto::FieldOperator, &str)>>,
    orderby_value: Option<Vec<(String, bool)>>,
    limit: Option<i32>,
) -> Result<Query> {
    let url = firebase_url_query(auth.project_id());

    let query_request = make_structured_query(collection_id, where_value, orderby_value, limit);

    let resp = exp_backoff_async(
        || async {
            let resp = auth
                .client_async()
                .post(&url)
                .bearer_auth(auth.access_token().to_owned())
                .json(&query_request)
                .send()
                .await
                .map_err(|err| backoff::Error::Permanent(FirebaseError::from(err)))?;

            let status = resp.status().as_u16();

            match extract_google_api_error_async(resp, || collection_id.to_owned()).await {
                Ok(new_resp) => Ok(new_resp),
                Err(err) => {
                    if retryable_http_status(status) {
                        Err(backoff::Error::Transient(err))
                    } else {
                        Err(backoff::Error::Permanent(err))
                    }
                }
            }
        },
        FIRESTORE_REQUEST_RETRY_MAX_ELAPSED_TIME,
    )
    .await?;

    let json: Option<Vec<dto::RunQueryResponse>> = resp.json().await?;

    Ok(Query(json.unwrap_or_default().into_iter()))
}

/// This type is returned as a result by [`query`].
/// Use it as an iterator. The query API returns a list of document references, not the documents itself.
///
/// If you just need the meta data like the document name or update time, you are already settled.
/// To fetch the document itself, use [`read_by_name`].
///
/// Please note that this API acts as an iterator of same-like documents.
/// This type is not suitable if you want to list documents of different types.
pub struct Query(IntoIter<dto::RunQueryResponse>);

impl Iterator for Query {
    type Item = dto::Document;

    // Skip empty entries
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(r) = self.0.next() {
            if let Some(document) = r.document {
                return Some(document);
            }
        }
        return None;
    }
}

fn make_structured_query(
    collection_id: &str,
    where_value: Option<Vec<(serde_json::Value, dto::FieldOperator, &str)>>,
    orderby_value: Option<Vec<(String, bool)>>,
    limit: Option<i32>,
) -> dto::RunQueryRequest {
    let mut structured_query = dto::StructuredQuery {
        select: Some(dto::Projection { fields: None }),
        order_by: None,
        from: Some(vec![dto::CollectionSelector {
            collection_id: Some(collection_id.to_owned()),
            ..Default::default()
        }]),
        where_: None,
        limit,
        ..Default::default()
    };

    if let Some(wv) = where_value {
        let mut filters = vec![];

        for (v, operator, field) in wv {
            let value = crate::firebase_rest_to_rust::serde_value_to_firebase_value(&v);
            let f = dto::Filter {
                field_filter: Some(dto::FieldFilter {
                    value,
                    op: operator,
                    field: dto::FieldReference {
                        field_path: field.to_owned(),
                    },
                }),
                ..Default::default()
            };
            filters.push(f);
        }

        structured_query.where_ = Some(dto::Filter {
            composite_filter: Some(dto::CompositeFilter {
                filters,
                op: "AND".to_string(),
            }),
            ..Default::default()
        });
    }

    if let Some(ov) = orderby_value {
        let mut orders = vec![];
        for (f, asc) in ov {
            let mut o = dto::Order {
                field: Some(dto::FieldReference {
                    field_path: f.to_owned(),
                }),
                ..Default::default()
            };
            o.direction = if asc { None } else { Some("desc".to_owned()) };
            orders.push(o);
        }
        structured_query.order_by = Some(orders);
    }

    dto::RunQueryRequest {
        structured_query: Some(structured_query),
        ..Default::default()
    }
}
