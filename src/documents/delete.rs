use super::*;

///
/// Deletes the document at the given path.
///
/// You cannot use this directly with paths from [`list`] and [`query`] document metadata objects.
/// Those contain an absolute document path. Use [`abs_to_rel`] to convert to a relative path.
///
/// ## Arguments
/// * 'auth' The authentication token
/// * 'path' The relative collection path and document id, for example "my_collection/document_id"
/// * 'fail_if_not_existing' If true this method will return an error if the document does not exist.
pub fn delete(auth: &impl FirebaseAuthBearer, path: &str, fail_if_not_existing: bool) -> Result<()> {
    let url = firebase_url(auth.project_id(), path);

    let query_request = dto::Write {
        current_document: Some(dto::Precondition {
            exists: match fail_if_not_existing {
                true => Some(true),
                false => None,
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    exp_backoff(
        || {
            let resp = auth
                .client()
                .delete(&url)
                .bearer_auth(auth.access_token().to_owned())
                .json(&query_request)
                .send()
                .map_err(|err| backoff::Error::Permanent(FirebaseError::from(err)))?;

            let status = resp.status().as_u16();

            match extract_google_api_error(resp, || path.to_owned()) {
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

    Ok({})
}

//#[unstable(feature = "unstable", issue = "1234", reason = "Not yet decided if _async suffix or own module namespace")]
///
/// Deletes the document at the given path.
///
/// You cannot use this directly with paths from [`list`] and [`query`] document metadata objects.
/// Those contain an absolute document path. Use [`abs_to_rel`] to convert to a relative path.
///
/// ## Arguments
/// * 'auth' The authentication token
/// * 'path' The relative collection path and document id, for example "my_collection/document_id"
/// * 'fail_if_not_existing' If true this method will return an error if the document does not exist.
#[cfg(feature = "unstable")]
pub async fn delete_async(auth: &impl FirebaseAuthBearer, path: &str, fail_if_not_existing: bool) -> Result<()> {
    let url = firebase_url(auth.project_id(), path);

    let query_request = dto::Write {
        current_document: Some(dto::Precondition {
            exists: match fail_if_not_existing {
                true => Some(true),
                false => None,
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    exp_backoff_async(
        || async {
            let resp = auth
                .client_async()
                .delete(&url)
                .bearer_auth(auth.access_token().to_owned())
                .json(&query_request)
                .send()
                .await
                .map_err(|err| backoff::Error::Permanent(FirebaseError::from(err)))?;

            let status = resp.status().as_u16();

            match extract_google_api_error_async(resp, || path.to_owned()).await {
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

    Ok({})
}
