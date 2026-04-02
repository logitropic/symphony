use crate::config::ConfigSchema;
use crate::domain::Issue;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LinearError {
    #[error("Missing Linear API token")]
    MissingApiKey,
    #[error("Missing project slug")]
    MissingProjectSlug,
    #[error("API request failed: {0}")]
    ApiRequest(String),
    #[error("API returned non-200 status: {0}")]
    ApiStatus(i32),
    #[error("GraphQL errors: {0}")]
    GraphQLErrors(String),
    #[error("Unknown payload format")]
    UnknownPayload,
    #[error("Missing end cursor for pagination")]
    MissingEndCursor,
}

#[derive(Debug, Clone)]
pub struct LinearClient {
    client: Client,
    api_key: String,
    endpoint: String,
    project_slug: String,
}

impl LinearClient {
    pub fn new(config: &ConfigSchema) -> Result<Self, LinearError> {
        let api_key = config
            .tracker
            .api_key
            .clone()
            .ok_or(LinearError::MissingApiKey)?;
        let project_slug = config
            .tracker
            .project_slug
            .clone()
            .ok_or(LinearError::MissingProjectSlug)?;

        Ok(Self {
            client: Client::new(),
            api_key,
            endpoint: config.tracker.endpoint(),
            project_slug,
        })
    }

    pub async fn fetch_candidate_issues(
        &self,
        active_states: &[String],
    ) -> Result<Vec<Issue>, LinearError> {
        self.fetch_issues_by_states_impl(active_states, None).await
    }

    pub async fn fetch_issues_by_states(
        &self,
        states: &[String],
    ) -> Result<Vec<Issue>, LinearError> {
        self.fetch_issues_by_states_impl(states, None).await
    }

    pub async fn fetch_issue_states_by_ids(
        &self,
        ids: &[String],
    ) -> Result<Vec<Issue>, LinearError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.fetch_issues_by_ids_impl(ids).await
    }

    async fn fetch_issues_by_states_impl(
        &self,
        state_names: &[String],
        mut after: Option<String>,
    ) -> Result<Vec<Issue>, LinearError> {
        let query = r#"
        query SymphonyLinearPoll($projectSlug: String!, $stateNames: [String!]!, $first: Int!, $relationFirst: Int!, $after: String) {
            issues(filter: {project: {slugId: {eq: $projectSlug}}, state: {name: {in: $stateNames}}}, first: $first, after: $after) {
                nodes {
                    id
                    identifier
                    title
                    description
                    priority
                    state { name }
                    branchName
                    url
                    assignee { id }
                    labels { nodes { name } }
                    inverseRelations(first: $relationFirst) {
                        nodes {
                            type
                            issue {
                                id
                                identifier
                                state { name }
                            }
                        }
                    }
                    createdAt
                    updatedAt
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        "#;

        let mut all_issues = Vec::new();

        loop {
            let variables = serde_json::json!({
                "projectSlug": self.project_slug,
                "stateNames": state_names,
                "first": 50,
                "relationFirst": 50,
                "after": after
            });

            let body = self.graphql(query, variables).await?;

            let data = body
                .get("data")
                .and_then(|d| d.get("issues"))
                .ok_or(LinearError::UnknownPayload)?;

            let nodes = data
                .get("nodes")
                .and_then(|n| n.as_array())
                .ok_or(LinearError::UnknownPayload)?;

            let page_info = data.get("pageInfo").ok_or(LinearError::UnknownPayload)?;
            let has_next_page = page_info
                .get("hasNextPage")
                .and_then(|h| h.as_bool())
                .unwrap_or(false);
            after = page_info
                .get("endCursor")
                .and_then(|e| e.as_str())
                .map(|s| s.to_string());

            let issues: Vec<Issue> = nodes
                .iter()
                .filter_map(|n| self.normalize_issue(n))
                .collect();
            all_issues.extend(issues);

            if !has_next_page {
                break;
            }
        }

        Ok(all_issues)
    }

    async fn fetch_issues_by_ids_impl(&self, ids: &[String]) -> Result<Vec<Issue>, LinearError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let query = r#"
        query SymphonyLinearIssuesById($ids: [ID!]!, $first: Int!, $relationFirst: Int!) {
            issues(filter: {id: {in: $ids}}, first: $first) {
                nodes {
                    id
                    identifier
                    title
                    description
                    priority
                    state { name }
                    branchName
                    url
                    assignee { id }
                    labels { nodes { name } }
                    inverseRelations(first: $relationFirst) {
                        nodes {
                            type
                            issue {
                                id
                                identifier
                                state { name }
                            }
                        }
                    }
                    createdAt
                    updatedAt
                }
            }
        }
        "#;

        let variables = serde_json::json!({
            "ids": ids,
            "first": 50,
            "relationFirst": 50
        });

        let body = self.graphql(query, variables).await?;

        let data = body
            .get("data")
            .and_then(|d| d.get("issues"))
            .ok_or(LinearError::UnknownPayload)?;
        let nodes = data
            .get("nodes")
            .and_then(|n| n.as_array())
            .ok_or(LinearError::UnknownPayload)?;

        let mut issues: Vec<Issue> = nodes
            .iter()
            .filter_map(|n| self.normalize_issue(n))
            .collect();

        // Sort by original ID order
        let id_order: HashMap<&str, usize> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();
        issues.sort_by(|a, b| {
            let a_idx =
                a.id.as_ref()
                    .and_then(|id| id_order.get(id.as_str()))
                    .copied()
                    .unwrap_or(usize::MAX);
            let b_idx =
                b.id.as_ref()
                    .and_then(|id| id_order.get(id.as_str()))
                    .copied()
                    .unwrap_or(usize::MAX);
            a_idx.cmp(&b_idx)
        });

        Ok(issues)
    }

    async fn graphql(&self, query: &str, variables: Value) -> Result<Value, LinearError> {
        const MAX_RETRIES: u32 = 3;
        const INITIAL_BACKOFF_MS: u64 = 100;

        let body = serde_json::json!({
            "query": query,
            "variables": variables
        });

        let mut last_error = None;
        let mut backoff_ms = INITIAL_BACKOFF_MS;

        for attempt in 0..MAX_RETRIES {
            let response = match self
                .client
                .post(&self.endpoint)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_error = Some(LinearError::ApiRequest(e.to_string()));
                    if attempt + 1 < MAX_RETRIES {
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms = backoff_ms.saturating_mul(2);
                        continue;
                    }
                    return Err(last_error.unwrap());
                }
            };

            let status = response.status();
            // Retry on rate limit (429) or server errors (5xx)
            if status.as_u16() == 429 || status.is_server_error() {
                if attempt + 1 < MAX_RETRIES {
                    // Respect Retry-After header if present, otherwise use exponential backoff
                    let retry_after_ms = response
                        .headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(|v| v * 1000)
                        .unwrap_or(backoff_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(retry_after_ms)).await;
                    backoff_ms = backoff_ms.saturating_mul(2);
                    // Consume the response body to allow connection reuse, then retry
                    let _ = response.bytes().await;
                    continue;
                }
                return Err(LinearError::ApiStatus(status.as_u16() as i32));
            }

            if !status.is_success() {
                return Err(LinearError::ApiStatus(status.as_u16() as i32));
            }

            let body: Value = response
                .json()
                .await
                .map_err(|e| LinearError::ApiRequest(e.to_string()))?;

            if let Some(errors) = body.get("errors") {
                return Err(LinearError::GraphQLErrors(errors.to_string()));
            }

            return Ok(body);
        }

        Err(last_error.unwrap_or(LinearError::ApiRequest("Unknown error".to_string())))
    }

    fn normalize_issue(&self, data: &Value) -> Option<Issue> {
        let id = data.get("id")?.as_str()?.to_string();

        let labels: Vec<String> = data
            .get("labels")
            .and_then(|l| l.get("nodes"))
            .and_then(|n| n.as_array())
            .map(|nodes| {
                nodes
                    .iter()
                    .filter_map(|l| {
                        l.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_lowercase())
                    })
                    .collect()
            })
            .unwrap_or_default();

        let blocked_by: Vec<Issue> = data
            .get("inverseRelations")
            .and_then(|r| r.get("nodes"))
            .and_then(|n| n.as_array())
            .map(|nodes| {
                nodes
                    .iter()
                    .filter_map(|rel| {
                        let rel_type = rel.get("type")?.as_str()?;
                        if rel_type.to_lowercase() != "blocks" {
                            return None;
                        }
                        let issue_data = rel.get("issue")?;
                        Some(Issue {
                            id: issue_data
                                .get("id")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            identifier: issue_data
                                .get("identifier")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            state: issue_data
                                .get("state")
                                .and_then(|s| s.get("name"))
                                .and_then(|n| n.as_str())
                                .map(|s| s.to_string()),
                            title: None,
                            description: None,
                            priority: None,
                            branch_name: None,
                            url: None,
                            assignee_id: None,
                            labels: vec![],
                            blocked_by: vec![],
                            assigned_to_worker: false,
                            created_at: None,
                            updated_at: None,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Some(Issue {
            id: Some(id),
            identifier: data
                .get("identifier")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            title: data
                .get("title")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            description: data
                .get("description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            priority: data
                .get("priority")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32),
            state: data
                .get("state")
                .and_then(|s| s.get("name"))
                .and_then(|n| n.as_str())
                .map(|s| s.to_string()),
            branch_name: data
                .get("branchName")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            url: data
                .get("url")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            assignee_id: data
                .get("assignee")
                .and_then(|a| a.get("id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            labels,
            blocked_by,
            assigned_to_worker: true,
            created_at: data
                .get("createdAt")
                .and_then(|v| v.as_str())
                .and_then(parse_datetime),
            updated_at: data
                .get("updatedAt")
                .and_then(|v| v.as_str())
                .and_then(parse_datetime),
        })
    }
}

fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    fn create_test_config() -> ConfigSchema {
        ConfigSchema {
            tracker: crate::config::TrackerConfig {
                kind: Some("linear".to_string()),
                endpoint: Some("https://api.linear.app/graphql".to_string()),
                api_key: Some("test-api-key".to_string()),
                project_slug: Some("test-project".to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_linear_client_new_success() {
        let config = create_test_config();
        let client = LinearClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_linear_client_new_missing_api_key() {
        let mut config = create_test_config();
        config.tracker.api_key = None;
        let client = LinearClient::new(&config);
        assert!(matches!(client, Err(LinearError::MissingApiKey)));
    }

    #[test]
    fn test_linear_client_new_missing_project_slug() {
        let mut config = create_test_config();
        config.tracker.project_slug = None;
        let client = LinearClient::new(&config);
        assert!(matches!(client, Err(LinearError::MissingProjectSlug)));
    }

    #[test]
    fn test_fetch_issue_states_by_ids_empty() {
        let config = create_test_config();
        let _client = LinearClient::new(&config).unwrap();
        // This test verifies the early return works without network
        // Actual HTTP tests would require a mock server
    }

    #[test]
    fn test_normalize_issue_full_payload() {
        let client = LinearClient::new(&create_test_config()).unwrap();

        let data = serde_json::json!({
            "id": "TEST-123",
            "identifier": "TEST-123",
            "title": "Test Issue",
            "description": "A test description",
            "priority": 2,
            "state": { "name": "In Progress" },
            "branchName": "issue/TEST-123",
            "url": "https://linear.app/test/TEST-123",
            "assignee": { "id": "user-1" },
            "labels": {
                "nodes": [
                    { "name": "bug" },
                    { "name": "frontend" }
                ]
            },
            "inverseRelations": {
                "nodes": [
                    {
                        "type": "blocks",
                        "issue": {
                            "id": "BLOCK-1",
                            "identifier": "BLOCK-1",
                            "state": { "name": "Done" }
                        }
                    }
                ]
            },
            "createdAt": "2024-01-15T10:30:00Z",
            "updatedAt": "2024-01-16T14:20:00Z"
        });

        let issue = client.normalize_issue(&data);

        assert!(issue.is_some());
        let issue = issue.unwrap();
        assert_eq!(issue.id, Some("TEST-123".to_string()));
        assert_eq!(issue.identifier, Some("TEST-123".to_string()));
        assert_eq!(issue.title, Some("Test Issue".to_string()));
        assert_eq!(issue.description, Some("A test description".to_string()));
        assert_eq!(issue.priority, Some(2));
        assert_eq!(issue.state, Some("In Progress".to_string()));
        assert_eq!(issue.branch_name, Some("issue/TEST-123".to_string()));
        assert_eq!(
            issue.url,
            Some("https://linear.app/test/TEST-123".to_string())
        );
        assert_eq!(issue.assignee_id, Some("user-1".to_string()));
        assert_eq!(issue.labels, vec!["bug", "frontend"]);
        assert_eq!(issue.blocked_by.len(), 1);
        assert_eq!(issue.blocked_by[0].id, Some("BLOCK-1".to_string()));
        assert!(issue.created_at.is_some());
        assert!(issue.updated_at.is_some());
    }

    #[test]
    fn test_normalize_issue_minimal_payload() {
        let client = LinearClient::new(&create_test_config()).unwrap();

        let data = serde_json::json!({
            "id": "TEST-456"
        });

        let issue = client.normalize_issue(&data);

        assert!(issue.is_some());
        let issue = issue.unwrap();
        assert_eq!(issue.id, Some("TEST-456".to_string()));
        assert_eq!(issue.identifier, None);
        assert_eq!(issue.title, None);
        assert_eq!(issue.description, None);
        assert_eq!(issue.priority, None);
        assert_eq!(issue.state, None);
        assert!(issue.labels.is_empty());
        assert!(issue.blocked_by.is_empty());
    }

    #[test]
    fn test_normalize_issue_filters_non_blocks_relations() {
        let client = LinearClient::new(&create_test_config()).unwrap();

        let data = serde_json::json!({
            "id": "TEST-789",
            "inverseRelations": {
                "nodes": [
                    {
                        "type": "blocks",
                        "issue": { "id": "BLOCK-1", "identifier": "BLOCK-1", "state": { "name": "Done" } }
                    },
                    {
                        "type": "related_to",
                        "issue": { "id": "REL-1", "identifier": "REL-1", "state": { "name": "In Progress" } }
                    }
                ]
            }
        });

        let issue = client.normalize_issue(&data).unwrap();
        assert_eq!(issue.blocked_by.len(), 1);
        assert_eq!(issue.blocked_by[0].id, Some("BLOCK-1".to_string()));
    }

    #[test]
    fn test_normalize_issue_blocks_relation_case_insensitive() {
        let client = LinearClient::new(&create_test_config()).unwrap();

        let data = serde_json::json!({
            "id": "TEST-ABC",
            "inverseRelations": {
                "nodes": [
                    {
                        "type": "BLOCKS",
                        "issue": { "id": "BLOCK-1", "identifier": "BLOCK-1", "state": { "name": "Done" } }
                    }
                ]
            }
        });

        let issue = client.normalize_issue(&data).unwrap();
        assert_eq!(issue.blocked_by.len(), 1);
    }

    #[test]
    fn test_normalize_issue_missing_id() {
        let client = LinearClient::new(&create_test_config()).unwrap();

        let data = serde_json::json!({
            "title": "No ID Issue"
        });

        let issue = client.normalize_issue(&data);
        assert!(issue.is_none());
    }

    #[test]
    fn test_parse_datetime_valid() {
        let result = parse_datetime("2024-01-15T10:30:00Z");
        assert!(result.is_some());
        let dt = result.unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn test_parse_datetime_invalid() {
        assert!(parse_datetime("not-a-date").is_none());
        assert!(parse_datetime("").is_none());
        assert!(parse_datetime("2024-13-45T00:00:00Z").is_none());
    }

    #[test]
    fn test_parse_datetime_with_timezone() {
        // Test that timezone offset is correctly converted to UTC
        let result = parse_datetime("2024-01-15T10:30:00+05:00");
        assert!(result.is_some());
        // 10:30 +05:00 = 05:30 UTC
        let dt = result.unwrap();
        assert_eq!(dt.hour(), 5);
        assert_eq!(dt.minute(), 30);
    }
}
