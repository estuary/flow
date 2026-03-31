//! Static SCIM 2.0 discovery endpoints.
//!
//! These return fixed JSON describing our SCIM capabilities (deprovisioning only,
//! no groups, no bulk, no password management).

use axum::Json;

/// GET /ServiceProviderConfig — describes supported SCIM features.
pub async fn service_provider_config() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
        "documentationUri": "https://docs.estuary.dev",
        "patch": { "supported": true },
        "bulk": { "supported": false, "maxOperations": 0, "maxPayloadSize": 0 },
        "filter": { "supported": true, "maxResults": 100 },
        "changePassword": { "supported": false },
        "sort": { "supported": false },
        "etag": { "supported": false },
        "authenticationSchemes": [{
            "type": "oauthbearertoken",
            "name": "OAuth Bearer Token",
            "description": "Authentication scheme using the OAuth Bearer Token Standard",
            "specUri": "https://www.rfc-editor.org/info/rfc6750",
            "primary": true,
        }],
    }))
}

/// GET /Schemas — describes the User schema we support.
pub async fn schemas() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": 1,
        "Resources": [user_schema()],
    }))
}

/// GET /ResourceTypes — describes the User resource type.
pub async fn resource_types() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": 1,
        "Resources": [{
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
            "id": "User",
            "name": "User",
            "endpoint": "/Users",
            "schema": "urn:ietf:params:scim:schemas:core:2.0:User",
        }],
    }))
}

fn user_schema() -> serde_json::Value {
    serde_json::json!({
        "id": "urn:ietf:params:scim:schemas:core:2.0:User",
        "name": "User",
        "description": "User Account",
        "attributes": [
            {
                "name": "userName",
                "type": "string",
                "multiValued": false,
                "required": true,
                "caseExact": false,
                "mutability": "readOnly",
                "returned": "default",
                "uniqueness": "server",
            },
            {
                "name": "active",
                "type": "boolean",
                "multiValued": false,
                "required": false,
                "mutability": "readWrite",
                "returned": "default",
            },
            {
                "name": "displayName",
                "type": "string",
                "multiValued": false,
                "required": false,
                "mutability": "readOnly",
                "returned": "default",
            },
        ],
    })
}
