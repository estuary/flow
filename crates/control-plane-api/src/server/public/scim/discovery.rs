//! Static SCIM 2.0 discovery endpoints.
//!
//! These return fixed JSON describing our SCIM capabilities: user provisioning
//! and deprovisioning, plus group-based access management.

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

/// GET /Schemas — describes the User and Group schemas we support.
pub async fn schemas() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": 2,
        "Resources": [user_schema(), group_schema()],
    }))
}

/// GET /ResourceTypes — describes the User and Group resource types.
pub async fn resource_types() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": 2,
        "Resources": [
            {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
                "id": "User",
                "name": "User",
                "endpoint": "/Users",
                "schema": "urn:ietf:params:scim:schemas:core:2.0:User",
            },
            {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
                "id": "Group",
                "name": "Group",
                "endpoint": "/Groups",
                "schema": "urn:ietf:params:scim:schemas:core:2.0:Group",
            },
        ],
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
                "mutability": "readWrite",
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
                "mutability": "readWrite",
                "returned": "default",
            },
        ],
    })
}

fn group_schema() -> serde_json::Value {
    serde_json::json!({
        "id": "urn:ietf:params:scim:schemas:core:2.0:Group",
        "name": "Group",
        "description": "Group (maps to a catalog prefix + capability)",
        "attributes": [
            {
                "name": "displayName",
                "type": "string",
                "multiValued": false,
                "required": true,
                "caseExact": true,
                "mutability": "readOnly",
                "returned": "default",
                "uniqueness": "server",
                "description": "Catalog prefix and capability, e.g. 'acmeCo/widgets/:admin'",
            },
            {
                "name": "members",
                "type": "complex",
                "multiValued": true,
                "required": false,
                "mutability": "readWrite",
                "returned": "default",
                "subAttributes": [
                    {
                        "name": "value",
                        "type": "string",
                        "multiValued": false,
                        "required": true,
                        "description": "User UUID",
                    },
                    {
                        "name": "display",
                        "type": "string",
                        "multiValued": false,
                        "required": false,
                        "description": "User email",
                    },
                ],
            },
        ],
    })
}
