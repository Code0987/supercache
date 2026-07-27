// Package openapi embeds SuperCache OpenAPI 3 specifications.
package openapi

import _ "embed"

// AdminSpec is the Admin HTTP OpenAPI document (YAML).
//
//go:embed admin.openapi.yaml
var AdminSpec []byte

// CacheSpec is the Cache gRPC reference OpenAPI document (YAML).
//
//go:embed cache.openapi.yaml
var CacheSpec []byte
