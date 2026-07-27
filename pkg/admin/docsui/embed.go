// Package docsui embeds the Swagger UI shell for SuperCache API docs.
package docsui

import _ "embed"

// IndexHTML is the Swagger UI host page (loads assets from CDN).
//
//go:embed index.html
var IndexHTML []byte
