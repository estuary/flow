package common

import "strings"

// RewriterConfig rewrites the path under which journal fragments are stored
// by finding and replacing a portion of the journal's name in the final
// constructed path. Its use is uncommon and not recommended, but it can help
// in the implementation of new journal naming taxonomies which don't disrupt
// journal fragments that are already written.
//
//	var cfg = RewriterConfig{
//	    Replace: "/old-path/page-views/
//	    Find:    "/bar/v1/page-views/",
//	}
//	// Remaps journal name => fragment store URL:
//	//  "/foo/bar/v1/page-views/part-000" => "s3://my-bucket/foo/old-path/page-views/part-000" // Matched.
//	//  "/foo/bar/v2/page-views/part-000" => "s3://my-bucket/foo/bar/v2/page-views/part-000"   // Not matched.
type RewriterConfig struct {
	// Find is the string to replace in the unmodified journal name.
	Find string
	// Replace is the string with which Find is replaced in the constructed store path.
	Replace string
}

// RewritePath replace the first occurrence of the find string with the replace
// string in journal path |j| and appends it to the fragment store path |s|,
// effectively rewriting the default Journal path. If find is empty or not
// found, the original |j| is appended.
func (cfg RewriterConfig) RewritePath(s, j string) string {
	if cfg.Find == "" {
		return s + j
	}
	return s + strings.Replace(j, cfg.Find, cfg.Replace, 1)
}
