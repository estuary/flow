package bindings

/*
#include "../../crates/bindings/flow_bindings.h"
*/
import "C"

// ConnectorBuildIdentity reports the source identity compiled into the VMM launcher.
func ConnectorBuildIdentity() string {
	return C.GoString(C.connector_build_identity())
}
