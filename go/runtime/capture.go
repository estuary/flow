package runtime

import (
	"fmt"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
)

func NewCaptureApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (Application, error) {
	var taskName = shard.Spec().LabelSet.ValueOf(labels.TaskName)
	if taskName == "" {
		return nil, fmt.Errorf("missing value of shard label: '%s'", labels.TaskName)
	}
	var task, _, _, err = host.Catalog.GetTask(taskName)
	if err != nil {
		return nil, fmt.Errorf("reading catalog task spec: %w", err)
	}
	if task.Capture == nil {
		return nil, fmt.Errorf("Expected task to be a capture")
	}
	switch task.Capture.EndpointType {
	case pf.EndpointType_KINESIS:
		return newKinesisCaptureApp(host, shard, recorder)
	default:
		return nil, fmt.Errorf("EndpointType '%s' is not supported for captures", task.Capture.EndpointType.String())
	}
}
