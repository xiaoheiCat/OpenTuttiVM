package eventstream

import (
	"testing"

	eventprotocol "github.com/xiaoheiCat/OpenTuttiVM/services/tuttid/api/events/generated"
)

func TestDefaultCatalogMatchesGeneratedEventDefinitions(t *testing.T) {
	t.Parallel()

	catalog := DefaultCatalog()
	topics := catalog.Topics()
	if len(topics) != len(eventprotocol.BusinessEventDefinitions) {
		t.Fatalf("catalog topics = %d, generated definitions = %d", len(topics), len(eventprotocol.BusinessEventDefinitions))
	}

	for _, generatedDefinition := range eventprotocol.BusinessEventDefinitions {
		topic, ok := catalog.Topic(string(generatedDefinition.Topic))
		if !ok {
			t.Fatalf("catalog missing generated topic %q", generatedDefinition.Topic)
		}
		if topic.Version != generatedDefinition.Version {
			t.Fatalf("topic %q version = %d, want %d", topic.Name, topic.Version, generatedDefinition.Version)
		}

		switch generatedDefinition.Direction {
		case eventprotocol.DirectionClientToServer:
			if !topic.ClientCanPublish {
				t.Fatalf("topic %q should allow client publish", topic.Name)
			}
			if topic.ClientCanSubscribe {
				t.Fatalf("topic %q should not allow client subscribe", topic.Name)
			}
		case eventprotocol.DirectionServerToClient:
			if topic.ClientCanPublish {
				t.Fatalf("topic %q should not allow client publish", topic.Name)
			}
			if !topic.ClientCanSubscribe {
				t.Fatalf("topic %q should allow client subscribe", topic.Name)
			}
		default:
			t.Fatalf("unexpected generated direction %q for topic %q", generatedDefinition.Direction, generatedDefinition.Topic)
		}
	}
}
