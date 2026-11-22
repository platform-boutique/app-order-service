package orders

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func counterValue(t *testing.T, event string) float64 {
	t.Helper()
	var m dto.Metric
	if err := ordersTotal.WithLabelValues(event).Write(&m); err != nil || m.GetCounter() == nil {
		t.Fatalf("series %s not readable: %v", event, err)
	}
	return m.GetCounter().GetValue()
}

func TestCountTransitionCountsOnlyPlacedAndFailed(t *testing.T) {
	p0, f0, c0 := counterValue(t, eventPlaced), counterValue(t, eventFailed), counterValue(t, eventCreated)
	countTransition(StatePlaced)
	countTransition(StateFailed)
	countTransition(StateFailed)
	countTransition(StatePending) // not a committed transition target: never counted
	if got := counterValue(t, eventPlaced) - p0; got != 1 {
		t.Errorf("placed +%v, want +1", got)
	}
	if got := counterValue(t, eventFailed) - f0; got != 2 {
		t.Errorf("failed +%v, want +2", got)
	}
	if got := counterValue(t, eventCreated) - c0; got != 0 {
		t.Errorf("created +%v, want +0", got)
	}
}
