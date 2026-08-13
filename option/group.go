package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds                 []string `json:"outbounds"`
	Default                   string   `json:"default,omitempty"`
	InterruptExistConnections bool     `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	Outbounds                 []string           `json:"outbounds"`
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
}

// TODO(mmotyshen): add URLTest options? Or maybe, there should be an option to specify any
// existing outbound as a fallback?
type SmartOutboundOptions struct {
	Outbounds []string `json:"outbounds"`

	// A value in the range [0, 1] that represents the required success ratio
	// among the latest sampled requests for the outbound to be considerered
	// to be a leading one for a given domain.
	RequiredSuccessRate float64 `json:"required_success_rate"`

	// How often alternative outbounds should be rechecked for a given domain.
	RecheckInterval badoption.Duration `json:"recheck_interval,omitempty"`

	// How many last requests for each pair of domain and outbound should be stored.
	RequestSampleCount int `json:"request_sample_count"`

	// The amount of request samples that should be stored for an outbound
	// to be considered to be a leading one for a given domain.
	RequiredRequestSampleCount int `json:"required_request_sample_count"`
}
