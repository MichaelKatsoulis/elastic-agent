// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package fleetapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"github.com/hashicorp/go-multierror"

	"github.com/elastic/elastic-agent/internal/pkg/agent/errors"
	"github.com/elastic/elastic-agent/internal/pkg/fleetapi/client"
)

type HintsRequest struct {
	AgentId    string `json:"agent_id,omitempty"`
	Type       string `json:"type,omitempty"`
	Kubernetes struct {
		Container struct {
			Id       string `json:"id"`
			Image    string `json:"image"`
			Name     string `json:"name"`
			Port     string `json:"port,omitempty"`
			PortName string `json:"port_name,omitempty"`
			Runtime  string `json:"runtime"`
		} `json:"container"`
		Namespace   string            `json:"namespace"`
		Annotations map[string]string `json:"annotations"`
		Labels      map[string]string `json:"labels"`
		Pod         struct {
			Ip   string `json:"ip"`
			Name string `json:"name"`
			Uid  string `json:"uid"`
		} `json:"pod"`
	} `json:"kubernetes"`
}

// Validate validates the enrollment request before sending it to the API.
func (h *HintsRequest) Validate() error {
	var err error

	if len(h.Type) == 0 {
		err = multierror.Append(err, errors.New("missing hints type"))
	}

	return err
}

type HintsResponse struct {
	Action string `json:"action"`
}

// Validate validates the response send from the server.
func (h *HintsResponse) Validate() error {
	var err error

	if h.Action != "created" {
		err = multierror.Append(err, errors.New("hints not created"))
	}

	return err
}

// HintsCmd is the command to be executed to send hints to Fleet Server.
type HintsCmd struct {
	client client.Sender
	info   agentInfo
}

// Execute enroll the Agent in the Fleet Server.
func (e *HintsCmd) Execute(ctx context.Context, r *HintsRequest) (*HintsResponse, error) {

	const hintsPath = "/api/fleet/agents/%s/hints"

	if err := r.Validate(); err != nil {
		return nil, err
	}

	b, err := json.Marshal(r)
	if err != nil {
		return nil, errors.New(err, "fail to encode the hints request")
	}
	p := fmt.Sprintf(hintsPath, e.info.AgentID())
	fmt.Printf("path is %s", p)
	resp, err := e.client.Send(ctx, "POST", p, nil, nil, bytes.NewBuffer(b))
	if err != nil {
		var et *url.Error
		if errors.As(err, et) {
			return nil, et.Err
		}

		var netOp *net.OpError
		if errors.As(err, netOp) {
			return nil, ErrConnRefused
		}

		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, ErrTooManyRequests
	}

	if resp.StatusCode != http.StatusOK {
		return nil, client.ExtractError(resp.Body)
	}

	hintsResponse := &HintsResponse{}
	fmt.Printf("body is %+v", resp.Body)
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(hintsResponse); err != nil {
		return nil, errors.New(err, "fail to decode hints response")
	}

	if err := hintsResponse.Validate(); err != nil {
		return nil, err
	}

	return hintsResponse, nil
}

// NewHintsCmd creates a new EnrollCmd.
func NewHintsCmd(info agentInfo, client client.Sender) *HintsCmd {
	return &HintsCmd{
		client: client,
		info:   info,
	}
}
