package api

import "net/http"

type organizationMetadataResponse struct {
	IPs      organizationIPs      `json:"ips"`
	Provider organizationProvider `json:"provider"`
}

type organizationIPs struct {
	API []string `json:"api"`
	Git []string `json:"git"`
}

type organizationProvider struct {
	Name   string `json:"name"`
	Region string `json:"region"`
}

func handleOrganizationMetadata(w http.ResponseWriter, _ *http.Request) {
	// No ingress CIDRs are published and deployment location is unknown.
	// These contract-defined empty values do not imply ingress is absent.
	writeJSON(w, http.StatusOK, organizationMetadataResponse{
		IPs: organizationIPs{API: []string{}, Git: []string{}},
	})
}
