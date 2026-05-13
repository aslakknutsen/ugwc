#!/usr/bin/env bash
# Shared defaults for hack/setup.sh and hack/conformance.sh — source only, do not execute.
#
# GatewayClass metadata.name used by conformance tests and created by setup.sh:
: "${UGWC_CONFORMANCE_GATEWAY_CLASS:=ugwc-conformance}"
# Must match UGWC_CONTROLLER_NAMES / charts/ugwc values and built-in profiles:
: "${UGWC_GATEWAY_CONTROLLER_NAME:=ugwc.io/default-controller}"
# Pin for CRD install and gateway-api test checkout (override before sourcing if needed):
: "${GATEWAY_API_REF:=v1.5.1}"

export UGWC_CONFORMANCE_GATEWAY_CLASS UGWC_GATEWAY_CONTROLLER_NAME GATEWAY_API_REF
